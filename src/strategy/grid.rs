use crate::config::endpoints::WsConnectionStatus;
use crate::config::strategy_config::{GridConfig, GridStrategyConfig};
use crate::error::AppError;
use crate::exchange::binance_model::{Order, OrderUpdate, SymbolInfo, WsEvent};
use crate::exchange::traits::Exchange;
use crate::utils::precision::{
    adjust_price_by_filter, adjust_quantity_by_filter, validate_min_notional,
};
use crate::utils::symbol::Symbol;
use crate::SHUTDOWN;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 网格状态，保存买卖订单
#[derive(Clone)]
struct GridState {
    buy_orders: Vec<Order>,
    sell_orders: Vec<Order>,
}

impl GridState {
    /// 创建一个新的 GridState 实例
    fn new() -> Self {
        Self {
            buy_orders: Vec::new(),
            sell_orders: Vec::new(),
        }
    }

    /// 处理 WebSocket 消息

    /// 检查网格均匀性
    fn check_grid_uniformity(&self, config: &GridConfig) -> bool {
        let expected_count = config.grid_num as usize / 2;

        // 检查订单数量是否在合理范围内（允许±2个订单的偏差）
        let buy_count = self.buy_orders.len();
        let sell_count = self.sell_orders.len();

        if buy_count < expected_count.saturating_sub(2)
            || buy_count > expected_count + 2
            || sell_count < expected_count.saturating_sub(2)
            || sell_count > expected_count + 2
        {
            return false;
        }

        // 如果订单数量太少，直接返回false
        if buy_count < 2 || sell_count < 2 {
            return false;
        }

        // 检查买单价格间距（允许更大的容差）
        let mut buy_prices: Vec<_> = self.buy_orders.iter().map(|o| o.price).collect();
        buy_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let spacing_tolerance = config.grid_spacing * 0.1; // 10%的容差
        for i in 0..buy_prices.len() - 1 {
            let actual_spacing = buy_prices[i + 1] - buy_prices[i];
            if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                return false;
            }
        }

        // 检查卖单价格间距（允许更大的容差）
        let mut sell_prices: Vec<_> = self.sell_orders.iter().map(|o| o.price).collect();
        sell_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        for i in 0..sell_prices.len() - 1 {
            let actual_spacing = sell_prices[i + 1] - sell_prices[i];
            if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                return false;
            }
        }

        true
    }
}

/// 网格策略
pub struct GridStrategy {
    config: GridStrategyConfig,
    states: HashMap<Symbol, GridState>,
}

impl GridStrategy {
    /// 创建一个新的 GridStrategy 实例
    pub fn new(config: GridStrategyConfig) -> Self {
        let mut states = HashMap::new();
        // 为每个网格配置创建一个状态
        for grid_config in &config.grid_configs {
            states.insert(grid_config.symbol.clone(), GridState::new());
        }
        Self { config, states }
    }

    /// 运行策略
    pub async fn run(&mut self, exchange: Arc<dyn Exchange + Send + Sync>) -> Result<(), AppError> {
        // 为每个网格配置启动一个 runner
        let mut handles = Vec::new();

        for grid_config in &self.config.grid_configs {
            let exchange_clone = exchange.clone();
            let grid_config_clone = grid_config.clone();
            let state = self.states.get_mut(&grid_config.symbol).unwrap().clone(); // 获取状态

            let handle = tokio::spawn(async move {
                // 创建并运行 GridRunner
                match GridRunner::new(grid_config_clone, exchange_clone, state).await {
                    Ok(mut runner) => runner.run().await,
                    Err(e) => {
                        eprintln!("创建网格 runner 失败: {e}");
                        Err(e)
                    }
                }
            });
            handles.push(handle);
        }

        // 等待所有任务完成，如果任何一个失败就返回错误
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}             // 任务成功完成
                Ok(Err(e)) => return Err(e), // 任务返回错误
                Err(e) => return Err(AppError::Other(format!("任务执行失败: {}", e))), // 任务被取消或panic
            }
        }

        Ok(())
    }
}

/// 网格策略的执行器
#[derive(Clone)]
struct GridRunner {
    config: GridConfig,
    exchange: Arc<dyn Exchange + Send + Sync>,
    state: GridState,
    symbol_info: SymbolInfo,
    ws_status: Arc<tokio::sync::Mutex<WsConnectionStatus>>,
}

impl GridRunner {
    /// 根据成交订单调整网格
    async fn adjust_grid(&mut self, filled_order: OrderUpdate) -> Result<(), AppError> {
        // 获取成交价格和数量
        let filled_price = filled_order.last_filled_price;
        let quantity = filled_order.order_last_filled_quantity;
        let grid_levels = self.config.grid_num as f64;

        // 简化日志输出
        if filled_order.side == "BUY" {
            println!("收到{} 多单成交", self.config.symbol.to_binance());
        } else {
            println!("收到{} 空单成交", self.config.symbol.to_binance());
        }

        // 先从状态中移除已成交的订单
        self.state
            .buy_orders
            .retain(|o| o.client_order_id != filled_order.client_order_id);
        self.state
            .sell_orders
            .retain(|o| o.client_order_id != filled_order.client_order_id);

        // 如果是买单成交（多单成交）
        if filled_order.side == "BUY" {

            // 批量提交两个订单
            // 第1个：成交价+网格间距的空单
            let new_sell_price = filled_price + self.config.grid_spacing;
            println!("[{}] 多单成交，新空单价格: {}", self.config.symbol.to_binance(), new_sell_price);

            // 第2个：成交价-(网格间距*每边网格数量)的多单
            let grid_count = (self.config.grid_num / 2) as f64;
            let new_buy_price = filled_price - (self.config.grid_spacing * grid_count);
            println!("[{}] 多单成交，新多单价格: {}", self.config.symbol.to_binance(), new_buy_price);

            // 提交第1个订单：空单
            let adjusted_sell_price = match adjust_price_by_filter(new_sell_price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("新空单价格精度调整失败: {}", e)));
                }
            };

            // 根据USDT价值计算空单数量
            let sell_base_quantity = self.config.order_value / adjusted_sell_price;
            let adjusted_sell_quantity = match adjust_quantity_by_filter(sell_base_quantity, &self.symbol_info) {
                Ok(q) => q,
                Err(e) => {
                    return Err(AppError::Other(format!("空单数量精度调整失败: {}", e)));
                }
            };

            if let Err(_) = validate_min_notional(adjusted_sell_price, adjusted_sell_quantity, &self.symbol_info) {
                // 跳过不满足最小名义价值要求的订单
            } else {
                match self.exchange.place_order(
                    &self.config.symbol,
                    "SELL",
                    "LIMIT",
                    adjusted_sell_quantity,
                    Some(adjusted_sell_price),
                ).await {
                    Ok(sell_order) => {
                        println!("[{}] 新空单提交成功", self.config.symbol.to_binance());
                        self.state.sell_orders.push(sell_order);
                    }
                    Err(e) => {
                        println!("[{}] 提交新空单失败: {}", self.config.symbol.to_binance(), e);
                        return Err(e);
                    }
                }
            }

            // 提交第2个订单：多单
            let adjusted_buy_price = match adjust_price_by_filter(new_buy_price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("新多单价格精度调整失败: {}", e)));
                }
            };

            // 根据USDT价值计算多单数量
            let buy_base_quantity = self.config.order_value / adjusted_buy_price;
            let adjusted_buy_quantity = match adjust_quantity_by_filter(buy_base_quantity, &self.symbol_info) {
                Ok(q) => q,
                Err(e) => {
                    return Err(AppError::Other(format!("多单数量精度调整失败: {}", e)));
                }
            };

            if let Err(_) = validate_min_notional(adjusted_buy_price, adjusted_buy_quantity, &self.symbol_info) {
                // 跳过不满足最小名义价值要求的订单
            } else {
                match self.exchange.place_order(
                    &self.config.symbol,
                    "BUY",
                    "LIMIT",
                    adjusted_buy_quantity,
                    Some(adjusted_buy_price),
                ).await {
                    Ok(buy_order) => {
                        println!("[{}] 新多单提交成功", self.config.symbol.to_binance());
                        self.state.buy_orders.push(buy_order);
                    }
                    Err(e) => {
                        println!("[{}] 提交新多单失败: {}", self.config.symbol.to_binance(), e);
                        return Err(e);
                    }
                }
            }

            // 取消最高价格的空单
            if let Some(order_to_cancel) = self.state.sell_orders.iter().max_by(|a, b| {
                a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
            }) {
                let order_id_to_cancel = order_to_cancel.order_id;
                match self.exchange.cancel_order(&self.config.symbol, order_id_to_cancel).await {
                    Ok(_) => {
                        println!("[{}] 取消最高价空单成功", self.config.symbol.to_binance());
                        self.state.sell_orders.retain(|o| o.order_id != order_id_to_cancel);
                    }
                    Err(_) => {
                        println!("[{}] 取消空单失败", self.config.symbol.to_binance());
                    }
                }
            }
        } else {
            // 如果是卖单成交（空单成交）

            // 批量提交两个订单
            // 第1个：成交价-网格间距的多单
            let new_buy_price = filled_price - self.config.grid_spacing;
            println!("[{}] 空单成交，新多单价格: {}", self.config.symbol.to_binance(), new_buy_price);

            // 第2个：成交价+(网格间距*每边网格数量)的空单
            let grid_count = (self.config.grid_num / 2) as f64;
            let new_sell_price = filled_price + (self.config.grid_spacing * grid_count);
            println!("[{}] 空单成交，新空单价格: {}", self.config.symbol.to_binance(), new_sell_price);

            // 提交第1个订单：多单
            let adjusted_buy_price = match adjust_price_by_filter(new_buy_price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("新多单价格精度调整失败: {}", e)));
                }
            };

            // 根据USDT价值计算多单数量
            let buy_base_quantity = self.config.order_value / adjusted_buy_price;
            let adjusted_buy_quantity = match adjust_quantity_by_filter(buy_base_quantity, &self.symbol_info) {
                Ok(q) => q,
                Err(e) => {
                    return Err(AppError::Other(format!("多单数量精度调整失败: {}", e)));
                }
            };

            if let Err(_) = validate_min_notional(adjusted_buy_price, adjusted_buy_quantity, &self.symbol_info) {
                // 跳过不满足最小名义价值要求的订单
            } else {
                match self.exchange.place_order(
                    &self.config.symbol,
                    "BUY",
                    "LIMIT",
                    adjusted_buy_quantity,
                    Some(adjusted_buy_price),
                ).await {
                    Ok(buy_order) => {
                        println!("[{}] 新多单提交成功", self.config.symbol.to_binance());
                        self.state.buy_orders.push(buy_order);
                    }
                    Err(e) => {
                        println!("[{}] 提交新多单失败: {}", self.config.symbol.to_binance(), e);
                        return Err(e);
                    }
                }
            }

            // 提交第2个订单：空单
            let adjusted_sell_price = match adjust_price_by_filter(new_sell_price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("新空单价格精度调整失败: {}", e)));
                }
            };

            // 根据USDT价值计算空单数量
            let sell_base_quantity = self.config.order_value / adjusted_sell_price;
            let adjusted_sell_quantity = match adjust_quantity_by_filter(sell_base_quantity, &self.symbol_info) {
                Ok(q) => q,
                Err(e) => {
                    return Err(AppError::Other(format!("空单数量精度调整失败: {}", e)));
                }
            };

            if let Err(_) = validate_min_notional(adjusted_sell_price, adjusted_sell_quantity, &self.symbol_info) {
                // 跳过不满足最小名义价值要求的订单
            } else {
                match self.exchange.place_order(
                    &self.config.symbol,
                    "SELL",
                    "LIMIT",
                    adjusted_sell_quantity,
                    Some(adjusted_sell_price),
                ).await {
                    Ok(sell_order) => {
                        println!("[{}] 新空单提交成功", self.config.symbol.to_binance());
                        self.state.sell_orders.push(sell_order);
                    }
                    Err(e) => {
                        println!("[{}] 提交新空单失败: {}", self.config.symbol.to_binance(), e);
                        return Err(e);
                    }
                }
            }

            // 取消最低价格的多单
            if let Some(order_to_cancel) = self.state.buy_orders.iter().min_by(|a, b| {
                a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
            }) {
                let order_id_to_cancel = order_to_cancel.order_id;
                match self.exchange.cancel_order(&self.config.symbol, order_id_to_cancel).await {
                    Ok(_) => {
                        println!("[{}] 取消最低价多单成功", self.config.symbol.to_binance());
                        self.state.buy_orders.retain(|o| o.order_id != order_id_to_cancel);
                    }
                    Err(_) => {
                        println!("[{}] 取消多单失败", self.config.symbol.to_binance());
                    }
                }
            }
        }

        // 网格调整完成

        Ok(())
    }

    /// 创建一个新的 GridRunner 实例
    async fn new(
        config: GridConfig,
        exchange: Arc<dyn Exchange + Send + Sync>,
        state: GridState,
    ) -> Result<Self, AppError> {
        // 获取交易所信息
        let exchange_info = exchange
            .get_exchange_info(config.symbol.market_type)
            .await?;
        // 查找交易对信息
        let symbol_info = exchange_info
            .symbols
            .into_iter()
            .find(|s| s.symbol == config.symbol.to_binance())
            .ok_or_else(|| {
                AppError::Other(format!("交易对未找到: {}", config.symbol.to_binance()))
            })?;

        Ok(Self {
            config,
            exchange,
            state,
            symbol_info,
            ws_status: Arc::new(tokio::sync::Mutex::new(WsConnectionStatus::Disconnected)),
        })
    }

    /// 运行网格策略的主循环
    async fn run(&mut self) -> Result<(), AppError> {
        // 策略主循环
        // 事件循环
        loop {
            // 检查全局关闭信号
            if SHUTDOWN.load(Ordering::SeqCst) {
                println!(
                    "收到关闭信号，正在停止网格策略 {}",
                    self.config.symbol.to_binance()
                );
                return Ok(());
            }

            // 初始化网格
            if let Err(e) = self.initialize_grid().await {
                println!(
                    "初始化网格失败 {}: {}. 60秒后重试.",
                    self.config.symbol.to_binance(),
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                continue;
            }

            // 创建一个通道用于 websocket 消息
            let (tx, mut rx) = tokio::sync::mpsc::channel(100);

            // 克隆 self 用于 websocket 任务
            let mut self_clone_for_ws = self.clone();
            // 启动 websocket 消息处理任务
            let mut ws_handle =
                tokio::spawn(async move { self_clone_for_ws.handle_websocket_messages(tx).await });

            loop {
                tokio::select! {
                    // 检查关闭信号
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                        if SHUTDOWN.load(Ordering::SeqCst) {
                            println!("收到关闭信号，正在停止网格策略 {}", self.config.symbol.to_binance());
                            ws_handle.abort();
                            return Ok(());
                        }
                    }
                    // 从 websocket 通道接收消息
                    Some(msg) = rx.recv() => {
                        // 如果是订单更新事件
                        if let WsEvent::OrderTradeUpdate { event_time, transaction_time, order } = msg {
                            // 检查订单是否是当前策略的，并且状态是已成交
                            if order.symbol == self.config.symbol.to_binance() && order.order_status == "FILLED" {
                                println!("检测到{} 订单成交，开始调整网格", self.config.symbol.to_binance());
                                // 调整网格
                                if let Err(e) = self.adjust_grid(order).await {
                                    eprintln!("调整网格失败: {e}");
                                } else {
                                    println!("{} 网格调整完成", self.config.symbol.to_binance());
                                }
                            }
                        }
                    }
                    // 每3分钟检查一次网格均匀性
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(3 * 60)) => {
                        // 只有在WebSocket连接正常时才进行网格均匀性检查
                         let ws_status = {
                             let status_guard = self.ws_status.lock().await;
                             status_guard.clone()
                         };

                        if matches!(ws_status, WsConnectionStatus::Connected) {
                            if !self.state.check_grid_uniformity(&self.config) {
                            println!("{} 检查不通过重置网格", self.config.symbol.to_binance());
                            ws_handle.abort();
                            break; // 跳出内层循环以重新初始化
                        } else {
                            println!("{} 均匀检查通过", self.config.symbol.to_binance());
                        }
                        } else {
                            println!("WebSocket连接状态异常 {:?}, 跳过网格均匀性检查", ws_status);
                        }
                    }
                    // websocket 任务结束
                    result = &mut ws_handle => {
                        match result {
                            Ok(Ok(())) => {
                                // WebSocket正常结束
                                println!("WebSocket连接正常结束 {}", self.config.symbol.to_binance());
                                break; // 跳出内层循环以重新初始化
                            }
                            Ok(Err(e)) => {
                                // WebSocket连接失败
                                let error_msg = format!("WebSocket连接失败 {}: {}", self.config.symbol.to_binance(), e);
                                eprintln!("❌ {}", error_msg);
                                return Err(e); // 返回错误，停止策略
                            }
                            Err(e) => {
                                // 任务被取消或panic
                                let error_msg = format!("WebSocket任务异常结束 {}: {}", self.config.symbol.to_binance(), e);
                                eprintln!("❌ {}", error_msg);
                                return Err(AppError::Other(error_msg)); // 返回错误，停止策略
                            }
                        }
                    }
                }
            }
        }
    }

    /// 初始化网格，包括取消现有订单、获取最新价格和下新订单
    async fn initialize_grid(&mut self) -> Result<(), AppError> {
        println!("正在为 {} 初始化网格", self.config.symbol.to_binance());

        // 1. Cancel all existing orders for the symbol
        // 1. 取消该交易对的所有现有订单
        self.exchange.cancel_all_orders(&self.config.symbol).await?;

        // 2. Get the latest price
        // 2. 获取最新价格
        let latest_price = self.exchange.get_price(&self.config.symbol).await?;

        // 3. Calculate grid levels
        // 3. 计算网格价格水平
        let (buy_prices, sell_prices) = self.calculate_grid_prices(latest_price);

        // 4. Place orders using batch orders
        // 4. 使用批量下单
        let base_quantity = self.config.order_value / latest_price;

        // 根据交易所规则调整数量精度
        let quantity = match adjust_quantity_by_filter(base_quantity, &self.symbol_info) {
            Ok(q) => q,
            Err(e) => {
                return Err(AppError::Other(format!("数量精度调整失败: {}", e)));
            }
        };

        // 计算调整后数量

        // 准备批量买单
        let mut buy_batch_orders = Vec::new();
        for price in &buy_prices {
            // 根据交易所规则调整价格精度
            let adjusted_price = match adjust_price_by_filter(*price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("买单价格精度调整失败: {}", e)));
                }
            };

            // 验证最小名义价值
            if let Err(_) = validate_min_notional(adjusted_price, quantity, &self.symbol_info) {
                continue;
            }
            buy_batch_orders.push((
                "BUY".to_string(),
                "LIMIT".to_string(),
                quantity,
                Some(adjusted_price),
            ));
        }

        // 准备批量卖单
        let mut sell_batch_orders = Vec::new();
        for price in &sell_prices {
            // 根据交易所规则调整价格精度
            let adjusted_price = match adjust_price_by_filter(*price, &self.symbol_info) {
                Ok(p) => p,
                Err(e) => {
                    return Err(AppError::Other(format!("卖单价格精度调整失败: {}", e)));
                }
            };

            // 验证最小名义价值
            if let Err(_) = validate_min_notional(adjusted_price, quantity, &self.symbol_info) {
                continue;
            }
            sell_batch_orders.push((
                "SELL".to_string(),
                "LIMIT".to_string(),
                quantity,
                Some(adjusted_price),
            ));
        }

        // 分批下单（每批最多5个）
        let mut buy_orders = Vec::new();
        for chunk in buy_batch_orders.chunks(5) {
            let orders = self
                .exchange
                .place_batch_orders(&self.config.symbol, chunk.to_vec())
                .await?;
            buy_orders.extend(orders);
        }

        let mut sell_orders = Vec::new();
        for chunk in sell_batch_orders.chunks(5) {
            let orders = self
                .exchange
                .place_batch_orders(&self.config.symbol, chunk.to_vec())
                .await?;
            sell_orders.extend(orders);
        }

        self.state.buy_orders = buy_orders;
        self.state.sell_orders = sell_orders;

        // 网格初始化完成
        Ok(())
    }

    /// 根据基础价格计算网格的买卖价格
    fn calculate_grid_prices(&self, base_price: f64) -> (Vec<f64>, Vec<f64>) {
        let mut buy_prices = Vec::new();
        let mut sell_prices = Vec::new();
        let grid_count = self.config.grid_num / 2;

        // 计算每个网格水平的价格
        for i in 1..=grid_count {
            let price_diff = self.config.grid_spacing * i as f64;
            buy_prices.push(base_price - price_diff);
            sell_prices.push(base_price + price_diff);
        }

        (buy_prices, sell_prices)
    }

    /// 处理 WebSocket 消息，将其发送到通道
    async fn handle_websocket_messages(
        &mut self,
        tx: tokio::sync::mpsc::Sender<WsEvent>,
    ) -> Result<(), AppError> {
        let exchange = self.exchange.clone();
        let symbol = self.config.symbol.clone();
        let ws_status = self.ws_status.clone();

        let on_message: Box<
            dyn FnMut(WsEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        > = Box::new(move |msg: WsEvent| {
            let tx_clone = tx.clone();
            let ws_status_clone = ws_status.clone();
            Box::pin(async move {
                // 更新WebSocket连接状态为已连接
                {
                    let mut status_guard = ws_status_clone.lock().await;
                    *status_guard = WsConnectionStatus::Connected;
                }

                if let Err(e) = tx_clone.send(msg).await {
                    eprintln!("无法将ws消息发送到通道: {e}");
                }
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        // 设置连接状态为连接中
        {
            let mut status_guard = self.ws_status.lock().await;
            *status_guard = WsConnectionStatus::Connecting;
        }

        match exchange.connect_ws(symbol.market_type, on_message).await {
            Ok(()) => {
                println!("✅ {} WebSocket连接成功建立", symbol.to_binance());
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("{} 的 WebSocket 连接错误: {}", symbol.to_binance(), e);
                eprintln!("❌ {}", error_msg);
                // 设置连接状态为错误
                {
                    let mut status_guard = self.ws_status.lock().await;
                    *status_guard = WsConnectionStatus::Error(error_msg.clone());
                }
                Err(AppError::Other(error_msg))
            }
        }
    }
}
