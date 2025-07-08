use crate::config::endpoints::WsConnectionStatus;
use crate::config::strategy_config::{GridConfig, GridStrategyConfig};
use crate::error::AppError;
use crate::exchange::binance_model::{Order, OrderUpdate, SymbolInfo, WsEvent};
use crate::exchange::traits::Exchange;
use crate::utils::symbol::Symbol;
use crate::SHUTDOWN;
use crate::{strategy_error, strategy_info, strategy_warn};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// 网格均匀性检查结果
#[derive(Debug, Clone)]
struct GridUniformityResult {
    pub is_uniform: bool,
    pub buy_spacing_consistency: f64, // 做多网格间距一致程度 (百分比)
    pub sell_spacing_consistency: f64, // 做空网格间距一致程度 (百分比)
    pub grid_count_consistency: f64,  // 网格数量一致程度 (百分比)
}

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

    /// 检查网格均匀性
    fn check_grid_uniformity(&self, config: &GridConfig) -> GridUniformityResult {
        let expected_count = config.grid_num as usize / 2;
        let buy_count = self.buy_orders.len();
        let sell_count = self.sell_orders.len();

        // 计算网格数量一致程度
        let grid_count_consistency = if expected_count == 0 {
            100.0
        } else {
            let buy_consistency = (1.0
                - (buy_count as f64 - expected_count as f64).abs() / expected_count as f64)
                .max(0.0)
                * 100.0;
            let sell_consistency = (1.0
                - (sell_count as f64 - expected_count as f64).abs() / expected_count as f64)
                .max(0.0)
                * 100.0;
            (buy_consistency + sell_consistency) / 2.0
        };

        // 如果订单数量太少，返回低一致性结果
        if buy_count < 2 || sell_count < 2 {
            return GridUniformityResult {
                is_uniform: false,
                buy_spacing_consistency: 0.0,
                sell_spacing_consistency: 0.0,
                grid_count_consistency,
            };
        }

        // 计算买单价格间距一致性
        let mut buy_prices: Vec<_> = self.buy_orders.iter().map(|o| o.price).collect();
        buy_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let buy_spacing_consistency = if buy_prices.len() < 2 {
            0.0
        } else {
            let mut total_deviation = 0.0;
            let mut spacing_count = 0;

            for i in 0..buy_prices.len() - 1 {
                let actual_spacing = buy_prices[i + 1] - buy_prices[i];
                let deviation = (actual_spacing - config.grid_spacing).abs() / config.grid_spacing;
                total_deviation += deviation;
                spacing_count += 1;
            }

            if spacing_count > 0 {
                let avg_deviation = total_deviation / spacing_count as f64;
                (1.0 - avg_deviation).max(0.0) * 100.0
            } else {
                0.0
            }
        };

        // 计算卖单价格间距一致性
        let mut sell_prices: Vec<_> = self.sell_orders.iter().map(|o| o.price).collect();
        sell_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let sell_spacing_consistency = if sell_prices.len() < 2 {
            0.0
        } else {
            let mut total_deviation = 0.0;
            let mut spacing_count = 0;

            for i in 0..sell_prices.len() - 1 {
                let actual_spacing = sell_prices[i + 1] - sell_prices[i];
                let deviation = (actual_spacing - config.grid_spacing).abs() / config.grid_spacing;
                total_deviation += deviation;
                spacing_count += 1;
            }

            if spacing_count > 0 {
                let avg_deviation = total_deviation / spacing_count as f64;
                (1.0 - avg_deviation).max(0.0) * 100.0
            } else {
                0.0
            }
        };

        // 判断是否均匀（使用原有的容差逻辑）
        let spacing_tolerance = config.grid_spacing * 0.1; // 10%的容差
        let mut is_uniform = true;

        // 检查数量是否在合理范围内
        if buy_count < expected_count.saturating_sub(2)
            || buy_count > expected_count + 2
            || sell_count < expected_count.saturating_sub(2)
            || sell_count > expected_count + 2
        {
            is_uniform = false;
        }

        // 检查买单间距
        if is_uniform {
            for i in 0..buy_prices.len() - 1 {
                let actual_spacing = buy_prices[i + 1] - buy_prices[i];
                if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                    is_uniform = false;
                    break;
                }
            }
        }

        // 检查卖单间距
        if is_uniform {
            for i in 0..sell_prices.len() - 1 {
                let actual_spacing = sell_prices[i + 1] - sell_prices[i];
                if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                    is_uniform = false;
                    break;
                }
            }
        }

        GridUniformityResult {
            is_uniform,
            buy_spacing_consistency,
            sell_spacing_consistency,
            grid_count_consistency,
        }
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
    // 缓存的精度信息，避免重复计算
    price_precision: u32,
    quantity_precision: u32,
    min_notional: f64,
    // 快速处理通道
    fast_adjust_tx: Option<tokio::sync::mpsc::Sender<crate::exchange::binance_model::OrderUpdate>>,
}

impl GridRunner {
    /// 快速格式化价格，使用缓存的精度
    fn format_price_fast(&self, price: f64) -> f64 {
        let multiplier = 10_f64.powi(self.price_precision as i32);
        (price * multiplier).round() / multiplier
    }

    /// 快速格式化数量，使用缓存的精度
    fn format_quantity_fast(&self, quantity: f64) -> f64 {
        let multiplier = 10_f64.powi(self.quantity_precision as i32);
        (quantity * multiplier).round() / multiplier
    }

    /// 并行处理多个批次的订单
    async fn place_orders_optimized(
        &self,
        orders: Vec<(String, String, f64, Option<f64>)>,
    ) -> Result<Vec<Order>, AppError> {
        const BATCH_SIZE: usize = 5;
        let mut all_results = Vec::new();

        if orders.is_empty() {
            return Ok(all_results);
        }

        // 将订单分割成多个批次
        let chunks: Vec<_> = orders.chunks(BATCH_SIZE).collect();

        // 并行处理所有批次
        let futures: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                self.exchange
                    .place_batch_orders(&self.config.symbol, chunk.to_vec())
            })
            .collect();

        let results = futures_util::future::join_all(futures).await;

        // 处理结果
        for result in results {
            match result {
                Ok(orders) => all_results.extend(orders),
                Err(e) => return Err(e),
            }
        }

        Ok(all_results)
    }

    /// 根据成交订单调整网格
    async fn adjust_grid(&mut self, filled_order: OrderUpdate) -> Result<(), AppError> {
        // 获取成交价格和数量
        let filled_price = filled_order.last_filled_price;
        let quantity = filled_order.order_last_filled_quantity;
        let grid_levels = self.config.grid_num as f64;

        // 简化日志输出 - 只记录关键信息
        if filled_order.side == "BUY" {
            strategy_info!(
                &self.config.symbol.to_binance(),
                "多单成交 价格:{}",
                filled_price
            );
        } else {
            strategy_info!(
                &self.config.symbol.to_binance(),
                "空单成交 价格:{}",
                filled_price
            );
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
            // 计算新订单价格
            let new_sell_price = filled_price + self.config.grid_spacing;
            let grid_count = (self.config.grid_num / 2) as f64;
            let new_buy_price = filled_price - (self.config.grid_spacing * grid_count);

            // 使用快速格式化
            let adjusted_sell_price = self.format_price_fast(new_sell_price);
            let adjusted_buy_price = self.format_price_fast(new_buy_price);

            // 计算数量
            let sell_base_quantity = self.config.order_value / adjusted_sell_price;
            let buy_base_quantity = self.config.order_value / adjusted_buy_price;
            let adjusted_sell_quantity = self.format_quantity_fast(sell_base_quantity);
            let adjusted_buy_quantity = self.format_quantity_fast(buy_base_quantity);

            // 准备新订单
            let mut new_orders = Vec::new();

            // 验证并添加空单
            if adjusted_sell_price * adjusted_sell_quantity >= self.min_notional {
                new_orders.push((
                    "SELL".to_string(),
                    "LIMIT".to_string(),
                    adjusted_sell_quantity,
                    Some(adjusted_sell_price),
                ));
            }

            // 验证并添加多单
            if adjusted_buy_price * adjusted_buy_quantity >= self.min_notional {
                new_orders.push((
                    "BUY".to_string(),
                    "LIMIT".to_string(),
                    adjusted_buy_quantity,
                    Some(adjusted_buy_price),
                ));
            }

            // 找到要取消的订单 - 买单成交时取消最高价卖单
            let order_to_cancel = self
                .state
                .sell_orders
                .iter()
                .max_by(|a, b| {
                    a.price
                        .partial_cmp(&b.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|o| o.order_id);

            // 并行批量操作：同时提交新订单和取消旧订单
            let (place_result, cancel_result) =
                tokio::join!(self.place_orders_optimized(new_orders), async {
                    if let Some(order_id) = order_to_cancel {
                        self.exchange
                            .cancel_order(&self.config.symbol, order_id)
                            .await
                    } else {
                        Ok(crate::exchange::binance_model::Order {
                            order_id: 0,
                            symbol: String::new(),
                            status: String::new(),
                            client_order_id: String::new(),
                            price: 0.0,
                            avg_price: 0.0,
                            orig_qty: 0.0,
                            executed_qty: 0.0,
                            cum_quote: 0.0,
                            time_in_force: String::new(),
                            order_type: String::new(),
                            side: String::new(),
                            stop_price: 0.0,
                            time: None,
                            update_time: 0,
                            working_type: String::new(),
                            activate_price: None,
                            price_rate: None,
                            orig_type: String::new(),
                            position_side: String::new(),
                            close_position: false,
                            price_protect: false,
                            reduce_only: false,
                            price_match: None,
                            self_trade_prevention_mode: None,
                            good_till_date: None,
                        })
                    }
                });

            // 处理下单结果
            if let Ok(orders) = place_result {
                for order in orders {
                    if order.side == "BUY" {
                        self.state.buy_orders.push(order);
                    } else {
                        self.state.sell_orders.push(order);
                    }
                }
            } else if let Err(e) = place_result {
                return Err(e);
            }

            // 处理取消结果
            match (cancel_result, order_to_cancel) {
                (Ok(_), Some(order_id)) => {
                    self.state.sell_orders.retain(|o| o.order_id != order_id);
                }
                (Err(e), Some(order_id)) => {
                    strategy_warn!("grid", "取消最高价卖单失败: {}, 订单ID: {}", e, order_id);
                }
                _ => {}
            }
        } else {
            // 如果是卖单成交（空单成交）
            // 计算新订单价格
            let new_buy_price = filled_price - self.config.grid_spacing;
            let grid_count = (self.config.grid_num / 2) as f64;
            let new_sell_price = filled_price + (self.config.grid_spacing * grid_count);

            // 使用快速格式化
            let adjusted_buy_price = self.format_price_fast(new_buy_price);
            let adjusted_sell_price = self.format_price_fast(new_sell_price);

            // 计算数量
            let buy_base_quantity = self.config.order_value / adjusted_buy_price;
            let sell_base_quantity = self.config.order_value / adjusted_sell_price;
            let adjusted_buy_quantity = self.format_quantity_fast(buy_base_quantity);
            let adjusted_sell_quantity = self.format_quantity_fast(sell_base_quantity);

            // 准备新订单
            let mut new_orders = Vec::new();

            // 验证并添加多单
            if adjusted_buy_price * adjusted_buy_quantity >= self.min_notional {
                new_orders.push((
                    "BUY".to_string(),
                    "LIMIT".to_string(),
                    adjusted_buy_quantity,
                    Some(adjusted_buy_price),
                ));
            }

            // 验证并添加空单
            if adjusted_sell_price * adjusted_sell_quantity >= self.min_notional {
                new_orders.push((
                    "SELL".to_string(),
                    "LIMIT".to_string(),
                    adjusted_sell_quantity,
                    Some(adjusted_sell_price),
                ));
            }

            // 找到要取消的订单 - 卖单成交时取消最低价买单
            let order_to_cancel = self
                .state
                .buy_orders
                .iter()
                .min_by(|a, b| {
                    a.price
                        .partial_cmp(&b.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|o| o.order_id);

            // 并行批量操作：同时提交新订单和取消旧订单
            let (place_result, cancel_result) =
                tokio::join!(self.place_orders_optimized(new_orders), async {
                    if let Some(order_id) = order_to_cancel {
                        self.exchange
                            .cancel_order(&self.config.symbol, order_id)
                            .await
                    } else {
                        Ok(crate::exchange::binance_model::Order {
                            order_id: 0,
                            symbol: String::new(),
                            status: String::new(),
                            client_order_id: String::new(),
                            price: 0.0,
                            avg_price: 0.0,
                            orig_qty: 0.0,
                            executed_qty: 0.0,
                            cum_quote: 0.0,
                            time_in_force: String::new(),
                            order_type: String::new(),
                            side: String::new(),
                            stop_price: 0.0,
                            time: None,
                            update_time: 0,
                            working_type: String::new(),
                            activate_price: None,
                            price_rate: None,
                            orig_type: String::new(),
                            position_side: String::new(),
                            close_position: false,
                            price_protect: false,
                            reduce_only: false,
                            price_match: None,
                            self_trade_prevention_mode: None,
                            good_till_date: None,
                        })
                    }
                });

            // 处理下单结果
            if let Ok(orders) = place_result {
                for order in orders {
                    if order.side == "BUY" {
                        self.state.buy_orders.push(order);
                    } else {
                        self.state.sell_orders.push(order);
                    }
                }
            } else if let Err(e) = place_result {
                return Err(e);
            }

            // 处理取消结果
            match (cancel_result, order_to_cancel) {
                (Ok(_), Some(order_id)) => {
                    self.state.buy_orders.retain(|o| o.order_id != order_id);
                }
                (Err(e), Some(order_id)) => {
                    strategy_warn!("grid", "取消最低价买单失败: {}, 订单ID: {}", e, order_id);
                }
                _ => {}
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

        // 缓存精度信息
        let price_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::PriceFilter { tick_size, .. } => {
                    Some(tick_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let quantity_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::LotSize { step_size, .. } => {
                    Some(step_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let min_notional = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::MinNotional { notional } => {
                    Some(notional.parse::<f64>().unwrap_or(10.0))
                }
                _ => None,
            })
            .unwrap_or(10.0);

        // 创建快速处理通道
        let (fast_adjust_tx, _) = tokio::sync::mpsc::channel(100);

        Ok(Self {
            config,
            exchange,
            state,
            symbol_info,
            ws_status: Arc::new(tokio::sync::Mutex::new(WsConnectionStatus::Disconnected)),
            price_precision,
            quantity_precision,
            min_notional,

            fast_adjust_tx: Some(fast_adjust_tx),
        })
    }

    /// 运行网格策略的主循环
    async fn run(&mut self) -> Result<(), AppError> {
        // 策略主循环
        // 事件循环
        loop {
            // 检查全局关闭信号
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(
                    "grid",
                    "收到关闭信号，正在停止网格策略 {}",
                    self.config.symbol.to_binance()
                );
                return Ok(());
            }

            // 初始化网格
            if let Err(e) = self.initialize_grid().await {
                strategy_error!(
                    "grid",
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

            // 创建网格健康检查定时器
            let health_check_interval = self.config.health_check_interval_seconds.unwrap_or(180);
            let mut health_check_timer =
                tokio::time::interval(tokio::time::Duration::from_secs(health_check_interval));
            health_check_timer.tick().await; // 跳过第一次立即触发

            // 创建关闭信号检查定时器
            let mut shutdown_check_timer =
                tokio::time::interval(tokio::time::Duration::from_secs(1));

            loop {
                tokio::select! {
                    // 检查关闭信号
                    _ = shutdown_check_timer.tick() => {
                        if SHUTDOWN.load(Ordering::SeqCst) {
                            strategy_info!("grid", "收到关闭信号，正在停止网格策略 {}", self.config.symbol.to_binance());
                            ws_handle.abort();
                            return Ok(());
                        }
                    }
                    // 从 websocket 通道接收消息 - 快速响应
                    Some(msg) = rx.recv() => {
                        // 如果是订单更新事件
                        if let WsEvent::OrderTradeUpdate { event_time, transaction_time, order } = msg {
                            // 检查订单是否是当前策略的，并且状态是已成交
                            if order.symbol == self.config.symbol.to_binance() && order.order_status == "FILLED" {
                                // 检查订单类型和成交方式
                                let is_market_order = order.order_type == "MARKET";
                                let is_taker = !order.is_maker; // is_maker为false表示是吃单方

                                // 使用快速处理通道进行非阻塞处理
                                if let Some(ref tx) = self.fast_adjust_tx {
                                    let _ = tx.try_send(order.clone());
                                }

                                if is_market_order || is_taker {
                                    // 市价单成交或吃单方成交，重置网格
                                    if is_market_order {
                                        strategy_warn!("grid", "{} 检测到市价单成交，重置网格。订单ID: {}, 类型: {}",
                                            self.config.symbol.to_binance(), order.order_id, order.order_type);
                                    } else {
                                        strategy_warn!("grid", "{} 检测到吃单方成交，重置网格。订单ID: {}, 是否挂单方: {}",
                                            self.config.symbol.to_binance(), order.order_id, order.is_maker);
                                    }
                                    ws_handle.abort();
                                    break; // 跳出内层循环以重新初始化网格
                                } else {
                                    // 限价单且为挂单方成交，在单独任务中调整网格，避免阻塞主循环
                                    let mut runner_clone = self.clone();
                                    let order_clone = order.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = runner_clone.adjust_grid(order_clone).await {
                                            strategy_error!("grid", "调整网格失败: {e}");
                                        }
                                    });
                                }
                            }
                        }
                    }
                    // 网格健康检查定时器触发
                    _ = health_check_timer.tick() => {
                        // 从交易所API获取实时的开放订单
                        match self.exchange.get_open_orders(&self.config.symbol).await {
                            Ok(open_orders) => {
                                // 更新本地状态以保持同步
                                self.state.buy_orders = open_orders.iter().filter(|o| o.side == "BUY").cloned().collect();
                                self.state.sell_orders = open_orders.iter().filter(|o| o.side == "SELL").cloned().collect();

                                let uniformity_result = self.state.check_grid_uniformity(&self.config);
                                let expected_count = self.config.grid_num / 2;
                                let buy_count = self.state.buy_orders.len();
                                let sell_count = self.state.sell_orders.len();

                                // 计算总体均匀性（类似Python代码中的uniformity计算）
                                let overall_uniformity = if expected_count == 0 {
                                    1.0
                                } else {
                                    (buy_count.min(sell_count) as f64) / (expected_count as f64)
                                };

                                strategy_info!(
                                    "grid",
                                    "{} 网格健康检查。买单: {}, 卖单: {}, 期望: {}, 均匀性: {:.2}%, 买单间距一致性: {:.1}%, 卖单间距一致性: {:.1}%, 数量一致性: {:.1}% [实时API数据]",
                                    self.config.symbol.to_binance(),
                                    buy_count,
                                    sell_count,
                                    expected_count,
                                    overall_uniformity * 100.0,
                                    uniformity_result.buy_spacing_consistency,
                                    uniformity_result.sell_spacing_consistency,
                                    uniformity_result.grid_count_consistency
                                );

                                // 检查是否需要重置网格
                                let uniformity_threshold = self.config.uniformity_threshold.unwrap_or(0.95);
                                let needs_reset = !uniformity_result.is_uniform ||
                                                overall_uniformity < uniformity_threshold ||
                                                buy_count == 0 || sell_count == 0;

                                if needs_reset {
                                    if buy_count == 0 || sell_count == 0 {
                                        strategy_warn!("grid", "{} 网格订单不完整，买单/卖单为空。", self.config.symbol.to_binance());
                                    } else {
                                        strategy_warn!("grid", "{} 网格均匀性检查不通过，重置网格", self.config.symbol.to_binance());
                                    }
                                    ws_handle.abort();
                                    break; // 跳出内层循环以重新初始化
                                }
                            }
                            Err(e) => {
                                strategy_error!("grid", "{} 获取开放订单失败: {}，使用本地状态进行健康检查", self.config.symbol.to_binance(), e);

                                // 降级到使用本地状态
                                let uniformity_result = self.state.check_grid_uniformity(&self.config);
                                let expected_count = self.config.grid_num / 2;
                                let buy_count = self.state.buy_orders.len();
                                let sell_count = self.state.sell_orders.len();

                                let overall_uniformity = if expected_count == 0 {
                                    1.0
                                } else {
                                    (buy_count.min(sell_count) as f64) / (expected_count as f64)
                                };

                                strategy_info!(
                                    "grid",
                                    "{} 网格健康检查。买单: {}, 卖单: {}, 期望: {}, 均匀性: {:.2}% [本地状态数据]",
                                    self.config.symbol.to_binance(),
                                    buy_count,
                                    sell_count,
                                    expected_count,
                                    overall_uniformity * 100.0
                                );

                                // 如果API调用失败且本地状态显示订单为空，则重置网格
                                if buy_count == 0 || sell_count == 0 {
                                    strategy_warn!("grid", "{} API调用失败且本地状态显示订单为空，重置网格", self.config.symbol.to_binance());
                                    ws_handle.abort();
                                    break;
                                }
                            }
                        }
                    }
                    // websocket 任务结束
                    result = &mut ws_handle => {
                        match result {
                            Ok(Ok(())) => {
                                // WebSocket正常结束
                                strategy_info!("grid", "WebSocket连接正常结束 {}", self.config.symbol.to_binance());
                                break; // 跳出内层循环以重新初始化
                            }
                            Ok(Err(e)) => {
                                // WebSocket连接失败
                                let error_msg = format!("WebSocket连接失败 {}: {}", self.config.symbol.to_binance(), e);
                                strategy_error!("grid", "❌ {}", error_msg);
                                return Err(e); // 返回错误，停止策略
                            }
                            Err(e) => {
                                // 任务被取消或panic
                                let error_msg = format!("WebSocket任务异常结束 {}: {}", self.config.symbol.to_binance(), e);
                                strategy_error!("grid", "❌ {}", error_msg);
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
        strategy_info!(
            "grid",
            "正在为 {} 初始化网格",
            self.config.symbol.to_binance()
        );

        // 1. Cancel all existing orders for the symbol
        // 1. 取消该交易对的所有现有订单
        self.exchange.cancel_all_orders(&self.config.symbol).await?;

        // 2. Clear state
        // 2. 清空状态
        self.state.buy_orders.clear();
        self.state.sell_orders.clear();

        // 3. Get the latest price
        // 3. 获取最新价格
        let latest_price = self.exchange.get_price(&self.config.symbol).await?;

        // 4. Calculate grid levels
        // 4. 计算网格价格水平
        let (buy_prices, sell_prices) = self.calculate_grid_prices(latest_price);

        // 5. Prepare all orders using fast formatting
        // 5. 使用快速格式化准备所有订单
        let mut all_orders = Vec::new();

        // 准备买单 - 使用快速格式化
        for price in buy_prices {
            let adjusted_price = self.format_price_fast(price);
            let base_quantity = self.config.order_value / adjusted_price;
            let adjusted_quantity = self.format_quantity_fast(base_quantity);

            if adjusted_price * adjusted_quantity >= self.min_notional {
                all_orders.push((
                    "BUY".to_string(),
                    "LIMIT".to_string(),
                    adjusted_quantity,
                    Some(adjusted_price),
                ));
            }
        }

        // 准备卖单 - 使用快速格式化
        for price in sell_prices {
            let adjusted_price = self.format_price_fast(price);
            let base_quantity = self.config.order_value / adjusted_price;
            let adjusted_quantity = self.format_quantity_fast(base_quantity);

            if adjusted_price * adjusted_quantity >= self.min_notional {
                all_orders.push((
                    "SELL".to_string(),
                    "LIMIT".to_string(),
                    adjusted_quantity,
                    Some(adjusted_price),
                ));
            }
        }

        // 6. Use optimized parallel batch processing
        // 6. 使用优化的并行批次处理
        match self.place_orders_optimized(all_orders).await {
            Ok(orders) => {
                for order in orders {
                    if order.side == "BUY" {
                        self.state.buy_orders.push(order);
                    } else {
                        self.state.sell_orders.push(order);
                    }
                }
            }
            Err(e) => {
                return Err(AppError::Other(format!("网格初始化失败: {}", e)));
            }
        }

        strategy_info!(
            "grid",
            "网格初始化完成，买单数量: {}, 卖单数量: {}",
            self.state.buy_orders.len(),
            self.state.sell_orders.len()
        );

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
                    strategy_error!("grid", "无法将ws消息发送到通道: {e}");
                }
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        // 设置连接状态为连接中
        {
            let mut status_guard = self.ws_status.lock().await;
            *status_guard = WsConnectionStatus::Connecting;
        }

        match exchange.connect_ws(symbol.market_type, on_message).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // 设置连接状态为错误
                let error_msg = format!("WebSocket连接失败: {}", e);
                {
                    let mut status_guard = self.ws_status.lock().await;
                    *status_guard = WsConnectionStatus::Error(error_msg.clone());
                }
                Err(AppError::Other(error_msg))
            }
        }
    }
}
