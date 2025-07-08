use crate::error::AppError;
use crate::exchange::binance::Binance;
use crate::exchange::binance_model::{ExchangeInfo, Filter, Order};
use crate::exchange::traits::Exchange;
use crate::utils::precision::{
    adjust_price_by_filter, adjust_quantity_by_filter, validate_min_notional,
};
use crate::utils::symbol::{MarketType, Symbol};
use crate::SHUTDOWN;
use crate::{strategy_error, strategy_info, strategy_warn};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

/// Avellaneda-Stoikov做市策略配置
#[derive(Debug, Clone)]
pub struct AvellanedaStoikovConfig {
    /// 策略名称
    pub name: String,
    /// 是否启用
    pub enabled: bool,
    /// 交易对
    pub symbol: String,
    /// 每单金额(USDT)
    pub order_amount_usdt: f64,
    /// 最大持仓(USDT)
    pub max_position_usdt: f64,
    /// 风险厌恶系数 (gamma)
    pub risk_aversion: f64,
    /// 市场冲击系数 (kappa)
    pub market_impact: f64,
    /// 订单到达率 (lambda)
    pub order_arrival_rate: f64,
    /// 波动率计算窗口(秒)
    pub volatility_window: u64,
    /// 订单刷新间隔(毫秒)
    pub refresh_interval_ms: u64,
    /// 最大持仓时间(秒)
    pub max_hold_time_seconds: u64,
    /// 止损百分比
    pub stop_loss_percentage: f64,
    /// 最小价差(基点，1基点=0.01%)
    pub min_spread_bps: f64,
}

impl Default for AvellanedaStoikovConfig {
    fn default() -> Self {
        Self {
            name: "AvellanedaStoikovStrategy".to_string(),
            enabled: true,
            symbol: "ETHUSDC".to_string(),
            order_amount_usdt: 10.0,
            max_position_usdt: 500.0,
            risk_aversion: 0.1,         // gamma: 风险厌恶系数
            market_impact: 0.01,        // kappa: 市场冲击系数
            order_arrival_rate: 1.0,    // lambda: 订单到达率
            volatility_window: 300,     // 5分钟波动率窗口
            refresh_interval_ms: 1000,  // 1秒刷新
            max_hold_time_seconds: 300, // 5分钟最大持仓
            stop_loss_percentage: 0.1,  // 10%止损
            min_spread_bps: 5.0,        // 5基点最小价差
        }
    }
}

/// 订单信息
#[derive(Debug, Clone)]
pub struct OrderInfo {
    pub order_id: i64,
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub timestamp: i64,
}

/// 持仓信息
#[derive(Debug, Clone)]
pub struct PositionInfo {
    pub side: String, // "LONG" or "SHORT"
    pub quantity: f64,
    pub entry_price: f64,
    pub timestamp: i64,
    pub unrealized_pnl: f64,
}

/// 市场数据
#[derive(Debug, Clone)]
pub struct MarketData {
    pub symbol: String,
    pub price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub timestamp: i64,
}

/// 价格历史数据点
#[derive(Debug, Clone)]
pub struct PricePoint {
    pub price: f64,
    pub timestamp: i64,
}

/// Avellaneda-Stoikov做市策略
pub struct AvellanedaStoikovStrategy {
    config: AvellanedaStoikovConfig,
    exchange: Arc<Binance>,
    symbol_obj: Symbol,
    active_orders: HashMap<i64, OrderInfo>,
    current_position: Option<PositionInfo>,
    market_data: Option<MarketData>,
    price_history: VecDeque<PricePoint>, // 价格历史用于计算波动率
    price_precision: i32,
    quantity_precision: i32,
    min_qty: f64,
    min_notional: f64,
    last_refresh_time: i64,
    current_inventory: f64, // 当前库存(以基础资产计)
    volatility: f64,        // 当前波动率
}

impl AvellanedaStoikovStrategy {
    /// 创建新的Avellaneda-Stoikov做市策略实例
    pub async fn new(
        config: AvellanedaStoikovConfig,
        exchange: Arc<Binance>,
    ) -> Result<Self, AppError> {
        let symbol_obj = Symbol::from_str(&config.symbol, MarketType::UsdFutures)
            .map_err(|e| AppError::Other(e))?;

        // 获取交易对信息
        let exchange_info = exchange.get_exchange_info(MarketType::UsdFutures).await?;
        let symbol_info = exchange_info
            .symbols
            .iter()
            .find(|s| s.symbol == config.symbol)
            .ok_or_else(|| AppError::Other(format!("找不到交易对信息: {}", config.symbol)))?;

        let price_precision = symbol_info.price_precision;
        let quantity_precision = symbol_info.quantity_precision;

        let min_qty = symbol_info
            .filters
            .iter()
            .find_map(|f| {
                if let Filter::LotSize { min_qty, .. } = f {
                    min_qty.parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.001);

        let min_notional = symbol_info
            .filters
            .iter()
            .find_map(|f| {
                if let Filter::MinNotional { notional } = f {
                    notional.parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(5.0);

        Ok(Self {
            config,
            exchange,
            symbol_obj,
            active_orders: HashMap::new(),
            current_position: None,
            market_data: None,
            price_history: VecDeque::new(),
            price_precision,
            quantity_precision,
            min_qty,
            min_notional,
            last_refresh_time: 0,
            current_inventory: 0.0,
            volatility: 0.01, // 初始波动率1%
        })
    }

    /// 运行策略
    pub async fn run(&mut self) -> Result<(), AppError> {
        strategy_info!(
            "avellaneda_stoikov",
            "[{}] Avellaneda-Stoikov做市策略启动",
            self.config.name
        );
        strategy_info!(
            "avellaneda_stoikov",
            "[{}] 配置: 交易对={}, 每单={}USDT, 最大持仓={}USDT, 风险厌恶={:.3}, 市场冲击={:.3}",
            self.config.name,
            self.config.symbol,
            self.config.order_amount_usdt,
            self.config.max_position_usdt,
            self.config.risk_aversion,
            self.config.market_impact
        );

        // 使用Arc和Mutex来共享状态
        let market_data = Arc::new(Mutex::new(None::<MarketData>));
        let active_orders = Arc::new(Mutex::new(HashMap::<i64, OrderInfo>::new()));
        let current_position = Arc::new(Mutex::new(None::<PositionInfo>));
        let price_history = Arc::new(Mutex::new(VecDeque::<PricePoint>::new()));
        let current_inventory = Arc::new(Mutex::new(0.0f64));
        let volatility = Arc::new(Mutex::new(0.01f64));

        // 启动WebSocket价格订阅
        let ws_task = {
            let market_data = market_data.clone();
            let price_history = price_history.clone();
            let volatility = volatility.clone();
            let config = self.config.clone();
            let exchange = self.exchange.clone();
            let symbol_obj = self.symbol_obj.clone();

            tokio::spawn(async move {
                Self::websocket_price_feed(
                    config,
                    exchange,
                    symbol_obj,
                    market_data,
                    price_history,
                    volatility,
                )
                .await
            })
        };

        // 启动策略主循环
        let strategy_task = {
            let market_data = market_data.clone();
            let active_orders = active_orders.clone();
            let current_position = current_position.clone();
            let current_inventory = current_inventory.clone();
            let volatility = volatility.clone();
            let config = self.config.clone();
            let exchange = self.exchange.clone();
            let symbol_obj = self.symbol_obj.clone();
            let price_precision = self.price_precision;
            let quantity_precision = self.quantity_precision;
            let min_qty = self.min_qty;
            let min_notional = self.min_notional;

            tokio::spawn(async move {
                Self::strategy_loop(
                    config,
                    exchange,
                    symbol_obj,
                    market_data,
                    active_orders,
                    current_position,
                    current_inventory,
                    volatility,
                    price_precision,
                    quantity_precision,
                    min_qty,
                    min_notional,
                )
                .await
            })
        };

        // 等待任一任务完成
        tokio::select! {
            result = ws_task => {
                match result {
                    Ok(Ok(_)) => strategy_info!("avellaneda_stoikov", "[{}] WebSocket连接正常结束", self.config.name),
                    Ok(Err(e)) => strategy_error!("avellaneda_stoikov", "[{}] WebSocket连接错误: {}", self.config.name, e),
                    Err(e) => strategy_error!("avellaneda_stoikov", "[{}] WebSocket任务错误: {}", self.config.name, e),
                }
            }
            result = strategy_task => {
                match result {
                    Ok(Ok(_)) => strategy_info!("avellaneda_stoikov", "[{}] 策略循环正常结束", self.config.name),
                    Ok(Err(e)) => strategy_error!("avellaneda_stoikov", "[{}] 策略循环错误: {}", self.config.name, e),
                    Err(e) => strategy_error!("avellaneda_stoikov", "[{}] 策略任务错误: {}", self.config.name, e),
                }
            }
        }

        // 清理所有订单
        self.cleanup_final_orders(active_orders, current_position)
            .await?;

        Ok(())
    }

    /// 启动WebSocket价格订阅
    async fn websocket_price_feed(
        config: AvellanedaStoikovConfig,
        exchange: Arc<Binance>,
        symbol_obj: Symbol,
        market_data: Arc<Mutex<Option<MarketData>>>,
        price_history: Arc<Mutex<VecDeque<PricePoint>>>,
        volatility: Arc<Mutex<f64>>,
    ) -> Result<(), AppError> {
        let symbol_lower = config.symbol.to_lowercase();
        let ws_url = format!("wss://fstream.binance.com/ws/{}@bookTicker", symbol_lower);

        strategy_info!(
            "avellaneda_stoikov",
            "[{}] 连接WebSocket: {}",
            config.name,
            ws_url
        );

        let (ws_stream, _) = connect_async(&ws_url)
            .await
            .map_err(|e| AppError::Other(format!("WebSocket连接失败: {}", e)))?;

        let (mut _write, mut read) = ws_stream.split();

        let mut last_refresh_time = 0i64;

        while let Some(msg) = read.next().await {
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(
                    "avellaneda_stoikov",
                    "[{}] 收到关闭信号，WebSocket退出",
                    config.name
                );
                break;
            }

            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = Self::process_websocket_message(
                        &config,
                        &text,
                        &market_data,
                        &price_history,
                        &volatility,
                        &mut last_refresh_time,
                    )
                    .await
                    {
                        strategy_error!(
                            "avellaneda_stoikov",
                            "[{}] 处理WebSocket消息失败: {}",
                            config.name,
                            e
                        );
                    }
                }
                Ok(Message::Close(_)) => {
                    strategy_info!("avellaneda_stoikov", "[{}] WebSocket连接关闭", config.name);
                    break;
                }
                Err(e) => {
                    strategy_error!(
                        "avellaneda_stoikov",
                        "[{}] WebSocket错误: {}",
                        config.name,
                        e
                    );
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// 处理WebSocket消息
    async fn process_websocket_message(
        config: &AvellanedaStoikovConfig,
        message: &str,
        market_data: &Arc<Mutex<Option<MarketData>>>,
        price_history: &Arc<Mutex<VecDeque<PricePoint>>>,
        volatility: &Arc<Mutex<f64>>,
        last_refresh_time: &mut i64,
    ) -> Result<(), AppError> {
        let data: Value = serde_json::from_str(message)
            .map_err(|e| AppError::Other(format!("解析WebSocket消息失败: {}", e)))?;

        // bookTicker格式: {"u":400900217,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}
        if let (Some(symbol), Some(bid_str), Some(ask_str)) =
            (data["s"].as_str(), data["b"].as_str(), data["a"].as_str())
        {
            if symbol == config.symbol {
                let bid_price = bid_str
                    .parse::<f64>()
                    .map_err(|e| AppError::Other(format!("解析买价失败: {}", e)))?;
                let ask_price = ask_str
                    .parse::<f64>()
                    .map_err(|e| AppError::Other(format!("解析卖价失败: {}", e)))?;
                let price = (bid_price + ask_price) / 2.0; // 中间价

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                // 更新市场数据
                {
                    let mut data = market_data.lock().unwrap();
                    *data = Some(MarketData {
                        symbol: symbol.to_string(),
                        price,
                        bid_price,
                        ask_price,
                        timestamp: now,
                    });
                }

                // 更新价格历史
                {
                    let mut history = price_history.lock().unwrap();
                    history.push_back(PricePoint {
                        price,
                        timestamp: now,
                    });

                    // 保持窗口大小
                    let window_ms = config.volatility_window * 1000;
                    while let Some(front) = history.front() {
                        if now - front.timestamp > window_ms as i64 {
                            history.pop_front();
                        } else {
                            break;
                        }
                    }
                }

                // 计算波动率
                Self::calculate_volatility(price_history, volatility);

                // 打印价格更新（每10秒打印一次）
                if now - *last_refresh_time > 10000 {
                    let vol = { *volatility.lock().unwrap() };
                    strategy_info!(
                        "avellaneda_stoikov",
                        "[{}] 价格更新: {} 买价={:.4} 卖价={:.4} 中间价={:.4} 波动率={:.4}",
                        config.name,
                        symbol,
                        bid_price,
                        ask_price,
                        price,
                        vol
                    );
                    *last_refresh_time = now;
                }
            }
        }

        Ok(())
    }

    /// 计算波动率
    fn calculate_volatility(
        price_history: &Arc<Mutex<VecDeque<PricePoint>>>,
        volatility: &Arc<Mutex<f64>>,
    ) {
        let history = price_history.lock().unwrap();

        if history.len() < 2 {
            return;
        }

        // 计算对数收益率
        let mut returns = Vec::new();
        for i in 1..history.len() {
            let prev_price = history[i - 1].price;
            let curr_price = history[i].price;
            if prev_price > 0.0 && curr_price > 0.0 {
                returns.push((curr_price / prev_price).ln());
            }
        }

        if returns.len() < 2 {
            return;
        }

        // 计算标准差
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;

        let std_dev = variance.sqrt();

        // 年化波动率 (假设每秒一个数据点)
        let annualized_vol = std_dev * (365.25_f64 * 24.0_f64 * 3600.0_f64).sqrt();

        {
            let mut vol = volatility.lock().unwrap();
            *vol = annualized_vol.max(0.001); // 最小波动率0.1%
        }
    }

    /// 运行策略主循环
    async fn strategy_loop(
        config: AvellanedaStoikovConfig,
        exchange: Arc<Binance>,
        symbol_obj: Symbol,
        market_data: Arc<Mutex<Option<MarketData>>>,
        active_orders: Arc<Mutex<HashMap<i64, OrderInfo>>>,
        current_position: Arc<Mutex<Option<PositionInfo>>>,
        current_inventory: Arc<Mutex<f64>>,
        volatility: Arc<Mutex<f64>>,
        price_precision: i32,
        quantity_precision: i32,
        min_qty: f64,
        min_notional: f64,
    ) -> Result<(), AppError> {
        let mut last_order_time = 0i64;

        loop {
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(
                    "avellaneda_stoikov",
                    "[{}] 收到关闭信号，策略退出",
                    config.name
                );
                break;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            // 检查并更新持仓状态
            if let Err(e) =
                Self::update_position_status(&config, &current_position, &current_inventory).await
            {
                strategy_error!(
                    "avellaneda_stoikov",
                    "[{}] 更新持仓状态失败: {}",
                    config.name,
                    e
                );
            }

            // 检查止损
            if let Err(e) = Self::check_stop_loss(
                &config,
                &exchange,
                &symbol_obj,
                &market_data,
                &current_position,
            )
            .await
            {
                strategy_error!(
                    "avellaneda_stoikov",
                    "[{}] 检查止损失败: {}",
                    config.name,
                    e
                );
            }

            // 检查最大持仓时间
            if let Err(e) =
                Self::check_max_hold_time(&config, &exchange, &symbol_obj, &current_position).await
            {
                strategy_error!(
                    "avellaneda_stoikov",
                    "[{}] 检查最大持仓时间失败: {}",
                    config.name,
                    e
                );
            }

            // 刷新订单（每隔指定时间）
            if now - last_order_time > config.refresh_interval_ms as i64 {
                if let Err(e) = Self::refresh_orders_avellaneda_stoikov(
                    &config,
                    &exchange,
                    &symbol_obj,
                    &market_data,
                    &active_orders,
                    &current_position,
                    &current_inventory,
                    &volatility,
                    price_precision,
                    quantity_precision,
                    min_qty,
                    min_notional,
                )
                .await
                {
                    strategy_error!(
                        "avellaneda_stoikov",
                        "[{}] 刷新订单失败: {}",
                        config.name,
                        e
                    );
                }
                last_order_time = now;
            }

            // 短暂休眠
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// 更新持仓状态
    async fn update_position_status(
        config: &AvellanedaStoikovConfig,
        current_position: &Arc<Mutex<Option<PositionInfo>>>,
        current_inventory: &Arc<Mutex<f64>>,
    ) -> Result<(), AppError> {
        // 这里应该调用交易所API获取实际持仓
        // 为了简化，我们暂时跳过实际的API调用

        // 更新库存
        let inventory = {
            let pos = current_position.lock().unwrap();
            match &*pos {
                Some(position) => {
                    if position.side == "LONG" {
                        position.quantity
                    } else {
                        -position.quantity
                    }
                }
                None => 0.0,
            }
        };

        {
            let mut inv = current_inventory.lock().unwrap();
            *inv = inventory;
        }

        Ok(())
    }

    /// 检查止损
    async fn check_stop_loss(
        config: &AvellanedaStoikovConfig,
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        market_data: &Arc<Mutex<Option<MarketData>>>,
        current_position: &Arc<Mutex<Option<PositionInfo>>>,
    ) -> Result<(), AppError> {
        let position = {
            let pos = current_position.lock().unwrap();
            pos.clone()
        };

        let market = {
            let data = market_data.lock().unwrap();
            data.clone()
        };

        if let (Some(position), Some(market_data)) = (position, market) {
            let current_price = market_data.price;
            let entry_price = position.entry_price;

            let pnl_percentage = if position.side == "LONG" {
                (current_price - entry_price) / entry_price
            } else {
                (entry_price - current_price) / entry_price
            };

            if pnl_percentage <= -config.stop_loss_percentage {
                strategy_warn!(
                    "avellaneda_stoikov",
                    "[{}] 触发止损: 持仓方向={}, 入场价={:.4}, 当前价={:.4}, 亏损={:.2}%",
                    config.name,
                    position.side,
                    entry_price,
                    current_price,
                    pnl_percentage * 100.0
                );

                // 执行止损
                Self::close_position(config, exchange, symbol_obj, current_position, "止损")
                    .await?;
            }
        }

        Ok(())
    }

    /// 检查最大持仓时间
    async fn check_max_hold_time(
        config: &AvellanedaStoikovConfig,
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        current_position: &Arc<Mutex<Option<PositionInfo>>>,
    ) -> Result<(), AppError> {
        let position = {
            let pos = current_position.lock().unwrap();
            pos.clone()
        };

        if let Some(position) = position {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let hold_time = (now - position.timestamp) / 1000; // 转换为秒

            if hold_time > config.max_hold_time_seconds as i64 {
                strategy_warn!(
                    "avellaneda_stoikov",
                    "[{}] 超过最大持仓时间: {}秒，强制平仓",
                    config.name,
                    hold_time
                );
                Self::close_position(config, exchange, symbol_obj, current_position, "超时平仓")
                    .await?;
            }
        }

        Ok(())
    }

    /// 基于Avellaneda-Stoikov模型刷新订单
    async fn refresh_orders_avellaneda_stoikov(
        config: &AvellanedaStoikovConfig,
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        market_data: &Arc<Mutex<Option<MarketData>>>,
        active_orders: &Arc<Mutex<HashMap<i64, OrderInfo>>>,
        current_position: &Arc<Mutex<Option<PositionInfo>>>,
        current_inventory: &Arc<Mutex<f64>>,
        volatility: &Arc<Mutex<f64>>,
        price_precision: i32,
        quantity_precision: i32,
        min_qty: f64,
        min_notional: f64,
    ) -> Result<(), AppError> {
        // 如果没有市场数据，跳过
        let market_data_clone = {
            let data = market_data.lock().unwrap();
            match &*data {
                Some(data) => data.clone(),
                None => return Ok(()),
            }
        };

        let inventory = { *current_inventory.lock().unwrap() };
        let vol = { *volatility.lock().unwrap() };

        // 取消所有现有订单
        Self::cancel_all_orders(config, exchange, symbol_obj, active_orders).await?;

        // Avellaneda-Stoikov模型计算
        let s = market_data_clone.price; // 当前价格
        let q = inventory; // 当前库存
        let gamma = config.risk_aversion; // 风险厌恶系数
        let sigma = vol; // 波动率
        let kappa = config.market_impact; // 市场冲击系数
        let lambda = config.order_arrival_rate; // 订单到达率

        // 计算保留价格 (reservation price)
        let reservation_price = s - q * gamma * sigma.powi(2);

        // 计算最优价差的一半
        let optimal_half_spread = gamma * sigma.powi(2) / (2.0 * lambda)
            + (2.0 * gamma * sigma.powi(2) / lambda).sqrt() / 2.0;

        // 应用最小价差限制（基点转换为小数）
        let min_half_spread = s * config.min_spread_bps / 20000.0; // 除以20000是因为价差的一半，且基点转百分比
        let final_half_spread = optimal_half_spread.max(min_half_spread);

        // 计算买卖价格
        let bid_price = reservation_price - final_half_spread;
        let ask_price = reservation_price + final_half_spread;

        // 计算订单数量（基于配置的订单金额）
        let bid_quantity = config.order_amount_usdt / bid_price;
        let ask_quantity = config.order_amount_usdt / ask_price;

        // 调整精度
        let bid_price_adjusted = Self::adjust_price(bid_price, price_precision);
        let ask_price_adjusted = Self::adjust_price(ask_price, price_precision);
        let bid_quantity_adjusted =
            Self::adjust_quantity(bid_quantity, quantity_precision, min_qty);
        let ask_quantity_adjusted =
            Self::adjust_quantity(ask_quantity, quantity_precision, min_qty);

        // 验证最小名义价值
        if bid_price_adjusted * bid_quantity_adjusted < min_notional {
            strategy_warn!(
                "avellaneda_stoikov",
                "[{}] 买单金额过小，跳过: {:.2}",
                config.name,
                bid_price_adjusted * bid_quantity_adjusted
            );
        } else {
            // 下买单
            match Self::place_limit_order(
                exchange,
                symbol_obj,
                "BUY",
                bid_quantity_adjusted,
                bid_price_adjusted,
            )
            .await
            {
                Ok(order) => {
                    strategy_info!(
                        "avellaneda_stoikov",
                        "[{}] AS买单: ID={}, 价格={:.4}, 数量={:.6}, 库存={:.3}, 波动率={:.4}",
                        config.name,
                        order.order_id,
                        bid_price_adjusted,
                        bid_quantity_adjusted,
                        inventory,
                        vol
                    );

                    let mut orders = active_orders.lock().unwrap();
                    orders.insert(
                        order.order_id,
                        OrderInfo {
                            order_id: order.order_id,
                            side: "BUY".to_string(),
                            price: bid_price_adjusted,
                            quantity: bid_quantity_adjusted,
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64,
                        },
                    );
                }
                Err(e) => {
                    strategy_error!("avellaneda_stoikov", "[{}] 买单失败: {}", config.name, e);
                }
            }
        }

        if ask_price_adjusted * ask_quantity_adjusted < min_notional {
            strategy_warn!(
                "avellaneda_stoikov",
                "[{}] 卖单金额过小，跳过: {:.2}",
                config.name,
                ask_price_adjusted * ask_quantity_adjusted
            );
        } else {
            // 下卖单
            match Self::place_limit_order(
                exchange,
                symbol_obj,
                "SELL",
                ask_quantity_adjusted,
                ask_price_adjusted,
            )
            .await
            {
                Ok(order) => {
                    strategy_info!(
                        "avellaneda_stoikov",
                        "[{}] AS卖单: ID={}, 价格={:.4}, 数量={:.6}, 库存={:.3}, 波动率={:.4}",
                        config.name,
                        order.order_id,
                        ask_price_adjusted,
                        ask_quantity_adjusted,
                        inventory,
                        vol
                    );

                    let mut orders = active_orders.lock().unwrap();
                    orders.insert(
                        order.order_id,
                        OrderInfo {
                            order_id: order.order_id,
                            side: "SELL".to_string(),
                            price: ask_price_adjusted,
                            quantity: ask_quantity_adjusted,
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64,
                        },
                    );
                }
                Err(e) => {
                    strategy_error!("avellaneda_stoikov", "[{}] 卖单失败: {}", config.name, e);
                }
            }
        }

        Ok(())
    }

    /// 下限价单
    async fn place_limit_order(
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        side: &str,
        quantity: f64,
        price: f64,
    ) -> Result<Order, AppError> {
        exchange
            .place_order(symbol_obj, side, "LIMIT", quantity, Some(price))
            .await
    }

    /// 取消所有订单
    async fn cancel_all_orders(
        config: &AvellanedaStoikovConfig,
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        active_orders: &Arc<Mutex<HashMap<i64, OrderInfo>>>,
    ) -> Result<(), AppError> {
        let order_ids: Vec<i64> = {
            let orders = active_orders.lock().unwrap();
            orders.keys().cloned().collect()
        };

        for order_id in order_ids {
            match exchange.cancel_order(symbol_obj, order_id).await {
                Ok(_) => {
                    let mut orders = active_orders.lock().unwrap();
                    orders.remove(&order_id);
                }
                Err(e) => {
                    strategy_error!(
                        "avellaneda_stoikov",
                        "[{}] 取消订单失败: {} - {}",
                        config.name,
                        order_id,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// 平仓
    async fn close_position(
        config: &AvellanedaStoikovConfig,
        exchange: &Arc<Binance>,
        symbol_obj: &Symbol,
        current_position: &Arc<Mutex<Option<PositionInfo>>>,
        reason: &str,
    ) -> Result<(), AppError> {
        let position = {
            let pos = current_position.lock().unwrap();
            pos.clone()
        };

        if let Some(position) = position {
            let side = if position.side == "LONG" {
                "SELL"
            } else {
                "BUY"
            };

            match exchange
                .place_order(symbol_obj, side, "MARKET", position.quantity, None)
                .await
            {
                Ok(order) => {
                    strategy_info!(
                        "avellaneda_stoikov",
                        "[{}] {}成功: 订单ID={}, 数量={:.6}",
                        config.name,
                        reason,
                        order.order_id,
                        position.quantity
                    );
                    let mut pos = current_position.lock().unwrap();
                    *pos = None;
                }
                Err(e) => {
                    strategy_error!(
                        "avellaneda_stoikov",
                        "[{}] {}失败: {}",
                        config.name,
                        reason,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// 清理所有订单
    async fn cleanup_final_orders(
        &self,
        active_orders: Arc<Mutex<HashMap<i64, OrderInfo>>>,
        current_position: Arc<Mutex<Option<PositionInfo>>>,
    ) -> Result<(), AppError> {
        strategy_info!(
            "avellaneda_stoikov",
            "[{}] 开始清理所有订单...",
            self.config.name
        );

        // 取消所有挂单
        Self::cancel_all_orders(
            &self.config,
            &self.exchange,
            &self.symbol_obj,
            &active_orders,
        )
        .await?;

        // 平掉所有持仓
        let has_position = {
            let pos = current_position.lock().unwrap();
            pos.is_some()
        };

        if has_position {
            Self::close_position(
                &self.config,
                &self.exchange,
                &self.symbol_obj,
                &current_position,
                "策略退出平仓",
            )
            .await?;
        }

        strategy_info!("avellaneda_stoikov", "[{}] 订单清理完成", self.config.name);
        Ok(())
    }

    /// 调整价格精度
    fn adjust_price(price: f64, price_precision: i32) -> f64 {
        let factor = 10_f64.powi(price_precision);
        (price * factor).round() / factor
    }

    /// 调整数量精度
    fn adjust_quantity(quantity: f64, quantity_precision: i32, min_qty: f64) -> f64 {
        let factor = 10_f64.powi(quantity_precision);
        let adjusted = (quantity * factor).floor() / factor;
        adjusted.max(min_qty)
    }
}
