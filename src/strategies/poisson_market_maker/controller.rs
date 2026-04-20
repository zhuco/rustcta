//! # 泊松队列做市策略 (Poisson Queue Market Making Strategy)
//!
//! 基于泊松分布模型的智能做市策略，动态调整买卖价差。
//!
//! ## 主要功能
//! - 使用泊松队列模型分析订单流
//! - 动态计算最优买卖价差
//! - 根据市场深度调整报价
//! - 自动库存风险管理

use super::config::*;
use super::state::{
    LocalOrderBook, MMStrategyState, OrderEventType, OrderFlowEvent, OrderIntent,
    PoissonParameters, SymbolInfo,
};

use chrono::{DateTime, Duration, Utc};
use crossbeam::queue::ArrayQueue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::analysis::{TradeCollector, TradeData};
use crate::core::{
    error::ExchangeError,
    exchange::Exchange,
    order_cache::OrderCache,
    types::*,
    websocket::{BaseWebSocketClient, WebSocketClient},
};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::strategies::common::{
    build_unified_risk_evaluator, RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits,
    StrategySnapshot, UnifiedRiskEvaluator,
};
pub struct PoissonMarketMaker {
    config: PoissonMMConfig,
    account_manager: Arc<AccountManager>,
    state: Arc<Mutex<MMStrategyState>>,
    order_flow_buffer: Arc<RwLock<VecDeque<OrderFlowEvent>>>,
    poisson_params: Arc<RwLock<PoissonParameters>>,
    ws_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    running: Arc<RwLock<bool>>,
    current_price: Arc<RwLock<f64>>,
    orderbook: Arc<RwLock<LocalOrderBook>>,
    collector: Option<Arc<TradeCollector>>,
    symbol_info: Arc<RwLock<Option<SymbolInfo>>>,
    is_dual_mode: Arc<RwLock<bool>>,
    order_cache: Arc<OrderCache>,
    user_stream_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    last_order_fetch: Arc<RwLock<DateTime<Utc>>>,
    last_bid_price: Arc<RwLock<f64>>,
    last_ask_price: Arc<RwLock<f64>>,
    last_position_update: Arc<RwLock<DateTime<Utc>>>,
    local_position: Arc<RwLock<f64>>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
}

impl PoissonMarketMaker {
    /// 创建策略实例
    pub fn new(config: PoissonMMConfig, account_manager: Arc<AccountManager>) -> Self {
        Self::with_collector(config, account_manager, None)
    }

    /// 创建带数据收集器的策略实例
    pub fn with_collector(
        config: PoissonMMConfig,
        account_manager: Arc<AccountManager>,
        collector: Option<Arc<TradeCollector>>,
    ) -> Self {
        let state = MMStrategyState {
            inventory: 0.0,
            long_inventory: 0.0,
            short_inventory: 0.0,
            avg_price: 0.0,
            long_avg_price: 0.0,
            short_avg_price: 0.0,
            active_buy_orders: HashMap::new(),
            active_sell_orders: HashMap::new(),
            buy_client_to_exchange: HashMap::new(),
            buy_exchange_to_client: HashMap::new(),
            sell_client_to_exchange: HashMap::new(),
            sell_exchange_to_client: HashMap::new(),
            order_slots: HashMap::new(),
            client_to_slot: HashMap::new(),
            exchange_to_slot: HashMap::new(),
            total_pnl: 0.0,
            daily_pnl: 0.0,
            trade_count: 0,
            start_time: Utc::now(),
        };

        // 使用initial_lambda初始化参数，避免等待太久
        let initial_lambda = config.poisson.initial_lambda;
        let initial_mu = initial_lambda * 1.2; // 初始成交率设为到达率的120%（保证队列稳定）
        let poisson_params = PoissonParameters {
            lambda_bid: initial_lambda,
            lambda_ask: initial_lambda,
            mu_bid: initial_mu,
            mu_ask: initial_mu,
            avg_queue_bid: initial_lambda / (initial_mu - initial_lambda).max(0.1), // L = λ/(μ-λ)
            avg_queue_ask: initial_lambda / (initial_mu - initial_lambda).max(0.1),
            last_update: Utc::now(),
            last_trade_time: None,
        };

        let risk_limits = Self::build_risk_limits(&config);
        let risk_evaluator =
            build_unified_risk_evaluator(config.name.clone(), None, Some(risk_limits.clone()));

        Self {
            config,
            account_manager,
            state: Arc::new(Mutex::new(state)),
            // 使用无锁队列代替VecDeque
            order_flow_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(10000))),
            poisson_params: Arc::new(RwLock::new(poisson_params)),
            ws_client: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
            current_price: Arc::new(RwLock::new(0.0)),
            orderbook: Arc::new(RwLock::new(LocalOrderBook {
                bids: Vec::new(),
                asks: Vec::new(),
                last_update: Utc::now(),
            })),
            collector,
            symbol_info: Arc::new(RwLock::new(None)),
            is_dual_mode: Arc::new(RwLock::new(false)),
            order_cache: Arc::new(OrderCache::new(1800)), // 30分钟缓存
            user_stream_client: Arc::new(RwLock::new(None)),
            last_order_fetch: Arc::new(RwLock::new(Utc::now() - Duration::hours(1))),
            last_bid_price: Arc::new(RwLock::new(0.0)),
            last_ask_price: Arc::new(RwLock::new(0.0)),
            last_position_update: Arc::new(RwLock::new(Utc::now() - Duration::hours(1))),
            local_position: Arc::new(RwLock::new(0.0)),
            risk_evaluator,
            risk_limits,
        }
    }

    /// 获取交易对信息
    async fn fetch_symbol_info(&self) -> Result<()> {
        log::info!("📋 获取交易对信息...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在 : {}", self.config.account.account_id))
            })?;

        // 获取市场类型
        let market_type = MarketType::Futures;

        match account
            .exchange
            .get_symbol_info(&self.config.trading.symbol, market_type)
            .await
        {
            Ok(info) => {
                // 解析交易对名称
                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDT").to_string();

                // 计算精度
                let price_precision = Self::calculate_precision(info.tick_size);
                let quantity_precision = Self::calculate_precision(info.step_size);

                let symbol_info = SymbolInfo {
                    base_asset,
                    quote_asset,
                    tick_size: info.tick_size,
                    step_size: info.step_size,
                    min_notional: info.min_notional.unwrap_or(10.0),
                    price_precision,
                    quantity_precision,
                };

                log::info!("✅ 交易对信息:");
                log::info!("  - 基础货币: {}", symbol_info.base_asset);
                log::info!("  - 报价货币: {}", symbol_info.quote_asset);
                log::info!(
                    "  - 价格精度: {} 位小数 (tick_size: {})",
                    symbol_info.price_precision,
                    info.tick_size
                );
                log::info!(
                    "  - 数量精度: {} 位小数 (step_size: {})",
                    symbol_info.quantity_precision,
                    info.step_size
                );
                log::info!(
                    "  - 最小名义价值: {} {}",
                    symbol_info.min_notional,
                    symbol_info.quote_asset
                );

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
            Err(e) => {
                log::warn!("⚠️ 无法获取交易对信息: {}，使用配置文件中的精度设置", e);

                // 使用配置文件中的精度作为后备方案
                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"TOKEN").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDT").to_string();

                let symbol_info = SymbolInfo {
                    base_asset,
                    quote_asset,
                    tick_size: 1.0 / 10_f64.powi(self.config.trading.price_precision as i32),
                    step_size: 1.0 / 10_f64.powi(self.config.trading.quantity_precision as i32),
                    min_notional: 10.0,
                    price_precision: self.config.trading.price_precision,
                    quantity_precision: self.config.trading.quantity_precision,
                };

                log::info!(
                    "  使用配置文件精度: 价格 {} 位，数量 {} 位",
                    symbol_info.price_precision,
                    symbol_info.quantity_precision
                );

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
        }
    }

    /// 计算精度（小数位数）
    fn calculate_precision(step: f64) -> usize {
        if step >= 1.0 {
            0
        } else {
            let s = format!("{:.10}", step);
            let parts: Vec<&str> = s.split('.').collect();
            if parts.len() > 1 {
                parts[1].trim_end_matches('0').len()
            } else {
                0
            }
        }
    }

    /// 获取报价货币
    async fn get_quote_asset(&self) -> String {
        self.symbol_info
            .read()
            .await
            .as_ref()
            .map(|info| info.quote_asset.clone())
            .unwrap_or_else(|| "USDT".to_string())
    }

    /// 检查持仓模式
    async fn check_position_mode(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        // 如果是Binance交易所，检查持仓模式
        if self.config.account.exchange.to_lowercase() == "binance" {
            // 使用反射调用BinanceExchange的方法
            use crate::exchanges::binance::BinanceExchange;

            if let Some(binance) = account.exchange.as_any().downcast_ref::<BinanceExchange>() {
                let is_dual = binance.get_position_mode().await?;
                *self.is_dual_mode.write().await = is_dual;

                log::info!(
                    "✅ Binance账户 {} 持仓模式: {}",
                    self.config.account.account_id,
                    if is_dual {
                        "双向持仓"
                    } else {
                        "单向持仓"
                    }
                );
            }
        }

        Ok(())
    }

    /// 判断是否双向持仓模式
    async fn is_dual_position_mode(&self) -> bool {
        *self.is_dual_mode.read().await
    }

    /// 启动策略
    pub async fn start(&self) -> Result<()> {
        log::info!("🚀 ========== 启动泊松队列做市策略 ==========");
        log::info!("📋 策略配置:");
        log::info!("  - 交易对: {}", self.config.trading.symbol);
        log::info!("  - 每单金额: {} USDC", self.config.trading.order_size_usdc);

        // 显示当前时间偏移（如果有）
        if let Some(time_sync) = crate::utils::time_sync::get_time_sync() {
            let offset = time_sync.get_offset_ms().await;
            if offset.abs() > 100 {
                log::info!("⏰ 当前时间偏移: {}ms", offset);
            }
        }

        // 获取交易对信息
        self.fetch_symbol_info().await?;

        // 检查账户持仓模式
        self.check_position_mode().await?;

        // 初始化用户数据流
        if let Err(e) = self.init_user_stream().await {
            log::warn!("无法初始化用户数据流: {}, 将使用轮询模式", e);
        }

        // 同步初始持仓
        log::info!("📊 同步初始持仓状态...");
        if let Err(e) = self.update_position_status().await {
            log::warn!("⚠️ 初始持仓同步失败: {}，将从0开始", e);
        }

        // 使用动态获取的交易对信息
        let symbol_info = self.symbol_info.read().await;
        if let Some(info) = symbol_info.as_ref() {
            log::info!(
                "  - 最大库存: {} {}",
                self.config.trading.max_inventory,
                info.base_asset
            );
            log::info!(
                "  - 价差范围: {}-{} bp",
                self.config.trading.min_spread_bp,
                self.config.trading.max_spread_bp
            );
            log::info!("  - 价格精度: {} 位小数", info.price_precision);
            log::info!("  - 数量精度: {} 位小数", info.quantity_precision);
        } else {
            log::info!("  - 最大库存: {}", self.config.trading.max_inventory);
            log::info!(
                "  - 价差范围: {}-{} bp",
                self.config.trading.min_spread_bp,
                self.config.trading.max_spread_bp
            );
        }

        // 设置运行标志
        *self.running.write().await = true;

        // 取消所有现有挂单
        log::info!("🔄 取消所有现有挂单...");
        if let Err(e) = self.cancel_all_orders().await {
            log::warn!("取消挂单时出现警告: {}", e);
        }

        // 1. 连接WebSocket获取实时数据
        self.connect_websocket().await?;

        // 2. 启动数据收集任务
        let collector = self.clone_for_task();
        tokio::spawn(async move {
            if let Err(e) = collector.collect_order_flow().await {
                log::error!("订单流收集失败: {}", e);
            }
        });

        // 3. 启动参数估计任务
        let estimator = self.clone_for_task();
        tokio::spawn(async move {
            if let Err(e) = estimator.estimate_poisson_parameters().await {
                log::error!("参数估计失败: {}", e);
            }
        });

        // 4. 启动做市主循环
        self.run_market_making().await?;

        Ok(())
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<()> {
        log::info!("⏹️ 停止泊松队列做市策略");

        *self.running.write().await = false;

        // 取消所有订单
        self.cancel_all_orders().await?;

        // 平掉所有持仓
        self.close_all_positions().await?;

        // 断开WebSocket
        if let Some(_ws) = self.ws_client.write().await.take() {
            // WebSocket会在drop时自动关闭
            log::info!("已断开WebSocket连接");
        }

        // 输出统计
        self.print_statistics().await;

        Ok(())
    }

    /// 连接WebSocket
    async fn connect_websocket(&self) -> Result<()> {
        log::info!("📡 连接WebSocket获取实时数据...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        // 根据交易所创建WebSocket URL
        let ws_url = match self.config.account.exchange.as_str() {
            "binance" => {
                // Binance期货WebSocket - 市场数据
                // 注意：用户数据流需要通过单独的WebSocket连接，这里先只订阅市场数据
                // 例如: ENA/USDT -> enausdt
                let symbol = self.config.trading.symbol.to_lowercase().replace("/", "");
                format!(
                    "wss://fstream.binance.com/stream?streams={}@depth20@100ms/{}@trade",
                    symbol, symbol
                )
            }
            _ => {
                return Err(ExchangeError::Other(format!(
                    "不支持的交易所: {}",
                    self.config.account.exchange
                )));
            }
        };

        // 创建WebSocket客户端
        let mut ws_client = BaseWebSocketClient::new(ws_url, self.config.account.exchange.clone());

        ws_client.connect().await?;

        let ws_client = Arc::new(Mutex::new(ws_client));

        *self.ws_client.write().await = Some(ws_client);

        log::info!("✅ WebSocket连接成功");
        Ok(())
    }

    /// 收集订单流数据
    async fn collect_order_flow(&self) -> Result<()> {
        log::info!("📊 开始收集订单流数据...");

        let mut message_count = 0;
        let mut last_log_time = Utc::now();

        while *self.running.read().await {
            // 从WebSocket接收消息
            if let Some(ws) = &*self.ws_client.read().await {
                let mut ws_guard = ws.lock().await;
                match ws_guard.receive().await {
                    Ok(Some(message)) => {
                        message_count += 1;
                        // 每10秒或每100条消息打印一次统计
                        let now = Utc::now();
                        if message_count % 100 == 0
                            || now.signed_duration_since(last_log_time).num_seconds() > 10
                        {
                            log::debug!("📊 已接收 {} 条WebSocket消息", message_count);
                            last_log_time = now;
                        }

                        // 将字符串消息解析为WsMessage
                        match self.parse_websocket_message(&message).await {
                            Ok(ws_msg) => {
                                if let Err(e) = self.process_ws_message(ws_msg).await {
                                    log::error!("处理WebSocket消息失败: {}", e);
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "解析WebSocket消息失败: {}, 消息前50字符: {:?}",
                                    e,
                                    &message.chars().take(50).collect::<String>()
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        log::error!("WebSocket接收错误: {}", e);
                        // 尝试重连
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        Ok(())
    }

    /// 解析WebSocket消息
    async fn parse_websocket_message(&self, message: &str) -> Result<WsMessage> {
        // 解析Binance WebSocket消息
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(message) {
            // 处理Binance流格式（有stream字段）
            let data = if json.get("stream").is_some() && json.get("data").is_some() {
                // 这是流格式消息，获取内部的data
                &json["data"]
            } else {
                // 直接消息格式
                &json
            };

            // 首先检查是否是用户数据流事件（ORDER_TRADE_UPDATE）
            if let Some(event_type) = data.get("e").and_then(|e| e.as_str()) {
                if event_type == "ORDER_TRADE_UPDATE" {
                    // 处理订单更新事件
                    let order_data = &data["o"];
                    let order_status = order_data["X"].as_str().unwrap_or("");

                    if order_status == "FILLED" || order_status == "PARTIALLY_FILLED" {
                        log::info!(
                            "📡 检测到ORDER_TRADE_UPDATE订单更新事件: 状态={}",
                            order_status
                        );

                        // 解析成交信息
                        let symbol = order_data["s"].as_str().unwrap_or("");
                        let order_id = order_data["c"].as_str().unwrap_or("");
                        let side = order_data["S"].as_str().unwrap_or("");
                        let price = order_data["ap"]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let executed_qty = order_data["z"]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        // 记录成交到数据库
                        if let Some(ref collector) = self.collector {
                            // 获取真实的交易时间
                            let trade_time = order_data["T"]
                                .as_i64()
                                .map(|ts| {
                                    DateTime::<Utc>::from_timestamp_millis(ts)
                                        .unwrap_or_else(|| Utc::now())
                                })
                                .unwrap_or_else(|| Utc::now());

                            let trade_data = TradeData {
                                trade_time,
                                strategy_name: self.config.name.clone(),
                                account_id: self.config.account.account_id.clone(),
                                exchange: self.config.account.exchange.clone(),
                                symbol: self.config.trading.symbol.clone(),
                                side: side.to_string(),
                                order_type: Some("Limit".to_string()),
                                price: Decimal::from_str(&price.to_string()).unwrap_or_default(),
                                amount: Decimal::from_str(&executed_qty.to_string())
                                    .unwrap_or_default(),
                                value: Some(
                                    Decimal::from_str(&(price * executed_qty).to_string())
                                        .unwrap_or_default(),
                                ),
                                fee: None,
                                fee_currency: Some("USDT".to_string()),
                                realized_pnl: None,
                                pnl_percentage: None,
                                order_id: order_id.to_string(),
                                parent_order_id: None,
                                position_side: None,
                                metadata: None,
                            };

                            if let Err(e) = collector.record_trade(trade_data).await {
                                log::error!("记录交易失败: {}", e);
                            } else {
                                log::info!(
                                    "✅ 通过WebSocket记录成交: {} {} @ {} x {}",
                                    symbol,
                                    side,
                                    price,
                                    executed_qty
                                );
                            }
                        }

                        // 返回Trade消息供策略处理
                        return Ok(WsMessage::Trade(Trade {
                            id: order_data["t"].to_string(),
                            symbol: self.config.trading.symbol.clone(),
                            price,
                            amount: executed_qty,
                            side: if side == "BUY" {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            },
                            timestamp: Utc::now(),
                            fee: None,
                            order_id: Some(order_id.to_string()),
                        }));
                    }
                }
            }

            // 检查是否包含stream字段（Binance格式）
            if let Some(stream) = json.get("stream").and_then(|s| s.as_str()) {
                if let Some(data) = json.get("data") {
                    if stream.contains("trade") {
                        // 解析成交数据
                        let price = data["p"]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let amount = data["q"]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let is_buyer_maker = data["m"].as_bool().unwrap_or(false);

                        return Ok(WsMessage::Trade(Trade {
                            id: data["t"].to_string(),
                            symbol: self.config.trading.symbol.clone(),
                            price,
                            amount,
                            side: if is_buyer_maker {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            },
                            timestamp: Utc::now(),
                            fee: None,
                            order_id: None,
                        }));
                    } else if stream.contains("depth") {
                        // 解析深度数据
                        let mut bids = Vec::new();
                        let mut asks = Vec::new();

                        if let Some(bid_array) = data["b"].as_array() {
                            for bid in bid_array.iter().take(20) {
                                if let Some(arr) = bid.as_array() {
                                    if arr.len() >= 2 {
                                        let price = arr[0]
                                            .as_str()
                                            .and_then(|p| p.parse::<f64>().ok())
                                            .unwrap_or(0.0);
                                        let qty = arr[1]
                                            .as_str()
                                            .and_then(|q| q.parse::<f64>().ok())
                                            .unwrap_or(0.0);
                                        bids.push([price, qty]);
                                    }
                                }
                            }
                        }

                        if let Some(ask_array) = data["a"].as_array() {
                            for ask in ask_array.iter().take(20) {
                                if let Some(arr) = ask.as_array() {
                                    if arr.len() >= 2 {
                                        let price = arr[0]
                                            .as_str()
                                            .and_then(|p| p.parse::<f64>().ok())
                                            .unwrap_or(0.0);
                                        let qty = arr[1]
                                            .as_str()
                                            .and_then(|q| q.parse::<f64>().ok())
                                            .unwrap_or(0.0);
                                        asks.push([price, qty]);
                                    }
                                }
                            }
                        }

                        return Ok(WsMessage::OrderBook(OrderBook {
                            symbol: self.config.trading.symbol.clone(),
                            bids,
                            asks,
                            timestamp: Utc::now(),
                            info: serde_json::Value::Null,
                        }));
                    }
                }
            }
        }

        Ok(WsMessage::Text(message.to_string()))
    }

    /// 处理WebSocket消息
    async fn process_ws_message(&self, message: WsMessage) -> Result<()> {
        match message {
            WsMessage::Trade(trade) => {
                // 记录成交事件
                let event = OrderFlowEvent {
                    timestamp: Utc::now(),
                    side: trade.side.clone(),
                    price: trade.price,
                    quantity: trade.amount,
                    event_type: OrderEventType::Trade,
                };

                // 更新当前价格
                *self.current_price.write().await = trade.price;

                // 添加到缓冲区
                let mut buffer = self.order_flow_buffer.write().await;
                buffer.push_back(event);

                // 限制缓冲区大小
                while buffer.len() > 10000 {
                    buffer.pop_front();
                }
            }
            WsMessage::OrderBook(depth) => {
                // 更新订单簿
                let mut orderbook = self.orderbook.write().await;
                orderbook.bids = depth.bids.iter().map(|b| (b[0], b[1])).collect();
                orderbook.asks = depth.asks.iter().map(|a| (a[0], a[1])).collect();
                orderbook.last_update = Utc::now();

                // 更新当前价格（使用最佳买卖价的中间价）
                if !depth.bids.is_empty() && !depth.asks.is_empty() {
                    let best_bid = depth.bids[0][0];
                    let best_ask = depth.asks[0][0];
                    let mid_price = (best_bid + best_ask) / 2.0;
                    if mid_price > 0.0 {
                        *self.current_price.write().await = mid_price;
                    }
                }

                // 不记录订单簿更新为订单流事件，只记录真实成交
            }
            _ => {}
        }

        Ok(())
    }

    /// 估计泊松参数
    async fn estimate_poisson_parameters(&self) -> Result<()> {
        log::info!("📈 开始估计泊松参数...");

        loop {
            if !*self.running.read().await {
                break;
            }

            // 获取观察窗口内的数据
            let window = Duration::seconds(self.config.poisson.observation_window_secs as i64);
            let now = Utc::now();
            let cutoff = now - window;

            let buffer = self.order_flow_buffer.read().await;
            let recent_events: Vec<_> = buffer
                .iter()
                .filter(|e| e.timestamp > cutoff)
                .cloned()
                .collect();
            drop(buffer);

            if recent_events.len() >= self.config.poisson.min_samples {
                // 计算到达率和成交率
                let (lambda_bid, lambda_ask, mu_bid, mu_ask) = self.calculate_rates(&recent_events);

                // 计算平均队列长度 (M/M/1模型: L = λ/(μ-λ))
                let avg_queue_bid = if mu_bid > lambda_bid {
                    lambda_bid / (mu_bid - lambda_bid)
                } else {
                    10.0 // 上限
                };

                let avg_queue_ask = if mu_ask > lambda_ask {
                    lambda_ask / (mu_ask - lambda_ask)
                } else {
                    10.0
                };

                // 更新参数(使用EMA平滑)
                let mut params = self.poisson_params.write().await;
                let alpha = self.config.poisson.smoothing_alpha;

                if params.lambda_bid == 0.0 {
                    // 首次初始化
                    params.lambda_bid = lambda_bid;
                    params.lambda_ask = lambda_ask;
                    params.mu_bid = mu_bid;
                    params.mu_ask = mu_ask;
                    params.avg_queue_bid = avg_queue_bid;
                    params.avg_queue_ask = avg_queue_ask;
                } else {
                    // EMA更新
                    params.lambda_bid = alpha * lambda_bid + (1.0 - alpha) * params.lambda_bid;
                    params.lambda_ask = alpha * lambda_ask + (1.0 - alpha) * params.lambda_ask;
                    params.mu_bid = alpha * mu_bid + (1.0 - alpha) * params.mu_bid;
                    params.mu_ask = alpha * mu_ask + (1.0 - alpha) * params.mu_ask;
                    params.avg_queue_bid =
                        alpha * avg_queue_bid + (1.0 - alpha) * params.avg_queue_bid;
                    params.avg_queue_ask =
                        alpha * avg_queue_ask + (1.0 - alpha) * params.avg_queue_ask;
                }

                params.last_update = now;

                log::debug!("泊松参数更新: λ_bid={:.2}, λ_ask={:.2}, μ_bid={:.2}, μ_ask={:.2}, L_bid={:.2}, L_ask={:.2}",
                    params.lambda_bid, params.lambda_ask, params.mu_bid, params.mu_ask,
                    params.avg_queue_bid, params.avg_queue_ask
                );
            }

            // 等待下次更新
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok(())
    }

    /// 计算到达率和成交率
    fn calculate_rates(&self, events: &[OrderFlowEvent]) -> (f64, f64, f64, f64) {
        if events.is_empty() {
            return (0.0, 0.0, 0.0, 0.0);
        }

        let duration = events
            .last()
            .unwrap()
            .timestamp
            .signed_duration_since(events.first().unwrap().timestamp)
            .num_seconds() as f64;

        if duration <= 0.0 {
            return (0.0, 0.0, 0.0, 0.0);
        }

        // 只统计成交事件，不统计订单簿更新
        let mut bid_trades = 0;
        let mut ask_trades = 0;

        for event in events {
            if let OrderEventType::Trade = event.event_type {
                match event.side {
                    OrderSide::Buy => bid_trades += 1,
                    OrderSide::Sell => ask_trades += 1,
                }
            }
        }

        // 使用成交率作为订单流强度的指标
        let bid_rate = bid_trades as f64 / duration;
        let ask_rate = ask_trades as f64 / duration;

        // 简化模型：到达率设为成交率的1.5倍（经验值）
        (
            bid_rate * 1.5, // λ_bid
            ask_rate * 1.5, // λ_ask
            bid_rate,       // μ_bid
            ask_rate,       // μ_ask
        )
    }

    /// 运行做市主循环
    async fn run_market_making(&self) -> Result<()> {
        log::info!("💹 开始做市交易...");

        // 等待数据初始化
        log::info!("⏳ 等待市场数据和参数初始化（10秒）...");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        let mut no_price_warning_count = 0;

        while *self.running.read().await {
            // 检查是否有价格数据
            let current_price = *self.current_price.read().await;
            if current_price <= 0.0 {
                no_price_warning_count += 1;
                if no_price_warning_count % 10 == 1 {
                    // 每10次警告一次
                    log::warn!("⚠️ 等待价格数据... 当前价格: {}", current_price);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            // 显示当前价格
            if no_price_warning_count > 0 {
                log::info!("✅ 收到价格数据: {} USDC", current_price);
                no_price_warning_count = 0;
            }

            // 1. 更新当前持仓状态（每5-10分钟一次）
            let now = Utc::now();
            let last_pos_update = *self.last_position_update.read().await;
            if now.signed_duration_since(last_pos_update).num_seconds() > 600 {
                // 10分钟
                self.update_position_status().await?;
                *self.last_position_update.write().await = now;
            }

            // 2. 计算最优价差
            let (bid_spread, ask_spread) = self.calculate_optimal_spread().await?;

            // 3. 检查是否需要刷新订单（价格变化超过0.1%）
            let should_refresh = self
                .should_refresh_orders(current_price, bid_spread, ask_spread)
                .await;

            if should_refresh {
                // 3a. 取消旧订单
                self.cancel_stale_orders().await?;

                // 4. 下新订单
                self.place_orders(bid_spread, ask_spread).await?;
            }

            // 5. 风险检查
            self.check_risk_limits().await?;

            // 显示状态
            let params = self.poisson_params.read().await;
            let state = self.state.lock().await;
            log::info!(
                "📊 状态 | 价格: {:.5} | λ: {:.2}/{:.2} | 挂单: {}/{} | 库存: {:.1}",
                current_price,
                params.lambda_bid,
                params.lambda_ask,
                state.active_buy_orders.len(),
                state.active_sell_orders.len(),
                state.inventory
            );
            drop(state);

            // 等待下次刷新
            tokio::time::sleep(tokio::time::Duration::from_secs(
                self.config.trading.refresh_interval_secs,
            ))
            .await;
        }

        Ok(())
    }

    /// 检查是否需要刷新订单
    async fn should_refresh_orders(
        &self,
        current_price: f64,
        bid_spread: f64,
        ask_spread: f64,
    ) -> bool {
        let (best_bid, best_ask) = {
            let orderbook = self.orderbook.read().await;
            if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
                return true;
            }
            (orderbook.bids[0].0, orderbook.asks[0].0)
        };

        let is_dual_mode = self.is_dual_position_mode().await;

        let (inventory, long_inventory, short_inventory, buy_orders, sell_orders, slot_infos) = {
            let state = self.state.lock().await;
            (
                state.inventory,
                state.long_inventory,
                state.short_inventory,
                state.active_buy_orders.clone(),
                state.active_sell_orders.clone(),
                state.order_slots.clone(),
            )
        };

        let tick_size = self.tick_size();
        let inventory_cap = self.config.trading.max_inventory * 0.9;
        let (can_buy, can_sell) = if is_dual_mode {
            (
                long_inventory < inventory_cap,
                short_inventory < inventory_cap,
            )
        } else {
            (inventory < inventory_cap, inventory > -inventory_cap)
        };

        let order_quantity =
            self.round_quantity(self.config.trading.order_size_usdc / current_price);
        if order_quantity <= 0.0 {
            return true;
        }

        let reduce_threshold = (order_quantity * 0.5).max(0.0001);
        let need_close_short = short_inventory > reduce_threshold;
        let need_close_long = long_inventory > reduce_threshold;
        let need_open_long = can_buy;
        let need_open_short = can_sell;

        let open_long_price =
            self.round_price_for_side(best_bid * (1.0 - bid_spread), OrderSide::Buy);
        let open_short_price =
            self.round_price_for_side(best_ask * (1.0 + ask_spread), OrderSide::Sell);

        let close_short_base = (best_bid - tick_size).max(tick_size);
        let close_short_price = self
            .round_price_for_side(close_short_base, OrderSide::Buy)
            .max(tick_size);

        let close_long_base = best_ask + tick_size;
        let close_long_price = self.round_price_for_side(close_long_base, OrderSide::Sell);

        let mut required_map: HashMap<OrderIntent, (bool, f64)> = HashMap::new();
        required_map.insert(OrderIntent::OpenLong, (need_open_long, open_long_price));
        required_map.insert(OrderIntent::OpenShort, (need_open_short, open_short_price));
        required_map.insert(
            OrderIntent::CloseShort,
            (need_close_short, close_short_price),
        );
        required_map.insert(OrderIntent::CloseLong, (need_close_long, close_long_price));

        for (intent, (required, _)) in &required_map {
            if *required && !slot_infos.contains_key(intent) {
                log::debug!("槽位 {:?} 缺失，需要刷新", intent);
                return true;
            }
        }

        for (intent, _info) in slot_infos.iter() {
            if !required_map
                .get(intent)
                .map(|(required, _)| *required)
                .unwrap_or(false)
            {
                log::debug!("槽位 {:?} 已不需要，准备撤单", intent);
                return true;
            }
        }

        let now = Utc::now();
        for (intent, (required, target_price)) in &required_map {
            if !*required {
                continue;
            }
            let info = match slot_infos.get(intent) {
                Some(info) => info,
                None => continue,
            };

            let maybe_order = match intent.side() {
                OrderSide::Buy => buy_orders.get(&info.exchange_id),
                OrderSide::Sell => sell_orders.get(&info.exchange_id),
            };

            let order = match maybe_order {
                Some(order) => order,
                None => {
                    log::debug!("槽位 {:?} 未找到本地挂单", intent);
                    return true;
                }
            };

            if now.signed_duration_since(order.timestamp).num_seconds() > 30 {
                log::debug!("槽位 {:?} 挂单超过30秒未更新", intent);
                return true;
            }

            if let Some(price) = order.price {
                if (price - target_price).abs() >= tick_size * 0.5 {
                    log::debug!(
                        "槽位 {:?} 价格偏离目标 (当前 {:.5} | 目标 {:.5})",
                        intent,
                        price,
                        target_price
                    );
                    return true;
                }
            } else {
                log::debug!("槽位 {:?} 缺少价格信息", intent);
                return true;
            }

            if (order.amount - order_quantity).abs() > 1e-9 {
                log::debug!(
                    "槽位 {:?} 数量与配置不一致 (当前 {:.5} | 目标 {:.5})",
                    intent,
                    order.amount,
                    order_quantity
                );
                return true;
            }
        }

        let last_bid = *self.last_bid_price.read().await;
        let last_ask = *self.last_ask_price.read().await;
        if last_bid > 0.0 && last_ask > 0.0 {
            let mid_price = (last_bid + last_ask) / 2.0;
            let price_change_pct = ((current_price - mid_price) / mid_price).abs();
            if price_change_pct > 0.001 {
                log::debug!(
                    "价格变化 {:.3}% 超过阈值，刷新挂单",
                    price_change_pct * 100.0
                );
                return true;
            }
        }

        false
    }

    /// 计算最优价差（动态调整版）
    async fn calculate_optimal_spread(&self) -> Result<(f64, f64)> {
        let params = self.poisson_params.read().await;
        let state = self.state.lock().await;

        // 基础价差(基点转换为小数)
        let base_spread = self.config.trading.min_spread_bp / 10000.0;

        // 根据成交频率动态调整价差
        let activity_factor = self.calculate_market_activity_factor(&params).await;
        let dynamic_spread = base_spread * activity_factor;

        // 根据队列长度调整价差
        // 队列越长，说明流动性越差，需要更大的价差
        let queue_adjustment = (params.avg_queue_bid + params.avg_queue_ask) / 20.0;

        // 根据最近成交时间调整
        let time_since_last_trade = if let Some(last_trade) = params.last_trade_time {
            (Utc::now() - last_trade).num_seconds() as f64
        } else {
            300.0 // 默认5分钟
        };

        // 超过30秒未成交，逐步缩小价差以增加成交机会
        let urgency_factor = if time_since_last_trade > 30.0 {
            (0.9_f64).max(1.0 - (time_since_last_trade - 30.0) / 300.0)
        } else {
            1.0
        };

        // 根据库存调整价差（库存偏斜惩罚）
        // max_inventory是USDT价值，计算当前库存的USDT价值比例
        let current_inventory_value = state.inventory * *self.current_price.read().await;
        let inventory_ratio = current_inventory_value / self.config.trading.max_inventory;
        let inventory_penalty = inventory_ratio.abs() * 0.001; // 每10%库存增加1bp

        // 计算买卖价差（结合所有动态因子）
        let mut bid_spread =
            (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;
        let mut ask_spread =
            (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;

        // 库存偏斜调整：持有多头时，降低买价提高卖价
        if inventory_ratio > 0.0 {
            bid_spread *= 1.0 + inventory_ratio * 0.5; // 多头时买价更保守
            ask_spread *= 1.0 - inventory_ratio * 0.3; // 卖价更激进
        } else if inventory_ratio < 0.0 {
            bid_spread *= 1.0 + inventory_ratio.abs() * 0.3; // 空头时买价更激进
            ask_spread *= 1.0 - inventory_ratio.abs() * 0.5; // 卖价更保守
        }

        // 限制在配置范围内
        let max_spread = self.config.trading.max_spread_bp / 10000.0;
        bid_spread = bid_spread.min(max_spread);
        ask_spread = ask_spread.min(max_spread);

        log::debug!(
            "计算价差: bid_spread={:.4}%, ask_spread={:.4}%",
            bid_spread * 100.0,
            ask_spread * 100.0
        );

        Ok((bid_spread, ask_spread))
    }

    /// 下单
    async fn place_orders(&self, bid_spread: f64, ask_spread: f64) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            log::debug!("等待价格数据...");
            return Ok(());
        }

        let (best_bid, best_ask) = {
            let orderbook = self.orderbook.read().await;
            if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
                log::debug!("等待订单簿数据...");
                return Ok(());
            }
            (orderbook.bids[0].0, orderbook.asks[0].0)
        };

        let is_dual_mode = self.is_dual_position_mode().await;
        let (inventory, long_inventory, short_inventory, active_slots) = {
            let state = self.state.lock().await;
            (
                state.inventory,
                state.long_inventory,
                state.short_inventory,
                state.order_slots.len(),
            )
        };

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let tick_size = self.tick_size();
        let inventory_cap = self.config.trading.max_inventory * 0.9;
        let (can_buy, can_sell) = if is_dual_mode {
            (
                long_inventory < inventory_cap,
                short_inventory < inventory_cap,
            )
        } else {
            (inventory < inventory_cap, inventory > -inventory_cap)
        };

        let order_quantity =
            self.round_quantity(self.config.trading.order_size_usdc / current_price);
        if order_quantity <= 0.0 {
            log::warn!("订单数量为0，跳过下单");
            return Ok(());
        }

        let reduce_threshold = (order_quantity * 0.5).max(0.0001);
        let need_close_short = short_inventory > reduce_threshold;
        let need_close_long = long_inventory > reduce_threshold;
        let need_open_long = can_buy;
        let need_open_short = can_sell;

        let open_long_price =
            self.round_price_for_side(best_bid * (1.0 - bid_spread), OrderSide::Buy);
        let open_short_price =
            self.round_price_for_side(best_ask * (1.0 + ask_spread), OrderSide::Sell);

        let close_short_base = (best_bid - tick_size).max(tick_size);
        let close_short_price = self
            .round_price_for_side(close_short_base, OrderSide::Buy)
            .max(tick_size);

        let close_long_base = best_ask + tick_size;
        let close_long_price = self.round_price_for_side(close_long_base, OrderSide::Sell);

        log::debug!(
            "⚖️ 槽位需求 | open_long={} open_short={} close_long={} close_short={} | active_slots={}",
            need_open_long,
            need_open_short,
            need_close_long,
            need_close_short,
            active_slots
        );

        let strategy_prefix = format!(
            "poisson_{}",
            self.config
                .trading
                .symbol
                .split('/')
                .next()
                .unwrap_or("")
                .to_lowercase()
        );

        self.ensure_slot_order(
            &account,
            OrderIntent::OpenLong,
            OrderSide::Buy,
            open_long_price,
            order_quantity,
            true,
            "GTX",
            Some("LONG"),
            false,
            need_open_long,
            tick_size,
            is_dual_mode,
            "BOL",
            "开多",
            &strategy_prefix,
        )
        .await?;

        self.ensure_slot_order(
            &account,
            OrderIntent::CloseShort,
            OrderSide::Buy,
            close_short_price,
            order_quantity,
            true,
            "GTX",
            Some("SHORT"),
            true,
            need_close_short,
            tick_size,
            is_dual_mode,
            "BCS",
            "平空",
            &strategy_prefix,
        )
        .await?;

        self.ensure_slot_order(
            &account,
            OrderIntent::OpenShort,
            OrderSide::Sell,
            open_short_price,
            order_quantity,
            true,
            "GTX",
            Some("SHORT"),
            false,
            need_open_short,
            tick_size,
            is_dual_mode,
            "SOS",
            "开空",
            &strategy_prefix,
        )
        .await?;

        self.ensure_slot_order(
            &account,
            OrderIntent::CloseLong,
            OrderSide::Sell,
            close_long_price,
            order_quantity,
            true,
            "GTX",
            Some("LONG"),
            true,
            need_close_long,
            tick_size,
            is_dual_mode,
            "SCL",
            "平多",
            &strategy_prefix,
        )
        .await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn ensure_slot_order(
        &self,
        account: &Arc<AccountInfo>,
        intent: OrderIntent,
        side: OrderSide,
        target_price: f64,
        order_quantity: f64,
        post_only: bool,
        time_in_force: &str,
        position_side_dual: Option<&str>,
        reduce_only: bool,
        required: bool,
        tick_size: f64,
        is_dual_mode: bool,
        tag: &str,
        log_label: &str,
        strategy_prefix: &str,
    ) -> Result<()> {
        let symbol = self.config.trading.symbol.clone();

        let (existing_info, existing_order) = {
            let state = self.state.lock().await;
            let info = state.order_slots.get(&intent).cloned();
            let order = info
                .as_ref()
                .and_then(|slot| match side {
                    OrderSide::Buy => state.active_buy_orders.get(&slot.exchange_id),
                    OrderSide::Sell => state.active_sell_orders.get(&slot.exchange_id),
                })
                .cloned();
            (info, order)
        };

        if !required {
            if let Some(info) = existing_info {
                log::info!("🗑️ 撤销{}挂单: {}", log_label, info.exchange_id);
                match account
                    .exchange
                    .cancel_order(&info.exchange_id, &symbol, MarketType::Futures)
                    .await
                {
                    Ok(_) => {
                        let mut state = self.state.lock().await;
                        if let Some((_, order)) = state.detach_order_by_exchange(&info.exchange_id)
                        {
                            self.order_cache.invalidate_order(&order.id).await;
                        }
                    }
                    Err(err) => {
                        if Self::is_order_missing_error(&err) {
                            log::info!("ℹ️ {}挂单已在交易所消失: {}", log_label, info.exchange_id);
                            {
                                let mut state = self.state.lock().await;
                                if let Some((_, order)) =
                                    state.detach_order_by_exchange(&info.exchange_id)
                                {
                                    self.order_cache.invalidate_order(&order.id).await;
                                }
                            }
                            let reason = format!("{}挂单缺失", log_label);
                            self.resync_position_from_exchange(&reason).await;
                        } else {
                            log::warn!(
                                "⚠️ 撤销{}挂单失败: {} ({})",
                                log_label,
                                info.exchange_id,
                                err
                            );
                        }
                    }
                }
            }
            return Ok(());
        }

        let mut need_replace = true;
        if let Some(order) = existing_order.as_ref() {
            if let Some(price) = order.price {
                if (price - target_price).abs() < tick_size * 0.5
                    && (order.amount - order_quantity).abs() < 1e-9
                {
                    need_replace = false;
                }
            }
        }

        if !need_replace {
            return Ok(());
        }

        if let Some(info) = existing_info {
            match account
                .exchange
                .cancel_order(&info.exchange_id, &symbol, MarketType::Futures)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.lock().await;
                    if let Some((_, order)) = state.detach_order_by_exchange(&info.exchange_id) {
                        self.order_cache.invalidate_order(&order.id).await;
                    }
                }
                Err(err) => {
                    if Self::is_order_missing_error(&err) {
                        log::info!("ℹ️ {}挂单已在交易所消失: {}", log_label, info.exchange_id);
                        {
                            let mut state = self.state.lock().await;
                            if let Some((_, order)) =
                                state.detach_order_by_exchange(&info.exchange_id)
                            {
                                self.order_cache.invalidate_order(&order.id).await;
                            }
                        }
                        let reason = format!("替换{}挂单", log_label);
                        self.resync_position_from_exchange(&reason).await;
                    } else {
                        log::warn!(
                            "⚠️ 替换{}挂单时取消失败: {} ({})",
                            log_label,
                            info.exchange_id,
                            err
                        );
                    }
                }
            }
        }

        let mut params = HashMap::new();
        params.insert("timeInForce".to_string(), time_in_force.to_string());
        if post_only {
            params.insert("postOnly".to_string(), "true".to_string());
        }

        if is_dual_mode {
            if let Some(ps) = position_side_dual {
                params.insert("positionSide".to_string(), ps.to_string());
            }
        } else if reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }

        let client_order_id =
            crate::utils::generate_order_id_with_tag(strategy_prefix, &account.exchange_name, tag);

        let order_request = OrderRequest {
            symbol: symbol.clone(),
            side,
            order_type: OrderType::Limit,
            amount: order_quantity,
            price: Some(target_price),
            market_type: MarketType::Futures,
            params: Some(params),
            client_order_id: Some(client_order_id.clone()),
            time_in_force: Some(time_in_force.to_string()),
            reduce_only: if !is_dual_mode && reduce_only {
                Some(true)
            } else {
                None
            },
            post_only: Some(post_only),
        };

        match account.exchange.create_order(order_request).await {
            Ok(order) => {
                {
                    let mut state = self.state.lock().await;
                    state.register_order(intent, client_order_id, order.clone());
                }

                match intent {
                    OrderIntent::OpenLong => {
                        *self.last_bid_price.write().await = target_price;
                    }
                    OrderIntent::OpenShort => {
                        *self.last_ask_price.write().await = target_price;
                    }
                    _ => {}
                }

                log::info!(
                    "✅ 下单成功 | {} | id={} | 价格={:.5} | 数量={:.4}",
                    log_label,
                    order.id,
                    target_price,
                    order_quantity
                );
            }
            Err(err) => {
                if Self::is_reduce_only_rejection(&err) {
                    log::warn!("⚠️ {}挂单失败（ReduceOnly被拒绝）: {}", log_label, err);
                    let reason = format!("{}挂单ReduceOnly被拒绝", log_label);
                    self.resync_position_from_exchange(&reason).await;
                } else {
                    log::error!("❌ {}挂单失败: {}", log_label, err);
                }
            }
        }

        Ok(())
    }

    /// 取消过期订单
    async fn cancel_stale_orders(&self) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            return Ok(());
        }

        let (buy_orders, sell_orders) = {
            let state = self.state.lock().await;
            (
                state.active_buy_orders.clone(),
                state.active_sell_orders.clone(),
            )
        };

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let mut buy_to_cancel = Vec::new();
        for (order_id, order) in &buy_orders {
            if let Some(price) = order.price {
                if price < current_price * 0.995 {
                    buy_to_cancel.push(order_id.clone());
                }
            }
        }

        let mut sell_to_cancel = Vec::new();
        for (order_id, order) in &sell_orders {
            if let Some(price) = order.price {
                if price > current_price * 1.005 {
                    sell_to_cancel.push(order_id.clone());
                }
            }
        }

        let mut need_resync = false;

        for order_id in buy_to_cancel {
            log::debug!("取消过期买单: {}", order_id);
            match account
                .exchange
                .cancel_order(&order_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.lock().await;
                    if let Some((_, order)) = state.detach_order_by_exchange(&order_id) {
                        self.order_cache.invalidate_order(&order.id).await;
                    }
                }
                Err(err) => {
                    if Self::is_order_missing_error(&err) {
                        log::debug!("买单已在交易所消失: {}", order_id);
                        {
                            let mut state = self.state.lock().await;
                            if let Some((_, order)) = state.detach_order_by_exchange(&order_id) {
                                self.order_cache.invalidate_order(&order.id).await;
                            }
                        }
                        need_resync = true;
                    } else {
                        log::warn!("取消买单失败 ({}): {}", order_id, err);
                    }
                }
            }
        }

        for order_id in sell_to_cancel {
            log::debug!("取消过期卖单: {}", order_id);
            match account
                .exchange
                .cancel_order(&order_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.lock().await;
                    if let Some((_, order)) = state.detach_order_by_exchange(&order_id) {
                        self.order_cache.invalidate_order(&order.id).await;
                    }
                }
                Err(err) => {
                    if Self::is_order_missing_error(&err) {
                        log::debug!("卖单已在交易所消失: {}", order_id);
                        {
                            let mut state = self.state.lock().await;
                            if let Some((_, order)) = state.detach_order_by_exchange(&order_id) {
                                self.order_cache.invalidate_order(&order.id).await;
                            }
                        }
                        need_resync = true;
                    } else {
                        log::warn!("取消卖单失败 ({}): {}", order_id, err);
                    }
                }
            }
        }

        if need_resync {
            self.resync_position_from_exchange("撤销过期挂单").await;
        }

        Ok(())
    }

    /// 更新持仓状态
    async fn update_position_status(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 获取当前持仓
        let positions = account
            .exchange
            .get_positions(Some(&self.config.trading.symbol))
            .await?;

        let mut long_position = 0.0;
        let mut short_position = 0.0;
        let mut long_avg_price = 0.0;
        let mut short_avg_price = 0.0;
        let mut long_pnl = 0.0;
        let mut short_pnl = 0.0;

        for position in &positions {
            let side = position.side.to_ascii_uppercase();
            if side == "LONG" || position.amount > 0.0 {
                long_position = position.amount.abs();
                long_avg_price = position.entry_price;
                long_pnl = position.unrealized_pnl;
            } else if side == "SHORT" || position.amount < 0.0 {
                short_position = position.amount.abs();
                short_avg_price = position.entry_price;
                short_pnl = position.unrealized_pnl;
            }
        }

        let net_inventory = long_position - short_position;

        {
            let mut state = self.state.lock().await;
            state.long_inventory = long_position;
            state.short_inventory = short_position;
            state.inventory = net_inventory;
            state.long_avg_price = long_avg_price;
            state.short_avg_price = short_avg_price;
            state.avg_price = if net_inventory > 0.0 {
                long_avg_price
            } else if net_inventory < 0.0 {
                short_avg_price
            } else {
                0.0
            };

            if long_pnl != 0.0 {
                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());
                log::debug!(
                    "多头持仓: {:.3} {} @ {:.5}, 未实现盈亏: {:.2} USDC",
                    state.long_inventory,
                    base_asset,
                    state.long_avg_price,
                    long_pnl
                );
            }

            if short_pnl != 0.0 {
                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());
                log::debug!(
                    "空头持仓: {:.3} {} @ {:.5}, 未实现盈亏: {:.2} USDC",
                    state.short_inventory,
                    base_asset,
                    state.short_avg_price,
                    short_pnl
                );
            }
        }

        *self.local_position.write().await = net_inventory;

        let mut state = self.state.lock().await;

        // 更新活跃订单状态（使用缓存）
        let open_orders = self.get_cached_open_orders().await?;

        // 检查并记录成交的订单
        let mut filled_orders = Vec::new();

        // 检查买单成交
        for (order_id, order_info) in state.active_buy_orders.iter() {
            if !open_orders.iter().any(|o| &o.id == order_id) {
                // 订单不在开放订单中，可能已成交
                filled_orders.push((order_id.clone(), OrderSide::Buy, order_info.clone()));
            }
        }

        // 检查卖单成交
        for (order_id, order_info) in state.active_sell_orders.iter() {
            if !open_orders.iter().any(|o| &o.id == order_id) {
                // 订单不在开放订单中，可能已成交
                filled_orders.push((order_id.clone(), OrderSide::Sell, order_info.clone()));
            }
        }

        // 记录成交到数据库
        if !filled_orders.is_empty() && self.collector.is_some() {
            // 获取最近的成交记录
            let trades = account
                .exchange
                .get_my_trades(
                    Some(&self.config.trading.symbol),
                    MarketType::Futures,
                    Some(20),
                )
                .await?;

            for (order_id, side, order_info) in filled_orders {
                // 查找对应的成交记录
                if let Some(trade) = trades.iter().find(|t| t.order_id == Some(order_id.clone())) {
                    // 记录到数据库
                    if let Some(ref collector) = self.collector {
                        let trade_data = TradeData {
                            trade_time: trade.timestamp,
                            strategy_name: self.config.name.clone(),
                            account_id: account.id.clone(),
                            exchange: self.config.account.exchange.clone(),
                            symbol: self.config.trading.symbol.clone(),
                            side: format!("{:?}", side),
                            order_type: Some("Limit".to_string()),
                            price: Decimal::from_str(&trade.price.to_string()).unwrap_or_default(),
                            amount: Decimal::from_str(&trade.amount.to_string())
                                .unwrap_or_default(),
                            value: Some(
                                Decimal::from_str(&(trade.price * trade.amount).to_string())
                                    .unwrap_or_default(),
                            ),
                            fee: trade.fee.as_ref().map(|f| {
                                Decimal::from_str(&f.cost.to_string()).unwrap_or_default()
                            }),
                            fee_currency: trade.fee.as_ref().map(|f| f.currency.clone()),
                            realized_pnl: None, // 做市策略的盈亏需要综合计算
                            pnl_percentage: None,
                            order_id: order_id.clone(),
                            parent_order_id: None,
                            position_side: None,
                            metadata: None,
                        };

                        if let Err(e) = collector.record_trade(trade_data).await {
                            log::error!("记录交易失败: {}", e);
                        } else {
                            state.trade_count += 1;
                            log::info!(
                                "📝 记录成交: {} {:?} @ {:.5} x {} (真实API成交)",
                                self.config.trading.symbol,
                                side,
                                trade.price,
                                trade.amount
                            );
                        }
                    }
                }
            }
        }

        // 清理已成交或取消的订单
        state.active_buy_orders.retain(|id, _| {
            open_orders
                .iter()
                .any(|o| &o.id == id && o.side == OrderSide::Buy)
        });

        state.active_sell_orders.retain(|id, _| {
            open_orders
                .iter()
                .any(|o| &o.id == id && o.side == OrderSide::Sell)
        });

        Ok(())
    }
    fn build_risk_limits(config: &PoissonMMConfig) -> StrategyRiskLimits {
        StrategyRiskLimits {
            warning_scale_factor: Some(0.8),
            danger_scale_factor: Some(0.4),
            stop_loss_pct: Some(config.risk.stop_loss_pct),
            max_inventory_notional: None,
            max_daily_loss: Some(config.risk.max_daily_loss),
            max_consecutive_losses: None,
            inventory_skew_limit: Some(config.risk.inventory_skew_limit),
            max_unrealized_loss: Some(config.risk.max_unrealized_loss),
        }
    }

    fn build_risk_snapshot(&self, state: &MMStrategyState, current_price: f64) -> StrategySnapshot {
        let net_inventory = state.long_inventory - state.short_inventory;
        let gross_inventory = state.long_inventory.max(state.short_inventory);
        let mut snapshot = StrategySnapshot::new(self.config.name.clone());
        snapshot.exposure.notional = current_price * net_inventory;
        snapshot.exposure.net_inventory = net_inventory;
        snapshot.exposure.long_position = state.long_inventory;
        snapshot.exposure.short_position = state.short_inventory;
        if self.config.trading.max_inventory > 0.0 {
            snapshot.exposure.inventory_ratio = Some(
                (current_price * gross_inventory / self.config.trading.max_inventory).min(10.0),
            );
        }

        snapshot.performance.realized_pnl = state.total_pnl;
        snapshot.performance.unrealized_pnl = net_inventory * (current_price - state.avg_price);
        snapshot.performance.daily_pnl = Some(state.daily_pnl);
        snapshot.performance.timestamp = Utc::now();
        snapshot.risk_limits = Some(self.risk_limits.clone());
        snapshot
    }

    async fn apply_risk_decision(&self, decision: RiskDecision) -> Result<()> {
        match decision.action {
            RiskAction::None => Ok(()),
            RiskAction::Notify { level, message } => {
                match level {
                    RiskNotifyLevel::Info => log::info!("ℹ️ 风险提示: {}", message),
                    RiskNotifyLevel::Warning => log::warn!("⚠️ 风险警告: {}", message),
                    RiskNotifyLevel::Danger => log::error!("❗ 风险危险: {}", message),
                }
                Ok(())
            }
            RiskAction::ScaleDown {
                scale_factor,
                reason,
            } => {
                log::warn!(
                    "⚠️ 泊松策略触发缩减: {} (scale={:.2})",
                    reason,
                    scale_factor
                );
                self.cancel_all_orders().await?;
                if scale_factor <= 0.0 {
                    self.close_all_positions().await?;
                }
                Ok(())
            }
            RiskAction::Halt { reason } => {
                log::error!("❌ 泊松策略触发停机: {}", reason);
                self.cancel_all_orders().await?;
                self.close_all_positions().await?;
                Ok(())
            }
        }
    }

    /// 风险检查
    async fn check_risk_limits(&self) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            return Ok(());
        }

        let snapshot = {
            let state = self.state.lock().await;
            if state.avg_price <= 0.0 {
                return Ok(());
            }
            self.build_risk_snapshot(&state, current_price)
        };

        let decision = self.risk_evaluator.evaluate(&snapshot).await;
        self.apply_risk_decision(decision).await?;

        let state = self.state.lock().await;
        let net_inventory = state.long_inventory - state.short_inventory;
        let gross_inventory = state.long_inventory.max(state.short_inventory);

        // 计算未实现盈亏
        let unrealized_pnl = net_inventory * (current_price - state.avg_price);

        // 检查止损
        if unrealized_pnl < -self.config.risk.max_unrealized_loss {
            log::warn!("⚠️ 触发止损，未实现亏损: {:.2} USDC", unrealized_pnl);

            // 发送微信通知
            let symbol = self
                .symbol_info
                .read()
                .await
                .as_ref()
                .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                .unwrap_or_else(|| self.config.trading.symbol.clone());
            let message = format!(
                "🚨 【泊松策略止损】\n\
                 ⚠️ 交易对: {}\n\
                 💸 未实现亏损: {:.2} USDC\n\
                 🎯 止损阈值: -{:.2} USDC\n\
                 📊 当前库存: {:.3}\n\
                 ⏰ 时间: {}",
                symbol,
                unrealized_pnl,
                self.config.risk.max_unrealized_loss,
                net_inventory,
                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
            );
            crate::utils::webhook::notify_critical("PoissonMM", &message).await;

            drop(state);
            self.close_all_positions().await?;
            return Ok(());
        }

        // 检查库存偏斜
        // max_inventory是USDT价值，直接计算当前库存的USDT价值
        let current_inventory_value = gross_inventory * current_price;
        let inventory_ratio = current_inventory_value / self.config.trading.max_inventory;
        if inventory_ratio > self.config.risk.inventory_skew_limit {
            log::error!(
                "❌ 库存偏斜过大: {:.1}%，立即平仓！",
                inventory_ratio * 100.0
            );

            // 发送微信通知
            let symbol = self
                .symbol_info
                .read()
                .await
                .as_ref()
                .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                .unwrap_or_else(|| self.config.trading.symbol.clone());
            let message = format!(
                "⚠️ 【泊松策略减仓】\n\
                 📈 交易对: {}\n\
                 📊 当前库存: {:.3} (价值: {:.2} USDC)\n\
                 ⚖️ 库存偏斜: {:.1}%\n\
                 🎯 偏斜阈值: {:.1}%\n\
                 💵 当前价格: {:.4}\n\
                 🔄 将平仓50%库存\n\
                 ⏰ 时间: {}",
                symbol,
                net_inventory,
                current_inventory_value,
                inventory_ratio * 100.0,
                self.config.risk.inventory_skew_limit * 100.0,
                current_price,
                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
            );
            crate::utils::webhook::notify_error("PoissonMM", &message).await;

            // 立即取消所有挂单
            self.cancel_all_orders().await?;

            // 使用市价单平仓50%库存
            let position_to_close_raw = net_inventory * 0.5;
            // 应用精度处理，确保符合交易所要求
            let position_to_close = self.round_quantity(position_to_close_raw);

            if position_to_close.abs() > 0.001 {
                let account = self
                    .account_manager
                    .get_account(&self.config.account.account_id)
                    .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;
                let exchange = account.exchange.clone();
                let symbol = self.config.trading.symbol.clone();

                if position_to_close > 0.0 {
                    // 多头，需要卖出
                    log::warn!(
                        "📉 市价卖出 {} {} 以降低库存（原始数量: {}）",
                        position_to_close,
                        symbol,
                        position_to_close_raw
                    );
                    let order_req = OrderRequest {
                        symbol: symbol.clone(),
                        side: OrderSide::Sell,
                        order_type: OrderType::Market,
                        amount: position_to_close,
                        price: None,
                        client_order_id: Some(format!("POISSON_RISK_{}", Utc::now().timestamp())),
                        market_type: MarketType::Futures,
                        params: Some(HashMap::from([
                            ("reduceOnly".to_string(), "true".to_string()),
                            ("positionSide".to_string(), "LONG".to_string()),
                        ])),
                        time_in_force: None,
                        reduce_only: Some(true),
                        post_only: None,
                    };
                    match exchange.create_order(order_req).await {
                        Ok(_) => log::info!("✅ 平仓订单已提交"),
                        Err(e) => log::error!("平仓失败: {}", e),
                    }
                } else {
                    // 空头，需要买入
                    log::warn!(
                        "📈 市价买入 {} {} 以降低库存（原始数量: {}）",
                        position_to_close.abs(),
                        symbol,
                        position_to_close_raw.abs()
                    );
                    let order_req = OrderRequest {
                        symbol: symbol.clone(),
                        side: OrderSide::Buy,
                        order_type: OrderType::Market,
                        amount: position_to_close.abs(),
                        price: None,
                        client_order_id: Some(format!("POISSON_RISK_{}", Utc::now().timestamp())),
                        market_type: MarketType::Futures,
                        params: Some(HashMap::from([
                            ("reduceOnly".to_string(), "true".to_string()),
                            ("positionSide".to_string(), "SHORT".to_string()),
                        ])),
                        time_in_force: None,
                        reduce_only: Some(true),
                        post_only: None,
                    };
                    match exchange.create_order(order_req).await {
                        Ok(_) => log::info!("✅ 平仓订单已提交"),
                        Err(e) => log::error!("平仓失败: {}", e),
                    }
                }

                // 暂停策略60秒，期间每10秒更新一次仓位
                log::warn!("⏸️ 暂停策略60秒等待平仓完成");
                for i in 0..6 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

                    // 更新仓位状态
                    if let Err(e) = self.update_position_status().await {
                        log::error!("更新仓位失败: {}", e);
                    } else {
                        // 检查仓位是否已经平掉
                        let updated_state = self.state.lock().await;
                        let updated_inventory = updated_state.inventory.abs();
                        let current_price = *self.current_price.read().await;
                        let inventory_value = updated_inventory * current_price;
                        let inventory_ratio = inventory_value / self.config.trading.max_inventory;

                        let symbol = self
                            .symbol_info
                            .read()
                            .await
                            .as_ref()
                            .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                            .unwrap_or_else(|| "UNKNOWN".to_string());
                        log::info!(
                            "📊 平仓进度 {}/6: 当前库存 {:.3} {}，偏斜 {:.1}%",
                            i + 1,
                            updated_state.inventory,
                            symbol,
                            inventory_ratio * 100.0
                        );

                        // 如果仓位已经恢复正常，提前结束等待
                        if inventory_ratio < self.config.risk.inventory_skew_limit * 0.8 {
                            log::info!("✅ 仓位已恢复正常，继续做市");

                            // 发送恢复通知
                            let recovery_message = format!(
                                "✅ 【泊松策略恢复】\n\
                                 📈 交易对: {}\n\
                                 📊 当前库存: {:.3}\n\
                                 ⚖️ 库存偏斜: {:.1}%\n\
                                 💵 当前价格: {:.4}\n\
                                 ✅ 策略已恢复正常做市\n\
                                 ⏰ 时间: {}",
                                symbol,
                                updated_state.inventory,
                                inventory_ratio * 100.0,
                                current_price,
                                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                            );
                            crate::utils::webhook::notify_error("PoissonMM", &recovery_message)
                                .await;
                            break;
                        }
                    }
                }
            }
        }

        // 检查日亏损
        if state.daily_pnl < -self.config.risk.max_daily_loss {
            log::error!("❌ 达到日最大亏损限制: {:.2} USDC", state.daily_pnl);
            drop(state);
            *self.running.write().await = false;
        }

        Ok(())
    }

    /// 取消所有订单
    async fn cancel_all_orders(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 使用批量取消API
        let cancelled = account
            .exchange
            .cancel_all_orders(Some(&self.config.trading.symbol), MarketType::Futures)
            .await?;

        log::info!("✅ 已取消所有订单，共 {} 个", cancelled.len());

        {
            let mut state = self.state.lock().await;
            let cleared_buys = state.active_buy_orders.len();
            let cleared_sells = state.active_sell_orders.len();
            if cleared_buys > 0 || cleared_sells > 0 {
                log::debug!(
                    "🧹 清空本地订单记录: 买单 {} 个 / 卖单 {} 个",
                    cleared_buys,
                    cleared_sells
                );
            }
            state.active_buy_orders.clear();
            state.active_sell_orders.clear();
        }

        // 同步失效订单缓存，防止旧记录影响识别
        self.order_cache
            .invalidate_open_orders(&self.config.trading.symbol)
            .await;

        Ok(())
    }

    /// 平掉所有持仓
    async fn close_all_positions(&self) -> Result<()> {
        let is_dual_mode = self.is_dual_position_mode().await;
        let (long_qty, short_qty, net_inventory) = {
            let state = self.state.lock().await;
            (state.long_inventory, state.short_inventory, state.inventory)
        };
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        if is_dual_mode {
            let mut closed = false;
            if long_qty > 0.0005 {
                let amount = self.round_quantity(long_qty);
                let close_long = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Sell,
                    order_type: OrderType::Market,
                    amount,
                    price: None,
                    market_type: MarketType::Futures,
                    params: Some(HashMap::from([
                        ("reduceOnly".to_string(), "true".to_string()),
                        ("positionSide".to_string(), "LONG".to_string()),
                    ])),
                    client_order_id: None,
                    time_in_force: None,
                    reduce_only: Some(true),
                    post_only: None,
                };
                account.exchange.create_order(close_long).await?;
                closed = true;
            }
            if short_qty > 0.0005 {
                let amount = self.round_quantity(short_qty);
                let close_short = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Market,
                    amount,
                    price: None,
                    market_type: MarketType::Futures,
                    params: Some(HashMap::from([
                        ("reduceOnly".to_string(), "true".to_string()),
                        ("positionSide".to_string(), "SHORT".to_string()),
                    ])),
                    client_order_id: None,
                    time_in_force: None,
                    reduce_only: Some(true),
                    post_only: None,
                };
                account.exchange.create_order(close_short).await?;
                closed = true;
            }
            if !closed {
                return Ok(());
            }
        } else {
            if net_inventory.abs() < 0.001 {
                return Ok(());
            }

            let side = if net_inventory > 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            let close_order = OrderRequest {
                symbol: self.config.trading.symbol.clone(),
                side,
                order_type: OrderType::Market,
                amount: self.round_quantity(net_inventory.abs()),
                price: None,
                market_type: MarketType::Futures,
                params: Some(HashMap::from([(
                    "reduceOnly".to_string(),
                    "true".to_string(),
                )])),
                client_order_id: None,
                time_in_force: None,
                reduce_only: Some(true),
                post_only: None,
            };

            account.exchange.create_order(close_order).await?;
        }

        log::info!("✅ 已平掉所有持仓");
        Ok(())
    }

    /// 输出统计信息
    async fn print_statistics(&self) {
        let state = self.state.lock().await;
        let params = self.poisson_params.read().await;
        let runtime = Utc::now().signed_duration_since(state.start_time);

        log::info!("========== 泊松做市策略统计 ==========");
        log::info!(
            "运行时间: {}小时{}分钟",
            runtime.num_hours(),
            runtime.num_minutes() % 60
        );
        log::info!("成交次数: {}", state.trade_count);
        log::info!("今日盈亏: {:.2} USDC", state.daily_pnl);
        log::info!("总盈亏: {:.2} USDC", state.total_pnl);
        let base_asset = self
            .symbol_info
            .read()
            .await
            .as_ref()
            .map(|info| info.base_asset.clone())
            .unwrap_or_else(|| "TOKEN".to_string());
        log::info!(
            "库存情况: 净仓 {:.2} {}, 多头 {:.2}, 空头 {:.2}",
            state.inventory,
            base_asset,
            state.long_inventory,
            state.short_inventory
        );
        log::info!("泊松参数:");
        log::info!("  - λ_bid: {:.2} 订单/秒", params.lambda_bid);
        log::info!("  - λ_ask: {:.2} 订单/秒", params.lambda_ask);
        log::info!("  - μ_bid: {:.2} 成交/秒", params.mu_bid);
        log::info!("  - μ_ask: {:.2} 成交/秒", params.mu_ask);
        log::info!("  - 平均队列(买): {:.2}", params.avg_queue_bid);
        log::info!("  - 平均队列(卖): {:.2}", params.avg_queue_ask);
        log::info!("=====================================");
    }

    fn tick_size(&self) -> f64 {
        if let Ok(guard) = self.symbol_info.try_read() {
            if let Some(info) = guard.as_ref() {
                if info.tick_size > 0.0 {
                    return info.tick_size;
                }
            }
        }

        1.0 / 10_f64.powi(self.config.trading.price_precision as i32)
    }

    /// 价格精度处理
    fn price_precision(&self) -> usize {
        if let Ok(guard) = self.symbol_info.try_read() {
            guard
                .as_ref()
                .map(|info| info.price_precision)
                .unwrap_or(self.config.trading.price_precision)
        } else {
            self.config.trading.price_precision
        }
    }

    fn round_price(&self, price: f64) -> f64 {
        let precision = self.price_precision();
        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
    }

    fn round_price_for_side(&self, price: f64, side: OrderSide) -> f64 {
        let precision = self.price_precision();
        let multiplier = 10_f64.powi(precision as i32);
        match side {
            OrderSide::Buy => ((price * multiplier).floor()) / multiplier,
            OrderSide::Sell => ((price * multiplier).ceil()) / multiplier,
        }
    }

    /// 数量精度处理
    fn round_quantity(&self, quantity: f64) -> f64 {
        // 优先使用动态获取的精度，否则使用配置文件中的精度
        let precision = if let Ok(guard) = self.symbol_info.try_read() {
            guard
                .as_ref()
                .map(|info| info.quantity_precision)
                .unwrap_or(self.config.trading.quantity_precision)
        } else {
            self.config.trading.quantity_precision
        };

        let multiplier = 10_f64.powi(precision as i32);
        (quantity * multiplier).round() / multiplier
    }

    /// 获取缓存的开放订单（优先使用缓存，减少API调用）
    async fn get_cached_open_orders(&self) -> Result<Vec<Order>> {
        // 首先尝试从缓存获取
        if let Some(cached_orders) = self
            .order_cache
            .get_open_orders(&self.config.trading.symbol)
            .await
        {
            // 检查缓存是否太旧（超过10分钟才同步）
            let now = Utc::now();
            let last_fetch = *self.last_order_fetch.read().await;

            if now.signed_duration_since(last_fetch).num_seconds() > 600 {
                log::info!("📋 定期同步交易所订单状态（10分钟）");

                // 后台同步，但不阻塞当前操作
                let account_id = self.config.account.account_id.clone();
                let symbol = self.config.trading.symbol.clone();
                let cache = self.order_cache.clone();
                let manager = self.account_manager.clone();
                let last_fetch_ref = self.last_order_fetch.clone();

                tokio::spawn(async move {
                    if let Some(account) = manager.get_account(&account_id) {
                        match account
                            .exchange
                            .get_open_orders(Some(&symbol), MarketType::Futures)
                            .await
                        {
                            Ok(orders) => {
                                // 更新缓存
                                cache.set_open_orders(symbol.clone(), orders.clone()).await;

                                // 更新时间戳
                                *last_fetch_ref.write().await = Utc::now();

                                // 检查差异
                                if let Some(old_cached) = cache.get_open_orders(&symbol).await {
                                    if old_cached.len() != orders.len() {
                                        log::debug!(
                                            "订单同步: 缓存 {} -> 实际 {}",
                                            old_cached.len(),
                                            orders.len()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("后台订单同步失败: {}", e);
                            }
                        }
                    }
                });
            }

            return Ok(cached_orders);
        }

        // 缓存为空，必须从API获取
        log::info!("📋 缓存为空，从交易所获取订单");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 从API获取新订单
        let orders = account
            .exchange
            .get_open_orders(Some(&self.config.trading.symbol), MarketType::Futures)
            .await?;

        // 更新缓存
        self.order_cache
            .set_open_orders(self.config.trading.symbol.clone(), orders.clone())
            .await;

        // 更新最后获取时间
        *self.last_order_fetch.write().await = Utc::now();

        // 记录订单统计
        let buy_orders = orders.iter().filter(|o| o.side == OrderSide::Buy).count();
        let sell_orders = orders.iter().filter(|o| o.side == OrderSide::Sell).count();
        log::info!(
            "📊 从交易所获取订单: 买单 {} 个, 卖单 {} 个",
            buy_orders,
            sell_orders
        );

        Ok(orders)
    }

    /// 初始化用户数据流
    async fn init_user_stream(&self) -> Result<()> {
        log::info!("🔌 初始化用户数据流WebSocket...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 调试：检查 exchange 的实际类型
        log::info!(
            "🔍🔍 账户 {} 的交易所类型: {}",
            account.id,
            account.exchange.name()
        );

        // 尝试向下转型到BinanceExchange以使用自动续期功能
        if let Some(binance_exchange) = account
            .exchange
            .as_any()
            .downcast_ref::<crate::exchanges::binance::BinanceExchange>(
        ) {
            log::info!("🔍 使用BinanceExchange的自动续期ListenKey");

            // 使用带自动续期的ListenKey创建
            match binance_exchange
                .create_listen_key_with_auto_renewal(MarketType::Futures)
                .await
            {
                Ok(listen_key) => {
                    log::info!(
                        "✅ 获得ListenKey（已启动自动续期）: {}...",
                        &listen_key[..8.min(listen_key.len())]
                    );
                    self.setup_user_stream_with_key(listen_key).await?;
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("⚠️ 自动续期ListenKey创建失败: {}，回退到普通模式", e);
                    // 回退到普通模式
                }
            }
        }

        // 回退：创建普通用户数据流
        let listen_key = account
            .exchange
            .create_user_data_stream(MarketType::Futures)
            .await?;
        log::info!(
            "✅ 获得ListenKey（手动续期）: {}...",
            &listen_key[..8.min(listen_key.len())]
        );
        self.setup_user_stream_with_key(listen_key).await?;

        log::info!("✅ 用户数据流初始化完成");
        Ok(())
    }

    /// 设置用户数据流WebSocket
    async fn setup_user_stream_with_key(&self, listen_key: String) -> Result<()> {
        // 构建WebSocket URL
        let ws_url = format!("wss://fstream.binance.com/ws/{}", listen_key);

        // 创建WebSocket连接
        let mut user_stream_client =
            BaseWebSocketClient::new(ws_url.clone(), self.config.account.exchange.clone());

        if let Err(e) = user_stream_client.connect().await {
            log::error!("❌ 用户数据流WebSocket连接失败: {}", e);
            return Err(e);
        }

        // 保存客户端引用
        let client_arc = Arc::new(Mutex::new(user_stream_client));
        *self.user_stream_client.write().await = Some(client_arc.clone());

        // 启动消息处理任务（直接在spawn中处理，避免额外的函数调用）
        let processor = self.clone_for_task();
        tokio::spawn(async move {
            log::info!("📨 开始处理用户数据流消息");
            let mut message_count = 0;

            while *processor.running.read().await {
                // 从保存的客户端读取消息
                if let Some(ws_client) = &*processor.user_stream_client.read().await {
                    let mut client_guard = ws_client.lock().await;
                    match client_guard.receive().await {
                        Ok(Some(message)) => {
                            message_count += 1;
                            if message_count % 100 == 1 {
                                log::debug!("📨 已处理 {} 条用户数据流消息", message_count);
                            }

                            if let Err(e) = processor.handle_user_stream_message(&message).await {
                                log::error!("处理用户数据流消息失败: {}", e);
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            log::error!("接收用户数据流消息失败: {}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                } else {
                    log::warn!("⚠️ 用户数据流客户端未初始化，退出处理循环");
                    break;
                }
            }

            log::info!(
                "📨 用户数据流消息处理结束（共处理 {} 条消息）",
                message_count
            );
        });

        log::info!("✅ 用户数据流WebSocket连接成功");
        Ok(())
    }

    /// 处理用户数据流消息
    async fn handle_user_stream_message(&self, message: &str) -> Result<()> {
        let json: serde_json::Value = serde_json::from_str(message)?;

        // 添加调试日志
        if let Some(event_type) = json.get("e").and_then(|v| v.as_str()) {
            log::info!("🔔 收到用户数据流事件: {}", event_type);

            match event_type {
                "ORDER_TRADE_UPDATE" => {
                    log::info!("📡 处理ORDER_TRADE_UPDATE事件");
                    // 订单更新 (期货)
                    self.handle_order_update(&json).await?;
                }
                "ACCOUNT_UPDATE" => {
                    // 账户更新
                    log::info!("📊 收到账户更新事件");
                }
                _ => {
                    log::debug!("收到未处理的事件类型: {}", event_type);
                }
            }
        } else {
            log::debug!(
                "收到非事件消息: {}",
                message.chars().take(100).collect::<String>()
            );
        }

        Ok(())
    }

    /// 处理订单更新
    async fn handle_order_update(&self, json: &serde_json::Value) -> Result<()> {
        let Some(order_obj) = json.get("o") else {
            return Ok(());
        };

        let event_symbol = order_obj
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if event_symbol.is_empty() {
            return Ok(());
        }

        // Binance 期货用户流返回的符号没有斜杠，需规范化对比
        let expected_symbol = self.config.trading.symbol.replace('/', "").to_uppercase();

        if event_symbol.to_uppercase() != expected_symbol {
            return Ok(());
        }

        let exchange_order_id = order_obj.get("i").and_then(|v| match v {
            serde_json::Value::Number(num) => Some(num.to_string()),
            serde_json::Value::String(s) => Some(s.clone()),
            _ => None,
        });

        let client_order_id = order_obj
            .get("c")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let order_ref = exchange_order_id
            .as_ref()
            .or(client_order_id.as_ref())
            .map(|s| s.as_str())
            .unwrap_or("UNKNOWN");

        let status = order_obj.get("X").and_then(|v| v.as_str());

        if let Some(status_str) = status {
            match status_str {
                "FILLED" => {
                    log::info!(
                        "🎯 收到订单成交通知: 交易所ID={:?}, 客户端ID={:?}",
                        exchange_order_id,
                        client_order_id
                    );

                    let mut state = self.state.lock().await;

                    if let Some(side) = order_obj.get("S").and_then(|v| v.as_str()) {
                        if let Some(qty) = order_obj
                            .get("z")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            let price = order_obj
                                .get("ap")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);

                            log::info!(
                                "📦 订单成交详情: {} {} @ {} x {}",
                                self.config.trading.symbol,
                                side,
                                price,
                                qty
                            );

                            let position_mode = order_obj
                                .get("ps")
                                .and_then(|v| v.as_str())
                                .unwrap_or("BOTH");
                            let reduce_only = order_obj
                                .get("R")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);

                            let mut local_pos = self.local_position.write().await;
                            let ps_upper = position_mode.to_ascii_uppercase();
                            match ps_upper.as_str() {
                                "LONG" => {
                                    if side == "BUY" && !reduce_only {
                                        state.long_inventory += qty;
                                    } else {
                                        state.long_inventory =
                                            (state.long_inventory - qty).max(0.0);
                                    }
                                }
                                "SHORT" => {
                                    if side == "SELL" && !reduce_only {
                                        state.short_inventory += qty;
                                    } else {
                                        state.short_inventory =
                                            (state.short_inventory - qty).max(0.0);
                                    }
                                }
                                _ => {
                                    if reduce_only {
                                        if *local_pos > 0.0 {
                                            *local_pos -= qty;
                                        } else {
                                            *local_pos += qty;
                                        }
                                    } else if side == "BUY" {
                                        *local_pos += qty;
                                    } else {
                                        *local_pos -= qty;
                                    }

                                    if *local_pos >= 0.0 {
                                        state.long_inventory = (*local_pos).max(0.0);
                                        state.short_inventory = 0.0;
                                    } else {
                                        state.long_inventory = 0.0;
                                        state.short_inventory = (*local_pos).abs();
                                    }
                                }
                            }

                            state.inventory = state.long_inventory - state.short_inventory;
                            *local_pos = state.inventory;

                            log::debug!(
                                "📦 更新本地持仓: 净={} 多头={} 空头={} [{}] (side: {}, ps: {}, reduce: {})",
                                state.inventory,
                                state.long_inventory,
                                state.short_inventory,
                                self.config.trading.symbol,
                                side,
                                ps_upper,
                                reduce_only
                            );
                        }
                    }

                    let mut is_buy_filled = false;
                    let mut is_sell_filled = false;
                    let mut intents_filled: Vec<OrderIntent> = Vec::new();
                    let mut orders_to_invalidate: Vec<String> = Vec::new();

                    if let Some(ref id) = exchange_order_id {
                        if let Some((intent, order)) = state.detach_order_by_exchange(id) {
                            match intent.side() {
                                OrderSide::Buy => is_buy_filled = true,
                                OrderSide::Sell => is_sell_filled = true,
                            }
                            orders_to_invalidate.push(order.id.clone());
                            intents_filled.push(intent);
                        }
                    }

                    if let Some(ref id) = client_order_id {
                        if let Some((intent, order)) = state.detach_order_by_client(id) {
                            match intent.side() {
                                OrderSide::Buy => is_buy_filled = true,
                                OrderSide::Sell => is_sell_filled = true,
                            }
                            orders_to_invalidate.push(order.id.clone());
                            intents_filled.push(intent);
                        }
                    }

                    state.trade_count += 1;

                    log::info!(
                        "📦 泊松策略订单 {} 已成交 (买单成交={}, 卖单成交={})",
                        order_ref,
                        is_buy_filled,
                        is_sell_filled
                    );

                    drop(state);

                    for order_id in orders_to_invalidate {
                        self.order_cache.invalidate_order(&order_id).await;
                    }

                    if !intents_filled.is_empty() {
                        self.update_poisson_params_on_fill().await;

                        if let Err(e) = self.handle_filled_intents(intents_filled).await {
                            log::error!("成交后刷新挂单失败: {}", e);
                        } else {
                            log::info!("🔁 成交后已按配对刷新挂单");
                        }
                    }
                }
                "CANCELED" | "EXPIRED" | "REJECTED" => {
                    let mut state = self.state.lock().await;
                    if let Some(ref id) = exchange_order_id {
                        state.active_buy_orders.remove(id);
                        state.active_sell_orders.remove(id);
                    }
                    if let Some(ref id) = client_order_id {
                        state.active_buy_orders.remove(id);
                        state.active_sell_orders.remove(id);
                    }
                    log::debug!(
                        "泊松策略订单 {:?}/{:?} 状态: {}",
                        exchange_order_id,
                        client_order_id,
                        status_str
                    );
                }
                _ => {}
            }

            if let Some(ref id) = exchange_order_id {
                self.order_cache.invalidate_order(id).await;
            }
            if let Some(ref id) = client_order_id {
                self.order_cache.invalidate_order(id).await;
            }
        }

        Ok(())
    }

    /// 成交后动态更新泊松参数
    async fn update_poisson_params_on_fill(&self) {
        let mut params = self.poisson_params.write().await;

        // 记录成交时间
        let now = Utc::now();
        params.last_trade_time = Some(now);

        // 更新到达率（基于最近成交频率）
        let time_diff = (now - params.last_update).num_seconds() as f64;
        if time_diff > 0.0 {
            // 使用指数移动平均更新lambda
            let instant_lambda = 1.0 / time_diff * 60.0; // 转换为每分钟到达率
            params.lambda_bid = params.lambda_bid * 0.9 + instant_lambda * 0.1;
            params.lambda_ask = params.lambda_ask * 0.9 + instant_lambda * 0.1;

            log::debug!(
                "📡 更新泊松参数: λ_bid={:.2}, λ_ask={:.2}",
                params.lambda_bid,
                params.lambda_ask
            );
        }
        params.last_update = now;
    }

    /// 计算市场活跃度因子
    async fn calculate_market_activity_factor(&self, params: &PoissonParameters) -> f64 {
        // 基于lambda值评估市场活跃度
        let avg_lambda = (params.lambda_bid + params.lambda_ask) / 2.0;

        // lambda越高，市场越活跃，价差可以更小
        if avg_lambda > 10.0 {
            0.8 // 高活跃度，缩小价差20%
        } else if avg_lambda > 5.0 {
            0.9 // 中等活跃度，缩小价差10%
        } else if avg_lambda < 1.0 {
            1.2 // 低活跃度，增大价差20%
        } else {
            1.0 // 正常价差
        }
    }

    /// 保持用户数据流活跃
    async fn keep_user_stream_alive(&self, listen_key: String) {
        log::info!("💓 启动用户数据流保活任务");

        while *self.running.read().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(1800)).await; // 每30分钟

            let account = match self
                .account_manager
                .get_account(&self.config.account.account_id)
            {
                Some(acc) => acc,
                None => continue,
            };

            let result = account
                .exchange
                .keepalive_user_data_stream(&listen_key, MarketType::Futures)
                .await;

            match result {
                Ok(_) => log::debug!("✅ 用户数据流保活成功"),
                Err(e) => log::error!("❌ 用户数据流保活失败: {}", e),
            }
        }

        log::info!("💔 用户数据流保活任务结束");
    }

    /// 克隆用于任务
    fn clone_for_task(&self) -> Self {
        Self {
            config: self.config.clone(),
            account_manager: self.account_manager.clone(),
            state: self.state.clone(),
            order_flow_buffer: self.order_flow_buffer.clone(),
            poisson_params: self.poisson_params.clone(),
            ws_client: self.ws_client.clone(),
            running: self.running.clone(),
            current_price: self.current_price.clone(),
            orderbook: self.orderbook.clone(),
            collector: self.collector.clone(),
            symbol_info: self.symbol_info.clone(),
            is_dual_mode: self.is_dual_mode.clone(),
            order_cache: self.order_cache.clone(),
            user_stream_client: self.user_stream_client.clone(),
            last_order_fetch: self.last_order_fetch.clone(),
            last_bid_price: self.last_bid_price.clone(),
            last_ask_price: self.last_ask_price.clone(),
            last_position_update: self.last_position_update.clone(),
            local_position: self.local_position.clone(),
            risk_evaluator: self.risk_evaluator.clone(),
            risk_limits: self.risk_limits.clone(),
        }
    }

    async fn resync_position_from_exchange(&self, reason: &str) {
        match self.update_position_status().await {
            Ok(_) => {
                *self.last_position_update.write().await = Utc::now();
                log::debug!("🔄 已同步持仓状态 ({})", reason);
            }
            Err(e) => {
                log::warn!("⚠️ 持仓同步失败 ({}): {}", reason, e);
            }
        }
    }

    async fn remove_local_order(&self, exchange_id: &str) {
        let detached = {
            let mut state = self.state.lock().await;
            state.detach_order_by_exchange(exchange_id)
        };

        if let Some((_, order)) = detached {
            self.order_cache.invalidate_order(&order.id).await;
        }
    }

    async fn cancel_slot_order(&self, intent: OrderIntent, reason: &str) {
        let slot_info = {
            let state = self.state.lock().await;
            state.order_slots.get(&intent).cloned()
        };

        let info = match slot_info {
            Some(info) => info,
            None => return,
        };

        let account = match self
            .account_manager
            .get_account(&self.config.account.account_id)
        {
            Some(acc) => acc,
            None => return,
        };

        let symbol = self.config.trading.symbol.clone();
        let log_label = Self::intent_log_label(&intent);

        log::info!(
            "🗑️ 撤销{}挂单: {} ({})",
            log_label,
            info.exchange_id,
            reason
        );

        match account
            .exchange
            .cancel_order(&info.exchange_id, &symbol, MarketType::Futures)
            .await
        {
            Ok(_) => {
                self.remove_local_order(&info.exchange_id).await;
            }
            Err(err) => {
                if Self::is_order_missing_error(&err) {
                    log::info!("ℹ️ {}挂单已在交易所消失: {}", log_label, info.exchange_id);
                    self.remove_local_order(&info.exchange_id).await;
                    let reason = format!("{}挂单缺失", log_label);
                    self.resync_position_from_exchange(&reason).await;
                } else {
                    log::warn!(
                        "⚠️ 撤销{}挂单失败: {} ({})",
                        log_label,
                        info.exchange_id,
                        err
                    );
                }
            }
        }
    }

    async fn handle_filled_intents(&self, intents: Vec<OrderIntent>) -> Result<()> {
        if intents.is_empty() {
            return Ok(());
        }

        let mut intents_to_cancel: HashSet<OrderIntent> = HashSet::new();
        for intent in &intents {
            intents_to_cancel.insert(intent.counterpart());
        }

        for intent in intents_to_cancel {
            self.cancel_slot_order(intent, "成交触发刷新").await;
        }

        let (bid_spread, ask_spread) = self.calculate_optimal_spread().await?;
        self.place_orders(bid_spread, ask_spread).await?;

        Ok(())
    }

    fn is_order_missing_error(err: &ExchangeError) -> bool {
        match err {
            ExchangeError::OrderNotFound { .. } => true,
            ExchangeError::ApiError { message, .. } => Self::matches_unknown_order_message(message),
            ExchangeError::OrderError(message) => Self::matches_unknown_order_message(message),
            ExchangeError::Other(message) => Self::matches_unknown_order_message(message),
            _ => false,
        }
    }

    fn is_reduce_only_rejection(err: &ExchangeError) -> bool {
        match err {
            ExchangeError::ApiError { code, message } => {
                *code == -2022 || *code == -5022 || Self::matches_reduce_only_message(message)
            }
            ExchangeError::OrderError(message) => Self::matches_reduce_only_message(message),
            ExchangeError::Other(message) => Self::matches_reduce_only_message(message),
            _ => false,
        }
    }

    fn matches_unknown_order_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        Self::unknown_order_patterns()
            .iter()
            .any(|pattern| lower.contains(pattern))
    }

    fn unknown_order_patterns() -> &'static [&'static str] {
        &[
            "unknown order",
            "order not exist",
            "order was not found",
            "order does not exist",
            "order not found",
            "-2011",
        ]
    }

    fn intent_log_label(intent: &OrderIntent) -> &'static str {
        match intent {
            OrderIntent::OpenLong => "开多",
            OrderIntent::CloseLong => "平多",
            OrderIntent::OpenShort => "开空",
            OrderIntent::CloseShort => "平空",
        }
    }

    fn intent_tag(intent: &OrderIntent) -> &'static str {
        match intent {
            OrderIntent::OpenLong => "BOL",
            OrderIntent::CloseLong => "SCL",
            OrderIntent::OpenShort => "SOS",
            OrderIntent::CloseShort => "BCS",
        }
    }

    fn intent_position_side(intent: &OrderIntent) -> Option<&'static str> {
        match intent {
            OrderIntent::OpenLong | OrderIntent::CloseLong => Some("LONG"),
            OrderIntent::OpenShort | OrderIntent::CloseShort => Some("SHORT"),
        }
    }

    fn matches_reduce_only_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        Self::reduce_only_patterns()
            .iter()
            .any(|pattern| lower.contains(pattern))
    }

    fn reduce_only_patterns() -> &'static [&'static str] {
        &[
            "reduceonly",
            "reduce only",
            "-2022",
            "post only order will be rejected",
        ]
    }
}

// 类型别名
type Result<T> = std::result::Result<T, ExchangeError>;
