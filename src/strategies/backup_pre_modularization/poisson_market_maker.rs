//! # 泊松队列做市策略 (Poisson Queue Market Making Strategy)
//!
//! 基于泊松分布模型的智能做市策略，动态调整买卖价差。
//!
//! ## 主要功能
//! - 使用泊松队列模型分析订单流
//! - 动态计算最优买卖价差
//! - 根据市场深度调整报价
//! - 自动库存风险管理

use chrono::{DateTime, Duration, Utc};
use crossbeam::queue::ArrayQueue;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
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
use crate::cta::account_manager::AccountManager;
use rust_decimal::Decimal;
use std::str::FromStr;

/// 泊松队列做市策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonMMConfig {
    /// 策略名称
    pub name: String,
    /// 是否启用
    pub enabled: bool,
    /// 版本
    pub version: String,

    /// 账户配置
    pub account: PoissonAccountConfig,

    /// 交易配置
    pub trading: PoissonTradingConfig,

    /// 泊松模型参数
    pub poisson: PoissonModelConfig,

    /// 风险管理
    pub risk: PoissonRiskConfig,
}

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonAccountConfig {
    pub account_id: String,
    pub exchange: String,
}

/// 交易配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonTradingConfig {
    /// 交易对
    pub symbol: String,
    /// 每单金额(USDC)
    pub order_size_usdc: f64,
    /// 最大库存(基础货币数量，如DOGE、LINK的数量)
    pub max_inventory: f64,
    /// 最小价差(基点bp)
    pub min_spread_bp: f64,
    /// 最大价差(基点bp)
    pub max_spread_bp: f64,
    /// 订单刷新间隔(秒)
    pub refresh_interval_secs: u64,
    /// 价格精度
    pub price_precision: usize,
    /// 数量精度
    pub quantity_precision: usize,
}

/// 泊松模型配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonModelConfig {
    /// 观察窗口(秒)
    pub observation_window_secs: u64,
    /// 最小样本数
    pub min_samples: usize,
    /// 平滑系数(EMA)
    pub smoothing_alpha: f64,
    /// 队列深度档位
    pub depth_levels: usize,
    /// 置信区间
    pub confidence_interval: f64,
    /// 初始lambda值（当没有足够样本时使用）
    pub initial_lambda: f64,
}

/// 风险配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonRiskConfig {
    /// 最大未实现亏损
    pub max_unrealized_loss: f64,
    /// 最大日亏损
    pub max_daily_loss: f64,
    /// 库存偏斜限制(0.5表示50%)
    pub inventory_skew_limit: f64,
    /// 止损价格偏离
    pub stop_loss_pct: f64,
}

/// 订单流事件
#[derive(Debug, Clone)]
struct OrderFlowEvent {
    timestamp: DateTime<Utc>,
    side: OrderSide,
    price: f64,
    quantity: f64,
    event_type: OrderEventType,
}

#[derive(Debug, Clone)]
enum OrderEventType {
    NewOrder, // 新订单进入订单簿
    Trade,    // 成交
    Cancel,   // 取消
}

/// 泊松参数
#[derive(Debug, Clone)]
struct PoissonParameters {
    /// 买单到达率(λ_bid)
    lambda_bid: f64,
    /// 卖单到达率(λ_ask)
    lambda_ask: f64,
    /// 买单成交率(μ_bid)
    mu_bid: f64,
    /// 卖单成交率(μ_ask)
    mu_ask: f64,
    /// 平均队列长度(买)
    avg_queue_bid: f64,
    /// 平均队列长度(卖)
    avg_queue_ask: f64,
    /// 更新时间
    last_update: DateTime<Utc>,
    /// 最后成交时间
    last_trade_time: Option<DateTime<Utc>>,
}

/// 策略状态
/// 交易对信息
#[derive(Debug, Clone)]
struct SymbolInfo {
    /// 基础货币名称（如 ENA, ARB, SUI）
    base_asset: String,
    /// 报价货币名称（如 USDT, USDC）
    quote_asset: String,
    /// 最小价格变动
    tick_size: f64,
    /// 最小数量变动
    step_size: f64,
    /// 最小名义价值
    min_notional: f64,
    /// 价格精度(小数位)
    price_precision: usize,
    /// 数量精度(小数位)
    quantity_precision: usize,
}

#[derive(Debug, Clone)]
struct MMStrategyState {
    /// 当前库存(基础货币数量，正为多头，负为空头)
    inventory: f64,
    /// 当前持仓均价
    avg_price: f64,
    /// 活跃买单
    active_buy_orders: HashMap<String, Order>,
    /// 活跃卖单
    active_sell_orders: HashMap<String, Order>,
    /// 累计盈亏
    total_pnl: f64,
    /// 今日盈亏
    daily_pnl: f64,
    /// 成交次数
    trade_count: u64,
    /// 启动时间
    start_time: DateTime<Utc>,
}

/// 泊松队列做市策略
pub struct PoissonMarketMaker {
    /// 配置
    config: PoissonMMConfig,
    /// 账户管理器
    account_manager: Arc<AccountManager>,
    /// 策略状态
    state: Arc<Mutex<MMStrategyState>>,
    /// 订单流缓冲
    order_flow_buffer: Arc<RwLock<VecDeque<OrderFlowEvent>>>,
    /// 泊松参数
    poisson_params: Arc<RwLock<PoissonParameters>>,
    /// WebSocket客户端
    ws_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    /// 运行标志
    running: Arc<RwLock<bool>>,
    /// 当前价格
    current_price: Arc<RwLock<f64>>,
    /// 订单簿快照
    orderbook: Arc<RwLock<LocalOrderBook>>,
    /// 数据收集器
    collector: Option<Arc<TradeCollector>>,
    /// 交易对信息
    symbol_info: Arc<RwLock<Option<SymbolInfo>>>,
    /// 是否双向持仓模式
    is_dual_mode: Arc<RwLock<bool>>,
    /// 订单缓存
    order_cache: Arc<OrderCache>,
    /// 用户数据流客户端
    user_stream_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    /// 上次订单获取时间
    last_order_fetch: Arc<RwLock<DateTime<Utc>>>,
    /// 上次下单价格（用于智能刷新）
    last_bid_price: Arc<RwLock<f64>>,
    /// 上次下单价格（用于智能刷新）
    last_ask_price: Arc<RwLock<f64>>,
    /// 上次持仓更新时间
    last_position_update: Arc<RwLock<DateTime<Utc>>>,
    /// 本地持仓跟踪
    local_position: Arc<RwLock<f64>>,
}

/// 内部订单簿缓存
#[derive(Debug, Clone)]
struct LocalOrderBook {
    bids: Vec<(f64, f64)>, // (price, quantity)
    asks: Vec<(f64, f64)>,
    last_update: DateTime<Utc>,
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
            avg_price: 0.0,
            active_buy_orders: HashMap::new(),
            active_sell_orders: HashMap::new(),
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
            let should_refresh = self.should_refresh_orders(current_price).await;

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
    async fn should_refresh_orders(&self, current_price: f64) -> bool {
        let state = self.state.lock().await;

        // 1. 没有订单时需要挂单
        if state.active_buy_orders.is_empty() && state.active_sell_orders.is_empty() {
            return true;
        }

        // 2. 检查30秒超时
        let now = Utc::now();
        for order in state.active_buy_orders.values() {
            if now.signed_duration_since(order.timestamp).num_seconds() > 30 {
                log::debug!("买单超过30秒未成交，需要刷新");
                return true;
            }
        }

        for order in state.active_sell_orders.values() {
            if now.signed_duration_since(order.timestamp).num_seconds() > 30 {
                log::debug!("卖单超过30秒未成交，需要刷新");
                return true;
            }
        }

        // 3. 检查价格变化
        let last_bid = *self.last_bid_price.read().await;
        let last_ask = *self.last_ask_price.read().await;

        if last_bid > 0.0 && last_ask > 0.0 {
            let mid_price = (last_bid + last_ask) / 2.0;
            let price_change_pct = ((current_price - mid_price) / mid_price).abs();

            if price_change_pct > 0.001 {
                // 0.1%
                log::debug!("价格变化 {:.3}%，需要刷新订单", price_change_pct * 100.0);
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

        // 获取盘口最优价格
        let (best_bid, best_ask) = {
            let orderbook = self.orderbook.read().await;
            if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
                log::debug!("等待订单簿数据...");
                return Ok(());
            }
            (orderbook.bids[0].0, orderbook.asks[0].0)
        };

        let state = self.state.lock().await;

        // 检查库存限制
        // 永续合约可以双向开仓，不需要库存就能开空
        let can_buy = state.inventory < self.config.trading.max_inventory * 0.9; // 多头仓位限制
        let can_sell = state.inventory > -self.config.trading.max_inventory * 0.9; // 空头仓位限制（负库存）

        // 计算订单数量（固定6 USDT）
        let order_quantity = self.config.trading.order_size_usdc / current_price;
        let order_quantity = self.round_quantity(order_quantity);

        log::debug!(
            "准备下单 - 价格: {:.5}, 数量: {}, 买价差: {:.2}%, 卖价差: {:.2}%",
            current_price,
            order_quantity,
            bid_spread * 100.0,
            ask_spread * 100.0
        );
        log::debug!(
            "订单状态 - 买单: {}, 卖单: {}, 库存: {:.1}, can_buy: {}, can_sell: {}",
            state.active_buy_orders.len(),
            state.active_sell_orders.len(),
            state.inventory,
            can_buy,
            can_sell
        );

        // 释放state锁，避免死锁
        drop(state);

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 检查订单平衡
        let (buy_count, sell_count) = {
            let state = self.state.lock().await;
            (
                state.active_buy_orders.len(),
                state.active_sell_orders.len(),
            )
        };

        // 维持买卖平衡，每边最多1个订单
        // 修正：只有在订单平衡的情况下才能下新单
        let orders_balanced = buy_count == sell_count && buy_count <= 1;
        // 正确的订单需求判断：总是保持买卖单各一个
        let need_buy_order = buy_count == 0; // 没有买单时需要挂
        let need_sell_order = sell_count == 0; // 没有卖单时需要挂

        // 处理订单不平衡情况
        // 正确逻辑：如果有超过一个订单，先取消多余的
        if buy_count > 1 || sell_count > 1 {
            log::warn!(
                "⚠️ 订单过多: 买单={}, 卖单={}，取消多余订单",
                buy_count,
                sell_count
            );
            if let Err(e) = self.cancel_all_orders().await {
                log::error!("取消订单失败: {}", e);
            }
            return Ok(());
        }

        // 记录挂单状态
        if buy_count == 1 && sell_count == 1 {
            log::debug!("✅ 订单平衡: 买单=1, 卖单=1");
        } else if buy_count == 0 || sell_count == 0 {
            log::debug!("🔄 需要补充订单: 买单={}, 卖单={}", buy_count, sell_count);
        }

        // 下买单
        if can_buy && need_buy_order {
            let state = self.state.lock().await;
            if state.active_buy_orders.is_empty() {
                drop(state); // 释放锁

                // 使用盘口价格：在最佳买价基础上减去价差
                let buy_price = best_bid * (1.0 - bid_spread);
                let buy_price = self.round_price(buy_price);

                // 记录本次下单价格
                *self.last_bid_price.write().await = buy_price;

                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());

                // 使用毫秒级时间戳日志
                let timestamp = chrono::Utc::now();
                log::debug!(
                    "[{}] 📗 准备下买单: {} {} @ {:.5} {} (价差: -{:.2}%)",
                    timestamp.format("%H:%M:%S%.3f"),
                    order_quantity,
                    base_asset,
                    buy_price,
                    self.get_quote_asset().await,
                    bid_spread * 100.0
                );

                // 获取持仓模式参数
                let mut buy_params = HashMap::from([
                    ("postOnly".to_string(), "true".to_string()),
                    ("timeInForce".to_string(), "GTX".to_string()), // GTX = Post-only
                ]);

                // 如果是双向持仓模式，添加positionSide
                if self.is_dual_position_mode().await {
                    buy_params.insert("positionSide".to_string(), "LONG".to_string());
                }

                // 使用标准化的订单ID生成器
                let strategy_name = format!(
                    "poisson_{}",
                    self.config
                        .trading
                        .symbol
                        .split('/')
                        .next()
                        .unwrap_or("")
                        .to_lowercase()
                );
                let order_id = crate::utils::generate_order_id_with_tag(
                    &strategy_name,
                    &account.exchange_name,
                    "B",
                );

                let buy_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(buy_price),
                    market_type: MarketType::Futures,
                    params: Some(buy_params),
                    client_order_id: Some(order_id),
                    time_in_force: Some("GTX".to_string()),
                    reduce_only: None,
                    post_only: Some(true),
                };

                match account.exchange.create_order(buy_order).await {
                    Ok(order) => {
                        log::debug!("✅ 买单成功: ID={}, 状态={:?}", order.id, order.status);

                        let mut state = self.state.lock().await;
                        state.active_buy_orders.insert(order.id.clone(), order);
                    }
                    Err(e) => {
                        log::error!("❌ 买单失败: {}", e);
                    }
                }
            }
        }

        // 下卖单
        if can_sell && need_sell_order {
            let state = self.state.lock().await;
            if state.active_sell_orders.is_empty() {
                drop(state); // 释放锁

                // 使用盘口价格：在最佳卖价基础上加上价差
                let sell_price = best_ask * (1.0 + ask_spread);
                let sell_price = self.round_price(sell_price);

                // 记录本次下单价格
                *self.last_ask_price.write().await = sell_price;

                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());

                log::debug!(
                    "📕 准备下卖单: {} {} @ {:.5} {} (价差: +{:.2}%)",
                    order_quantity,
                    base_asset,
                    sell_price,
                    self.get_quote_asset().await,
                    ask_spread * 100.0
                );

                // 获取持仓模式参数
                let mut sell_params = HashMap::from([
                    ("postOnly".to_string(), "true".to_string()),
                    ("timeInForce".to_string(), "GTX".to_string()), // GTX = Post-only
                ]);

                // 如果是双向持仓模式，添加positionSide
                if self.is_dual_position_mode().await {
                    sell_params.insert("positionSide".to_string(), "SHORT".to_string());
                }

                // 使用标准化的订单ID生成器
                let strategy_name = format!(
                    "poisson_{}",
                    self.config
                        .trading
                        .symbol
                        .split('/')
                        .next()
                        .unwrap_or("")
                        .to_lowercase()
                );
                let order_id = crate::utils::generate_order_id_with_tag(
                    &strategy_name,
                    &account.exchange_name,
                    "S",
                );

                let sell_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Sell,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(sell_price),
                    market_type: MarketType::Futures,
                    params: Some(sell_params),
                    client_order_id: Some(order_id),
                    time_in_force: Some("GTX".to_string()),
                    reduce_only: None,
                    post_only: Some(true),
                };

                match account.exchange.create_order(sell_order).await {
                    Ok(order) => {
                        log::debug!("✅ 卖单成功: ID={}, 状态={:?}", order.id, order.status);

                        let mut state = self.state.lock().await;
                        state.active_sell_orders.insert(order.id.clone(), order);
                    }
                    Err(e) => {
                        log::error!("❌ 卖单失败: {}", e);
                    }
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

        let state = self.state.lock().await;
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 检查买单是否需要取消
        for (order_id, order) in &state.active_buy_orders {
            if let Some(price) = order.price {
                // 如果买单价格偏离当前价格太远，取消
                if price < current_price * 0.995 {
                    log::debug!("取消过期买单: {}", order_id);
                    let _ = account
                        .exchange
                        .cancel_order(order_id, &self.config.trading.symbol, MarketType::Futures)
                        .await;
                }
            }
        }

        // 检查卖单是否需要取消
        for (order_id, order) in &state.active_sell_orders {
            if let Some(price) = order.price {
                // 如果卖单价格偏离当前价格太远，取消
                if price > current_price * 1.005 {
                    log::debug!("取消过期卖单: {}", order_id);
                    let _ = account
                        .exchange
                        .cancel_order(order_id, &self.config.trading.symbol, MarketType::Futures)
                        .await;
                }
            }
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

        let mut state = self.state.lock().await;

        if let Some(position) = positions.first() {
            // 使用amount字段，它包含了正负号
            // 正值表示多头，负值表示空头
            let new_inventory = position.amount;

            // 同步本地持仓
            *self.local_position.write().await = new_inventory;
            state.inventory = new_inventory;
            state.avg_price = position.entry_price;

            // 更新盈亏
            if position.unrealized_pnl != 0.0 {
                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());
                log::debug!(
                    "持仓: {} {} @ {:.5}, 未实现盈亏: {:.2} USDC",
                    state.inventory,
                    base_asset,
                    state.avg_price,
                    position.unrealized_pnl
                );
            }
        }

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

    /// 风险检查
    async fn check_risk_limits(&self) -> Result<()> {
        let state = self.state.lock().await;
        let current_price = *self.current_price.read().await;

        if current_price <= 0.0 || state.avg_price <= 0.0 {
            return Ok(());
        }

        // 计算未实现盈亏
        let unrealized_pnl = state.inventory * (current_price - state.avg_price);

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
                state.inventory,
                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
            );
            crate::utils::webhook::notify_critical("PoissonMM", &message).await;

            drop(state);
            self.close_all_positions().await?;
            return Ok(());
        }

        // 检查库存偏斜
        // max_inventory是USDT价值，直接计算当前库存的USDT价值
        let current_inventory_value = state.inventory.abs() * current_price;
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
                state.inventory,
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
            let position_to_close_raw = state.inventory * 0.5;
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
                        params: None,
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
                        params: None,
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
        let _ = account
            .exchange
            .cancel_all_orders(Some(&self.config.trading.symbol), MarketType::Futures)
            .await?;

        log::info!("✅ 已取消所有订单");
        Ok(())
    }

    /// 平掉所有持仓
    async fn close_all_positions(&self) -> Result<()> {
        let state = self.state.lock().await;
        if state.inventory.abs() < 0.001 {
            return Ok(());
        }

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let side = if state.inventory > 0.0 {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let close_order = OrderRequest {
            symbol: self.config.trading.symbol.clone(),
            side,
            order_type: OrderType::Market,
            amount: self.round_quantity(state.inventory.abs()),
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
        log::info!("最终库存: {:.2} {}", state.inventory, base_asset);
        log::info!("泊松参数:");
        log::info!("  - λ_bid: {:.2} 订单/秒", params.lambda_bid);
        log::info!("  - λ_ask: {:.2} 订单/秒", params.lambda_ask);
        log::info!("  - μ_bid: {:.2} 成交/秒", params.mu_bid);
        log::info!("  - μ_ask: {:.2} 成交/秒", params.mu_ask);
        log::info!("  - 平均队列(买): {:.2}", params.avg_queue_bid);
        log::info!("  - 平均队列(卖): {:.2}", params.avg_queue_ask);
        log::info!("=====================================");
    }

    /// 价格精度处理
    fn round_price(&self, price: f64) -> f64 {
        // 优先使用动态获取的精度，否则使用配置文件中的精度
        let precision = if let Ok(guard) = self.symbol_info.try_read() {
            guard
                .as_ref()
                .map(|info| info.price_precision)
                .unwrap_or(self.config.trading.price_precision)
        } else {
            self.config.trading.price_precision
        };

        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
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
        // 更新订单缓存
        if let Some(order_id) = json
            .get("o")
            .and_then(|o| o.get("c"))
            .and_then(|v| v.as_str())
        {
            let status = json
                .get("o")
                .and_then(|o| o.get("X"))
                .and_then(|v| v.as_str());

            if let Some(status_str) = status {
                match status_str {
                    "FILLED" => {
                        log::info!("🎯 收到订单成交通知: {}", order_id);

                        // 订单成交，从活跃订单中移除
                        let mut state = self.state.lock().await;

                        // 更新订单缓存（减少API调用）
                        // 注意：OrderCache没有remove_order方法，需要重新获取订单列表
                        // 这里暂时不处理，让定期同步来更新缓存

                        // 更新本地持仓（期货合约逻辑）
                        if let Some(side) = json
                            .get("o")
                            .and_then(|o| o.get("S"))
                            .and_then(|v| v.as_str())
                        {
                            if let Some(qty) = json
                                .get("o")
                                .and_then(|o| o.get("z"))
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                // 获取成交价格
                                let price = json
                                    .get("o")
                                    .and_then(|o| o.get("ap"))
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

                                // 获取position side (BOTH/LONG/SHORT) 和 reduceOnly标志
                                let ps = json
                                    .get("o")
                                    .and_then(|o| o.get("ps"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("BOTH");
                                let reduce_only = json
                                    .get("o")
                                    .and_then(|o| o.get("R"))
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);

                                let mut local_pos = self.local_position.write().await;

                                // 单向持仓模式（BOTH）的处理
                                if ps == "BOTH" {
                                    if reduce_only {
                                        // 平仓订单：根据当前持仓方向调整
                                        if *local_pos > 0.0 {
                                            // 平多仓
                                            *local_pos -= qty;
                                            state.inventory -= qty;
                                        } else {
                                            // 平空仓
                                            *local_pos += qty;
                                            state.inventory += qty;
                                        }
                                    } else {
                                        // 开仓订单
                                        if side == "BUY" {
                                            *local_pos += qty; // 开多或平空
                                            state.inventory += qty;
                                        } else {
                                            *local_pos -= qty; // 开空或平多
                                            state.inventory -= qty;
                                        }
                                    }
                                }

                                log::debug!(
                                    "📦 更新本地持仓: {} {} (side: {}, ps: {}, reduce: {})",
                                    *local_pos,
                                    self.config.trading.symbol,
                                    side,
                                    ps,
                                    reduce_only
                                );
                            }
                        }

                        // 记录成交方向
                        let is_buy_filled = state.active_buy_orders.remove(order_id).is_some();
                        let is_sell_filled = state.active_sell_orders.remove(order_id).is_some();
                        state.trade_count += 1;
                        log::info!("📦 泊松策略订单 {} 已成交", order_id);

                        // 释放锁后立即补单
                        drop(state);

                        // 成交即补：立即补充成交方向的订单
                        if is_buy_filled || is_sell_filled {
                            log::info!(
                                "🔄 成交即补：立即补充{}订单",
                                if is_buy_filled { "买" } else { "卖" }
                            );

                            // 动态更新泊松参数
                            self.update_poisson_params_on_fill().await;

                            // 立即执行补单
                            if let Err(e) = self.execute_immediate_replenishment().await {
                                log::error!("补单失败: {}", e);
                            }
                        }
                    }
                    "CANCELED" | "EXPIRED" | "REJECTED" => {
                        // 订单取消/过期/拒绝，从活跃订单中移除
                        let mut state = self.state.lock().await;
                        state.active_buy_orders.remove(order_id);
                        state.active_sell_orders.remove(order_id);
                        log::debug!("泊松策略订单 {} 状态: {}", order_id, status_str);
                    }
                    _ => {}
                }

                // 清除缓存中的该订单
                self.order_cache.invalidate_order(order_id).await;
            }
        }

        Ok(())
    }

    /// 成交后立即补单
    async fn execute_immediate_replenishment(&self) -> Result<()> {
        // 获取当前价差
        let (bid_spread, ask_spread) = self.calculate_optimal_spread().await?;

        // 立即下新订单
        self.place_orders(bid_spread, ask_spread).await?;

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
        }
    }
}

// 类型别名
type Result<T> = std::result::Result<T, ExchangeError>;
