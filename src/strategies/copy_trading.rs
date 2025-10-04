//! # 跟单策略 (Copy Trading Strategy)
//!
//! 自动跟随指定交易员的交易信号，在多个交易所账户上执行相同的交易操作。
//!
//! ## 主要功能
//! - 监听主账户的交易信号
//! - 自动在跟单账户上复制交易
//! - 支持多账户同时跟单
//! - 自动风险控制和仓位管理

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use super::Strategy;
use crate::core::config::Config;
use crate::core::{
    error::ExchangeError,
    exchange::Exchange,
    types::{
        MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Symbol, Trade,
        WsMessage,
    },
    websocket::{ConnectionState, MessageHandler, WebSocketClient},
};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::utils::symbol::SymbolConverter;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// 跟单策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyTradingConfig {
    /// 策略名称
    pub name: String,
    /// 是否启用
    pub enabled: bool,

    /// 信号源配置
    pub signal_source: SignalSourceConfig,

    /// 跟单目标配置
    pub target_exchanges: Vec<TargetExchangeConfig>,

    /// 风险管理
    pub risk_management: RiskManagement,

    /// 执行配置
    pub execution: ExecutionConfig,
}

/// 信号源配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSourceConfig {
    /// 交易所名称 (bitmart)
    pub exchange: String,
    /// 账户ID
    pub account_id: String,
    /// 环境变量前缀
    pub env_prefix: String,
    /// 要跟踪的交易对（空则跟踪所有）
    pub symbols: Vec<String>,
    /// WebSocket订阅类型
    pub subscription_type: String, // "user_trades" or "positions"
}

/// 跟单目标交易所配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetExchangeConfig {
    /// 交易所名称
    pub exchange: String,
    /// 账户ID
    pub account_id: String,
    /// 环境变量前缀
    pub env_prefix: String,
    /// 是否启用
    pub enabled: bool,
    /// 仓位倍数（相对于信号源）
    pub position_multiplier: f64,
}

/// 风险管理配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskManagement {
    /// 每笔交易使用保证金比例（默认2%）
    pub position_size_percent: f64,
    /// 最大持仓数量
    pub max_positions: usize,
    /// 最大单笔损失（USDT）
    pub max_loss_per_trade: f64,
    /// 每日最大损失（USDT）
    pub daily_max_loss: f64,
    /// 是否使用双向持仓
    pub hedge_mode: bool,
}

/// 执行配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// 订单类型（market/limit）
    pub order_type: String,
    /// 滑点容忍度（市价单）
    pub slippage_tolerance: f64,
    /// 是否跳过找不到的交易对
    pub skip_missing_symbols: bool,
    /// 重试次数
    pub max_retries: u32,
    /// 延迟毫秒（避免频繁请求）
    pub delay_ms: u64,
}

/// 跟单策略状态
#[derive(Debug, Clone)]
struct CopyTradingState {
    /// 当前持仓
    positions: HashMap<String, PositionInfo>,
    /// 今日盈亏
    daily_pnl: f64,
    /// 总盈亏
    total_pnl: f64,
    /// 策略启动时间
    start_time: DateTime<Utc>,
    /// 上次更新时间
    last_update: DateTime<Utc>,
    /// 跟单计数
    copy_count: u64,
    /// 失败计数
    failed_count: u64,
}

/// 持仓信息
#[derive(Debug, Clone)]
struct PositionInfo {
    /// 交易对
    symbol: String,
    /// 方向
    side: OrderSide,
    /// 数量
    amount: f64,
    /// 开仓均价
    entry_price: f64,
    /// 开仓时间
    entry_time: DateTime<Utc>,
    /// 未实现盈亏
    unrealized_pnl: f64,
    /// 已实现盈亏
    realized_pnl: f64,
}

/// 跨交易所跟单策略
pub struct CopyTradingStrategy {
    config: CopyTradingConfig,
    account_manager: Arc<AccountManager>,
    state: Arc<Mutex<CopyTradingState>>,
    running: Arc<RwLock<bool>>,
    symbol_converter: Option<SymbolConverter>,
    // 缓存交易对精度信息
    precision_cache: Arc<RwLock<HashMap<String, (u32, u32)>>>, // (price_precision, amount_precision)
}

impl CopyTradingStrategy {
    /// 创建跟单策略
    pub fn new(config: CopyTradingConfig, account_manager: Arc<AccountManager>) -> Self {
        let state = CopyTradingState {
            positions: HashMap::new(),
            daily_pnl: 0.0,
            total_pnl: 0.0,
            start_time: Utc::now(),
            last_update: Utc::now(),
            copy_count: 0,
            failed_count: 0,
        };

        Self {
            config,
            account_manager,
            state: Arc::new(Mutex::new(state)),
            running: Arc::new(RwLock::new(false)),
            symbol_converter: None,
            precision_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 启动策略
    pub async fn start(&self) -> Result<()> {
        *self.running.write().await = true;
        self.start_signal_listener().await
    }

    /// 启动信号源监听
    async fn start_signal_listener(&self) -> Result<()> {
        log::info!("🚀 ========== 启动跟单策略信号监听 ==========");
        let signal_config = &self.config.signal_source;

        log::info!("📋 信号源配置:");
        log::info!("  - 交易所: {}", signal_config.exchange);
        log::info!("  - 账户ID: {}", signal_config.account_id);
        log::info!("  - 跟踪交易对: {:?}", signal_config.symbols);
        log::info!("  - 订阅类型: {}", signal_config.subscription_type);

        // 获取信号源账户
        log::debug!("🔍 获取信号源账户: {}", signal_config.account_id);
        let signal_account = self
            .account_manager
            .get_account(&signal_config.account_id)
            .ok_or_else(|| {
                let err = format!("信号源账户 {} 不存在", signal_config.account_id);
                log::error!("❌ {}", err);
                ExchangeError::Other(err)
            })?;
        log::info!("✅ 成功获取信号源账户");

        // 启动WebSocket连接任务（包含重连逻辑）
        let running = self.running.clone();
        let ws_url = self.get_signal_ws_url(&signal_config.exchange)?;
        let exchange = signal_config.exchange.clone();
        let strategy = self.clone_refs();
        let signal_account_clone = signal_account.clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let max_retries = 10;

            while *running.read().await {
                log::debug!("🔌 WebSocket URL: {}", ws_url);

                let mut ws_client = crate::core::websocket::BaseWebSocketClient::new(
                    ws_url.clone(),
                    exchange.clone(),
                );

                // 连接WebSocket
                log::debug!("🔗 正在连接WebSocket... (第{}次尝试)", retry_count + 1);
                let connect_start = std::time::Instant::now();

                match ws_client.connect().await {
                    Ok(_) => {
                        log::debug!("✅ WebSocket连接成功 (耗时: {:?})", connect_start.elapsed());
                        retry_count = 0; // 重置重试计数

                        // 订阅用户数据流
                        log::debug!("📡 开始订阅用户数据流...");
                        if let Err(e) = strategy
                            .subscribe_user_data(&mut ws_client, signal_account_clone.as_ref())
                            .await
                        {
                            log::error!("❌ 订阅失败: {}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                        log::debug!("✅ 用户数据流订阅成功");

                        // 创建心跳channel
                        let (heartbeat_tx, mut heartbeat_rx) = tokio::sync::mpsc::channel::<()>(10);

                        // 启动心跳定时器任务（只负责定时，不发送）
                        let exchange_clone = exchange.clone();
                        let heartbeat_timer = tokio::spawn(async move {
                            // Bitmart需要更频繁的心跳
                            let heartbeat_interval =
                                if exchange_clone == "bitmart" { 10 } else { 20 };
                            let mut interval = tokio::time::interval(
                                tokio::time::Duration::from_secs(heartbeat_interval),
                            );
                            interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                            log::debug!("💓 心跳定时器启动，间隔: {}秒", heartbeat_interval);

                            loop {
                                interval.tick().await;
                                if heartbeat_tx.send(()).await.is_err() {
                                    log::debug!("心跳channel已关闭");
                                    break;
                                }
                            }
                        });

                        // 创建消息处理器
                        log::debug!("🔧 创建消息处理器");
                        let handler = Box::new(SignalHandler {
                            strategy: strategy.clone_refs(),
                        });

                        // 启动消息接收（使用select!同时处理心跳和消息）
                        log::debug!("📻 开始接收WebSocket消息...");
                        let mut msg_count = 0;
                        loop {
                            tokio::select! {
                                // 处理WebSocket消息
                                result = ws_client.receive() => {
                                    match result {
                                        Ok(Some(msg)) => {
                                            msg_count += 1;

                                            // 只记录重要消息
                                            if msg.contains("error") || msg.contains("Error") {
                                                log::error!("❌ 检测到错误消息: {}", msg);
                                            } else if msg.contains("pong") || msg.contains("System") {
                                                // 心跳响应，不打印
                                                log::trace!("收到心跳响应");
                                                continue;
                                            } else if msg.contains("success") && msg.contains("access") {
                                                // 登录成功
                                                log::debug!("✅ WebSocket认证成功");
                                            } else if msg.contains("success") && msg.contains("subscribe") {
                                                // 订阅成功
                                                log::debug!("✅ 订单频道订阅成功");
                                            } else if msg.contains("order") && !msg.contains("subscribe") {
                                                // 真正的订单消息（不是订阅确认）
                                                log::info!("🎯 收到订单更新: {}",
                                                    if msg.len() > 200 {
                                                        format!("{}...", &msg[..200])
                                                    } else {
                                                        msg.clone()
                                                    }
                                                );
                                            } else if msg.contains("trade") || msg.contains("Trade") {
                                                log::info!("💱 收到成交消息: {}",
                                                    if msg.len() > 200 {
                                                        format!("{}...", &msg[..200])
                                                    } else {
                                                        msg.clone()
                                                    }
                                                );
                                            } else if msg.contains("position") || msg.contains("Position") {
                                                log::info!("📊 收到持仓消息: {}",
                                                    if msg.len() > 200 {
                                                        format!("{}...", &msg[..200])
                                                    } else {
                                                        msg.clone()
                                                    }
                                                );
                                            } else {
                                                // 其他消息只在debug级别记录
                                                log::debug!("收到消息: {}",
                                                    if msg.len() > 100 {
                                                        format!("{}...", &msg[..100])
                                                    } else {
                                                        msg.clone()
                                                    }
                                                );
                                            }

                                            // 将字符串消息转换为WsMessage
                                            let ws_msg = WsMessage::Text(msg);
                                            if let Err(e) = handler.handle_message(ws_msg).await {
                                                log::error!("WebSocket消息处理错误: {}", e);
                                            }
                                        }
                                        Ok(None) => {
                                            // 没有消息，继续
                                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                        }
                                        Err(e) => {
                                            log::error!("❌ WebSocket接收错误: {}", e);
                                            break;
                                        }
                                    }
                                }

                                // 处理心跳
                                Some(_) = heartbeat_rx.recv() => {
                                    log::trace!("发送心跳到 {}", exchange);
                                    if let Err(e) = ws_client.ping().await {
                                        log::warn!("❌ 发送心跳失败: {}", e);
                                        break;
                                    }
                                }
                            }
                        }

                        // 停止心跳定时器
                        heartbeat_timer.abort();
                        log::warn!("⚠️ WebSocket连接断开");
                    }
                    Err(e) => {
                        log::error!("❌ WebSocket连接失败: {}", e);
                        retry_count += 1;

                        if retry_count >= max_retries {
                            log::error!("❌ 达到最大重试次数，停止重连");
                            break;
                        }
                    }
                }

                // 等待后重连
                let wait_time = std::cmp::min(retry_count * 5, 60);
                log::info!("⏱️ {}秒后重新连接...", wait_time);
                tokio::time::sleep(tokio::time::Duration::from_secs(wait_time as u64)).await;
            }

            log::warn!("⚠️ WebSocket监听任务已结束");
        });

        log::info!("✅ 信号监听启动完成");
        Ok(())
    }

    /// 获取信号源WebSocket URL
    fn get_signal_ws_url(&self, exchange: &str) -> Result<String> {
        match exchange {
            "bitmart" => {
                // 使用期货WebSocket URL
                Ok("wss://openapi-ws-v2.bitmart.com/api?protocol=1.1".to_string())
            }
            "binance" => Ok("wss://fstream.binance.com/ws".to_string()),
            _ => Err(Box::new(ExchangeError::Other(format!(
                "不支持的信号源交易所: {}",
                exchange
            )))),
        }
    }

    /// 订阅用户数据流
    async fn subscribe_user_data(
        &self,
        ws_client: &mut impl WebSocketClient,
        account: &AccountInfo,
    ) -> Result<()> {
        // 根据交易所类型构建订阅消息
        match self.config.signal_source.exchange.as_str() {
            "bitmart" => {
                // Bitmart需要先登录
                let timestamp = chrono::Utc::now().timestamp_millis();
                let api_key = std::env::var("BITMART_API_KEY")
                    .map_err(|_| ExchangeError::Other("BITMART_API_KEY not set".to_string()))?;
                // 兼容 BITMART_API_SECRET 和 BITMART_SECRET_KEY
                let secret = std::env::var("BITMART_API_SECRET")
                    .or_else(|_| std::env::var("BITMART_SECRET_KEY"))
                    .map_err(|_| {
                        ExchangeError::Other(
                            "BITMART_API_SECRET or BITMART_SECRET_KEY not set".to_string(),
                        )
                    })?;
                let memo = std::env::var("BITMART_MEMO")
                    .map_err(|_| ExchangeError::Other("BITMART_MEMO not set".to_string()))?;

                // 构建签名字符串 - Bitmart期货使用特殊格式
                let sign_str = format!("{}#{}#bitmart.WebSocket", timestamp, memo);
                let sign = self.calculate_hmac_sha256(&sign_str, &secret)?;

                // 发送登录消息 - Bitmart格式
                let login_msg = serde_json::json!({
                    "action": "access",
                    "args": [
                        api_key.clone(),
                        timestamp.to_string(),
                        sign,
                        "web"
                    ]
                });

                log::debug!("🔐 发送Bitmart登录消息");
                ws_client.send(login_msg.to_string()).await?;

                // 等待登录响应
                tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

                // 订阅私有频道 - 只订阅订单频道
                let subscribe_msg = serde_json::json!({
                    "action": "subscribe",
                    "args": [
                        "futures/order"            // 只订阅期货订单
                    ]
                });
                log::debug!("📡 发送订阅消息");
                ws_client.send(subscribe_msg.to_string()).await?;

                // 等待订阅响应
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
            "binance" => {
                // Binance需要先获取listenKey
                // TODO: 实现获取listenKey
                log::warn!("⚠️ Binance WebSocket订阅尚未实现");
            }
            _ => {
                log::warn!("⚠️ 不支持的交易所: {}", self.config.signal_source.exchange);
            }
        }

        Ok(())
    }

    /// 计算HMAC SHA256签名
    fn calculate_hmac_sha256(&self, message: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|e| ExchangeError::Other(format!("HMAC初始化失败: {}", e)))?;
        mac.update(message.as_bytes());

        let result = mac.finalize();
        let code_bytes = result.into_bytes();

        Ok(hex::encode(code_bytes))
    }

    /// 处理开仓信号
    async fn handle_open_position(&self, signal: &PositionSignal) -> Result<()> {
        let now = chrono::Local::now();
        log::info!(
            "[收到开仓信号] {} {} @ {:.4}",
            now.format("%H:%M:%S"),
            signal.symbol,
            signal.price
        );

        let mut state = self.state.lock().await;

        // 检查是否已经有该持仓
        log::debug!("🔍 检查现有持仓...");
        if let Some(existing_pos) = state.positions.get(&signal.symbol) {
            // 判断是否是同方向的持仓
            let new_side = match signal.side.as_str() {
                "BuyLong" => OrderSide::Buy,
                "SellShort" => OrderSide::Sell,
                _ => OrderSide::Buy,
            };

            if existing_pos.side == new_side {
                log::warn!("⚠️ 已存在同方向 {} 的持仓，跳过此信号", signal.symbol);
                return Ok(());
            } else {
                log::info!("🔄 检测到反向开仓，先平掉旧持仓再开新仓");
                // 移除旧持仓记录
                state.positions.remove(&signal.symbol);
            }
        }

        // 遍历目标交易所执行跟单
        for target_config in &self.config.target_exchanges {
            if !target_config.enabled {
                continue;
            }

            // 执行跟单
            let now = chrono::Local::now();
            match self.copy_position_to_exchange(signal, target_config).await {
                Ok(_) => {
                    log::info!(
                        "[跟单成功] {} {} {} @ {:.4}",
                        now.format("%H:%M:%S"),
                        target_config.account_id,
                        signal.symbol,
                        signal.price
                    );
                    state.copy_count += 1;
                }
                Err(e) => {
                    log::error!(
                        "[跟单失败] {} {} {} @ {:.4} - {}",
                        now.format("%H:%M:%S"),
                        target_config.account_id,
                        signal.symbol,
                        signal.price,
                        e
                    );
                    state.failed_count += 1;
                }
            }
        }

        // 记录持仓
        // 将String类型的side转换为OrderSide
        let order_side = match signal.side.as_str() {
            "BuyLong" | "Buy" => OrderSide::Buy,
            "SellShort" | "Sell" => OrderSide::Sell,
            "SellLong" => OrderSide::Buy,  // 平多仓时原始持仓是买入
            "BuyShort" => OrderSide::Sell, // 平空仓时原始持仓是卖出
            _ => {
                log::warn!("未知的订单方向: {}, 默认使用Buy", signal.side);
                OrderSide::Buy
            }
        };

        state.positions.insert(
            signal.symbol.clone(),
            PositionInfo {
                symbol: signal.symbol.clone(),
                side: order_side,
                amount: signal.amount,
                entry_price: signal.price,
                entry_time: Utc::now(),
                unrealized_pnl: 0.0,
                realized_pnl: 0.0,
            },
        );

        Ok(())
    }

    /// 处理平仓信号
    async fn handle_close_position(&self, signal: &PositionSignal) -> Result<()> {
        let now = chrono::Local::now();
        log::info!(
            "[收到平仓信号] {} {} @ {:.4}",
            now.format("%H:%M:%S"),
            signal.symbol,
            signal.price
        );

        let mut state = self.state.lock().await;

        // 如果是平仓后立即反向开仓（如平多后开空），不需要检查持仓
        // 因为可能刚刚平掉了反向的持仓
        let has_position = state.positions.contains_key(&signal.symbol);
        if !has_position {
            log::info!("⚠️ 没有找到对应持仓，可能是平仓后反向开仓的中间状态");
            // 不返回，继续处理，以便后续可以立即开新仓
        }

        // 遍历目标交易所执行平仓
        for target_config in &self.config.target_exchanges {
            if !target_config.enabled {
                continue;
            }

            // 执行平仓
            if let Err(e) = self
                .close_position_on_exchange(&signal.symbol, target_config)
                .await
            {
                log::error!("❌ {} 平仓失败: {}", target_config.exchange, e);
            }
        }

        // 移除持仓记录
        if let Some(position) = state.positions.remove(&signal.symbol) {
            // 更新盈亏统计
            state.daily_pnl += position.realized_pnl;
            state.total_pnl += position.realized_pnl;
        }

        Ok(())
    }

    /// 复制持仓到目标交易所
    async fn copy_position_to_exchange(
        &self,
        signal: &PositionSignal,
        target_config: &TargetExchangeConfig,
    ) -> Result<()> {
        log::debug!("🎯 开始复制持仓到 {}", target_config.exchange);

        // 获取目标账户
        log::debug!("🔍 获取目标账户: {}", target_config.account_id);
        let target_account = self
            .account_manager
            .get_account(&target_config.account_id)
            .ok_or_else(|| {
                let err = format!("目标账户 {} 不存在", target_config.account_id);
                log::error!("❌ {}", err);
                ExchangeError::Other(err)
            })?;
        log::debug!("✅ 成功获取目标账户");

        // 转换交易对符号
        log::debug!("🔄 转换交易对符号: {}", signal.symbol);
        let target_symbol = self.convert_symbol(
            &signal.symbol,
            &self.config.signal_source.exchange,
            &target_config.exchange,
        )?;
        log::debug!("📝 目标交易对: {} -> {}", signal.symbol, target_symbol);

        // 获取交易对精度
        log::debug!("🔍 获取交易对精度信息...");
        let precision_start = std::time::Instant::now();
        let (price_precision, amount_precision) = self
            .get_symbol_precision(&target_symbol, &target_account)
            .await?;
        log::debug!(
            "📏 精度信息: 价格 {} 位, 数量 {} 位 (耗时: {:?})",
            price_precision,
            amount_precision,
            precision_start.elapsed()
        );

        // 获取账户余额
        log::debug!("💳 获取账户余额...");
        let balance_start = std::time::Instant::now();
        let balance = target_account
            .exchange
            .get_balance(MarketType::Futures)
            .await
            .map_err(|e| {
                log::error!("❌ 获取余额失败: {}", e);
                e
            })?;
        let available_balance = balance
            .iter()
            .find(|b| b.currency == "USDT")
            .map(|b| b.free)
            .unwrap_or(0.0);
        log::debug!(
            "💰 可用余额: {} USDT (耗时: {:?})",
            available_balance,
            balance_start.elapsed()
        );

        // 计算下单金额（账户保证金的2%）
        let position_value =
            available_balance * self.config.risk_management.position_size_percent / 100.0;
        log::debug!(
            "📊 仓位价值计算: {} USDT * {}% = {} USDT",
            available_balance,
            self.config.risk_management.position_size_percent,
            position_value
        );

        let order_amount = self.round_amount(position_value / signal.price, amount_precision);
        log::debug!(
            "📊 订单数量计算: {} USDT / {} = {} (精度调整后)",
            position_value,
            signal.price,
            order_amount
        );

        // 应用仓位倍数
        let final_amount = order_amount * target_config.position_multiplier;
        log::debug!(
            "📊 最终下单数量: {} * {}x = {}",
            order_amount,
            target_config.position_multiplier,
            final_amount
        );

        let order_value = final_amount * signal.price;
        log::debug!(
            "📝 准备下单: {} {} @ {} x {}",
            target_symbol,
            signal.side,
            signal.price,
            final_amount
        );

        // 构建订单请求 - 处理双向持仓
        // 解析信号的方向
        let order_side = match signal.side.as_str() {
            "BuyLong" => OrderSide::Buy,    // 买入开多
            "SellLong" => OrderSide::Sell,  // 卖出平多
            "SellShort" => OrderSide::Sell, // 卖出开空
            "BuyShort" => OrderSide::Buy,   // 买入平空
            _ => OrderSide::Buy,            // 默认
        };

        log::info!("📝 订单方向: {:?} (原始信号: {})", order_side, signal.side);

        let mut order_request = OrderRequest::new(
            target_symbol.clone(),
            order_side,
            if self.config.execution.order_type == "market" {
                OrderType::Market
            } else {
                OrderType::Limit
            },
            final_amount,
            if self.config.execution.order_type == "limit" {
                Some(self.round_price(signal.price, price_precision))
            } else {
                None
            },
            MarketType::Futures,
        );

        // 为Binance期货订单设置positionSide参数（如果是双向持仓模式）
        if target_config.exchange == "binance" && self.config.risk_management.hedge_mode {
            let mut params = HashMap::new();

            // 根据信号类型设置positionSide
            let position_side = match signal.side.as_str() {
                "BuyLong" => "LONG",    // 买入开多
                "SellLong" => "LONG",   // 卖出平多
                "SellShort" => "SHORT", // 卖出开空
                "BuyShort" => "SHORT",  // 买入平空
                _ => {
                    log::warn!("未知的信号类型: {}，默认不设置positionSide", signal.side);
                    ""
                }
            };

            if !position_side.is_empty() {
                params.insert("positionSide".to_string(), position_side.to_string());
                log::debug!("设置Binance期货订单 positionSide: {}", position_side);
            }

            order_request.params = Some(params);
        }

        // 执行下单
        log::debug!("🚀 正在执行下单...");
        let order_start = std::time::Instant::now();

        match target_account.exchange.create_order(order_request).await {
            Ok(order) => {
                log::debug!(
                    "✅ 订单成功: ID={}, 状态={:?}, 耗时={:?}",
                    order.id,
                    order.status,
                    order_start.elapsed()
                );
                Ok(())
            }
            Err(e) => {
                // 记录错误但不中断执行
                log::debug!("❌ 下单失败: {}", e);

                // 检查是否是无效交易对
                if e.to_string().contains("Invalid symbol") || e.to_string().contains("-1121") {
                    log::warn!(
                        "⚠️ {} 不支持交易对 {}，跳过开仓",
                        target_config.exchange,
                        target_symbol
                    );
                    return Ok(());
                }

                // 检查是否是最小订单价值错误
                if e.to_string().contains("notional") || e.to_string().contains("minimum") {
                    log::debug!("⚠️ 可能是订单价值低于交易对最小要求，跳过此订单");
                }

                // 返回错误但让调用方决定是否继续
                Err(Box::new(e))
            }
        }
    }

    /// 平仓指定交易对
    async fn close_position_on_exchange(
        &self,
        symbol: &str,
        target_config: &TargetExchangeConfig,
    ) -> Result<()> {
        log::debug!("🔻 开始平仓操作: {} @ {}", symbol, target_config.exchange);

        log::debug!("🔍 获取目标账户: {}", target_config.account_id);
        let target_account = self
            .account_manager
            .get_account(&target_config.account_id)
            .ok_or_else(|| {
                let err = format!("目标账户 {} 不存在", target_config.account_id);
                log::error!("❌ {}", err);
                ExchangeError::Other(err)
            })?;

        // 转换交易对符号
        log::debug!("🔄 转换交易对符号...");
        let target_symbol = self.convert_symbol(
            symbol,
            &self.config.signal_source.exchange,
            &target_config.exchange,
        )?;
        log::debug!("📝 目标交易对: {}", target_symbol);

        // 获取当前持仓
        log::debug!("🔍 查询当前持仓...");
        log::debug!("  - 账户ID: {}", target_config.account_id);
        log::debug!("  - 交易所: {}", target_account.exchange_name);
        log::debug!("  - 目标交易对: {}", target_symbol);
        let positions_start = std::time::Instant::now();
        let positions = match target_account
            .exchange
            .get_positions(Some(&target_symbol))
            .await
        {
            Ok(positions) => positions,
            Err(e) => {
                // 检查是否是无效交易对错误
                if e.to_string().contains("Invalid symbol") || e.to_string().contains("-1121") {
                    log::warn!(
                        "⚠️ {} 不支持交易对 {}，跳过平仓",
                        target_account.exchange_name,
                        target_symbol
                    );
                    return Ok(());
                } else {
                    log::error!("❌ 获取持仓失败: {}", e);
                    log::error!("  - 交易所: {}", target_account.exchange_name);
                    log::error!("  - 交易对: {}", target_symbol);
                    return Err(Box::new(e));
                }
            }
        };
        log::debug!(
            "📊 找到 {} 个持仓 (耗时: {:?})",
            positions.len(),
            positions_start.elapsed()
        );

        for position in &positions {
            if position.contracts > 0.0 {
                log::debug!(
                    "📋 准备平仓: {} {} x {} (盈亏: {} USDT)",
                    target_symbol,
                    position.side,
                    position.contracts,
                    position.unrealized_pnl
                );

                // 构建平仓订单
                let close_side = if position.side == "LONG" || position.side == "Buy" {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };

                log::debug!(
                    "📝 构建平仓订单: {:?} {} 张",
                    close_side,
                    position.contracts.abs()
                );

                let mut close_order = OrderRequest::new(
                    target_symbol.clone(),
                    close_side,
                    OrderType::Market,
                    position.contracts.abs(),
                    None,
                    MarketType::Futures,
                );

                // 为Binance设置正确的positionSide参数
                let mut params = HashMap::new();
                params.insert("reduce_only".to_string(), "true".to_string());

                if target_config.exchange == "binance" {
                    // Binance双向持仓模式需要设置positionSide
                    let position_side = if position.side == "LONG" || position.side == "Buy" {
                        "LONG" // 平多仓
                    } else {
                        "SHORT" // 平空仓
                    };
                    params.insert("positionSide".to_string(), position_side.to_string());
                    log::debug!("设置Binance平仓 positionSide: {}", position_side);
                }

                close_order.params = Some(params);

                log::info!("🚀 执行平仓...");
                let close_start = std::time::Instant::now();

                target_account
                    .exchange
                    .create_order(close_order)
                    .await
                    .map_err(|e| {
                        log::error!("❌ 平仓失败: {}", e);
                        e
                    })?;

                log::info!("✅ 平仓成功!");
                log::info!("  - 交易所: {}", target_config.exchange);
                log::info!("  - 交易对: {}", target_symbol);
                log::info!("  - 耗时: {:?}", close_start.elapsed());
            } else {
                log::debug!("⏭️ 跳过空仓位");
            }
        }

        if positions.is_empty() {
            log::warn!("⚠️ 没有找到需要平仓的持仓");
        }

        Ok(())
    }

    /// 平掉所有持仓
    async fn close_all_positions(&self) -> Result<()> {
        let state = self.state.lock().await;
        let positions: Vec<String> = state.positions.keys().cloned().collect();
        drop(state);

        for symbol in positions {
            for target_config in &self.config.target_exchanges {
                if target_config.enabled {
                    if let Err(e) = self
                        .close_position_on_exchange(&symbol, target_config)
                        .await
                    {
                        log::error!("平仓失败 {}: {}", symbol, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// 转换交易对符号
    fn convert_symbol(
        &self,
        symbol: &str,
        from_exchange: &str,
        to_exchange: &str,
    ) -> Result<String> {
        // 简单的符号转换逻辑
        // Bitmart期货使用 LTCUSDT 格式（无下划线），现货使用 LTC_USDT 格式
        let unified = if from_exchange == "bitmart" {
            // 检查是否包含下划线（现货格式）
            if symbol.contains("_") {
                symbol.replace("_", "/")
            } else if symbol.ends_with("USDT") {
                // 期货格式 LTCUSDT -> LTC/USDT
                format!("{}/USDT", &symbol[..symbol.len() - 4])
            } else {
                symbol.to_string()
            }
        } else if from_exchange == "binance" {
            // 假设是BTCUSDT格式，需要插入/
            // 简化处理，假设Last 4字符是USDT
            if symbol.ends_with("USDT") {
                format!("{}/USDT", &symbol[..symbol.len() - 4])
            } else {
                symbol.to_string()
            }
        } else {
            symbol.to_string()
        };

        // 从unified转换到目标交易所格式
        // 对于期货交易，Binance也需要BASE/QUOTE格式
        let target_symbol = if to_exchange == "binance" {
            // Binance期货使用 BASE/QUOTE 格式
            unified // 保持 LTC/USDT 格式
        } else if to_exchange == "okx" {
            unified.replace("/", "-")
        } else if to_exchange == "bitmart" {
            unified.replace("/", "_")
        } else {
            unified
        };

        Ok(target_symbol)
    }

    /// 获取交易对精度
    async fn get_symbol_precision(
        &self,
        symbol: &str,
        account: &AccountInfo,
    ) -> Result<(u32, u32)> {
        // 先检查缓存
        if let Some(precision) = self.precision_cache.read().await.get(symbol) {
            return Ok(*precision);
        }

        // 从交易所获取
        let symbol_info = account
            .exchange
            .get_symbol_info(symbol, MarketType::Futures)
            .await?;
        let price_precision = self.calculate_precision(symbol_info.tick_size);
        let amount_precision = self.calculate_precision(symbol_info.min_order_size);

        // 缓存结果
        self.precision_cache
            .write()
            .await
            .insert(symbol.to_string(), (price_precision, amount_precision));

        Ok((price_precision, amount_precision))
    }

    /// 计算精度
    fn calculate_precision(&self, step: f64) -> u32 {
        if step == 0.0 {
            return 0;
        }
        let s = format!("{:.8}", step);
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 1 {
            parts[1].trim_end_matches('0').len() as u32
        } else {
            0
        }
    }

    /// 四舍五入价格
    fn round_price(&self, price: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
    }

    /// 四舍五入数量
    fn round_amount(&self, amount: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (amount * multiplier).round() / multiplier
    }

    /// 启动监控任务
    async fn start_monitoring_task(&self) {
        let state = self.state.clone();
        let running = self.running.clone();
        let name = self.config.name.clone();

        tokio::spawn(async move {
            log::info!("📊 监控任务已启动，每5分钟更新一次统计");
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
            let mut tick_count = 0;

            while *running.read().await {
                interval.tick().await;
                tick_count += 1;

                let state = state.lock().await;
                let run_time = Utc::now() - state.start_time;

                log::info!("📊 ========== 跟单统计 [第{}次] ==========", tick_count);
                log::info!("📈 策略: {}", name);
                log::info!(
                    "⏱️ 运行时长: {} 小时 {} 分钟",
                    run_time.num_hours(),
                    run_time.num_minutes() % 60
                );
                log::info!("📋 交易统计:");
                log::info!("  - 跟单成功: {} 次", state.copy_count);
                log::info!("  - 跟单失败: {} 次", state.failed_count);
                log::info!(
                    "  - 成功率: {:.2}%",
                    if state.copy_count + state.failed_count > 0 {
                        state.copy_count as f64 / (state.copy_count + state.failed_count) as f64
                            * 100.0
                    } else {
                        0.0
                    }
                );
                log::info!("📦 持仓统计:");
                log::info!("  - 当前持仓数: {} 个", state.positions.len());
                if !state.positions.is_empty() {
                    for (symbol, pos) in state.positions.iter() {
                        log::debug!(
                            "    - {}: {:?} {} @ {}",
                            symbol,
                            pos.side,
                            pos.amount,
                            pos.entry_price
                        );
                    }
                }
                log::info!("💰 盈亏统计:");
                log::info!("  - 今日盈亏: {:.2} USDT", state.daily_pnl);
                log::info!("  - 总盈亏: {:.2} USDT", state.total_pnl);
                log::info!(
                    "  - 平均每笔: {:.2} USDT",
                    if state.copy_count > 0 {
                        state.total_pnl / state.copy_count as f64
                    } else {
                        0.0
                    }
                );
                log::info!("📊 ==========================================");
            }

            log::warn!("⚠️ 监控任务已停止");
        });
    }

    /// 克隆引用（用于异步任务）
    fn clone_refs(&self) -> Self {
        Self {
            config: self.config.clone(),
            account_manager: self.account_manager.clone(),
            state: self.state.clone(),
            running: self.running.clone(),
            symbol_converter: None,
            precision_cache: self.precision_cache.clone(),
        }
    }
}

/// 信号
#[derive(Debug, Clone)]
struct PositionSignal {
    symbol: String,
    side: String, // 使用字符串来存储方向信息
    amount: f64,
    price: f64,
    action: SignalAction,
}

#[derive(Debug, Clone)]
enum SignalAction {
    Open,
    Close,
    Increase,
    Decrease,
}

/// 信号处理器
struct SignalHandler {
    strategy: CopyTradingStrategy,
}

#[async_trait::async_trait]
impl MessageHandler for SignalHandler {
    async fn handle_message(&self, message: WsMessage) -> std::result::Result<(), ExchangeError> {
        match message {
            WsMessage::Text(text) => {
                // 处理文本消息（Bitmart的JSON格式）
                if let Err(e) = self.parse_bitmart_order_message(&text).await {
                    log::debug!("解析消息失败（可能不是订单消息）: {}", e);
                }
            }
            WsMessage::Trade(trade) => {
                // 处理成交信息
                self.handle_trade_signal(&trade)
                    .await
                    .map_err(|e| ExchangeError::Other(e.to_string()))?;
            }
            WsMessage::Order(order) => {
                // 处理订单信息
                self.handle_order_signal(&order)
                    .await
                    .map_err(|e| ExchangeError::Other(e.to_string()))?;
            }
            _ => {}
        }
        Ok(())
    }

    // handle_state_change方法已从 trait 中移除

    async fn handle_error(&self, error: ExchangeError) -> std::result::Result<(), ExchangeError> {
        log::error!("WebSocket错误: {}", error);
        Ok(())
    }
}

// 实现Strategy trait
#[async_trait::async_trait]
impl Strategy for CopyTradingStrategy {
    async fn name(&self) -> String {
        self.config.name.clone()
    }

    async fn on_tick(&self, ticker: crate::core::types::Ticker) -> Result<()> {
        // 跟单策略不需要处理ticker
        Ok(())
    }

    async fn on_order_update(&self, order: Order) -> Result<()> {
        // 跟单策略通过WebSocket处理订单更新
        Ok(())
    }

    async fn on_trade(&self, trade: crate::core::types::Trade) -> Result<()> {
        // 跟单策略通过WebSocket处理成交
        Ok(())
    }

    async fn get_status(&self) -> Result<String> {
        let state = self.state.lock().await;
        Ok(format!(
            "跟单成功: {}, 失败: {}, 持仓: {}, 今日盈亏: {:.2}, 总盈亏: {:.2}",
            state.copy_count,
            state.failed_count,
            state.positions.len(),
            state.daily_pnl,
            state.total_pnl
        ))
    }
}

impl SignalHandler {
    /// 解析Bitmart期货订单消息
    async fn parse_bitmart_order_message(&self, text: &str) -> Result<()> {
        // 解析JSON
        let json: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeError::ParseError(format!("解析JSON失败: {}", e)))?;

        // 检查是否是订单消息
        if let Some(group) = json.get("group").and_then(|v| v.as_str()) {
            if group == "futures/order" {
                // 处理订单数据
                if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                    for order_data in data {
                        if let Some(order) = order_data.get("order") {
                            // 解析订单信息
                            let symbol = order.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                            let side = order.get("side").and_then(|v| v.as_i64()).unwrap_or(0);
                            let state = order.get("state").and_then(|v| v.as_i64()).unwrap_or(0);
                            let deal_size = order
                                .get("deal_size")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            let deal_avg_price = order
                                .get("deal_avg_price")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);

                            // state: 2=status_check(待成交), 4=status_finish(完全成交)
                            // Bitmart Hedge mode side 定义:
                            // 1 = buy_open_long (买入开多)
                            // 2 = buy_close_short (买入平空)
                            // 3 = sell_close_long (卖出平多)
                            // 4 = sell_open_short (卖出开空)

                            // 记录所有订单状态，用于调试
                            if side == 1 || side == 4 {
                                // 开仓订单
                                log::info!(
                                    "📌 检测到开仓订单 - 状态:{}, 方向:{}, 数量:{}",
                                    state,
                                    match side {
                                        1 => "买入开多",
                                        4 => "卖出开空",
                                        _ => "未知",
                                    },
                                    deal_size
                                );
                            }

                            if state == 4 && deal_size > 0.0 {
                                log::info!("🎯 检测到Bitmart成交订单:");
                                log::info!("  - 交易对: {}", symbol);
                                log::info!(
                                    "  - 方向: {} (side={})",
                                    match side {
                                        1 => "买入开多",
                                        2 => "买入平空",
                                        3 => "卖出平多",
                                        4 => "卖出开空",
                                        _ => "未知",
                                    },
                                    side
                                );
                                log::info!("  - 成交数量: {}", deal_size);
                                log::info!("  - 成交均价: {}", deal_avg_price);

                                // 构建信号
                                // Bitmart Hedge mode side 定义:
                                // 1 = buy_open_long (买入开多)
                                // 2 = buy_close_short (买入平空)
                                // 3 = sell_close_long (卖出平多)
                                // 4 = sell_open_short (卖出开空)
                                let signal = PositionSignal {
                                    symbol: symbol.to_string(),
                                    side: match side {
                                        1 => "BuyLong".to_string(),   // 买入开多
                                        2 => "BuyShort".to_string(),  // 买入平空
                                        3 => "SellLong".to_string(),  // 卖出平多
                                        4 => "SellShort".to_string(), // 卖出开空
                                        _ => "Unknown".to_string(),
                                    },
                                    amount: deal_size,
                                    price: deal_avg_price,
                                    action: match side {
                                        1 | 4 => SignalAction::Open,  // 开仓 (1=开多, 4=开空)
                                        2 | 3 => SignalAction::Close, // 平仓 (2=平空, 3=平多)
                                        _ => SignalAction::Open,
                                    },
                                };

                                // 触发跟单
                                log::info!(
                                    "🎲 信号动作: {:?}, Side: {}",
                                    signal.action,
                                    signal.side
                                );
                                match signal.action {
                                    SignalAction::Open => {
                                        log::info!(
                                            "📈 触发开仓跟单 - {} {}",
                                            signal.symbol,
                                            match signal.side.as_str() {
                                                "BuyLong" => "买入开多",
                                                "SellShort" => "卖出开空",
                                                _ => &signal.side,
                                            }
                                        );
                                        self.strategy.handle_open_position(&signal).await?;
                                    }
                                    SignalAction::Close => {
                                        log::info!(
                                            "📉 触发平仓跟单 - {} {}",
                                            signal.symbol,
                                            match signal.side.as_str() {
                                                "SellLong" => "卖出平多",
                                                "BuyShort" => "买入平空",
                                                _ => &signal.side,
                                            }
                                        );
                                        self.strategy.handle_close_position(&signal).await?;

                                        // 检查是否需要立即反向开仓
                                        // Bitmart: 2=卖出平多 -> 可能后续3=卖出开空
                                        //          4=买入平空 -> 可能后续1=买入开多
                                        log::debug!("⚠️ 平仓完成，准备接收可能的反向开仓信号");
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_trade_signal(&self, trade: &Trade) -> Result<()> {
        // 解析成交信息，判断是开仓还是平仓
        // TODO: 根据实际Bitmart消息格式解析

        // 将OrderSide转换为String
        let side_str = match trade.side {
            OrderSide::Buy => "Buy".to_string(),
            OrderSide::Sell => "Sell".to_string(),
        };

        let signal = PositionSignal {
            symbol: trade.symbol.clone(),
            side: side_str,
            amount: trade.amount,
            price: trade.price,
            action: SignalAction::Open, // 需要根据实际情况判断
        };

        match signal.action {
            SignalAction::Open | SignalAction::Increase => {
                self.strategy.handle_open_position(&signal).await?;
            }
            SignalAction::Close | SignalAction::Decrease => {
                self.strategy.handle_close_position(&signal).await?;
            }
        }

        Ok(())
    }

    async fn handle_order_signal(&self, order: &Order) -> Result<()> {
        // 处理订单信号
        if order.status == OrderStatus::Closed {
            log::info!(
                "📝 订单成交: {} {:?} {} @ {}",
                order.symbol,
                order.side,
                order.amount,
                order.price.unwrap_or(0.0)
            );
        }
        Ok(())
    }
}
