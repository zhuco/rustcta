use super::config::*;
use super::state::ASState;

use crate::strategies::common::{
    build_unified_risk_evaluator, RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits,
    StrategySnapshot, UnifiedRiskEvaluator,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use log;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::types::{
    MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Position, WsMessage,
};
use crate::core::websocket::WebSocketClient;
use crate::cta::AccountManager;
use crate::exchanges::binance::{BinanceExchange, BinanceWebSocketClient};
use crate::utils::init_global_time_sync;
use crate::utils::order_id::generate_order_id;
use crate::utils::unified_logger::UnifiedLogger;
use tokio::time::Duration;

/// Avellaneda-Stoikov做市策略
pub struct AvellanedaStoikovStrategy {
    config: ASConfig,
    account_manager: Arc<AccountManager>,
    state: Arc<RwLock<ASState>>,
    logger: Arc<UnifiedLogger>,
    ws_client: Option<Arc<tokio::sync::Mutex<BinanceWebSocketClient>>>,
    listen_key: Option<String>,

    // 市场数据缓存
    price_history: Arc<RwLock<VecDeque<f64>>>,
    volume_history: Arc<RwLock<VecDeque<f64>>>,
    orderbook_cache: Arc<RwLock<HashMap<String, f64>>>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
}

impl AvellanedaStoikovStrategy {
    /// 创建新的A-S策略实例
    pub async fn new(config: ASConfig, account_manager: Arc<AccountManager>) -> Result<Self> {
        // 确保时间同步
        let _ = init_global_time_sync();

        // 初始化日志（使用策略专用日志）
        use crate::utils::unified_logger::LogConfig;
        let log_config = LogConfig {
            root_dir: "logs".to_string(),
            default_level: config.strategy.log_level.clone(),
            max_file_size_mb: 10,
            retention_days: 30,
            console_output: true,
            format: "[{timestamp}] [{level}] [{module}] {message}".to_string(),
        };
        let logger = Arc::new(UnifiedLogger::new(log_config).unwrap());
        // 初始化状态
        let state = ASState {
            mid_price: 0.0,
            bid_price: 0.0,
            ask_price: 0.0,
            volatility: 0.001, // 初始波动率设为0.1%避免被风险检查拒绝
            inventory: 0.0,
            inventory_value: 0.0,
            active_buy_order: None,
            active_sell_order: None,
            last_buy_price: 0.0,
            last_sell_price: 0.0,
            total_trades: 0,
            buy_fills: 0,
            sell_fills: 0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            consecutive_losses: 0,
            daily_pnl: 0.0,
            max_drawdown: 0.0,
            strategy_start_time: Utc::now(),
            last_update_time: Utc::now(),
            last_volatility_update: Utc::now(),
        };

        let risk_limits = Self::build_risk_limits(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        Ok(Self {
            config,
            account_manager,
            state: Arc::new(RwLock::new(state)),
            logger,
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            volume_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            orderbook_cache: Arc::new(RwLock::new(HashMap::new())),
            ws_client: None,
            listen_key: None,
            risk_evaluator,
            risk_limits,
        })
    }

    /// 计算预留价格 (Reservation Price)
    async fn calculate_reservation_price(&self) -> f64 {
        let state = self.state.read().await;
        let gamma = self.config.as_params.risk_aversion;
        let sigma = state.volatility;
        let inventory = state.inventory;
        let time_to_end = self.config.as_params.time_horizon_seconds as f64;

        // r = mid_price - q * γ * σ² * (T-t)
        let reservation_adjustment = inventory * gamma * sigma.powi(2) * time_to_end / 3600.0;
        state.mid_price - reservation_adjustment
    }

    /// 计算最优价差
    async fn calculate_optimal_spread(&self) -> f64 {
        let state = self.state.read().await;
        let gamma = self.config.as_params.risk_aversion;
        let k = self.config.as_params.order_book_intensity;
        let sigma = state.volatility;
        let time_to_end = self.config.as_params.time_horizon_seconds as f64;

        // s = γσ²(T-t) + (2/γ)ln(1 + γ/k)
        let time_component = gamma * sigma.powi(2) * time_to_end / 3600.0;
        let intensity_component = (2.0 / gamma) * (1.0 + gamma / k).ln();

        let base_spread = time_component + intensity_component;

        // 应用价差限制
        let spread_bp = base_spread * 10000.0;
        let clamped_spread_bp = spread_bp
            .max(self.config.trading.min_spread_bp)
            .min(self.config.trading.max_spread_bp);

        clamped_spread_bp / 10000.0
    }

    /// 计算波动率
    async fn calculate_volatility(&self) -> f64 {
        let prices = self.price_history.read().await;

        if prices.len() < self.config.as_params.volatility.lookback_periods {
            return 0.01; // 默认1%波动率
        }

        // 计算收益率
        let mut returns = Vec::new();
        for i in 1..prices.len() {
            let ret = (prices[i] / prices[i - 1]).ln();
            returns.push(ret);
        }

        // 计算标准差（使用指数加权）
        let decay = self.config.as_params.volatility.decay_factor;
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        let mut weight = 1.0;

        for (i, ret) in returns.iter().enumerate().rev() {
            weighted_sum += weight * ret.powi(2);
            weight_sum += weight;
            weight *= decay;
        }

        let variance = weighted_sum / weight_sum;
        let volatility = variance.sqrt();

        // 年化波动率 - 使用合理的缩放因子
        // 假设价格每20秒更新一次，一年有365*24*3600/20 = 1,576,800个周期
        let periods_per_year =
            365.0 * 24.0 * 3600.0 / self.config.as_params.volatility.update_interval as f64;
        volatility * periods_per_year.sqrt()
    }

    /// 生成报价
    async fn generate_quotes(&self) -> (f64, f64) {
        let reservation_price = self.calculate_reservation_price().await;
        let optimal_spread = self.calculate_optimal_spread().await;
        let half_spread = optimal_spread / 2.0;

        let state = self.state.read().await;

        // 库存偏斜调整
        let mut bid_adjustment = 0.0;
        let mut ask_adjustment = 0.0;

        if self.config.as_params.inventory_skew.enabled {
            let inventory_ratio = state.inventory / self.config.trading.max_inventory;
            let target_ratio = self.config.as_params.inventory_skew.target_inventory_ratio;
            let skew_factor = self.config.as_params.inventory_skew.skew_factor;

            let inventory_imbalance = inventory_ratio - target_ratio;

            // 库存过多，降低买价，提高卖价
            if inventory_imbalance > 0.0 {
                bid_adjustment = -inventory_imbalance * skew_factor * half_spread;
                ask_adjustment = inventory_imbalance * skew_factor * half_spread * 0.5;
            }
            // 库存过少，提高买价，降低卖价
            else {
                bid_adjustment = -inventory_imbalance * skew_factor * half_spread * 0.5;
                ask_adjustment = inventory_imbalance * skew_factor * half_spread;
            }
        }

        let bid = reservation_price - half_spread + bid_adjustment;
        let ask = reservation_price + half_spread + ask_adjustment;

        // 应用精度（简单四舍五入）
        let price_precision = self.config.trading.price_precision as i32;
        let multiplier = 10_f64.powi(price_precision);
        let bid = (bid * multiplier).round() / multiplier;
        let ask = (ask * multiplier).round() / multiplier;

        (bid, ask)
    }
    fn build_risk_limits(config: &ASConfig) -> StrategyRiskLimits {
        StrategyRiskLimits {
            warning_scale_factor: Some(0.8),
            danger_scale_factor: Some(0.5),
            stop_loss_pct: if config.risk.stop_loss.enabled {
                Some(config.risk.stop_loss.stop_loss_pct)
            } else {
                None
            },
            max_inventory_notional: Some(config.risk.inventory_risk.max_position_value),
            max_daily_loss: Some(config.risk.max_daily_loss),
            max_consecutive_losses: Some(config.risk.emergency_stop.consecutive_losses),
            inventory_skew_limit: Some(config.risk.inventory_risk.imbalance_threshold),
            max_unrealized_loss: Some(config.risk.max_unrealized_loss),
        }
    }

    fn build_risk_snapshot(&self, state: &ASState) -> StrategySnapshot {
        let mut snapshot = StrategySnapshot::new(self.config.strategy.name.clone());
        snapshot.exposure.notional = state.inventory_value;
        snapshot.exposure.net_inventory = state.inventory;
        if self.config.risk.inventory_risk.max_position_value > 0.0 {
            snapshot.exposure.inventory_ratio = Some(
                (state.inventory_value / self.config.risk.inventory_risk.max_position_value)
                    .min(10.0),
            );
        }

        snapshot.performance.realized_pnl = state.realized_pnl;
        snapshot.performance.unrealized_pnl = state.unrealized_pnl;
        snapshot.performance.daily_pnl = Some(state.daily_pnl);
        snapshot.performance.drawdown_pct = Some(-state.max_drawdown.abs());
        snapshot.performance.consecutive_losses = Some(state.consecutive_losses);
        snapshot.performance.timestamp = Utc::now();
        snapshot.risk_limits = Some(self.risk_limits.clone());
        snapshot
    }

    async fn apply_risk_decision(&self, decision: RiskDecision) -> Result<bool> {
        match decision.action {
            RiskAction::None => Ok(true),
            RiskAction::Notify { level, message } => {
                match level {
                    RiskNotifyLevel::Info => log::info!("ℹ️ 风险提示: {}", message),
                    RiskNotifyLevel::Warning => log::warn!("⚠️ 风险警告: {}", message),
                    RiskNotifyLevel::Danger => log::error!("❗ 风险危险: {}", message),
                }
                Ok(true)
            }
            RiskAction::ScaleDown {
                scale_factor,
                reason,
            } => {
                log::warn!("⚠️ A-S策略触发缩减: {} (scale={:.2})", reason, scale_factor);
                self.cancel_all_orders().await?;
                if scale_factor <= 0.0 {
                    self.sync_inventory().await?;
                }
                Ok(false)
            }
            RiskAction::Halt { reason } => {
                log::error!("❌ A-S策略触发停机: {}", reason);
                self.cancel_all_orders().await?;
                Ok(false)
            }
        }
    }

    /// 检查风险限制
    async fn check_risk_limits(&self) -> Result<bool> {
        let state = self.state.read().await;
        let max_vol = self.config.risk.volatility_limits.max_volatility / 100.0;
        if state.volatility > max_vol {
            log::warn!("⚠️ 波动率过高: {:.2}%", state.volatility * 100.0);
            return Ok(false);
        }

        let min_vol = self.config.risk.volatility_limits.min_volatility / 100.0;
        if state.volatility < min_vol && state.volatility > 0.0 {
            log::debug!("波动率较低: {:.2}%（继续交易）", state.volatility * 100.0);
        }

        let snapshot = self.build_risk_snapshot(&state);
        drop(state);

        let decision = self.risk_evaluator.evaluate(&snapshot).await;
        self.apply_risk_decision(decision).await
    }

    /// 同步库存
    async fn sync_inventory(&self) -> Result<()> {
        // 获取账户
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // 对于永续合约，应该获取持仓而不是余额
        let positions = account.positions.read().await;

        // 查找对应交易对的持仓
        let actual_inventory = positions
            .iter()
            .find(|p| p.symbol == self.config.trading.symbol)
            .map(|p| p.contracts) // 使用contracts字段表示持仓数量
            .unwrap_or(0.0);

        // 更新本地库存
        let mut state = self.state.write().await;
        if (state.inventory - actual_inventory).abs() > 0.001 {
            log::info!(
                "📊 同步库存: 本地 {:.4} -> 实际 {:.4}",
                state.inventory,
                actual_inventory
            );
            state.inventory = actual_inventory;
        }

        Ok(())
    }

    /// 下单
    async fn place_orders(&self, bid: f64, ask: f64) -> Result<()> {
        let mut state = self.state.write().await;

        // 计算订单数量（应用精度）
        let order_size_usdc = self.config.trading.order_size_usdc;
        let qty_precision = self.config.trading.quantity_precision as i32;
        let qty_multiplier = 10_f64.powi(qty_precision);
        let buy_qty = ((order_size_usdc / bid) * qty_multiplier).round() / qty_multiplier;
        let sell_qty = ((order_size_usdc / ask) * qty_multiplier).round() / qty_multiplier;

        // 获取账户
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // 创建买单
        if state.inventory < self.config.trading.max_inventory {
            let buy_order = Order {
                id: format!("AS_BUY_{}", chrono::Utc::now().timestamp_millis()),
                symbol: self.config.trading.symbol.clone(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                price: Some(bid),
                amount: buy_qty,
                filled: 0.0,
                remaining: buy_qty,
                status: OrderStatus::Open,
                market_type: crate::core::types::MarketType::Futures,
                timestamp: Utc::now(),
                last_trade_timestamp: None,
                info: serde_json::json!({}),
            };

            // 转换为OrderRequest
            let buy_request = OrderRequest {
                symbol: buy_order.symbol.clone(),
                side: buy_order.side.clone(),
                order_type: buy_order.order_type.clone(),
                amount: buy_order.amount,
                price: buy_order.price,
                market_type: crate::core::types::MarketType::Futures,
                params: None,
                reduce_only: Some(false),
                post_only: Some(true),
                time_in_force: Some("GTX".to_string()),
                client_order_id: Some(buy_order.id.clone()),
            };

            match account.exchange.create_order(buy_request).await {
                Ok(placed_order) => {
                    state.active_buy_order = Some(placed_order.id.clone());
                    state.last_buy_price = bid;
                    log::info!("📗 A-S买单成功: {} @ {:.2}", buy_qty, bid);
                }
                Err(e) => {
                    log::error!("❌ A-S买单失败: {}", e);
                }
            }
        }

        // 创建卖单
        if state.inventory > -self.config.trading.max_inventory {
            let sell_order = Order {
                id: format!("AS_SELL_{}", chrono::Utc::now().timestamp_millis() + 1), // +1避免同时生成时ID重复
                symbol: self.config.trading.symbol.clone(),
                side: OrderSide::Sell,
                order_type: OrderType::Limit,
                price: Some(ask),
                amount: sell_qty,
                filled: 0.0,
                remaining: sell_qty,
                status: OrderStatus::Open,
                market_type: crate::core::types::MarketType::Futures,
                timestamp: Utc::now(),
                last_trade_timestamp: None,
                info: serde_json::json!({}),
            };

            // 转换为OrderRequest
            let sell_request = OrderRequest {
                symbol: sell_order.symbol.clone(),
                side: sell_order.side.clone(),
                order_type: sell_order.order_type.clone(),
                amount: sell_order.amount,
                price: sell_order.price,
                market_type: crate::core::types::MarketType::Futures,
                params: None,
                reduce_only: Some(false),
                post_only: Some(true),
                time_in_force: Some("GTX".to_string()),
                client_order_id: Some(sell_order.id.clone()),
            };

            match account.exchange.create_order(sell_request).await {
                Ok(placed_order) => {
                    state.active_sell_order = Some(placed_order.id.clone());
                    state.last_sell_price = ask;
                    log::info!("📕 A-S卖单成功: {} @ {:.2}", sell_qty, ask);
                }
                Err(e) => {
                    log::error!("❌ A-S卖单失败: {}", e);
                }
            }
        }

        Ok(())
    }

    /// 取消现有订单
    async fn cancel_existing_orders(&self) -> Result<()> {
        let state = self.state.read().await;
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // 取消买单
        if let Some(buy_order_id) = &state.active_buy_order {
            if let Err(e) = account
                .exchange
                .cancel_order(
                    &self.config.trading.symbol,
                    buy_order_id,
                    crate::core::types::MarketType::Futures,
                )
                .await
            {
                log::debug!("取消买单失败: {}", e);
            }
        }

        // 取消卖单
        if let Some(sell_order_id) = &state.active_sell_order {
            if let Err(e) = account
                .exchange
                .cancel_order(
                    &self.config.trading.symbol,
                    sell_order_id,
                    crate::core::types::MarketType::Futures,
                )
                .await
            {
                log::debug!("取消卖单失败: {}", e);
            }
        }

        Ok(())
    }

    /// 更新库存
    async fn update_inventory(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        let positions = account
            .exchange
            .get_positions(Some(&self.config.trading.symbol))
            .await?;
        let mut state = self.state.write().await;

        // 查找对应交易对的仓位
        for position in positions {
            if position.symbol == self.config.trading.symbol {
                // 使用amount字段，它包含了正负号
                state.inventory = position.amount;
                state.inventory_value = state.inventory * state.mid_price;

                // 更新未实现盈亏
                state.unrealized_pnl = position.unrealized_pnl;

                break;
            }
        }

        Ok(())
    }

    /// 撤销所有挂单
    async fn cancel_all_orders(&self) -> Result<()> {
        log::info!("🔄 撤销所有挂单...");

        // 直接调用account_manager的撤单功能
        self.account_manager
            .cancel_all_orders(
                &self.config.account.account_id,
                Some(&self.config.trading.symbol.as_str()),
            )
            .await?;

        log::info!("✅ 已撤销所有订单");
        Ok(())
    }

    /// 启动WebSocket连接
    async fn start_websocket(&mut self) -> Result<()> {
        // 检查WebSocket配置是否启用
        if !self.config.data_sources.websocket.enabled {
            log::info!("📡 WebSocket未启用，使用REST API轮询模式");
            return Ok(());
        }

        log::info!("🔌 启动WebSocket连接...");

        // 获取账户信息
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // 转换为BinanceExchange类型以访问特定方法
        let binance_exchange = account
            .exchange
            .as_any()
            .downcast_ref::<BinanceExchange>()
            .ok_or_else(|| anyhow::anyhow!("Not a Binance exchange"))?;

        // 获取listenKey
        let market_type = match self.config.trading.market_type.to_lowercase().as_str() {
            "futures" => MarketType::Futures,
            "spot" => MarketType::Spot,
            _ => MarketType::Futures, // 默认期货
        };

        if let Ok(listen_key) = binance_exchange.create_listen_key(market_type).await {
            log::info!("✅ 获取listenKey成功");
            self.listen_key = Some(listen_key.clone());

            // 创建WebSocket URL（根据市场类型选择）
            let ws_url = match market_type {
                MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
                MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
            };
            let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);

            // 连接WebSocket
            if let Err(e) = ws_client.connect().await {
                log::error!("❌ WebSocket连接失败: {}", e);
                return Err(anyhow::anyhow!("WebSocket connection failed: {}", e));
            }

            log::info!("📡 WebSocket连接成功，开始监听用户数据流");
            self.ws_client = Some(Arc::new(tokio::sync::Mutex::new(ws_client)));

            // 启动消息处理任务
            self.spawn_websocket_handler().await;
        } else {
            log::warn!("⚠️ 获取listenKey失败，将使用REST API模式");
        }

        Ok(())
    }

    /// 启动WebSocket消息处理器
    async fn spawn_websocket_handler(&self) {
        let ws_client = self.ws_client.clone();
        let state = self.state.clone();
        let symbol = self.config.trading.symbol.clone();

        tokio::spawn(async move {
            if let Some(client) = ws_client {
                let mut client = client.lock().await;

                loop {
                    // 接收消息
                    match client.receive().await {
                        Ok(Some(msg)) => {
                            // 尝试使用parse_binance_message解析为WsMessage
                            if let Ok(ws_msg) = client.parse_binance_message(&msg) {
                                match ws_msg {
                                    WsMessage::Order(order) => {
                                        // 过滤交易对
                                        if order.symbol == symbol.replace("/", "") {
                                            log::info!(
                                                "📬 收到订单更新: {} {:?} {:?} @ {:?} qty: {}",
                                                order.symbol,
                                                order.side,
                                                order.status,
                                                order.price,
                                                order.amount
                                            );

                                            // 更新状态
                                            if order.status == OrderStatus::Closed {
                                                let mut state = state.write().await;
                                                if order.side == OrderSide::Buy {
                                                    state.buy_fills += 1;
                                                    state.inventory += order.amount;
                                                } else {
                                                    state.sell_fills += 1;
                                                    state.inventory -= order.amount;
                                                }
                                                state.total_trades += 1;
                                            }
                                        }
                                    }
                                    WsMessage::Balance(_balance) => {
                                        log::debug!("📊 收到账户余额更新");
                                    }
                                    WsMessage::ExecutionReport(exec) => {
                                        // 处理执行报告
                                        if exec.symbol == symbol.replace("/", "") {
                                            log::info!(
                                                "📬 收到执行报告: {} {:?} {:?} @ {} qty: {}",
                                                exec.symbol,
                                                exec.side,
                                                exec.status,
                                                exec.price,
                                                exec.amount
                                            );

                                            if exec.status == OrderStatus::Closed {
                                                let mut state = state.write().await;
                                                if exec.side == OrderSide::Buy {
                                                    state.buy_fills += 1;
                                                    state.inventory += exec.amount;
                                                } else {
                                                    state.sell_fills += 1;
                                                    state.inventory -= exec.amount;
                                                }
                                                state.total_trades += 1;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            } else if let Ok(json) = serde_json::from_str::<serde_json::Value>(&msg)
                            {
                                // 回退到原始JSON解析
                                if json["e"] == "ORDER_TRADE_UPDATE" {
                                    let order_symbol = json["o"]["s"].as_str().unwrap_or("");

                                    // 过滤交易对
                                    if order_symbol == symbol.replace("/", "") {
                                        let side = json["o"]["S"].as_str().unwrap_or("");
                                        let status = json["o"]["X"].as_str().unwrap_or("");
                                        let price = json["o"]["p"]
                                            .as_str()
                                            .unwrap_or("0")
                                            .parse::<f64>()
                                            .unwrap_or(0.0);
                                        let qty = json["o"]["q"]
                                            .as_str()
                                            .unwrap_or("0")
                                            .parse::<f64>()
                                            .unwrap_or(0.0);

                                        log::info!(
                                            "📬 收到订单更新: {} {} {} @ {} qty: {}",
                                            order_symbol,
                                            side,
                                            status,
                                            price,
                                            qty
                                        );

                                        // 更新状态
                                        if status == "FILLED" {
                                            let mut state = state.write().await;
                                            if side == "BUY" {
                                                state.buy_fills += 1;
                                                state.inventory += qty;
                                            } else if side == "SELL" {
                                                state.sell_fills += 1;
                                                state.inventory -= qty;
                                            }
                                            state.total_trades += 1;
                                        }
                                    }
                                }
                                // 处理账户更新
                                else if json["e"] == "ACCOUNT_UPDATE" {
                                    log::debug!("📊 收到账户更新");
                                }
                            }
                        }
                        Ok(None) => {
                            // 连接关闭
                            log::warn!("⚠️ WebSocket连接关闭");
                            break;
                        }
                        Err(e) => {
                            log::error!("❌ 接收WebSocket消息失败: {}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        });
    }

    /// 启动策略
    pub async fn start(&mut self) -> Result<()> {
        log::info!(
            "🚀 启动Avellaneda-Stoikov策略: {}",
            self.config.strategy.name
        );
        log::info!(
            "📊 交易对: {}, 风险厌恶: {}, 时间窗口: {}小时",
            self.config.trading.symbol,
            self.config.as_params.risk_aversion,
            self.config.as_params.time_horizon_seconds / 3600
        );

        // 启动WebSocket
        if let Err(e) = self.start_websocket().await {
            log::warn!("⚠️ WebSocket启动失败: {}，将使用REST API模式", e);
        }

        // 主循环
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            self.config.trading.refresh_interval_secs,
        ));

        // 启动时先撤销所有挂单
        if let Err(e) = self.cancel_all_orders().await {
            log::error!("撤销挂单失败: {}", e);
        }

        // 记录当前挂单价格
        let mut current_bid = 0.0;
        let mut current_ask = 0.0;
        let price_threshold = 0.001; // 0.1%价格变化阈值

        // 库存同步计时器
        let mut last_inventory_sync = Utc::now();
        let inventory_sync_interval = 180; // 3分钟同步一次

        loop {
            interval.tick().await;

            // 更新市场数据
            if let Err(e) = self.update_market_data().await {
                log::error!("更新市场数据失败: {}", e);
                continue;
            }

            // 更新波动率
            let now = Utc::now();
            let state = self.state.write().await;
            if (now - state.last_volatility_update).num_seconds()
                > self.config.as_params.volatility.update_interval as i64
            {
                drop(state); // 释放锁
                let new_volatility = self.calculate_volatility().await;
                let mut state = self.state.write().await;
                state.volatility = new_volatility;
                state.last_volatility_update = now;
                log::debug!("📈 更新波动率: {:.2}%", new_volatility * 100.0);
            } else {
                drop(state);
            }

            // 更新库存
            if let Err(e) = self.update_inventory().await {
                log::error!("更新库存失败: {}", e);
                continue;
            }

            // 检查风险限制
            match self.check_risk_limits().await {
                Ok(true) => {
                    // 风险检查通过，继续交易
                }
                Ok(false) => {
                    log::warn!("⚠️ 风险检查未通过，暂停交易");
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    continue;
                }
                Err(e) => {
                    log::error!("风险检查失败: {}", e);
                    continue;
                }
            }

            // 定期同步库存
            let now = Utc::now();
            if now.signed_duration_since(last_inventory_sync).num_seconds()
                > inventory_sync_interval
            {
                if let Err(e) = self.sync_inventory().await {
                    log::error!("同步库存失败: {}", e);
                }
                last_inventory_sync = now;
            }

            // 生成新报价
            let (bid, ask) = self.generate_quotes().await;

            // 检查价格是否需要更新（避免除零错误）
            let price_changed = if current_bid == 0.0 || current_ask == 0.0 {
                true // 初次下单
            } else {
                (bid - current_bid).abs() / current_bid > price_threshold
                    || (ask - current_ask).abs() / current_ask > price_threshold
            };

            if price_changed {
                log::info!(
                    "💹 A-S报价更新 - 买: {:.2}, 卖: {:.2}, 价差: {:.2}bp",
                    bid,
                    ask,
                    (ask - bid) / bid * 10000.0
                );

                // 取消现有订单
                if let Err(e) = self.cancel_existing_orders().await {
                    log::error!("取消订单失败: {}", e);
                }

                // 下新单
                if let Err(e) = self.place_orders(bid, ask).await {
                    log::error!("下单失败: {}", e);
                } else {
                    current_bid = bid;
                    current_ask = ask;
                }
            } else {
                log::debug!("价格变化小于阈值，保持当前订单");
            }

            // 每30秒记录一次状态
            static mut LAST_LOG_TIME: Option<i64> = None;
            let now_timestamp = Utc::now().timestamp();
            unsafe {
                if LAST_LOG_TIME.is_none() || now_timestamp - LAST_LOG_TIME.unwrap() > 30 {
                    let state = self.state.read().await;
                    log::info!(
                        "📊 策略状态 - 库存: {:.4}, 未实现盈亏: {:.2}, 日盈亏: {:.2}",
                        state.inventory,
                        state.unrealized_pnl,
                        state.daily_pnl
                    );
                    LAST_LOG_TIME = Some(now_timestamp);
                }
            }
        }
    }

    /// 停止策略
    pub async fn stop(&mut self) -> Result<()> {
        log::info!("🛑 停止Avellaneda-Stoikov策略");

        // 取消所有订单
        self.cancel_existing_orders().await?;

        // 记录最终状态
        let state = self.state.read().await;
        log::info!("📊 最终统计:");
        log::info!(
            "  总交易: {}, 买成交: {}, 卖成交: {}",
            state.total_trades,
            state.buy_fills,
            state.sell_fills
        );
        log::info!(
            "  实现盈亏: {:.2}, 未实现盈亏: {:.2}",
            state.realized_pnl,
            state.unrealized_pnl
        );

        Ok(())
    }

    /// 更新市场数据
    async fn update_market_data(&mut self) -> Result<()> {
        // 获取账户和交易所
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // 获取订单簿
        let orderbook = account
            .exchange
            .get_orderbook(
                &self.config.trading.symbol,
                crate::core::types::MarketType::Futures,
                Some(5),
            )
            .await?;

        // 更新价格
        let mut state = self.state.write().await;
        if let (Some(best_bid), Some(best_ask)) = (orderbook.bids.first(), orderbook.asks.first()) {
            state.bid_price = best_bid[0];
            state.ask_price = best_ask[0];
            state.mid_price = (state.bid_price + state.ask_price) / 2.0;

            // 更新价格历史
            drop(state); // 释放状态锁
            let mut prices = self.price_history.write().await;
            prices.push_back(self.state.read().await.mid_price);
            if prices.len() > 1000 {
                prices.pop_front();
            }
        }

        Ok(())
    }
}
