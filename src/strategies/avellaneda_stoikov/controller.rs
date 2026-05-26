use super::config::*;
use super::state::{ASOrderState, ASState, MarketRules};

use crate::core::exchange::Exchange;
use crate::core::types::{
    ExecutionReport, MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType, WsMessage,
};
use crate::core::websocket::WebSocketClient;
use crate::cta::{AccountInfo, AccountManager};
use crate::exchanges::binance::{BinanceExchange, BinanceWebSocketClient};
use crate::strategies::common::{
    build_unified_risk_evaluator, RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits,
    StrategySnapshot, UnifiedRiskEvaluator,
};
use crate::utils::init_global_time_sync;
use crate::utils::unified_logger::UnifiedLogger;
use anyhow::{anyhow, Result};
use chrono::Utc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

const PRICE_REFRESH_THRESHOLD: f64 = 0.001;
const INVENTORY_SYNC_INTERVAL_SECS: i64 = 180;
const LOG_INTERVAL_SECS: i64 = 30;

/// Avellaneda-Stoikov做市策略。
pub struct AvellanedaStoikovStrategy {
    config: ASConfig,
    account_manager: Arc<AccountManager>,
    state: Arc<RwLock<ASState>>,
    #[allow(dead_code)]
    logger: Arc<UnifiedLogger>,
    ws_client: Option<Arc<tokio::sync::Mutex<BinanceWebSocketClient>>>,
    listen_key: Option<String>,
    market_type: MarketType,
    running: Arc<AtomicBool>,
    ws_task: Option<JoinHandle<()>>,

    price_history: Arc<RwLock<VecDeque<f64>>>,
    volume_history: Arc<RwLock<VecDeque<f64>>>,
    orderbook_cache: Arc<RwLock<HashMap<String, f64>>>,
    market_rules: Arc<RwLock<MarketRules>>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
}

impl AvellanedaStoikovStrategy {
    /// 创建新的A-S策略实例。
    pub async fn new(config: ASConfig, account_manager: Arc<AccountManager>) -> Result<Self> {
        let _ = init_global_time_sync();

        use crate::utils::unified_logger::LogConfig;
        let log_config = LogConfig {
            root_dir: "logs".to_string(),
            default_level: config.strategy.log_level.clone(),
            max_file_size_mb: 10,
            retention_days: 30,
            console_output: true,
            format: "[{timestamp}] [{level}] [{module}] {message}".to_string(),
        };
        let logger = Arc::new(
            UnifiedLogger::new(log_config)
                .map_err(|err| anyhow!("failed to initialize AS logger: {}", err))?,
        );

        let market_type = Self::parse_market_type(&config.trading.market_type);
        let now = Utc::now();
        let state = ASState {
            mid_price: 0.0,
            bid_price: 0.0,
            ask_price: 0.0,
            volatility: 0.001,
            inventory: 0.0,
            inventory_value: 0.0,
            active_buy_order: None,
            active_sell_order: None,
            last_buy_price: 0.0,
            last_sell_price: 0.0,
            open_orders: HashMap::new(),
            total_trades: 0,
            buy_fills: 0,
            sell_fills: 0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            fees_paid: 0.0,
            avg_entry_price: 0.0,
            equity_peak: 0.0,
            consecutive_losses: 0,
            daily_pnl: 0.0,
            max_drawdown: 0.0,
            strategy_start_time: now,
            last_update_time: now,
            last_volatility_update: now,
        };

        let risk_limits = Self::build_risk_limits(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );
        let market_rules = MarketRules::from_config(
            config.trading.price_precision,
            config.trading.quantity_precision,
            market_type,
        );

        Ok(Self {
            config,
            account_manager,
            state: Arc::new(RwLock::new(state)),
            logger,
            ws_client: None,
            listen_key: None,
            market_type,
            running: Arc::new(AtomicBool::new(false)),
            ws_task: None,
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            volume_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            orderbook_cache: Arc::new(RwLock::new(HashMap::new())),
            market_rules: Arc::new(RwLock::new(market_rules)),
            risk_evaluator,
            risk_limits,
        })
    }

    fn parse_market_type(value: &str) -> MarketType {
        match value.trim().to_ascii_lowercase().as_str() {
            "spot" => MarketType::Spot,
            "futures" | "perpetual" => MarketType::Futures,
            other => {
                log::warn!("未识别的market_type: '{}', 默认使用Spot", other);
                MarketType::Spot
            }
        }
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn validate_config(&self) -> Result<()> {
        if !self.config.strategy.enabled {
            return Err(anyhow!("strategy is disabled in config"));
        }
        if self.config.trading.symbol.trim().is_empty() {
            return Err(anyhow!("trading.symbol is required"));
        }
        if self.config.trading.order_size_usdc <= 0.0 {
            return Err(anyhow!("trading.order_size_usdc must be positive"));
        }
        if self.config.trading.max_inventory <= 0.0 {
            return Err(anyhow!("trading.max_inventory must be positive"));
        }
        if self.config.trading.min_spread_bp <= 0.0
            || self.config.trading.max_spread_bp < self.config.trading.min_spread_bp
        {
            return Err(anyhow!(
                "invalid spread bounds min={} max={}",
                self.config.trading.min_spread_bp,
                self.config.trading.max_spread_bp
            ));
        }
        if self.config.trading.refresh_interval_secs == 0 {
            return Err(anyhow!("trading.refresh_interval_secs must be positive"));
        }
        if self.config.as_params.risk_aversion <= 0.0 {
            return Err(anyhow!("as_params.risk_aversion must be positive"));
        }
        if self.config.as_params.order_book_intensity <= 0.0 {
            return Err(anyhow!("as_params.order_book_intensity must be positive"));
        }
        if self.config.as_params.time_horizon_seconds == 0 {
            return Err(anyhow!("as_params.time_horizon_seconds must be positive"));
        }
        let vol = &self.config.as_params.volatility;
        if vol.lookback_periods < 2 {
            return Err(anyhow!("volatility.lookback_periods must be >= 2"));
        }
        if vol.update_interval == 0 {
            return Err(anyhow!("volatility.update_interval must be positive"));
        }
        if !(0.0..=1.0).contains(&vol.decay_factor) || vol.decay_factor == 0.0 {
            return Err(anyhow!("volatility.decay_factor must be in (0, 1]"));
        }
        Ok(())
    }

    async fn account(&self) -> Result<Arc<AccountInfo>> {
        self.account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow!("Account not found: {}", self.config.account.account_id))
    }

    async fn sync_market_rules(&self) -> Result<()> {
        let account = self.account().await?;
        match account
            .exchange
            .get_symbol_info(&self.config.trading.symbol, self.market_type())
            .await
        {
            Ok(info) => {
                if !info.is_trading {
                    return Err(anyhow!(
                        "symbol {} is not trading on {:?}",
                        self.config.trading.symbol,
                        self.market_type()
                    ));
                }
                let rules = MarketRules::from_exchange(
                    info.tick_size,
                    info.step_size,
                    info.min_notional.unwrap_or(0.0),
                    info.min_order_size,
                    info.max_order_size,
                    self.market_type(),
                );
                log::info!(
                    "A-S精度同步: symbol={} tick={} step={} min_notional={} min_qty={} max_qty={} market={:?}",
                    self.config.trading.symbol,
                    rules.tick_size,
                    rules.step_size,
                    rules.min_notional,
                    rules.min_order_size,
                    rules.max_order_size,
                    rules.market_type
                );
                *self.market_rules.write().await = rules;
            }
            Err(err) => {
                log::warn!(
                    "A-S获取交易所精度失败，使用配置精度: symbol={} err={}",
                    self.config.trading.symbol,
                    err
                );
            }
        }
        Ok(())
    }

    async fn current_liquidity_spread_multiplier(&self) -> f64 {
        let Some(market_specific) = &self.config.market_specific else {
            return 1.0;
        };
        let Some(liquidity_check) = &market_specific.liquidity_check else {
            return 1.0;
        };
        if !liquidity_check.enabled {
            return 1.0;
        }

        let cache = self.orderbook_cache.read().await;
        let depth_usd = cache.get("top_depth_usd").copied().unwrap_or(0.0);
        if depth_usd > 0.0 && depth_usd < liquidity_check.min_depth_usd {
            liquidity_check.low_liquidity_spread_multiplier.max(1.0)
        } else {
            1.0
        }
    }

    fn dry_run_enabled(&self) -> bool {
        self.config
            .debug
            .as_ref()
            .map(|debug| debug.dry_run)
            .unwrap_or(false)
    }

    /// 计算预留价格 (Reservation Price)
    async fn calculate_reservation_price(&self) -> f64 {
        let state = self.state.read().await;
        let gamma = self.config.as_params.risk_aversion;
        let sigma = state.volatility;
        let inventory = state.inventory;
        let time_to_end = self.config.as_params.time_horizon_seconds as f64;

        let reservation_adjustment = inventory * gamma * sigma.powi(2) * time_to_end / 3600.0;
        state.mid_price - reservation_adjustment
    }

    /// 计算最优价差。
    async fn calculate_optimal_spread(&self) -> f64 {
        let state = self.state.read().await;
        let gamma = self.config.as_params.risk_aversion;
        let k = self.config.as_params.order_book_intensity;
        let sigma = state.volatility;
        let time_to_end = self.config.as_params.time_horizon_seconds as f64;

        let time_component = gamma * sigma.powi(2) * time_to_end / 3600.0;
        let intensity_component = (2.0 / gamma) * (1.0 + gamma / k).ln();
        let mut base_spread = time_component + intensity_component;
        if let Some(market_specific) = &self.config.market_specific {
            if let Some(volatility_handling) = &market_specific.volatility_handling {
                if volatility_handling.adaptive_spread {
                    let daily_vol = state.volatility / 365.0_f64.sqrt();
                    let multiplier = if daily_vol < 0.05 {
                        volatility_handling.volatility_multiplier.low
                    } else if daily_vol < 0.08 {
                        volatility_handling.volatility_multiplier.medium
                    } else {
                        volatility_handling.volatility_multiplier.high
                    };
                    base_spread *= multiplier.max(1.0);
                }
            }
        }

        let spread_bp = base_spread * 10000.0;
        let clamped_spread_bp = spread_bp
            .max(self.config.trading.min_spread_bp)
            .min(self.config.trading.max_spread_bp);

        clamped_spread_bp / 10000.0
    }

    /// 计算年化波动率。
    async fn calculate_volatility(&self) -> f64 {
        let prices = self.price_history.read().await;
        if prices.len() < self.config.as_params.volatility.lookback_periods {
            return self.config.risk.volatility_limits.min_volatility.max(1.0) / 100.0;
        }

        let mut returns = Vec::with_capacity(prices.len().saturating_sub(1));
        for i in 1..prices.len() {
            if prices[i] > 0.0 && prices[i - 1] > 0.0 {
                let ret = (prices[i] / prices[i - 1]).ln();
                if ret.is_finite() {
                    returns.push(ret);
                }
            }
        }

        if returns.is_empty() {
            return self.config.risk.volatility_limits.min_volatility.max(1.0) / 100.0;
        }

        let decay = self.config.as_params.volatility.decay_factor;
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        let mut weight = 1.0;

        for ret in returns.iter().rev() {
            weighted_sum += weight * ret.powi(2);
            weight_sum += weight;
            weight *= decay;
        }

        if weight_sum <= 0.0 {
            return self.config.risk.volatility_limits.min_volatility.max(1.0) / 100.0;
        }

        let variance = weighted_sum / weight_sum;
        let volatility = variance.sqrt();
        let periods_per_year =
            365.0 * 24.0 * 3600.0 / self.config.as_params.volatility.update_interval as f64;
        (volatility * periods_per_year.sqrt()).max(0.0001)
    }

    /// 生成报价。
    async fn generate_quotes(&self) -> Result<(f64, f64)> {
        let reservation_price = self.calculate_reservation_price().await;
        let optimal_spread = self.calculate_optimal_spread().await;
        let half_spread = optimal_spread / 2.0;

        let state = self.state.read().await;
        if state.mid_price <= 0.0 || !state.mid_price.is_finite() {
            return Err(anyhow!(
                "invalid mid price for quote generation: {}",
                state.mid_price
            ));
        }

        let mut bid_adjustment = 0.0;
        let mut ask_adjustment = 0.0;
        if self.config.as_params.inventory_skew.enabled && self.config.trading.max_inventory > 0.0 {
            let inventory_ratio = state.inventory / self.config.trading.max_inventory;
            let target_ratio = self.config.as_params.inventory_skew.target_inventory_ratio;
            let skew_factor = self.config.as_params.inventory_skew.skew_factor;
            let inventory_imbalance = inventory_ratio - target_ratio;

            if inventory_imbalance > 0.0 {
                bid_adjustment = -inventory_imbalance * skew_factor * half_spread;
                ask_adjustment = inventory_imbalance * skew_factor * half_spread * 0.5;
            } else {
                bid_adjustment = -inventory_imbalance * skew_factor * half_spread * 0.5;
                ask_adjustment = inventory_imbalance * skew_factor * half_spread;
            }
        }

        let raw_bid = reservation_price - half_spread + bid_adjustment;
        let raw_ask = reservation_price + half_spread + ask_adjustment;
        let rules = self.market_rules.read().await.clone();
        let min_tick = rules
            .tick_size
            .max(10_f64.powi(-(rules.price_digits as i32)));
        let liquidity_multiplier = self.current_liquidity_spread_multiplier().await;
        let adjusted_bid = (reservation_price - half_spread * liquidity_multiplier
            + bid_adjustment)
            .min(state.mid_price)
            .min(raw_bid);
        let adjusted_ask =
            (reservation_price + half_spread * liquidity_multiplier + ask_adjustment)
                .max(state.mid_price)
                .max(raw_ask);
        let bid = rules.round_bid(adjusted_bid.min(adjusted_ask - min_tick));
        let ask = rules.round_ask(adjusted_ask.max(bid + min_tick));

        if bid <= 0.0 || ask <= bid {
            return Err(anyhow!(
                "invalid quotes after precision: bid={} ask={} mid={} tick={}",
                bid,
                ask,
                state.mid_price,
                rules.tick_size
            ));
        }

        Ok((bid, ask))
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
                (state.inventory_value.abs() / self.config.risk.inventory_risk.max_position_value)
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
                self.running.store(false, Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    /// 检查风险限制。
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

    async fn sync_inventory(&self) -> Result<()> {
        self.update_inventory().await
    }

    async fn update_inventory(&self) -> Result<()> {
        let account = self.account().await?;

        match self.market_type() {
            MarketType::Futures => {
                let positions = account
                    .exchange
                    .get_positions(Some(&self.config.trading.symbol))
                    .await?;
                let mut state = self.state.write().await;
                let mut found = false;

                for position in positions {
                    if position.symbol == self.config.trading.symbol {
                        state.inventory = position.amount;
                        state.inventory_value = (state.inventory * state.mid_price).abs();
                        state.unrealized_pnl = position.unrealized_pnl;
                        Self::refresh_drawdown(&mut state);
                        found = true;
                        break;
                    }
                }

                if !found {
                    state.inventory = 0.0;
                    state.inventory_value = 0.0;
                    state.unrealized_pnl = 0.0;
                    state.avg_entry_price = 0.0;
                    Self::refresh_drawdown(&mut state);
                }
            }
            MarketType::Spot => {
                let balances = account.exchange.get_balance(MarketType::Spot).await?;
                let base_asset = self
                    .config
                    .trading
                    .symbol
                    .split('/')
                    .next()
                    .unwrap_or("")
                    .to_ascii_uppercase();

                let mut state = self.state.write().await;
                if let Some(balance) = balances
                    .iter()
                    .find(|b| b.currency.eq_ignore_ascii_case(&base_asset))
                {
                    state.inventory = balance.total;
                    state.inventory_value = (state.inventory * state.mid_price).abs();
                } else {
                    state.inventory = 0.0;
                    state.inventory_value = 0.0;
                }
                state.unrealized_pnl = 0.0;
                Self::refresh_drawdown(&mut state);
            }
        }

        Ok(())
    }

    fn refresh_drawdown(state: &mut ASState) {
        let equity = state.realized_pnl + state.unrealized_pnl;
        if equity > state.equity_peak {
            state.equity_peak = equity;
        }
        state.max_drawdown = (state.equity_peak - equity).max(0.0);
        state.daily_pnl = equity;
        state.last_update_time = Utc::now();
    }

    async fn place_orders(&self, bid: f64, ask: f64) -> Result<()> {
        let inventory = self.state.read().await.inventory;
        let mut requests = Vec::new();

        if inventory < self.config.trading.max_inventory {
            requests.push(self.build_order_request(OrderSide::Buy, bid).await?);
        }
        if inventory > -self.config.trading.max_inventory {
            requests.push(self.build_order_request(OrderSide::Sell, ask).await?);
        }

        if requests.is_empty() {
            log::warn!(
                "A-S库存达到边界，跳过挂单 inventory={:.6} max={:.6}",
                inventory,
                self.config.trading.max_inventory
            );
            return Ok(());
        }

        if self.dry_run_enabled() {
            for request in requests {
                log::info!(
                    "[DRY_RUN] A-S拟下单: {:?} {} @ {:?} qty={}",
                    request.side,
                    request.symbol,
                    request.price,
                    request.amount
                );
            }
            return Ok(());
        }

        let account = self.account().await?;
        for request in requests {
            let side = request.side;
            let price = request.price.unwrap_or(0.0);
            let amount = request.amount;
            let client_order_id = request.client_order_id.clone();

            match account.exchange.create_order(request).await {
                Ok(placed_order) => {
                    self.record_placed_order(&placed_order, client_order_id, side, price, amount)
                        .await;
                    match side {
                        OrderSide::Buy => log::info!("📗 A-S买单成功: {} @ {:.8}", amount, price),
                        OrderSide::Sell => log::info!("📕 A-S卖单成功: {} @ {:.8}", amount, price),
                    }
                }
                Err(e) => {
                    log::error!("❌ A-S{:?}单失败: {}", side, e);
                }
            }
        }

        Ok(())
    }

    async fn build_order_request(&self, side: OrderSide, price: f64) -> Result<OrderRequest> {
        if price <= 0.0 || !price.is_finite() {
            return Err(anyhow!("invalid order price: {}", price));
        }

        let rules = self.market_rules.read().await.clone();
        let price = match side {
            OrderSide::Buy => rules.round_bid(price),
            OrderSide::Sell => rules.round_ask(price),
        };
        let qty = self.calculate_order_qty(price, &rules)?;
        let client_order_id = Some(format!(
            "AS_{}_{}",
            match side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
            uuid::Uuid::new_v4().simple()
        ));

        Ok(OrderRequest {
            symbol: self.config.trading.symbol.clone(),
            side,
            order_type: OrderType::Limit,
            amount: qty,
            price: Some(price),
            market_type: self.market_type(),
            params: None,
            reduce_only: if self.market_type() == MarketType::Futures {
                Some(self.config.trading.order_config.reduce_only)
            } else {
                None
            },
            post_only: if self.config.trading.order_config.post_only {
                Some(true)
            } else {
                None
            },
            time_in_force: Some(self.config.trading.order_config.time_in_force.clone()),
            client_order_id,
        })
    }

    fn calculate_order_qty(&self, price: f64, rules: &MarketRules) -> Result<f64> {
        let mut qty = self.config.trading.order_size_usdc / price;
        qty = qty.max(rules.min_order_size);
        qty = rules.round_qty(qty);

        if rules.min_notional > 0.0 && qty * price + 1e-9 < rules.min_notional {
            qty = round_up_to_step(
                rules.min_notional / price,
                rules.step_size,
                rules.qty_digits,
            );
        }

        if qty <= 0.0 || !qty.is_finite() {
            return Err(anyhow!("calculated invalid order qty: {}", qty));
        }
        if qty > rules.max_order_size {
            return Err(anyhow!(
                "calculated qty {} exceeds max_order_size {}",
                qty,
                rules.max_order_size
            ));
        }
        if rules.min_notional > 0.0 && qty * price + 1e-9 < rules.min_notional {
            return Err(anyhow!(
                "order notional {:.8} below min_notional {:.8}",
                qty * price,
                rules.min_notional
            ));
        }

        Ok(qty)
    }

    async fn record_placed_order(
        &self,
        order: &Order,
        client_order_id: Option<String>,
        fallback_side: OrderSide,
        fallback_price: f64,
        fallback_amount: f64,
    ) {
        let now = Utc::now();
        let side = order.side;
        let price = order.price.unwrap_or(fallback_price);
        let original_qty = if order.amount > 0.0 {
            order.amount
        } else {
            fallback_amount
        };
        let filled_qty = order.filled.max(0.0);
        let status = order.status.clone();
        let order_id = order.id.clone();
        let client_order_id = client_order_id.or_else(|| {
            order
                .info
                .get("clientOrderId")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
        });

        let mut state = self.state.write().await;
        state.open_orders.insert(
            order_id.clone(),
            ASOrderState {
                exchange_order_id: order_id.clone(),
                client_order_id,
                side,
                price,
                original_qty,
                filled_qty,
                status: status.clone(),
                created_at: now,
                updated_at: now,
            },
        );

        match side {
            OrderSide::Buy => {
                state.active_buy_order = Some(order_id);
                state.last_buy_price = price;
            }
            OrderSide::Sell => {
                state.active_sell_order = Some(order_id);
                state.last_sell_price = price;
            }
        }

        if filled_qty > 0.0 {
            let fill_price = if price > 0.0 { price } else { fallback_price };
            Self::apply_fill_to_state(&mut state, fallback_side, filled_qty, fill_price, 0.0);
        }
    }

    async fn cancel_existing_orders(&self) -> Result<()> {
        let orders: Vec<String> = {
            let state = self.state.read().await;
            state
                .open_orders
                .values()
                .filter(|order| !order.is_terminal())
                .map(|order| order.exchange_order_id.clone())
                .collect()
        };

        for order_id in orders {
            self.cancel_tracked_order(&order_id).await?;
        }

        Ok(())
    }

    async fn cancel_tracked_order(&self, order_id: &str) -> Result<()> {
        if self.dry_run_enabled() {
            self.mark_order_terminal(order_id, OrderStatus::Canceled)
                .await;
            log::info!("[DRY_RUN] A-S拟撤单: order_id={}", order_id);
            return Ok(());
        }

        let account = self.account().await?;
        match account
            .exchange
            .cancel_order(order_id, &self.config.trading.symbol, self.market_type())
            .await
        {
            Ok(order) => {
                self.mark_order_terminal(&order.id, OrderStatus::Canceled)
                    .await;
                if self.config.monitoring.trade_logging.log_cancellations {
                    log::info!("A-S撤单成功: order_id={} side={:?}", order.id, order.side);
                }
            }
            Err(err) => {
                log::warn!("A-S撤单失败: order_id={} err={}", order_id, err);
                self.reconcile_open_orders().await?;
            }
        }

        Ok(())
    }

    async fn mark_order_terminal(&self, order_id: &str, status: OrderStatus) {
        let mut state = self.state.write().await;
        let side = if let Some(order) = state.open_orders.get_mut(order_id) {
            order.status = status;
            order.updated_at = Utc::now();
            Some(order.side)
        } else {
            None
        };

        match side {
            Some(OrderSide::Buy) if state.active_buy_order.as_deref() == Some(order_id) => {
                state.active_buy_order = None;
            }
            Some(OrderSide::Sell) if state.active_sell_order.as_deref() == Some(order_id) => {
                state.active_sell_order = None;
            }
            _ => {}
        }
    }

    async fn reconcile_open_orders(&self) -> Result<()> {
        let account = self.account().await?;
        let open_orders = account
            .exchange
            .get_open_orders(Some(&self.config.trading.symbol), self.market_type())
            .await?;

        let now = Utc::now();
        let mut state = self.state.write().await;
        let open_ids: HashSet<String> = open_orders.iter().map(|order| order.id.clone()).collect();

        for order in open_orders {
            let price = order.price.unwrap_or(0.0);
            let entry = state
                .open_orders
                .entry(order.id.clone())
                .or_insert(ASOrderState {
                    exchange_order_id: order.id.clone(),
                    client_order_id: order
                        .info
                        .get("clientOrderId")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned),
                    side: order.side,
                    price,
                    original_qty: order.amount,
                    filled_qty: order.filled,
                    status: order.status.clone(),
                    created_at: order.timestamp,
                    updated_at: now,
                });
            entry.price = price;
            entry.original_qty = order.amount;
            entry.filled_qty = order.filled.max(entry.filled_qty);
            entry.status = order.status.clone();
            entry.updated_at = now;
        }

        for tracked in state.open_orders.values_mut() {
            if !tracked.is_terminal() && !open_ids.contains(&tracked.exchange_order_id) {
                tracked.status = OrderStatus::Canceled;
                tracked.updated_at = now;
            }
        }

        state.active_buy_order = state
            .open_orders
            .values()
            .find(|order| order.side == OrderSide::Buy && !order.is_terminal())
            .map(|order| order.exchange_order_id.clone());
        state.active_sell_order = state
            .open_orders
            .values()
            .find(|order| order.side == OrderSide::Sell && !order.is_terminal())
            .map(|order| order.exchange_order_id.clone());

        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        log::info!("🔄 撤销所有挂单...");
        if self.dry_run_enabled() {
            let mut state = self.state.write().await;
            for order in state.open_orders.values_mut() {
                if !order.is_terminal() {
                    order.status = OrderStatus::Canceled;
                    order.updated_at = Utc::now();
                }
            }
            state.active_buy_order = None;
            state.active_sell_order = None;
            log::info!("[DRY_RUN] 已标记所有本地挂单为取消");
            return Ok(());
        }

        let account = self.account().await?;
        account
            .exchange
            .cancel_all_orders(
                Some(self.config.trading.symbol.as_str()),
                self.market_type(),
            )
            .await?;

        let mut state = self.state.write().await;
        for order in state.open_orders.values_mut() {
            if !order.is_terminal() {
                order.status = OrderStatus::Canceled;
                order.updated_at = Utc::now();
            }
        }
        state.active_buy_order = None;
        state.active_sell_order = None;

        log::info!("✅ 已撤销所有订单");
        Ok(())
    }

    async fn handle_order_update_state(state: &Arc<RwLock<ASState>>, order: Order, symbol: &str) {
        if !Self::symbol_matches(&order.symbol, symbol) {
            return;
        }

        let mut state = state.write().await;
        let now = Utc::now();
        let order_id = order.id.clone();
        let previous_filled = state
            .open_orders
            .get(&order_id)
            .map(|tracked| tracked.filled_qty)
            .unwrap_or(0.0);
        let cumulative_filled = order.filled.max(previous_filled);
        let delta_qty = (cumulative_filled - previous_filled).max(0.0);
        let price = order.price.unwrap_or(0.0);

        if let Some(tracked) = state.open_orders.get_mut(&order_id) {
            tracked.status = order.status.clone();
            tracked.price = price;
            tracked.original_qty = order.amount;
            tracked.filled_qty = cumulative_filled;
            tracked.updated_at = now;
        } else {
            state.open_orders.insert(
                order_id.clone(),
                ASOrderState {
                    exchange_order_id: order_id.clone(),
                    client_order_id: order
                        .info
                        .get("clientOrderId")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned),
                    side: order.side,
                    price,
                    original_qty: order.amount,
                    filled_qty: cumulative_filled,
                    status: order.status.clone(),
                    created_at: order.timestamp,
                    updated_at: now,
                },
            );
        }

        if delta_qty > 0.0 && price > 0.0 {
            Self::apply_fill_to_state(&mut state, order.side, delta_qty, price, 0.0);
        }

        if matches!(
            order.status,
            OrderStatus::Closed
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Rejected
        ) {
            match order.side {
                OrderSide::Buy if state.active_buy_order.as_deref() == Some(&order_id) => {
                    state.active_buy_order = None;
                }
                OrderSide::Sell if state.active_sell_order.as_deref() == Some(&order_id) => {
                    state.active_sell_order = None;
                }
                _ => {}
            }
        }
    }

    async fn handle_execution_report_state(
        state: &Arc<RwLock<ASState>>,
        exec: ExecutionReport,
        symbol: &str,
    ) {
        if !Self::symbol_matches(&exec.symbol, symbol) {
            return;
        }

        let mut state = state.write().await;
        let now = Utc::now();
        let order_id = exec.order_id.clone();
        let previous_filled = state
            .open_orders
            .get(&order_id)
            .map(|tracked| tracked.filled_qty)
            .unwrap_or(0.0);
        let cumulative_filled = exec.executed_amount.max(previous_filled);
        let delta_qty = (cumulative_filled - previous_filled).max(0.0);

        if let Some(tracked) = state.open_orders.get_mut(&order_id) {
            tracked.status = exec.status.clone();
            tracked.price = exec.price;
            tracked.original_qty = exec.amount;
            tracked.filled_qty = cumulative_filled;
            tracked.updated_at = now;
        } else {
            state.open_orders.insert(
                order_id.clone(),
                ASOrderState {
                    exchange_order_id: order_id.clone(),
                    client_order_id: exec.client_order_id.clone(),
                    side: exec.side,
                    price: exec.price,
                    original_qty: exec.amount,
                    filled_qty: cumulative_filled,
                    status: exec.status.clone(),
                    created_at: exec.timestamp,
                    updated_at: now,
                },
            );
        }

        if delta_qty > 0.0 {
            let fill_price = if exec.executed_price > 0.0 {
                exec.executed_price
            } else {
                exec.price
            };
            Self::apply_fill_to_state(
                &mut state,
                exec.side,
                delta_qty,
                fill_price,
                exec.commission,
            );
        }

        if matches!(
            exec.status,
            OrderStatus::Closed
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Rejected
        ) {
            match exec.side {
                OrderSide::Buy if state.active_buy_order.as_deref() == Some(&order_id) => {
                    state.active_buy_order = None;
                }
                OrderSide::Sell if state.active_sell_order.as_deref() == Some(&order_id) => {
                    state.active_sell_order = None;
                }
                _ => {}
            }
        }
    }

    fn apply_fill_to_state(
        state: &mut ASState,
        side: OrderSide,
        qty: f64,
        price: f64,
        commission: f64,
    ) {
        if qty <= 0.0 || price <= 0.0 {
            return;
        }

        let previous_equity = state.realized_pnl + state.unrealized_pnl;
        let signed_qty = match side {
            OrderSide::Buy => qty,
            OrderSide::Sell => -qty,
        };

        Self::apply_position_fill(state, signed_qty, price);
        state.inventory_value = (state.inventory * state.mid_price).abs();
        state.fees_paid += commission.max(0.0);
        state.realized_pnl -= commission.max(0.0);
        state.total_trades += 1;

        match side {
            OrderSide::Buy => state.buy_fills += 1,
            OrderSide::Sell => state.sell_fills += 1,
        }

        Self::refresh_drawdown(state);
        let new_equity = state.realized_pnl + state.unrealized_pnl;
        if new_equity < previous_equity {
            state.consecutive_losses += 1;
        } else {
            state.consecutive_losses = 0;
        }
    }

    fn apply_position_fill(state: &mut ASState, signed_qty: f64, price: f64) {
        let old_qty = state.inventory;
        let new_qty = old_qty + signed_qty;

        if old_qty.abs() < f64::EPSILON || old_qty.signum() == signed_qty.signum() {
            let total_abs = old_qty.abs() + signed_qty.abs();
            if total_abs > 0.0 {
                state.avg_entry_price =
                    (state.avg_entry_price * old_qty.abs() + price * signed_qty.abs()) / total_abs;
            }
        } else {
            let closing_qty = old_qty.abs().min(signed_qty.abs());
            let pnl_per_unit = if old_qty > 0.0 {
                price - state.avg_entry_price
            } else {
                state.avg_entry_price - price
            };
            state.realized_pnl += pnl_per_unit * closing_qty;

            if new_qty.abs() < f64::EPSILON {
                state.avg_entry_price = 0.0;
            } else if old_qty.signum() != new_qty.signum() {
                state.avg_entry_price = price;
            }
        }

        state.inventory = if new_qty.abs() < 1e-12 { 0.0 } else { new_qty };
    }

    fn symbol_matches(exchange_symbol: &str, configured_symbol: &str) -> bool {
        exchange_symbol.eq_ignore_ascii_case(configured_symbol)
            || exchange_symbol.eq_ignore_ascii_case(&configured_symbol.replace('/', ""))
    }

    /// 启动WebSocket连接。
    async fn start_websocket(&mut self) -> Result<()> {
        if !self.config.data_sources.websocket.enabled {
            log::info!("📡 WebSocket未启用，使用REST API轮询模式");
            return Ok(());
        }

        log::info!("🔌 启动WebSocket连接...");
        let account = self.account().await?;
        let binance_exchange = account
            .exchange
            .as_any()
            .downcast_ref::<BinanceExchange>()
            .ok_or_else(|| anyhow!("Not a Binance exchange"))?;
        let market_type = self.market_type();

        match binance_exchange.create_user_data_stream(market_type).await {
            Ok(listen_key) => {
                log::info!("✅ 获取listenKey成功");
                self.listen_key = Some(listen_key.clone());

                let ws_url = match market_type {
                    MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
                    MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
                };
                let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);

                if let Err(e) = ws_client.connect().await {
                    log::error!("❌ WebSocket连接失败: {}", e);
                    return Err(anyhow!("WebSocket connection failed: {}", e));
                }

                log::info!("📡 WebSocket连接成功，开始监听用户数据流");
                self.ws_client = Some(Arc::new(tokio::sync::Mutex::new(ws_client)));
                self.ws_task = Some(self.spawn_websocket_handler());
            }
            Err(err) => {
                log::warn!("⚠️ 获取listenKey失败，将使用REST API模式: {}", err);
            }
        }

        Ok(())
    }

    fn spawn_websocket_handler(&self) -> JoinHandle<()> {
        let ws_client = self.ws_client.clone();
        let state = self.state.clone();
        let symbol = self.config.trading.symbol.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            if let Some(client) = ws_client {
                let mut client = client.lock().await;

                while running.load(Ordering::Relaxed) {
                    match client.receive().await {
                        Ok(Some(msg)) => {
                            if let Ok(ws_msg) = client.parse_binance_message(&msg) {
                                match ws_msg {
                                    WsMessage::Order(order) => {
                                        if Self::symbol_matches(&order.symbol, &symbol) {
                                            log::info!(
                                                "📬 收到订单更新: {} {:?} {:?} @ {:?} filled: {}",
                                                order.symbol,
                                                order.side,
                                                order.status,
                                                order.price,
                                                order.filled
                                            );
                                            Self::handle_order_update_state(&state, order, &symbol)
                                                .await;
                                        }
                                    }
                                    WsMessage::ExecutionReport(exec) => {
                                        if Self::symbol_matches(&exec.symbol, &symbol) {
                                            log::info!(
                                                "📬 收到执行报告: {} {:?} {:?} @ {} cum_qty: {} avg: {}",
                                                exec.symbol,
                                                exec.side,
                                                exec.status,
                                                exec.price,
                                                exec.executed_amount,
                                                exec.executed_price
                                            );
                                            Self::handle_execution_report_state(
                                                &state, exec, &symbol,
                                            )
                                            .await;
                                        }
                                    }
                                    WsMessage::Balance(_) => {
                                        log::debug!("📊 收到账户余额更新");
                                    }
                                    _ => {}
                                }
                            } else if let Ok(json) = serde_json::from_str::<serde_json::Value>(&msg)
                            {
                                Self::handle_raw_binance_user_message(&state, &symbol, json).await;
                            }
                        }
                        Ok(None) => {
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
        })
    }

    async fn handle_raw_binance_user_message(
        state: &Arc<RwLock<ASState>>,
        symbol: &str,
        json: serde_json::Value,
    ) {
        if json["e"] != "ORDER_TRADE_UPDATE" {
            if json["e"] == "ACCOUNT_UPDATE" {
                log::debug!("📊 收到账户更新");
            }
            return;
        }

        let order_symbol = json["o"]["s"].as_str().unwrap_or("");
        if !Self::symbol_matches(order_symbol, symbol) {
            return;
        }

        let side = match json["o"]["S"].as_str().unwrap_or("") {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => return,
        };
        let status = match json["o"]["X"].as_str().unwrap_or("") {
            "NEW" => OrderStatus::Open,
            "FILLED" => OrderStatus::Closed,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "CANCELED" => OrderStatus::Canceled,
            "EXPIRED" => OrderStatus::Expired,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
        };
        let price = json["o"]["p"]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let amount = json["o"]["q"]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let executed_amount = json["o"]["z"]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let executed_price = json["o"]["ap"]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(price);
        let commission = json["o"]["n"]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);

        log::info!(
            "📬 收到订单更新: {} {:?} {:?} @ {} cum_qty: {}",
            order_symbol,
            side,
            status,
            price,
            executed_amount
        );

        Self::handle_execution_report_state(
            state,
            ExecutionReport {
                symbol: order_symbol.to_string(),
                order_id: json["o"]["i"].as_i64().unwrap_or_default().to_string(),
                client_order_id: json["o"]["c"].as_str().map(ToOwned::to_owned),
                side,
                order_type: OrderType::Limit,
                status,
                price,
                amount,
                executed_amount,
                executed_price,
                commission,
                commission_asset: json["o"]["N"].as_str().unwrap_or("").to_string(),
                timestamp: Utc::now(),
                is_maker: json["o"]["m"].as_bool().unwrap_or(true),
            },
            symbol,
        )
        .await;
    }

    /// 启动策略。
    pub async fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
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

        self.validate_config()?;
        self.sync_market_rules().await?;

        if let Err(e) = self.start_websocket().await {
            log::warn!("⚠️ WebSocket启动失败: {}，将使用REST API模式", e);
        }

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            self.config.trading.refresh_interval_secs,
        ));

        if let Err(e) = self.cancel_all_orders().await {
            log::error!("撤销挂单失败: {}", e);
        }
        self.update_inventory().await?;
        self.reconcile_open_orders().await?;

        let mut current_bid = 0.0;
        let mut current_ask = 0.0;
        let mut last_inventory_sync = Utc::now();
        let mut last_log_time: Option<i64> = None;

        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;

            if let Err(e) = self.update_market_data().await {
                log::error!("更新市场数据失败: {}", e);
                continue;
            }

            let now = Utc::now();
            let state = self.state.write().await;
            if (now - state.last_volatility_update).num_seconds()
                > self.config.as_params.volatility.update_interval as i64
            {
                drop(state);
                let new_volatility = self.calculate_volatility().await;
                let mut state = self.state.write().await;
                state.volatility = new_volatility;
                state.last_volatility_update = now;
                log::debug!("📈 更新波动率: {:.2}%", new_volatility * 100.0);
            } else {
                drop(state);
            }

            if let Err(e) = self.update_inventory().await {
                log::error!("更新库存失败: {}", e);
                continue;
            }

            match self.check_risk_limits().await {
                Ok(true) => {}
                Ok(false) => {
                    log::warn!("⚠️ 风险检查未通过，暂停交易");
                    let _ = self.cancel_existing_orders().await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    continue;
                }
                Err(e) => {
                    log::error!("风险检查失败: {}", e);
                    continue;
                }
            }

            let now = Utc::now();
            if now.signed_duration_since(last_inventory_sync).num_seconds()
                > INVENTORY_SYNC_INTERVAL_SECS
            {
                if let Err(e) = self.sync_inventory().await {
                    log::error!("同步库存失败: {}", e);
                }
                if let Err(e) = self.reconcile_open_orders().await {
                    log::error!("同步挂单失败: {}", e);
                }
                last_inventory_sync = now;
            }

            let (bid, ask) = match self.generate_quotes().await {
                Ok(quotes) => quotes,
                Err(e) => {
                    log::error!("生成报价失败: {}", e);
                    continue;
                }
            };

            let price_changed = if current_bid == 0.0 || current_ask == 0.0 {
                true
            } else {
                (bid - current_bid).abs() / current_bid > PRICE_REFRESH_THRESHOLD
                    || (ask - current_ask).abs() / current_ask > PRICE_REFRESH_THRESHOLD
            };

            if price_changed {
                log::info!(
                    "💹 A-S报价更新 - 买: {:.8}, 卖: {:.8}, 价差: {:.2}bp",
                    bid,
                    ask,
                    (ask - bid) / bid * 10000.0
                );

                if let Err(e) = self.cancel_existing_orders().await {
                    log::error!("取消订单失败: {}", e);
                }

                if let Err(e) = self.place_orders(bid, ask).await {
                    log::error!("下单失败: {}", e);
                } else {
                    current_bid = bid;
                    current_ask = ask;
                }
            } else {
                log::debug!("价格变化小于阈值，保持当前订单");
            }

            let now_timestamp = Utc::now().timestamp();
            if last_log_time.map_or(true, |last| now_timestamp - last > LOG_INTERVAL_SECS) {
                let state = self.state.read().await;
                log::info!(
                    "📊 策略状态 - 库存: {:.4}, 成本: {:.8}, 未实现盈亏: {:.2}, 已实现盈亏: {:.2}, 日盈亏: {:.2}, 手续费: {:.4}",
                    state.inventory,
                    state.avg_entry_price,
                    state.unrealized_pnl,
                    state.realized_pnl,
                    state.daily_pnl,
                    state.fees_paid
                );
                last_log_time = Some(now_timestamp);
            }
        }

        self.stop().await
    }

    pub async fn run_until_shutdown(&mut self) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
        let running = self.running.clone();
        let signal_task = tokio::spawn(async move {
            if let Err(err) = tokio::signal::ctrl_c().await {
                log::error!("监听退出信号失败: {}", err);
            } else {
                log::info!("收到退出信号，正在停止A-S策略...");
            }
            running.store(false, Ordering::Relaxed);
        });

        let result = self.start().await;
        signal_task.abort();
        result
    }

    /// 停止策略。
    pub async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        log::info!("🛑 停止Avellaneda-Stoikov策略");

        self.cancel_all_orders().await?;

        if let Some(task) = self.ws_task.take() {
            task.abort();
            let _ = task.await;
        }

        if let Some(listen_key) = self.listen_key.take() {
            if let Ok(account) = self.account().await {
                if let Err(err) = account
                    .exchange
                    .close_user_data_stream(&listen_key, self.market_type())
                    .await
                {
                    log::debug!("关闭用户数据流失败: {}", err);
                }
            }
        }

        let state = self.state.read().await;
        log::info!("📊 最终统计:");
        log::info!(
            "  总交易: {}, 买成交: {}, 卖成交: {}, 手续费: {:.4}",
            state.total_trades,
            state.buy_fills,
            state.sell_fills,
            state.fees_paid
        );
        log::info!(
            "  实现盈亏: {:.2}, 未实现盈亏: {:.2}, 最大回撤: {:.2}",
            state.realized_pnl,
            state.unrealized_pnl,
            state.max_drawdown
        );

        Ok(())
    }

    async fn update_market_data(&mut self) -> Result<()> {
        let account = self.account().await?;
        let orderbook = account
            .exchange
            .get_orderbook(
                &self.config.trading.symbol,
                self.market_type(),
                Some(self.config.market_data.orderbook_levels.max(5) as u32),
            )
            .await?;

        let (best_bid, best_ask) = match (orderbook.bids.first(), orderbook.asks.first()) {
            (Some(best_bid), Some(best_ask)) => (best_bid, best_ask),
            _ => {
                return Err(anyhow!(
                    "empty orderbook for {}",
                    self.config.trading.symbol
                ))
            }
        };

        if best_bid[0] <= 0.0 || best_ask[0] <= 0.0 || best_bid[0] >= best_ask[0] {
            return Err(anyhow!(
                "invalid orderbook top bid={} ask={}",
                best_bid[0],
                best_ask[0]
            ));
        }

        let mid_price = (best_bid[0] + best_ask[0]) / 2.0;
        let bid_qty = best_bid[1];
        let ask_qty = best_ask[1];
        let top_depth_usd = best_bid[0] * bid_qty + best_ask[0] * ask_qty;

        {
            let mut state = self.state.write().await;
            state.bid_price = best_bid[0];
            state.ask_price = best_ask[0];
            state.mid_price = mid_price;
            state.inventory_value = (state.inventory * state.mid_price).abs();
            state.unrealized_pnl = if state.inventory.abs() > 0.0 && state.avg_entry_price > 0.0 {
                if state.inventory > 0.0 {
                    (state.mid_price - state.avg_entry_price) * state.inventory.abs()
                } else {
                    (state.avg_entry_price - state.mid_price) * state.inventory.abs()
                }
            } else {
                state.unrealized_pnl
            };
            Self::refresh_drawdown(&mut state);
        }

        let mut prices = self.price_history.write().await;
        prices.push_back(mid_price);
        if prices.len() > 1000 {
            prices.pop_front();
        }
        drop(prices);

        let mut volume = self.volume_history.write().await;
        volume.push_back((bid_qty + ask_qty).max(0.0));
        if volume.len() > self.config.market_data.trades_buffer_size.max(1) {
            volume.pop_front();
        }
        drop(volume);

        let mut cache = self.orderbook_cache.write().await;
        cache.insert("best_bid".to_string(), best_bid[0]);
        cache.insert("best_ask".to_string(), best_ask[0]);
        cache.insert("bid_qty".to_string(), bid_qty);
        cache.insert("ask_qty".to_string(), ask_qty);
        cache.insert("top_depth_usd".to_string(), top_depth_usd);

        Ok(())
    }
}

fn round_up_to_step(value: f64, step: f64, digits: u32) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let rounded = if step > 0.0 {
        (value / step).ceil() * step
    } else {
        value
    };
    let factor = 10_f64.powi(digits as i32);
    (rounded * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn base_config() -> ASConfig {
        ASConfig {
            strategy: StrategyConfig {
                name: "AS_TEST".to_string(),
                version: "1.0.0".to_string(),
                strategy_type: "avellaneda_stoikov".to_string(),
                description: "test".to_string(),
                enabled: true,
                log_level: "ERROR".to_string(),
            },
            account: AccountConfig {
                account_id: "test".to_string(),
                exchange: "mock".to_string(),
                api_key_env: None,
            },
            trading: TradingConfig {
                symbol: "DCR/USDT".to_string(),
                market_type: "spot".to_string(),
                order_size_usdc: 5.0,
                max_inventory: 10.0,
                min_spread_bp: 10.0,
                max_spread_bp: 100.0,
                refresh_interval_secs: 1,
                price_precision: 2,
                quantity_precision: 3,
                order_config: OrderConfig {
                    post_only: true,
                    time_in_force: "GTC".to_string(),
                    reduce_only: false,
                },
            },
            as_params: ASParameters {
                risk_aversion: 0.8,
                order_book_intensity: 1.5,
                time_horizon_seconds: 900,
                volatility: VolatilityConfig {
                    lookback_periods: 3,
                    update_interval: 20,
                    decay_factor: 0.9,
                },
                inventory_skew: InventorySkewConfig {
                    enabled: true,
                    skew_factor: 0.5,
                    target_inventory_ratio: 0.0,
                },
                spread_adjustment: SpreadAdjustmentConfig {
                    volume_factor: 0.0,
                    depth_factor: 0.0,
                    pressure_factor: 0.0,
                },
            },
            market_data: MarketDataConfig {
                orderbook_levels: 5,
                trades_buffer_size: 100,
                kline: KlineConfig {
                    interval: "1m".to_string(),
                    history_size: 10,
                },
            },
            risk: RiskConfig {
                max_unrealized_loss: 100.0,
                max_daily_loss: 100.0,
                inventory_risk: InventoryRiskConfig {
                    max_position_value: 1_000.0,
                    imbalance_threshold: 0.8,
                },
                stop_loss: StopLossConfig {
                    enabled: true,
                    stop_loss_pct: 0.05,
                    cooldown_seconds: 60,
                },
                volatility_limits: VolatilityLimitsConfig {
                    max_volatility: 300.0,
                    min_volatility: 20.0,
                },
                emergency_stop: EmergencyStopConfig {
                    consecutive_losses: 3,
                    max_drawdown_pct: 0.1,
                },
            },
            performance: PerformanceConfig {
                order_aggregation: OrderAggregationConfig {
                    enabled: false,
                    min_price_diff_bp: 1.0,
                },
                smart_cancellation: SmartCancellationConfig {
                    enabled: true,
                    price_drift_threshold_bp: 10.0,
                },
                partial_fill: PartialFillConfig {
                    min_fill_ratio: 0.1,
                    immediate_replace: true,
                },
            },
            data_sources: DataSourcesConfig {
                websocket: WebSocketConfig {
                    enabled: false,
                    streams: vec![],
                    reconnect_interval: 5,
                },
                rest_api: RestApiConfig {
                    enabled: true,
                    refresh_interval: 30,
                    endpoints: vec![],
                },
            },
            monitoring: MonitoringConfig {
                metrics: MetricsConfig {
                    enabled: false,
                    export_interval: 60,
                },
                health_check: HealthCheckConfig {
                    enabled: false,
                    interval: 30,
                },
                trade_logging: TradeLoggingConfig {
                    enabled: false,
                    log_fills: false,
                    log_quotes: false,
                    log_cancellations: false,
                },
                alerts: AlertsConfig {
                    enabled: false,
                    channels: vec![],
                    thresholds: AlertThresholds {
                        inventory_imbalance: 0.5,
                        loss_limit: 0.5,
                        low_liquidity: 1,
                    },
                },
            },
            perpetual: PerpetualConfig {
                funding_rate: FundingRateConfig {
                    consider_funding: false,
                    threshold_pct: 0.0,
                },
                maintenance: MaintenanceConfig {
                    stop_before: 0,
                    resume_after: 0,
                },
            },
            market_specific: None,
            debug: None,
        }
    }

    fn empty_state() -> ASState {
        let now = Utc::now();
        ASState {
            mid_price: 100.0,
            bid_price: 99.9,
            ask_price: 100.1,
            volatility: 0.2,
            inventory: 0.0,
            inventory_value: 0.0,
            active_buy_order: None,
            active_sell_order: None,
            last_buy_price: 0.0,
            last_sell_price: 0.0,
            open_orders: HashMap::new(),
            total_trades: 0,
            buy_fills: 0,
            sell_fills: 0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            fees_paid: 0.0,
            avg_entry_price: 0.0,
            equity_peak: 0.0,
            consecutive_losses: 0,
            daily_pnl: 0.0,
            max_drawdown: 0.0,
            strategy_start_time: now,
            last_update_time: now,
            last_volatility_update: now,
        }
    }

    #[test]
    fn apply_fill_tracks_inventory_average_cost_and_realized_pnl() {
        let mut state = empty_state();

        AvellanedaStoikovStrategy::apply_fill_to_state(
            &mut state,
            OrderSide::Buy,
            2.0,
            100.0,
            0.01,
        );
        assert_eq!(state.inventory, 2.0);
        assert_eq!(state.avg_entry_price, 100.0);
        assert_eq!(state.buy_fills, 1);
        assert!((state.realized_pnl + 0.01).abs() < 1e-9);

        AvellanedaStoikovStrategy::apply_fill_to_state(
            &mut state,
            OrderSide::Sell,
            1.5,
            110.0,
            0.02,
        );
        assert!((state.inventory - 0.5).abs() < 1e-9);
        assert!((state.realized_pnl - 14.97).abs() < 1e-9);
        assert_eq!(state.sell_fills, 1);
        assert_eq!(state.total_trades, 2);
    }

    #[tokio::test]
    async fn execution_report_applies_only_incremental_cumulative_fill() {
        let state = Arc::new(RwLock::new(empty_state()));
        let symbol = "DCR/USDT";
        let now = Utc::now();

        AvellanedaStoikovStrategy::handle_execution_report_state(
            &state,
            ExecutionReport {
                symbol: "DCRUSDT".to_string(),
                order_id: "1".to_string(),
                client_order_id: Some("AS_BUY_1".to_string()),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                status: OrderStatus::PartiallyFilled,
                price: 100.0,
                amount: 1.0,
                executed_amount: 0.4,
                executed_price: 100.0,
                commission: 0.01,
                commission_asset: "USDT".to_string(),
                timestamp: now,
                is_maker: true,
            },
            symbol,
        )
        .await;

        AvellanedaStoikovStrategy::handle_execution_report_state(
            &state,
            ExecutionReport {
                symbol: "DCRUSDT".to_string(),
                order_id: "1".to_string(),
                client_order_id: Some("AS_BUY_1".to_string()),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                status: OrderStatus::Closed,
                price: 100.0,
                amount: 1.0,
                executed_amount: 1.0,
                executed_price: 101.0,
                commission: 0.01,
                commission_asset: "USDT".to_string(),
                timestamp: now,
                is_maker: true,
            },
            symbol,
        )
        .await;

        let state = state.read().await;
        assert!((state.inventory - 1.0).abs() < 1e-9);
        assert_eq!(state.total_trades, 2);
        assert!((state.avg_entry_price - 100.6).abs() < 1e-9);
        assert!(state.open_orders["1"].is_terminal());
    }

    #[tokio::test]
    async fn order_request_respects_min_notional_and_exchange_steps() {
        let manager = Arc::new(AccountManager::new(crate::core::config::Config {
            name: "mock".to_string(),
            testnet: true,
            spot_base_url: "".to_string(),
            futures_base_url: "".to_string(),
            ws_spot_url: "".to_string(),
            ws_futures_url: "".to_string(),
        }));
        let strategy = AvellanedaStoikovStrategy::new(base_config(), manager)
            .await
            .unwrap();
        *strategy.market_rules.write().await =
            MarketRules::from_exchange(0.01, 0.001, 5.0, 0.001, 1000.0, MarketType::Spot);

        let request = strategy
            .build_order_request(OrderSide::Buy, 19.997)
            .await
            .unwrap();
        assert_eq!(request.price, Some(19.99));
        assert!(request.amount * request.price.unwrap() >= 5.0);
        assert!((request.amount * 1000.0).fract().abs() < 1e-9);
    }

    #[test]
    fn risk_snapshot_uses_absolute_inventory_value() {
        let manager = Arc::new(AccountManager::new(crate::core::config::Config {
            name: "mock".to_string(),
            testnet: true,
            spot_base_url: "".to_string(),
            futures_base_url: "".to_string(),
            ws_spot_url: "".to_string(),
            ws_futures_url: "".to_string(),
        }));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let strategy = runtime
            .block_on(AvellanedaStoikovStrategy::new(base_config(), manager))
            .unwrap();

        let mut state = empty_state();
        state.inventory_value = -900.0;
        let snapshot = strategy.build_risk_snapshot(&state);
        assert_eq!(snapshot.exposure.inventory_ratio, Some(0.9));
    }

    #[tokio::test]
    async fn dry_run_cancel_marks_tracked_orders_without_exchange_io() {
        let mut config = base_config();
        config.debug = Some(DebugConfig {
            enabled: true,
            verbose_logging: false,
            dry_run: true,
            save_market_data: false,
        });
        let manager = Arc::new(AccountManager::new(crate::core::config::Config {
            name: "mock".to_string(),
            testnet: true,
            spot_base_url: "".to_string(),
            futures_base_url: "".to_string(),
            ws_spot_url: "".to_string(),
            ws_futures_url: "".to_string(),
        }));
        let strategy = AvellanedaStoikovStrategy::new(config, manager)
            .await
            .unwrap();
        {
            let mut state = strategy.state.write().await;
            state.active_buy_order = Some("dry-buy".to_string());
            state.open_orders.insert(
                "dry-buy".to_string(),
                ASOrderState {
                    exchange_order_id: "dry-buy".to_string(),
                    client_order_id: Some("AS_BUY_DRY".to_string()),
                    side: OrderSide::Buy,
                    price: 10.0,
                    original_qty: 1.0,
                    filled_qty: 0.0,
                    status: OrderStatus::Open,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            );
        }

        strategy.cancel_existing_orders().await.unwrap();

        let state = strategy.state.read().await;
        assert!(state.active_buy_order.is_none());
        assert_eq!(state.open_orders["dry-buy"].status, OrderStatus::Canceled);
    }

    #[test]
    fn config_accepts_market_specific_and_debug_sections() {
        let yaml = r#"
strategy:
  name: "AS_CFG"
  version: "1.0"
  type: "avellaneda_stoikov"
  description: "cfg"
  enabled: true
  log_level: "INFO"
account:
  account_id: "test"
  exchange: "binance"
trading:
  symbol: "DCR/USDT"
  market_type: "spot"
  order_size_usdc: 5.0
  max_inventory: 10.0
  min_spread_bp: 10.0
  max_spread_bp: 100.0
  refresh_interval_secs: 1
  price_precision: 2
  quantity_precision: 3
  order_config:
    post_only: true
    time_in_force: "GTC"
    reduce_only: false
as_params:
  risk_aversion: 1.0
  order_book_intensity: 1.0
  time_horizon_seconds: 900
  volatility:
    lookback_periods: 3
    update_interval: 20
    decay_factor: 0.9
  inventory_skew:
    enabled: true
    skew_factor: 0.5
    target_inventory_ratio: 0.0
  spread_adjustment:
    volume_factor: 0.0
    depth_factor: 0.0
    pressure_factor: 0.0
market_data:
  orderbook_levels: 5
  trades_buffer_size: 100
  kline:
    interval: "1m"
    history_size: 10
risk:
  max_unrealized_loss: 100.0
  max_daily_loss: 100.0
  inventory_risk:
    max_position_value: 1000.0
    imbalance_threshold: 0.8
  stop_loss:
    enabled: true
    stop_loss_pct: 0.05
    cooldown_seconds: 60
  volatility_limits:
    max_volatility: 300.0
    min_volatility: 20.0
  emergency_stop:
    consecutive_losses: 3
    max_drawdown_pct: 0.1
performance:
  order_aggregation:
    enabled: false
    min_price_diff_bp: 1.0
  smart_cancellation:
    enabled: true
    price_drift_threshold_bp: 10.0
  partial_fill:
    min_fill_ratio: 0.1
    immediate_replace: true
data_sources:
  websocket:
    enabled: false
    streams: []
    reconnect_interval: 5
  rest_api:
    enabled: true
    refresh_interval: 30
    endpoints: []
monitoring:
  metrics:
    enabled: false
    export_interval: 60
  health_check:
    enabled: false
    interval: 30
  trade_logging:
    enabled: false
    log_fills: false
    log_quotes: false
    log_cancellations: false
  alerts:
    enabled: false
    channels: []
    thresholds:
      inventory_imbalance: 0.5
      loss_limit: 0.5
      low_liquidity: 1
perpetual:
  funding_rate:
    consider_funding: false
    threshold_pct: 0.0
  maintenance:
    stop_before: 0
    resume_after: 0
market_specific:
  volatility_handling:
    adaptive_spread: true
    volatility_multiplier:
      low: 1.0
      medium: 1.3
      high: 1.8
  liquidity_check:
    enabled: true
    min_depth_usd: 5000.0
    low_liquidity_spread_multiplier: 1.5
debug:
  enabled: true
  verbose_logging: false
  dry_run: true
  save_market_data: false
"#;

        let config: ASConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.market_specific.is_some());
        assert!(config.debug.unwrap().dry_run);
    }
}
