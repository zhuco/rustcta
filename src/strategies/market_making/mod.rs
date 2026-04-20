use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{Mutex, RwLock};

use crate::core::types::{
    MarketType, Order, OrderBook, OrderRequest, OrderSide, OrderStatus, OrderType, Position,
    Ticker, Trade,
};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::strategies::common::application::strategy::{Strategy as AppStrategy, StrategyInstance};
use crate::strategies::common::snapshot::{
    StrategyExposureSnapshot, StrategyPerformanceSnapshot, StrategySnapshot,
};
use crate::strategies::common::{
    RiskAction, RiskDecision, StrategyDeps, StrategyState, StrategyStatus, UnifiedRiskEvaluator,
};
use crate::strategies::Strategy as LegacyStrategy;
use crate::utils::symbol::SymbolConverter;

use mid_price::MidPriceModel;
use order_manager::{ManagedOrder, OrderManager, QuotePair};
use precision::{apply_precision, PrecisionManager, ResolvedPrecision};
use risk::RiskController;
use skew::{InventorySkewModel, SkewAdjustment};
use spread::{QuoteContext, SpreadModel};
use trend::{TrendFilter, TrendSignal};
mod precision;

const TRADE_SIGN_ALPHA: f64 = 0.1;

/// 策略顶层配置
#[derive(Debug, Clone, Deserialize)]
pub struct MarketMakingConfig {
    pub strategy: StrategyMeta,
    pub account: StrategyAccount,
    pub symbols: Vec<SymbolConfig>,
    pub quoting: QuotingConfig,
    pub inventory: InventoryConfig,
    pub trend: TrendConfig,
    pub risk: MarketMakingRiskConfig,
    #[serde(default)]
    pub hedging: Option<HedgingConfig>,
    #[serde(default)]
    pub precision_management: Option<PrecisionManagementConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyMeta {
    pub name: String,
    #[serde(default)]
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyAccount {
    pub account_id: String,
    pub market_type: MarketType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrecisionManagementConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub write_back: bool,
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub lock_path: Option<String>,
    #[serde(default)]
    pub backup_suffix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_tick_size")]
    pub tick_size: f64,
    #[serde(default = "default_lot_size")]
    pub lot_size: f64,
    #[serde(default)]
    pub price_precision: Option<u32>,
    #[serde(default)]
    pub quantity_precision: Option<u32>,
    #[serde(default = "default_min_notional")]
    pub min_notional: f64,
    #[serde(default = "default_order_ttl")]
    pub order_ttl_secs: u64,
    #[serde(default = "default_base_size")]
    pub base_order_size: f64,
    #[serde(default = "default_min_order_size")]
    pub min_order_size: f64,
    #[serde(default)]
    pub max_inventory: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuotingConfig {
    #[serde(default = "default_refresh_ms")]
    pub refresh_interval_ms: u64,
    #[serde(default = "default_base_spread_bps")]
    pub base_spread_bps: f64,
    #[serde(default = "default_min_spread_bps")]
    pub min_spread_bps: f64,
    #[serde(default = "default_max_spread_bps")]
    pub max_spread_bps: f64,
    #[serde(default = "default_vol_sensitivity")]
    pub vol_sensitivity: f64,
    #[serde(default = "default_depth_sensitivity")]
    pub depth_sensitivity: f64,
    #[serde(default = "default_trend_sensitivity")]
    pub trend_sensitivity: f64,
    #[serde(default = "default_price_ema")]
    pub mid_price_alpha: f64,
    #[serde(default = "default_depth_levels")]
    pub min_depth_levels: usize,
    #[serde(default = "default_levels_per_side")]
    pub levels_per_side: usize,
    #[serde(default = "default_grid_step_bps")]
    pub grid_step_bps: f64,
    #[serde(default = "default_level_size_decay")]
    pub level_size_decay: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InventoryConfig {
    #[serde(default = "default_max_inventory")]
    pub max_inventory: f64,
    #[serde(default = "default_target_pct")]
    pub target_pct: f64,
    #[serde(default = "default_skew_strength")]
    pub skew_strength: f64,
    #[serde(default = "default_size_sensitivity")]
    pub size_sensitivity: f64,
    #[serde(default = "default_rebalance_threshold")]
    pub rebalance_threshold: f64,
    #[serde(default)]
    pub soft_limit: Option<f64>,
    #[serde(default)]
    pub hard_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrendConfig {
    #[serde(default = "default_fast_alpha")]
    pub ema_fast_alpha: f64,
    #[serde(default = "default_slow_alpha")]
    pub ema_slow_alpha: f64,
    #[serde(default = "default_slope_threshold")]
    pub slope_threshold: f64,
    #[serde(default = "default_trend_cooldown")]
    pub cooldown_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketMakingRiskConfig {
    #[serde(default = "default_max_inventory")]
    pub max_inventory_notional: f64,
    #[serde(default = "default_max_loss")]
    pub max_loss: f64,
    #[serde(default = "default_drawdown")]
    pub max_drawdown: f64,
    #[serde(default = "default_vol_spike_threshold")]
    pub vol_spike_threshold: f64,
    #[serde(default = "default_vol_widen_factor")]
    pub vol_widen_factor: f64,
    #[serde(default = "default_max_api_errors")]
    pub max_api_errors: u32,
}

#[derive(Debug, Clone, Deserialize, Default, Serialize)]
pub struct HedgingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_tick_size() -> f64 {
    0.01
}

fn default_lot_size() -> f64 {
    0.01
}

fn default_min_notional() -> f64 {
    5.0
}

fn default_order_ttl() -> u64 {
    10
}

fn default_base_size() -> f64 {
    1.0
}

fn default_min_order_size() -> f64 {
    0.5
}

fn default_refresh_ms() -> u64 {
    500
}

fn default_base_spread_bps() -> f64 {
    25.0
}

fn default_min_spread_bps() -> f64 {
    8.0
}

fn default_max_spread_bps() -> f64 {
    200.0
}

fn default_vol_sensitivity() -> f64 {
    35.0
}

fn default_depth_sensitivity() -> f64 {
    10.0
}

fn default_trend_sensitivity() -> f64 {
    5.0
}

fn default_price_ema() -> f64 {
    0.2
}

fn default_depth_levels() -> usize {
    5
}

fn default_levels_per_side() -> usize {
    1
}

fn default_grid_step_bps() -> f64 {
    5.0
}

fn default_level_size_decay() -> f64 {
    0.7
}

fn default_max_inventory() -> f64 {
    200.0
}

fn default_target_pct() -> f64 {
    0.0
}

fn default_skew_strength() -> f64 {
    0.3
}

fn default_size_sensitivity() -> f64 {
    0.3
}

fn default_rebalance_threshold() -> f64 {
    0.2
}

fn default_fast_alpha() -> f64 {
    0.3
}

fn default_slow_alpha() -> f64 {
    0.08
}

fn default_slope_threshold() -> f64 {
    0.6
}

fn default_trend_cooldown() -> u64 {
    15
}

fn default_max_loss() -> f64 {
    500.0
}

fn default_drawdown() -> f64 {
    1000.0
}

fn default_vol_spike_threshold() -> f64 {
    0.02
}

fn default_vol_widen_factor() -> f64 {
    1.5
}

fn default_max_api_errors() -> u32 {
    3
}

#[derive(Debug, Clone, Default)]
struct SymbolMetrics {
    vol_estimate: f64,
    depth_imbalance: f64,
    obi: f64,
    trade_sign_ema: f64,
    trend_bias: f64,
    last_mid: Option<f64>,
    price_digits: u32,
    qty_digits: u32,
    tick_size: f64,
    lot_size: f64,
}

#[derive(Debug, Clone, Default)]
struct InventoryState {
    position: f64,
    notional: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    avg_entry: f64,
}

#[derive(Debug)]
struct SymbolState {
    config: SymbolConfig,
    mid_model: MidPriceModel,
    spread_model: SpreadModel,
    skew_model: InventorySkewModel,
    trend_filter: TrendFilter,
    metrics: SymbolMetrics,
    inventory: InventoryState,
    last_ticker: Option<Ticker>,
    last_orderbook: Option<OrderBook>,
    bid_orders: Vec<ManagedOrder>,
    ask_orders: Vec<ManagedOrder>,
    last_quote_ts: Option<DateTime<Utc>>,
    api_error_streak: u32,
}

impl SymbolState {
    fn new(
        symbol_cfg: SymbolConfig,
        inventory_cfg: &InventoryConfig,
        trend_cfg: &TrendConfig,
        quote_cfg: &QuotingConfig,
        precision: Option<ResolvedPrecision>,
    ) -> Self {
        let prec = precision.unwrap_or(ResolvedPrecision {
            tick_size: symbol_cfg.tick_size,
            step_size: symbol_cfg.lot_size,
            price_digits: 4,
            qty_digits: 2,
            min_notional: symbol_cfg.min_notional,
        });
        let max_inv = symbol_cfg
            .max_inventory
            .unwrap_or(inventory_cfg.max_inventory);
        Self {
            mid_model: MidPriceModel::new(quote_cfg.mid_price_alpha, quote_cfg.min_depth_levels),
            spread_model: SpreadModel::new(quote_cfg.clone()),
            skew_model: InventorySkewModel::new(
                max_inv,
                inventory_cfg.skew_strength,
                inventory_cfg.size_sensitivity,
            ),
            trend_filter: TrendFilter::new(trend_cfg.clone()),
            config: symbol_cfg,
            metrics: SymbolMetrics {
                price_digits: prec.price_digits,
                qty_digits: prec.qty_digits,
                tick_size: prec.tick_size,
                lot_size: prec.step_size,
                ..SymbolMetrics::default()
            },
            inventory: InventoryState::default(),
            last_ticker: None,
            last_orderbook: None,
            bid_orders: Vec::new(),
            ask_orders: Vec::new(),
            last_quote_ts: None,
            api_error_streak: 0,
        }
    }

    fn record_ticker(&mut self, ticker: &Ticker) {
        self.last_ticker = Some(ticker.clone());
    }

    fn record_orderbook(&mut self, book: &OrderBook) {
        self.last_orderbook = Some(book.clone());
    }

    fn update_mid_price(&mut self) -> Option<f64> {
        let mid = self
            .mid_model
            .update(self.last_orderbook.as_ref(), self.last_ticker.as_ref());
        if let Some(price) = mid {
            if let Some(prev) = self.metrics.last_mid {
                let ret = ((price - prev) / prev.max(1.0)).abs();
                // 简易波动率估计：中间价绝对收益的 EWMA
                self.metrics.vol_estimate = 0.85 * self.metrics.vol_estimate + 0.15 * ret;
            } else {
                self.metrics.vol_estimate = 0.0;
            }
            self.metrics.last_mid = Some(price);
        }
        mid
    }

    fn update_depth_metrics(&mut self) {
        if let Some(book) = &self.last_orderbook {
            let mut bid_volume = 0.0;
            let mut ask_volume = 0.0;
            for level in book.bids.iter().take(5) {
                bid_volume += level[1];
            }
            for level in book.asks.iter().take(5) {
                ask_volume += level[1];
            }
            let total = (bid_volume + ask_volume).max(1e-6);
            self.metrics.depth_imbalance = (bid_volume - ask_volume) / total;
            self.metrics.obi = self.metrics.depth_imbalance;
        }
    }

    fn trend_signal(&mut self, mid: f64, now: DateTime<Utc>) -> TrendSignal {
        let signal = self.trend_filter.update(mid, now);
        self.metrics.trend_bias = self.trend_filter.trend_bias();
        signal
    }

    fn apply_trade(&mut self, trade: &Trade) {
        let signed_qty = match trade.side {
            OrderSide::Buy => trade.amount,
            OrderSide::Sell => -trade.amount,
        };

        let prev_pos = self.inventory.position;
        let new_pos = prev_pos + signed_qty;

        if prev_pos.abs() < f64::EPSILON || prev_pos.signum() == signed_qty.signum() {
            if new_pos.abs() > f64::EPSILON {
                self.inventory.avg_entry =
                    (self.inventory.avg_entry * prev_pos + trade.price * signed_qty) / new_pos;
            } else {
                self.inventory.avg_entry = 0.0;
            }
        } else {
            let closing_qty = signed_qty.abs().min(prev_pos.abs());
            let pnl = closing_qty * (trade.price - self.inventory.avg_entry) * prev_pos.signum();
            self.inventory.realized_pnl += pnl;
            if new_pos.signum() != prev_pos.signum() {
                self.inventory.avg_entry = trade.price;
            }
        }

        self.inventory.position = new_pos;
        self.inventory.notional = new_pos * trade.price;
        self.inventory.unrealized_pnl = (trade.price - self.inventory.avg_entry) * new_pos;
    }

    fn update_trade_sign_ema(&mut self, trade: &Trade, alpha: f64) {
        let sign = match trade.side {
            OrderSide::Buy => 1.0,
            OrderSide::Sell => -1.0,
        };
        let a = alpha.clamp(0.0, 1.0);
        self.metrics.trade_sign_ema = a * sign + (1.0 - a) * self.metrics.trade_sign_ema;
    }

    fn remove_order_by_id(&mut self, side: OrderSide, order_id: &str) {
        let orders = match side {
            OrderSide::Buy => &mut self.bid_orders,
            OrderSide::Sell => &mut self.ask_orders,
        };
        orders.retain(|o| {
            let matches_id = o
                .order_id
                .as_deref()
                .map(|id| id == order_id)
                .unwrap_or(false);
            let matches_client = o.client_id == order_id;
            !(matches_id || matches_client)
        });
    }
}

pub struct ProMarketMakingStrategy {
    config: MarketMakingConfig,
    account: Arc<AccountInfo>,
    account_manager: Arc<AccountManager>,
    order_manager: Arc<OrderManager>,
    risk_controller: Arc<RiskController>,
    symbol_states: Arc<RwLock<HashMap<String, SymbolState>>>,
    status: Arc<RwLock<StrategyStatus>>,
    running: Arc<RwLock<bool>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    quote_notifier: Arc<Notify>,
}

impl ProMarketMakingStrategy {
    fn new(config: MarketMakingConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        let risk_evaluator = deps.risk_evaluator.clone();

        let order_manager = Arc::new(OrderManager::new(
            account.clone(),
            config.account.market_type,
            Duration::seconds(config.quoting.refresh_interval_ms as i64 / 2)
                .max(Duration::seconds(symbol_default_ttl(&config.symbols) as i64)),
        ));

        let risk_controller = Arc::new(RiskController::new(
            config.risk.clone(),
            risk_evaluator.clone(),
        ));

        let status = StrategyStatus::new(config.strategy.name.clone())
            .with_state(StrategyState::Initializing);

        Ok(Self {
            config,
            account,
            account_manager,
            order_manager,
            risk_controller,
            symbol_states: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(status)),
            running: Arc::new(RwLock::new(false)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            quote_notifier: Arc::new(Notify::new()),
        })
    }

    async fn quoting_loop(self: Arc<Self>) {
        let fallback = std::time::Duration::from_secs(10);
        let mut first_run = true;

        loop {
            if !*self.running.read().await {
                break;
            }

            // 首轮立即刷新，其余由成交/订单事件触发，超时兜底
            if !first_run {
                tokio::select! {
                    _ = self.quote_notifier.notified() => {},
                    _ = tokio::time::sleep(fallback) => {
                        log::debug!("quote fallback tick");
                    }
                }
            } else {
                first_run = false;
            }

            let symbols: Vec<String> = {
                let states = self.symbol_states.read().await;
                states.keys().cloned().collect()
            };

            for symbol in symbols {
                if let Err(err) = self.pull_market_data(&symbol).await {
                    log::warn!("刷新 {} 行情失败: {}", symbol, err);
                    let mut states = self.symbol_states.write().await;
                    if let Some(state) = states.get_mut(&symbol) {
                        state.api_error_streak += 1;
                        if state.api_error_streak >= self.config.risk.max_api_errors {
                            log::warn!(
                                "{} 连续 API 错误 {}, 全撤单保护",
                                symbol,
                                state.api_error_streak
                            );
                            let _ = self.order_manager.cancel_symbol(&symbol, state).await;
                        }
                    }
                    continue;
                }
                if let Err(err) = self.refresh_symbol_quotes(&symbol).await {
                    log::warn!("刷新 {} 报价失败: {}", symbol, err);
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(
                self.config.quoting.refresh_interval_ms,
            ))
            .await;
        }
    }

    async fn sync_initial_positions(&self) -> Result<()> {
        match self.config.account.market_type {
            MarketType::Futures => {
                let positions: Vec<Position> = self.account.exchange.get_positions(None).await?;
                let mut states = self.symbol_states.write().await;
                for (symbol, state) in states.iter_mut() {
                    if let Some(pos) = positions.iter().find(|p| p.symbol == state.config.symbol) {
                        state.inventory.position = pos.amount;
                        state.inventory.avg_entry = pos.entry_price;
                        state.inventory.unrealized_pnl = pos.unrealized_pnl;
                        state.inventory.notional = pos.amount * pos.mark_price;
                        log::info!(
                            "{} 初始仓位同步: qty={:.4}, entry={:.4}, mark={:.4}, upnl={:.4}",
                            symbol,
                            pos.amount,
                            pos.entry_price,
                            pos.mark_price,
                            pos.unrealized_pnl
                        );
                    }
                }
            }
            MarketType::Spot => {
                let balances = self.account.exchange.get_balance(MarketType::Spot).await?;
                let mut states = self.symbol_states.write().await;
                for (symbol, state) in states.iter_mut() {
                    if let Some(base) = symbol.split('/').next() {
                        if let Some(balance) = balances
                            .iter()
                            .find(|b| b.currency.eq_ignore_ascii_case(base))
                        {
                            state.inventory.position = balance.total;
                            let mid = state.metrics.last_mid.unwrap_or(0.0);
                            state.inventory.notional = balance.total * mid;
                            log::info!("{} 初始现货持仓同步: qty={:.4}", symbol, balance.total);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn refresh_symbol_quotes(&self, symbol: &str) -> Result<()> {
        let mut states = self.symbol_states.write().await;
        let (quotes, signal, inv_abs, _soft_limit, hard_limit, hard_limit_hit) = {
            let state = states
                .get_mut(symbol)
                .ok_or_else(|| anyhow!("symbol {} not found", symbol))?;

            let mid = match state.update_mid_price() {
                Some(m) => m,
                None => return Ok(()),
            };
            state.inventory.notional = state.inventory.position * mid;
            state.inventory.unrealized_pnl =
                (mid - state.inventory.avg_entry) * state.inventory.position;
            state.update_depth_metrics();
            let signal = state.trend_signal(mid, Utc::now());

            let ctx = QuoteContext {
                mid_price: mid,
                vol_estimate: state.metrics.vol_estimate,
                depth_imbalance: state.metrics.depth_imbalance,
                obi: state.metrics.obi,
                trade_sign_ema: state.metrics.trade_sign_ema,
                trend_bias: state.metrics.trend_bias,
            };

            let mut spread = state.spread_model.spread(ctx);
            let skew = state.skew_model.compute(state.inventory.position);
            let base_max = state
                .config
                .max_inventory
                .unwrap_or(self.config.inventory.max_inventory);
            let soft_limit = self.config.inventory.soft_limit.unwrap_or(base_max * 0.6);
            let hard_limit = self.config.inventory.hard_limit.unwrap_or(base_max);
            let inv_abs = state.inventory.position.abs();
            let hard_limit_hit = inv_abs > hard_limit;
            let mut size_factor = 1.0;

            if inv_abs > soft_limit {
                size_factor *= (soft_limit / inv_abs).clamp(0.1, 1.0);
                spread *= 1.2;
            }

            if state.metrics.vol_estimate > self.config.risk.vol_spike_threshold {
                spread *= self.config.risk.vol_widen_factor.max(1.0);
                size_factor *= 0.5;
            }

            let levels = self.config.quoting.levels_per_side.max(1);
            let step_abs = mid * self.config.quoting.grid_step_bps / 10_000.0;
            let size_decay = self.config.quoting.level_size_decay.max(0.1);

            let mut level_quotes = Vec::with_capacity(levels);
            if !hard_limit_hit {
                for level in 0..levels {
                    let level_spread = spread + 2.0 * step_abs * level as f64;
                    // 再取一遍最新 mid，避免价格陈旧导致 PostOnly 被拒
                    let latest_mid = state.metrics.last_mid.unwrap_or(mid);
                    let mut pair =
                        QuotePair::from_ctx(&state.config, latest_mid, level_spread, skew);
                    pair = pair.scale(size_factor);
                    if level > 0 {
                        let scale = size_decay.powi(level as i32);
                        pair = pair.scale(scale);
                    }
                    level_quotes.push(pair);
                }
            }

            (
                level_quotes,
                signal,
                inv_abs,
                soft_limit,
                hard_limit,
                hard_limit_hit,
            )
        };

        if !signal.allows_quoting() {
            if let Some(state) = states.get_mut(symbol) {
                self.order_manager.cancel_symbol(symbol, state).await?;
            }
            return Ok(());
        }

        let snapshot = self.build_snapshot_locked(&*states);
        let mut decision = self.risk_controller.evaluate(&snapshot).await;
        if hard_limit_hit {
            decision.action = RiskAction::Halt {
                reason: format!("仓位 {:.2} 超过硬上限 {:.2}", inv_abs, hard_limit),
            };
        }

        let state = states
            .get_mut(symbol)
            .ok_or_else(|| anyhow!("symbol {} not found", symbol))?;

        if quotes.is_empty() && !matches!(decision.action, RiskAction::Halt { .. }) {
            self.order_manager.cancel_symbol(symbol, state).await?;
            return Ok(());
        }

        let halt_reason = match decision.action {
            RiskAction::None | RiskAction::Notify { .. } => {
                self.order_manager
                    .submit_quotes(symbol, state, &quotes)
                    .await?;
                None
            }
            RiskAction::ScaleDown { scale_factor, .. } => {
                let scaled: Vec<_> = quotes.iter().map(|q| q.scale(scale_factor)).collect();
                self.order_manager
                    .submit_quotes(symbol, state, &scaled)
                    .await?;
                None
            }
            RiskAction::Halt { reason } => Some(reason),
        };

        if let Some(reason) = halt_reason {
            drop(states);
            log::error!("风险控制停止策略: {}", reason);
            self.stop().await?;
            return Ok(());
        }

        state.last_quote_ts = Some(Utc::now());
        Ok(())
    }

    async fn pull_market_data(&self, symbol: &str) -> Result<()> {
        let (symbol_name, market_type) = {
            let states = self.symbol_states.read().await;
            let state = states
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not found", symbol))?;
            (state.config.symbol.clone(), self.config.account.market_type)
        };

        let ticker = self
            .account
            .exchange
            .get_ticker(&symbol_name, market_type)
            .await?;

        let orderbook = self
            .account
            .exchange
            .get_orderbook(&symbol_name, market_type, Some(20))
            .await?;

        let mut states = self.symbol_states.write().await;
        if let Some(state) = states.get_mut(symbol) {
            state.record_ticker(&ticker);
            state.record_orderbook(&orderbook);
            state.api_error_streak = 0;
        }
        Ok(())
    }

    async fn run_market_stream(self: Arc<Self>, symbol: String) {
        loop {
            if !*self.running.read().await {
                break;
            }

            let mut client = match self
                .account
                .exchange
                .create_websocket_client(self.config.account.market_type)
                .await
            {
                Ok(client) => client,
                Err(err) => {
                    log::error!("{} 创建WebSocket失败: {}", symbol, err);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            };

            if let Err(err) = client.connect().await {
                log::error!("{} WebSocket连接失败: {}", symbol, err);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                continue;
            }

            if let Err(err) = self.subscribe_public_stream(&symbol, client.as_mut()).await {
                log::error!("{} 订阅行情失败: {}", symbol, err);
                let _ = client.disconnect().await;
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                continue;
            }

            loop {
                if !*self.running.read().await {
                    let _ = client.disconnect().await;
                    return;
                }

                match client.receive().await {
                    Ok(Some(msg)) => {
                        if let Err(err) = self.handle_public_ws(&symbol, &msg).await {
                            log::trace!("{} 解析行情失败: {}", symbol, err);
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(err) => {
                        log::warn!("{} WebSocket接收错误: {}", symbol, err);
                        break;
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    async fn subscribe_public_stream(
        &self,
        symbol: &str,
        client: &mut dyn WebSocketClient,
    ) -> Result<()> {
        let stream_symbol = Self::binance_stream_symbol(symbol);
        let params = vec![
            format!("{}@ticker", stream_symbol),
            format!("{}@depth5@100ms", stream_symbol),
        ];
        let channels = params
            .iter()
            .map(|p| format!(r#""{}""#, p))
            .collect::<Vec<_>>()
            .join(",");
        let payload = format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":{}}}"#,
            channels,
            Utc::now().timestamp_millis()
        );
        client
            .send(payload)
            .await
            .map_err(|e| anyhow!(e.to_string()))
    }

    async fn handle_public_ws(&self, symbol: &str, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        let data = value.get("data").unwrap_or(&value);
        let event_type = data.get("e").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "24hrTicker" => {
                let ticker = Self::parse_binance_ticker(symbol, data)?;
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(symbol) {
                    state.record_ticker(&ticker);
                }
            }
            "depthUpdate" => {
                let book = Self::parse_binance_orderbook(symbol, data)?;
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(symbol) {
                    state.record_orderbook(&book);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn parse_binance_ticker(symbol: &str, data: &Value) -> Result<Ticker> {
        let timestamp = Self::timestamp_from_ms(data.get("E").and_then(|v| v.as_i64()));

        Ok(Ticker {
            symbol: symbol.to_string(),
            high: Self::parse_str_number(data.get("h")),
            low: Self::parse_str_number(data.get("l")),
            bid: Self::parse_str_number(data.get("b")),
            ask: Self::parse_str_number(data.get("a")),
            last: Self::parse_str_number(data.get("c")),
            volume: Self::parse_str_number(data.get("v")),
            timestamp,
        })
    }

    fn parse_binance_orderbook(symbol: &str, data: &Value) -> Result<OrderBook> {
        let timestamp = Self::timestamp_from_ms(data.get("E").and_then(|v| v.as_i64()));

        let bids = Self::parse_levels(data.get("b"));
        let asks = Self::parse_levels(data.get("a"));

        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp,
            info: serde_json::Value::Null,
        })
    }

    fn parse_levels(value: Option<&Value>) -> Vec<[f64; 2]> {
        value
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|entry| {
                        entry.as_array().and_then(|pair| {
                            let price = pair.get(0)?;
                            let qty = pair.get(1)?;
                            Some([
                                Self::parse_str_number(Some(price)),
                                Self::parse_str_number(Some(qty)),
                            ])
                        })
                    })
                    .collect::<Vec<[f64; 2]>>()
            })
            .unwrap_or_default()
    }

    fn parse_str_number(value: Option<&Value>) -> f64 {
        value
            .and_then(|v| {
                if let Some(n) = v.as_f64() {
                    Some(n)
                } else if let Some(s) = v.as_str() {
                    s.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    fn binance_stream_symbol(symbol: &str) -> String {
        symbol.replace('/', "").to_lowercase()
    }

    fn timestamp_from_ms(ms: Option<i64>) -> DateTime<Utc> {
        ms.and_then(|value| DateTime::<Utc>::from_timestamp_millis(value))
            .unwrap_or_else(|| Utc::now())
    }

    fn build_snapshot_locked(&self, states: &HashMap<String, SymbolState>) -> StrategySnapshot {
        let mut exposure = StrategyExposureSnapshot::default();
        let mut performance = StrategyPerformanceSnapshot::default();

        for state in states.values() {
            exposure.notional += state.inventory.position * state.metrics.last_mid.unwrap_or(0.0);
            exposure.net_inventory += state.inventory.position;
            performance.realized_pnl += state.inventory.realized_pnl;
            performance.unrealized_pnl += state.inventory.unrealized_pnl;
        }

        StrategySnapshot {
            name: self.config.strategy.name.clone(),
            exposure,
            performance,
            risk_limits: None,
        }
    }

    pub async fn handle_tick(&self, ticker: Ticker) -> Result<()> {
        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(&ticker.symbol) {
                state.record_ticker(&ticker);
            }
        }
        self.refresh_symbol_quotes(&ticker.symbol).await
    }

    pub async fn handle_order_update(&self, order: Order) -> Result<()> {
        let mut states = self.symbol_states.write().await;
        let state = match states.get_mut(&order.symbol) {
            Some(s) => s,
            None => return Ok(()),
        };

        if matches!(
            order.status,
            OrderStatus::Closed | OrderStatus::Canceled | OrderStatus::Expired
        ) {
            state.remove_order_by_id(order.side, order.id.as_str());
        }
        self.quote_notifier.notify_one();
        Ok(())
    }

    pub async fn handle_trade(&self, trade: Trade) -> Result<()> {
        if let Some(state) = self.symbol_states.write().await.get_mut(&trade.symbol) {
            state.apply_trade(&trade);
            state.update_trade_sign_ema(&trade, TRADE_SIGN_ALPHA);
        }
        // 成交触发重新报价
        self.quote_notifier.notify_one();
        self.maybe_trigger_hedge(&trade).await
    }

    async fn maybe_trigger_hedge(&self, trade: &Trade) -> Result<()> {
        if !self
            .config
            .hedging
            .as_ref()
            .map(|h| h.enabled)
            .unwrap_or(false)
        {
            return Ok(());
        }

        if let Some(endpoint) = self
            .config
            .hedging
            .as_ref()
            .and_then(|h| h.endpoint.clone())
        {
            log::debug!(
                "[hedge] trade={} side={:?} qty={:.4} -> endpoint {}",
                trade.symbol,
                trade.side,
                trade.amount,
                endpoint
            );
        }
        Ok(())
    }

    async fn run_user_stream(self: Arc<Self>) {
        loop {
            if !*self.running.read().await {
                break;
            }

            let listen_key = match self
                .account
                .exchange
                .create_user_data_stream(self.config.account.market_type)
                .await
            {
                Ok(key) => key,
                Err(err) => {
                    log::warn!("用户流获取listen_key失败: {}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            };

            let ws_url = match self.config.account.market_type {
                MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
                MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
            };

            let mut client = BaseWebSocketClient::new(ws_url.clone(), "binance".to_string());
            if let Err(err) = client.connect().await {
                log::warn!("用户流连接失败: {}", err);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }

            log::info!("用户流已连接 {}", ws_url);
            loop {
                if !*self.running.read().await {
                    let _ = client.disconnect().await;
                    return;
                }

                match client.receive().await {
                    Ok(Some(msg)) => {
                        if let Err(err) = self.handle_user_event(&msg).await {
                            log::debug!("用户流处理失败: {}", err);
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(err) => {
                        log::warn!("用户流接收错误: {}", err);
                        break;
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    async fn handle_user_event(&self, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        let event_type = value.get("e").and_then(|v| v.as_str()).unwrap_or("");
        if event_type != "ORDER_TRADE_UPDATE" {
            return Ok(());
        }

        let order_obj = value
            .get("o")
            .ok_or_else(|| anyhow!("用户流缺少订单字段"))?;

        let raw_symbol = order_obj
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let symbol = {
            let normalized = raw_symbol.replace('/', "").to_uppercase();
            let states = self.symbol_states.read().await;
            states
                .keys()
                .find(|s| s.replace('/', "").to_uppercase() == normalized)
                .cloned()
                .unwrap_or(raw_symbol.clone())
        };

        let side = match order_obj.get("S").and_then(|v| v.as_str()) {
            Some("BUY") => OrderSide::Buy,
            Some("SELL") => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        let status = match order_obj.get("X").and_then(|v| v.as_str()) {
            Some("NEW") => OrderStatus::Open,
            Some("PARTIALLY_FILLED") => OrderStatus::PartiallyFilled,
            Some("FILLED") => OrderStatus::Closed,
            Some("CANCELED") => OrderStatus::Canceled,
            Some("REJECTED") => OrderStatus::Rejected,
            _ => OrderStatus::Open,
        };

        let order_id = order_obj
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let client_id = order_obj
            .get("c")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let amount = order_obj
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let filled = order_obj
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_fill_qty = order_obj
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let price = order_obj
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let last_price = order_obj
            .get("L")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or(price)
            .unwrap_or(0.0);
        let ts = order_obj
            .get("T")
            .and_then(|v| v.as_i64())
            .and_then(|ms| DateTime::<Utc>::from_timestamp_millis(ms))
            .unwrap_or_else(|| Utc::now());

        let order = Order {
            id: order_id.clone(),
            symbol: symbol.clone(),
            side,
            order_type: OrderType::Limit,
            amount,
            price,
            filled,
            remaining: (amount - filled).max(0.0),
            status,
            market_type: self.config.account.market_type,
            timestamp: ts,
            last_trade_timestamp: Some(ts),
            info: order_obj.clone(),
        };
        self.handle_order_update(order).await?;

        if last_fill_qty > 0.0 {
            let trade = Trade {
                id: client_id.clone(),
                symbol: symbol.clone(),
                side,
                amount: last_fill_qty,
                price: last_price,
                timestamp: ts,
                order_id: Some(order_id),
                fee: None,
            };
            self.handle_trade(trade).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl StrategyInstance for ProMarketMakingStrategy {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        // 初始化精度与持仓
        {
            let mut states = self.symbol_states.write().await;
            for symbol_cfg in &self.config.symbols {
                if !symbol_cfg.enabled {
                    continue;
                }
                let mut cfg = symbol_cfg.clone();
                if let Some(pm) = self.config.precision_management.clone() {
                    let pmgr = precision::PrecisionManager::new(
                        self.account.clone(),
                        pm,
                        self.config.account.market_type,
                    );
                    let resolved = pmgr.resolve(&mut cfg).await?;
                    log::info!(
                        "精度同步 {}: tick_size={} step_size={} price_digits={} qty_digits={}",
                        cfg.symbol,
                        resolved.tick_size,
                        resolved.step_size,
                        resolved.price_digits,
                        resolved.qty_digits
                    );
                    let state = SymbolState::new(
                        cfg.clone(),
                        &self.config.inventory,
                        &self.config.trend,
                        &self.config.quoting,
                        Some(resolved),
                    );
                    states.insert(cfg.symbol.clone(), state);
                } else {
                    let state = SymbolState::new(
                        cfg.clone(),
                        &self.config.inventory,
                        &self.config.trend,
                        &self.config.quoting,
                        None,
                    );
                    states.insert(cfg.symbol.clone(), state);
                }
            }
        }

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Running;
            status.last_error = None;
            status.updated_at = Utc::now();
        }

        self.sync_initial_positions().await?;

        let quote_worker = Arc::new(self.clone_for_task());
        let quote_handle = {
            let worker = quote_worker.clone();
            tokio::spawn(async move {
                worker.quoting_loop().await;
            })
        };
        self.tasks.lock().await.push(quote_handle);

        for symbol in self.config.symbols.iter().filter(|s| s.enabled) {
            let ws_worker = Arc::new(self.clone_for_task());
            let symbol_name = symbol.symbol.clone();
            let handle = tokio::spawn(async move {
                ws_worker.run_market_stream(symbol_name).await;
            });
            self.tasks.lock().await.push(handle);
        }

        let user_worker = Arc::new(self.clone_for_task());
        let user_handle = tokio::spawn(async move {
            user_worker.run_user_stream().await;
        });
        self.tasks.lock().await.push(user_handle);

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if !*running {
                return Ok(());
            }
            *running = false;
        }

        let mut states = self.symbol_states.write().await;
        self.order_manager.cancel_all_symbols(&mut *states).await?;

        let mut tasks = self.tasks.lock().await;
        for handle in tasks.drain(..) {
            handle.abort();
        }

        let mut status = self.status.write().await;
        status.state = StrategyState::Stopped;
        status.updated_at = Utc::now();
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        Ok(self.status.read().await.clone())
    }
}

#[async_trait]
impl LegacyStrategy for ProMarketMakingStrategy {
    async fn name(&self) -> String {
        self.config.strategy.name.clone()
    }

    async fn on_tick(
        &self,
        ticker: Ticker,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.handle_tick(ticker).await.map_err(|e| e.into())
    }

    async fn on_order_update(
        &self,
        order: Order,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.handle_order_update(order).await.map_err(|e| e.into())
    }

    async fn on_trade(
        &self,
        trade: Trade,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.handle_trade(trade).await.map_err(|e| e.into())
    }

    async fn get_status(
        &self,
    ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let status = self.status.read().await;
        let states = self.symbol_states.read().await;
        let net_inv: f64 = states.values().map(|s| s.inventory.position).sum();
        let pnl: f64 = states
            .values()
            .map(|s| s.inventory.realized_pnl + s.inventory.unrealized_pnl)
            .sum();
        Ok(format!(
            "{} - net_inv={:.4}, pnl={:.2}",
            status.name, net_inv, pnl
        ))
    }
}

impl AppStrategy for ProMarketMakingStrategy {
    type Config = MarketMakingConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self> {
        Self::new(config, deps)
    }
}

impl ProMarketMakingStrategy {
    fn clone_for_task(&self) -> Self {
        Self {
            config: self.config.clone(),
            account: self.account.clone(),
            account_manager: self.account_manager.clone(),
            order_manager: self.order_manager.clone(),
            risk_controller: self.risk_controller.clone(),
            symbol_states: self.symbol_states.clone(),
            status: self.status.clone(),
            running: self.running.clone(),
            tasks: self.tasks.clone(),
            quote_notifier: self.quote_notifier.clone(),
        }
    }
}

fn symbol_default_ttl(symbols: &[SymbolConfig]) -> u64 {
    symbols.iter().map(|s| s.order_ttl_secs).max().unwrap_or(10)
}

// ========== 子模块实现 ==========

mod mid_price {
    use crate::core::types::{OrderBook, Ticker};

    #[derive(Debug)]
    pub struct MidPriceModel {
        ema_alpha: f64,
        last_mid: Option<f64>,
        min_levels: usize,
    }

    impl MidPriceModel {
        pub fn new(ema_alpha: f64, min_levels: usize) -> Self {
            Self {
                ema_alpha: ema_alpha.clamp(0.0, 1.0),
                last_mid: None,
                min_levels,
            }
        }

        pub fn update(
            &mut self,
            orderbook: Option<&OrderBook>,
            ticker: Option<&Ticker>,
        ) -> Option<f64> {
            let raw_mid = match orderbook {
                Some(book) if !book.bids.is_empty() && !book.asks.is_empty() => {
                    Some((book.bids[0][0] + book.asks[0][0]) * 0.5)
                }
                _ => ticker.map(|t| (t.bid + t.ask) * 0.5),
            }?;

            let smoothed = match self.last_mid {
                Some(prev) => prev + self.ema_alpha * (raw_mid - prev),
                None => raw_mid,
            };

            self.last_mid = Some(smoothed);
            Some(smoothed)
        }
    }
}

mod spread {
    use super::{QuotingConfig, SkewAdjustment, SymbolConfig};

    #[derive(Clone, Copy)]
    pub struct QuoteContext {
        pub mid_price: f64,
        pub vol_estimate: f64,
        pub depth_imbalance: f64,
        pub obi: f64,
        pub trade_sign_ema: f64,
        pub trend_bias: f64,
    }

    #[derive(Debug)]
    pub struct SpreadModel {
        cfg: QuotingConfig,
    }

    impl SpreadModel {
        pub fn new(cfg: QuotingConfig) -> Self {
            Self { cfg }
        }

        pub fn spread(&self, ctx: QuoteContext) -> f64 {
            let flow_bias = ctx.trade_sign_ema.abs() * self.cfg.trend_sensitivity;
            let orderbook_bias = ctx.obi.abs().max(ctx.depth_imbalance.abs());
            let mut spread_bps = self.cfg.base_spread_bps
                + ctx.vol_estimate * self.cfg.vol_sensitivity
                + orderbook_bias * self.cfg.depth_sensitivity
                + ctx.trend_bias.abs() * self.cfg.trend_sensitivity
                + flow_bias;

            spread_bps = spread_bps
                .max(self.cfg.min_spread_bps)
                .min(self.cfg.max_spread_bps);

            ctx.mid_price * spread_bps / 10_000.0
        }
    }

    impl super::order_manager::QuotePair {
        pub fn from_ctx(cfg: &SymbolConfig, mid: f64, spread: f64, skew: SkewAdjustment) -> Self {
            let half = spread / 2.0;
            let bid_price = ((mid - half) * (1.0 - skew.price_offset)).max(0.0);
            let ask_price = (mid + half) * (1.0 + skew.price_offset);
            let base = cfg.base_order_size.max(cfg.min_order_size);

            Self {
                bid_price,
                ask_price,
                bid_size: (base * skew.bid_size_factor).max(cfg.min_order_size),
                ask_size: (base * skew.ask_size_factor).max(cfg.min_order_size),
            }
        }
    }
}

mod skew {

    #[derive(Clone, Copy, Debug)]
    pub struct SkewAdjustment {
        pub price_offset: f64,
        pub bid_size_factor: f64,
        pub ask_size_factor: f64,
    }

    #[derive(Debug)]
    pub struct InventorySkewModel {
        max_inventory: f64,
        skew_strength: f64,
        size_sensitivity: f64,
    }

    impl InventorySkewModel {
        pub fn new(max_inventory: f64, skew_strength: f64, size_sensitivity: f64) -> Self {
            Self {
                max_inventory: max_inventory.max(1.0),
                skew_strength,
                size_sensitivity,
            }
        }

        pub fn compute(&self, inventory: f64) -> SkewAdjustment {
            let ratio = (inventory / self.max_inventory).clamp(-1.0, 1.0);
            let price_offset = ratio * self.skew_strength;
            let size_bias = 1.0 + ratio.abs() * self.size_sensitivity;

            if ratio > 0.0 {
                SkewAdjustment {
                    price_offset,
                    bid_size_factor: (1.0 - ratio.abs()).max(0.2),
                    ask_size_factor: size_bias,
                }
            } else {
                SkewAdjustment {
                    price_offset,
                    bid_size_factor: size_bias,
                    ask_size_factor: (1.0 - ratio.abs()).max(0.2),
                }
            }
        }
    }
}

mod trend {
    use chrono::{DateTime, Duration, Utc};

    use super::TrendConfig;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum TrendSignal {
        Neutral,
        Up,
        Down,
        Blocked,
    }

    impl TrendSignal {
        pub fn allows_quoting(&self) -> bool {
            !matches!(self, TrendSignal::Blocked)
        }
    }

    #[derive(Debug)]
    pub struct TrendFilter {
        cfg: TrendConfig,
        ema_fast: Option<f64>,
        ema_slow: Option<f64>,
        last_signal: TrendSignal,
        cooldown_until: Option<DateTime<Utc>>,
    }

    impl TrendFilter {
        pub fn new(cfg: TrendConfig) -> Self {
            Self {
                cfg,
                ema_fast: None,
                ema_slow: None,
                last_signal: TrendSignal::Neutral,
                cooldown_until: None,
            }
        }

        pub fn update(&mut self, price: f64, now: DateTime<Utc>) -> TrendSignal {
            self.ema_fast = Some(match self.ema_fast {
                Some(prev) => prev + self.cfg.ema_fast_alpha * (price - prev),
                None => price,
            });
            self.ema_slow = Some(match self.ema_slow {
                Some(prev) => prev + self.cfg.ema_slow_alpha * (price - prev),
                None => price,
            });

            if let Some(until) = self.cooldown_until {
                if now < until {
                    self.last_signal = TrendSignal::Blocked;
                    return TrendSignal::Blocked;
                }
            }

            let fast = self.ema_fast.unwrap_or(price);
            let slow = self.ema_slow.unwrap_or(price);
            let slope = (fast - slow) / slow.max(1.0);

            self.last_signal = if slope.abs() < self.cfg.slope_threshold {
                TrendSignal::Neutral
            } else if slope > 0.0 {
                TrendSignal::Up
            } else {
                TrendSignal::Down
            };

            if matches!(self.last_signal, TrendSignal::Up | TrendSignal::Down) {
                self.cooldown_until = Some(now + Duration::seconds(self.cfg.cooldown_secs as i64));
            }

            self.last_signal
        }

        pub fn trend_bias(&self) -> f64 {
            match self.last_signal {
                TrendSignal::Neutral => 0.0,
                TrendSignal::Up => 1.0,
                TrendSignal::Down => -1.0,
                TrendSignal::Blocked => 2.0,
            }
        }
    }
}

mod order_manager {
    use anyhow::{anyhow, Result};
    use chrono::{DateTime, Duration, Utc};
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::core::types::{MarketType, OrderRequest, OrderSide, OrderType};
    use crate::cta::account_manager::AccountInfo;

    use super::apply_precision;
    use super::SymbolState;

    #[derive(Clone, Debug)]
    pub struct ManagedOrder {
        pub client_id: String,
        pub order_id: Option<String>,
        pub price: f64,
        pub quantity: f64,
        pub side: OrderSide,
        pub placed_at: DateTime<Utc>,
    }

    #[derive(Clone, Copy, Debug)]
    pub struct QuotePair {
        pub bid_price: f64,
        pub ask_price: f64,
        pub bid_size: f64,
        pub ask_size: f64,
    }

    impl QuotePair {
        pub fn scale(&self, factor: f64) -> Self {
            let scale = factor.clamp(0.1, 1.0);
            Self {
                bid_price: self.bid_price,
                ask_price: self.ask_price,
                bid_size: self.bid_size * scale,
                ask_size: self.ask_size * scale,
            }
        }
    }

    pub struct OrderManager {
        account: Arc<AccountInfo>,
        market_type: MarketType,
        ttl: Duration,
    }

    impl OrderManager {
        pub fn new(account: Arc<AccountInfo>, market_type: MarketType, ttl: Duration) -> Self {
            Self {
                account,
                market_type,
                ttl,
            }
        }

        pub async fn submit_quotes(
            &self,
            symbol: &str,
            state: &mut SymbolState,
            quotes: &[QuotePair],
        ) -> Result<()> {
            log::trace!("submit {} quotes levels={} ", symbol, quotes.len());
            let bids: Vec<(f64, f64)> = quotes.iter().map(|q| (q.bid_price, q.bid_size)).collect();
            let asks: Vec<(f64, f64)> = quotes.iter().map(|q| (q.ask_price, q.ask_size)).collect();

            self.place_side_grid(symbol, state, OrderSide::Buy, &bids)
                .await?;
            self.place_side_grid(symbol, state, OrderSide::Sell, &asks)
                .await?;
            Ok(())
        }

        pub async fn cancel_symbol(&self, symbol: &str, state: &mut SymbolState) -> Result<()> {
            for order in state.bid_orders.drain(..) {
                self.cancel_existing(symbol, &order).await.ok();
            }
            for order in state.ask_orders.drain(..) {
                self.cancel_existing(symbol, &order).await.ok();
            }
            Ok(())
        }

        pub async fn cancel_all_symbols(
            &self,
            states: &mut HashMap<String, SymbolState>,
        ) -> Result<()> {
            for (symbol, state) in states.iter_mut() {
                let _ = self.cancel_symbol(symbol, state).await;
            }
            Ok(())
        }

        async fn place_side_grid(
            &self,
            symbol: &str,
            state: &mut SymbolState,
            side: OrderSide,
            targets: &[(f64, f64)],
        ) -> Result<()> {
            let orders = match side {
                OrderSide::Buy => &mut state.bid_orders,
                OrderSide::Sell => &mut state.ask_orders,
            };

            for (idx, (raw_price, raw_qty)) in targets.iter().enumerate() {
                let price = apply_precision(
                    *raw_price,
                    state.metrics.tick_size,
                    state.metrics.price_digits,
                );
                let qty =
                    apply_precision(*raw_qty, state.metrics.lot_size, state.metrics.qty_digits);

                if price <= 0.0 || qty <= 0.0 || price * qty < state.config.min_notional {
                    if let Some(existing) = orders.get(idx) {
                        self.cancel_existing(symbol, existing).await.ok();
                    }
                    if idx < orders.len() {
                        orders.remove(idx);
                    }
                    continue;
                }

                let needs_replace = match orders.get(idx) {
                    Some(order) => {
                        let age = Utc::now() - order.placed_at;
                        age > self.ttl
                            || (order.price - price).abs() > state.metrics.tick_size * 0.5
                            || (order.quantity - qty).abs() > state.metrics.lot_size * 0.5
                    }
                    None => true,
                };

                if !needs_replace {
                    continue;
                }

                if let Some(existing) = orders.get(idx) {
                    self.cancel_existing(symbol, existing).await.ok();
                }

                let client_id = Self::client_id(&state.config.symbol, side, idx);
                let mut request = OrderRequest::new(
                    state.config.symbol.clone(),
                    side,
                    OrderType::Limit,
                    qty,
                    Some(price),
                    self.market_type,
                );
                request.client_order_id = Some(client_id.clone());
                request.time_in_force = Some("GTC".to_string());
                request.post_only = Some(true);

                let order = self
                    .account
                    .exchange
                    .create_order(request)
                    .await
                    .map_err(|e| anyhow!("create_order failed: {}", e))?;

                let managed = ManagedOrder {
                    client_id,
                    order_id: Some(order.id.clone()),
                    price,
                    quantity: qty,
                    side,
                    placed_at: Utc::now(),
                };

                if idx < orders.len() {
                    orders[idx] = managed;
                } else {
                    orders.push(managed);
                }
            }

            while orders.len() > targets.len() {
                if let Some(existing) = orders.pop() {
                    self.cancel_existing(symbol, &existing).await.ok();
                }
            }

            Ok(())
        }

        async fn cancel_existing(&self, symbol: &str, order: &ManagedOrder) -> Result<()> {
            let order_id = order.order_id.as_deref().unwrap_or(&order.client_id);
            self.account
                .exchange
                .cancel_order(order_id, symbol, self.market_type)
                .await
                .map_err(|e| anyhow!("cancel_order failed: {}", e))?;
            Ok(())
        }

        fn client_id(symbol: &str, side: OrderSide, level: usize) -> String {
            let suffix: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(6)
                .map(char::from)
                .collect();
            format!(
                "mm-{}-{}-{}-{}",
                symbol.replace('/', ""),
                side,
                level,
                suffix
            )
        }
    }
}

mod risk {
    use std::sync::Arc;

    use crate::strategies::common::snapshot::StrategySnapshot;
    use crate::strategies::common::{RiskAction, RiskDecision, UnifiedRiskEvaluator};

    use super::MarketMakingRiskConfig;

    pub struct RiskController {
        cfg: MarketMakingRiskConfig,
        evaluator: Arc<dyn UnifiedRiskEvaluator>,
    }

    impl RiskController {
        pub fn new(cfg: MarketMakingRiskConfig, evaluator: Arc<dyn UnifiedRiskEvaluator>) -> Self {
            Self { cfg, evaluator }
        }

        pub async fn evaluate(&self, snapshot: &StrategySnapshot) -> RiskDecision {
            let mut decision = self.evaluator.evaluate(snapshot).await;

            let notional = snapshot.exposure.notional.abs();
            if notional > self.cfg.max_inventory_notional {
                let scale = (self.cfg.max_inventory_notional / notional).clamp(0.1, 1.0);
                decision.action = RiskAction::ScaleDown {
                    scale_factor: scale,
                    reason: format!(
                        "名义敞口 {:.2} 超过上限 {:.2}",
                        notional, self.cfg.max_inventory_notional
                    ),
                };
                return decision;
            }

            let pnl = snapshot.performance.realized_pnl + snapshot.performance.unrealized_pnl;
            if pnl < -self.cfg.max_loss
                || snapshot.performance.unrealized_pnl < -self.cfg.max_drawdown
            {
                decision.action = RiskAction::Halt {
                    reason: format!(
                        "损失 {:.2} 超过限额 (max_loss={}, max_drawdown={})",
                        pnl, self.cfg.max_loss, self.cfg.max_drawdown
                    ),
                };
            }

            decision
        }
    }
}
