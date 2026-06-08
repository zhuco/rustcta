use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{OrderSide, OrderType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeGridCoreConfig {
    pub strategy: StrategyInfo,
    pub execution: ExecutionConfig,
    pub precision_management: PrecisionManagementConfig,
    pub indicators: IndicatorConfig,
    pub schedule: ScheduleConfig,
    pub risk_control: RiskControlConfig,
    pub symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub notifications: Option<NotificationConfig>,
    #[serde(default)]
    pub websocket: Option<WebSocketConfig>,
    #[serde(default)]
    pub logging: Option<LoggingConfig>,
}

impl RangeGridCoreConfig {
    pub fn enabled_symbols(&self) -> impl Iterator<Item = &SymbolConfig> {
        self.symbols.iter().filter(|symbol| symbol.enabled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub strategy_type: String,
    pub market_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub startup_cancel_all: bool,
    pub shutdown_cancel_all: bool,
    pub thread_per_symbol: bool,
    pub evaluation_interval_secs: u64,
    pub cooldown_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrecisionManagementConfig {
    pub auto_fetch: bool,
    pub write_back: bool,
    pub lock_path: Option<String>,
    pub backup_suffix: Option<String>,
    #[serde(default)]
    pub config_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorConfig {
    pub lower_timeframe: LowerTimeframeConfig,
    pub higher_timeframe: HigherTimeframeConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LowerTimeframeConfig {
    pub timeframe: String,
    pub bollinger: BollingerConfig,
    pub rsi: RsiConfig,
    pub atr: AtrConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HigherTimeframeConfig {
    pub timeframe: String,
    pub adx_length: u64,
    pub adx_threshold: f64,
    pub bbw_window: usize,
    pub bbw_quantile: f64,
    pub slope_threshold: f64,
    #[serde(default)]
    pub choppiness: Option<ChoppinessConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BollingerConfig {
    pub length: usize,
    pub k: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RsiConfig {
    pub length: usize,
    pub overbought: f64,
    pub oversold: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtrConfig {
    pub length: usize,
    pub stop_multiplier: f64,
    pub trail_multiplier: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoppinessConfig {
    pub enabled: bool,
    pub threshold: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub timezone: String,
    pub sessions: Vec<SessionWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionWindow {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskControlConfig {
    pub max_leverage: f64,
    pub max_drawdown: f64,
    pub daily_loss_limit: f64,
    pub position_limit_per_symbol: f64,
    pub max_unrealized_loss_per_symbol: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub config_id: String,
    pub enabled: bool,
    pub account: AccountConfig,
    pub symbol: String,
    pub precision: SymbolPrecision,
    pub grid: GridConfig,
    pub regime_filters: RegimeFilterConfig,
    pub order: OrderConfig,
    pub market_exit: MarketExitConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub env_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub price_digits: Option<u32>,
    pub amount_digits: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridConfig {
    pub grid_spacing_pct: f64,
    pub levels_per_side: usize,
    pub base_order_notional: f64,
    pub max_position_notional: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeFilterConfig {
    pub min_liquidity_usd: f64,
    pub require_conditions: usize,
    pub adx_max: f64,
    pub bbw_quantile: f64,
    pub slope_threshold: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderConfig {
    pub post_only: bool,
    pub tif: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketExitConfig {
    pub use_market_order: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotificationConfig {
    #[serde(default)]
    pub wecom: Option<WeComConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeComConfig {
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub mentioned_list: Vec<String>,
    #[serde(default)]
    pub mentioned_mobile_list: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WebSocketConfig {
    #[serde(default)]
    pub subscribe_order_updates: bool,
    #[serde(default)]
    pub subscribe_trade_updates: bool,
    #[serde(default)]
    pub subscribe_ticker: bool,
    #[serde(default)]
    pub reconnect_on_disconnect: bool,
    #[serde(default)]
    pub heartbeat_interval: Option<u64>,
    #[serde(default)]
    pub log_all_trades: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LoggingConfig {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub console: bool,
    #[serde(default)]
    pub show_pnl: bool,
    #[serde(default)]
    pub show_position: bool,
    #[serde(default)]
    pub show_regime_changes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Range,
    TrendUp,
    TrendDown,
    Cooldown,
    Disabled,
}

impl MarketRegime {
    pub fn is_range(self) -> bool {
        matches!(self, MarketRegime::Range)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorSnapshot {
    pub timestamp: DateTime<Utc>,
    pub last_price: f64,
    pub lower_band_ratio: f64,
    pub z_score: f64,
    pub rsi: f64,
    pub atr: f64,
    pub adx: f64,
    pub bbw_percentile: f64,
    pub slope_metric: f64,
    pub choppiness: Option<f64>,
}

impl Default for IndicatorSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Utc::now(),
            last_price: 0.0,
            lower_band_ratio: 0.0,
            z_score: 0.0,
            rsi: 0.0,
            atr: 0.0,
            adx: 0.0,
            bbw_percentile: 0.0,
            slope_metric: 0.0,
            choppiness: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridOrderPlan {
    pub client_id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GridPlan {
    pub center_price: f64,
    pub orders: Vec<GridOrderPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairPrecision {
    pub price_digits: u32,
    pub amount_digits: u32,
    pub price_step: f64,
    pub amount_step: f64,
    pub min_notional: Option<f64>,
}

impl Default for PairPrecision {
    fn default() -> Self {
        Self {
            price_digits: 0,
            amount_digits: 0,
            price_step: 0.0,
            amount_step: 0.0,
            min_notional: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairRuntimeState {
    pub config: SymbolConfig,
    pub regime: MarketRegime,
    pub grid_active: bool,
    pub need_rebuild: bool,
    pub cooling_down: bool,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub precision: Option<PairPrecision>,
    pub indicator: Option<IndicatorSnapshot>,
    pub current_plan: Option<GridPlan>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub net_position: f64,
    pub current_price: f64,
    pub last_grid_rebuild: Option<DateTime<Utc>>,
    pub last_notification: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_center_price: Option<f64>,
    pub last_fill_price: Option<f64>,
}

impl PairRuntimeState {
    pub fn new(config: SymbolConfig) -> Self {
        Self {
            config,
            regime: MarketRegime::Disabled,
            grid_active: false,
            need_rebuild: false,
            cooling_down: false,
            cooldown_until: None,
            precision: None,
            indicator: None,
            current_plan: None,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            net_position: 0.0,
            current_price: 0.0,
            last_grid_rebuild: None,
            last_notification: None,
            started_at: None,
            pending_center_price: None,
            last_fill_price: None,
        }
    }

    pub fn mark_precision(&mut self, precision: PairPrecision) {
        self.precision = Some(precision);
    }

    pub fn mark_indicator(&mut self, indicator: IndicatorSnapshot) {
        self.current_price = indicator.last_price;
        self.indicator = Some(indicator);
    }

    pub fn mark_regime(&mut self, regime: MarketRegime) {
        self.regime = regime;
    }

    pub fn activate_grid(&mut self, plan: GridPlan) {
        self.grid_active = true;
        self.current_plan = Some(plan);
        let now = Utc::now();
        self.last_grid_rebuild = Some(now);
        self.started_at = self.started_at.or(Some(now));
    }

    pub fn deactivate_grid(&mut self) {
        self.grid_active = false;
        self.current_plan = None;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeDecision {
    pub new_regime: MarketRegime,
    pub activate_grid: bool,
    pub deactivate_grid: bool,
    pub reason: Option<String>,
}

impl RegimeDecision {
    pub fn stay(regime: MarketRegime) -> Self {
        Self {
            new_regime: regime,
            activate_grid: false,
            deactivate_grid: false,
            reason: None,
        }
    }
}

pub struct RangeRegimeClassifier<'a> {
    pub symbol_config: &'a SymbolConfig,
    pub indicator_config: &'a IndicatorConfig,
}

impl<'a> RangeRegimeClassifier<'a> {
    pub fn new(symbol_config: &'a SymbolConfig, indicator_config: &'a IndicatorConfig) -> Self {
        Self {
            symbol_config,
            indicator_config,
        }
    }

    pub fn evaluate(
        &self,
        snapshot: &IndicatorSnapshot,
        current_regime: MarketRegime,
        grid_active: bool,
    ) -> RegimeDecision {
        let filters = &self.symbol_config.regime_filters;
        let adx_ok = snapshot.adx <= filters.adx_max;
        let slope_abs = snapshot.slope_metric.abs();
        let slope_ok = slope_abs <= filters.slope_threshold;

        if !adx_ok || !slope_ok {
            let mut reasons = Vec::new();
            if !adx_ok {
                reasons.push(format!("ADX {:.2} > {:.2}", snapshot.adx, filters.adx_max));
            }
            if !slope_ok {
                reasons.push(format!(
                    "middle slope {:.2} > {:.2}",
                    slope_abs, filters.slope_threshold
                ));
            }
            let new_regime = if snapshot.slope_metric >= 0.0 {
                MarketRegime::TrendUp
            } else {
                MarketRegime::TrendDown
            };
            return RegimeDecision {
                new_regime,
                activate_grid: false,
                deactivate_grid: grid_active,
                reason: Some(format!("trend filters triggered: {}", reasons.join(" | "))),
            };
        }

        let mut satisfied = 0_usize;
        let mut reasons = Vec::new();
        if adx_ok {
            satisfied += 1;
            reasons.push(format!("ADX {:.2} <= {:.2}", snapshot.adx, filters.adx_max));
        }
        if snapshot.bbw_percentile <= filters.bbw_quantile {
            satisfied += 1;
            reasons.push(format!(
                "BBW percentile {:.2} <= {:.2}",
                snapshot.bbw_percentile, filters.bbw_quantile
            ));
        }
        if slope_ok {
            satisfied += 1;
            reasons.push(format!(
                "middle slope {:.2} <= {:.2}",
                slope_abs, filters.slope_threshold
            ));
        }

        let rsi_cfg = &self.indicator_config.lower_timeframe.rsi;
        if snapshot.rsi >= rsi_cfg.oversold && snapshot.rsi <= rsi_cfg.overbought {
            satisfied += 1;
            reasons.push(format!(
                "RSI {:.2} in [{:.0}, {:.0}]",
                snapshot.rsi, rsi_cfg.oversold, rsi_cfg.overbought
            ));
        }

        if snapshot.z_score.abs() <= 2.0 {
            satisfied += 1;
            reasons.push(format!("Z-score {:.2} abs <= 2", snapshot.z_score));
        }

        if satisfied >= filters.require_conditions {
            let mut decision = RegimeDecision {
                new_regime: MarketRegime::Range,
                activate_grid: !grid_active,
                deactivate_grid: false,
                reason: Some(reasons.join(" | ")),
            };
            if current_regime != MarketRegime::Range {
                decision.activate_grid = true;
            }
            return decision;
        }

        let new_regime = if snapshot.slope_metric >= 0.0 {
            MarketRegime::TrendUp
        } else {
            MarketRegime::TrendDown
        };
        RegimeDecision {
            new_regime,
            activate_grid: false,
            deactivate_grid: grid_active,
            reason: Some(format!(
                "conditions satisfied {} / {}, detected {:?}",
                satisfied, filters.require_conditions, new_regime
            )),
        }
    }
}

pub fn build_grid_plan(
    symbol_cfg: &SymbolConfig,
    precision: &PairPrecision,
    center_price: f64,
) -> Result<GridPlan> {
    if center_price <= 0.0 {
        return Err(anyhow!("center price must be greater than zero"));
    }

    let grid_cfg = &symbol_cfg.grid;
    let spacing_ratio = grid_cfg.grid_spacing_pct / 100.0;
    let levels = grid_cfg.levels_per_side.max(1);
    let total_notional = grid_cfg.base_order_notional * levels as f64;
    let adjusted_base = if total_notional > grid_cfg.max_position_notional && total_notional > 0.0 {
        grid_cfg.max_position_notional / levels as f64
    } else {
        grid_cfg.base_order_notional
    };

    let mut orders = Vec::with_capacity(levels * 2);
    for level in 1..=levels {
        let step = spacing_ratio * level as f64;
        let buy_price = round_to_precision(
            center_price * (1.0 - step),
            precision.price_digits,
            precision.price_step,
        );
        let sell_price = round_to_precision(
            center_price * (1.0 + step),
            precision.price_digits,
            precision.price_step,
        );
        let buy_qty = quantize_amount(
            adjusted_base / buy_price,
            precision.amount_digits,
            precision.amount_step,
        );
        let sell_qty = quantize_amount(
            adjusted_base / sell_price,
            precision.amount_digits,
            precision.amount_step,
        );

        push_order_if_notional_ok(
            &mut orders,
            symbol_cfg,
            level,
            OrderSide::Buy,
            buy_price,
            buy_qty,
            precision.min_notional,
        );
        push_order_if_notional_ok(
            &mut orders,
            symbol_cfg,
            level,
            OrderSide::Sell,
            sell_price,
            sell_qty,
            precision.min_notional,
        );
    }

    Ok(GridPlan {
        center_price,
        orders,
    })
}

fn push_order_if_notional_ok(
    orders: &mut Vec<GridOrderPlan>,
    symbol_cfg: &SymbolConfig,
    level: usize,
    side: OrderSide,
    price: f64,
    quantity: f64,
    min_notional: Option<f64>,
) {
    if min_notional.is_some_and(|min| price * quantity < min) {
        return;
    }
    orders.push(GridOrderPlan {
        client_id: grid_client_id(symbol_cfg, &side, level),
        price,
        quantity,
        side,
        order_type: OrderType::Limit,
    });
}

pub fn round_to_precision(value: f64, digits: u32, step: f64) -> f64 {
    let mut rounded = value;
    if step > 0.0 {
        rounded = (rounded / step).round() * step;
    }
    if digits == 0 {
        return rounded.round();
    }
    let factor = 10_f64.powi(digits as i32);
    (rounded * factor).round() / factor
}

pub fn quantize_amount(value: f64, digits: u32, step: f64) -> f64 {
    let mut amount = round_to_precision(value, digits, step);
    if amount <= 0.0 && step > 0.0 {
        amount = step;
    }
    amount
}

fn grid_client_id(symbol_cfg: &SymbolConfig, side: &OrderSide, level: usize) -> String {
    let side_tag = match side {
        OrderSide::Buy => "B",
        OrderSide::Sell => "S",
    };
    format!(
        "range_grid_{}_{}_{}_{}",
        safe_id_segment(&symbol_cfg.account.exchange),
        safe_id_segment(&symbol_cfg.symbol),
        side_tag,
        level
    )
}

fn safe_id_segment(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationGridCoreConfig {
    pub config_id: String,
    pub enabled: bool,
    pub account: AccountConfig,
    pub symbol: String,
    pub grid: OscillationGridConfig,
    pub market_condition: OscillationMarketCondition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationGridConfig {
    pub spacing: f64,
    pub spacing_type: OscillationSpacingType,
    pub order_amount: f64,
    pub orders_per_side: u32,
    pub max_position: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OscillationSpacingType {
    Arithmetic,
    Geometric,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationMarketCondition {
    pub max_trend_score: f64,
    pub min_volatility: f64,
    pub max_volatility: f64,
    pub check_interval: u64,
    pub auto_stop: bool,
    pub stop_loss_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OscillationOrderStatus {
    Planned,
    Open,
    Filled,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationGridOrder {
    pub order_id: String,
    pub client_order_id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: OrderSide,
    pub status: OscillationOrderStatus,
    pub placed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationGridState {
    pub grid_active: bool,
    pub center_price: f64,
    pub buy_orders: Vec<OscillationGridOrder>,
    pub sell_orders: Vec<OscillationGridOrder>,
    pub total_trades: u32,
    pub total_profit: f64,
    pub position_quantity: f64,
    pub position_value: f64,
    pub last_price: f64,
    pub last_trend_score: f64,
    pub daily_loss: f64,
    pub max_drawdown: f64,
    pub consecutive_losses: u32,
    pub need_grid_reset: bool,
}

impl OscillationGridState {
    pub fn new(center_price: f64) -> Self {
        Self {
            grid_active: false,
            center_price,
            buy_orders: Vec::new(),
            sell_orders: Vec::new(),
            total_trades: 0,
            total_profit: 0.0,
            position_quantity: 0.0,
            position_value: 0.0,
            last_price: center_price,
            last_trend_score: 0.0,
            daily_loss: 0.0,
            max_drawdown: 0.0,
            consecutive_losses: 0,
            need_grid_reset: false,
        }
    }

    pub fn activate_with_orders(&mut self, plan: OscillationGridPlan, now: DateTime<Utc>) {
        self.center_price = plan.center_price;
        self.buy_orders.clear();
        self.sell_orders.clear();
        for order in plan.orders {
            let record = OscillationGridOrder {
                order_id: order.client_id.clone(),
                client_order_id: order.client_id,
                price: order.price,
                quantity: order.quantity,
                side: order.side.clone(),
                status: OscillationOrderStatus::Planned,
                placed_at: now,
            };
            match order.side {
                OrderSide::Buy => self.buy_orders.push(record),
                OrderSide::Sell => self.sell_orders.push(record),
            }
        }
        self.grid_active = true;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationGridPlan {
    pub center_price: f64,
    pub orders: Vec<GridOrderPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationFill {
    pub order_id: String,
    pub side: OrderSide,
    pub filled_quantity: f64,
    pub fill_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OscillationFillTransition {
    pub replenish_order: GridOrderPlan,
    pub realized_profit_delta: f64,
}

pub fn build_oscillation_grid_plan(
    config: &OscillationGridCoreConfig,
    center_price: f64,
) -> Result<OscillationGridPlan> {
    if center_price <= 0.0 {
        return Err(anyhow!("center price must be greater than zero"));
    }
    if config.grid.spacing <= 0.0 {
        return Err(anyhow!("grid spacing must be greater than zero"));
    }
    if config.grid.order_amount <= 0.0 {
        return Err(anyhow!("order amount must be greater than zero"));
    }
    if config.grid.orders_per_side == 0 {
        return Err(anyhow!("orders per side must be greater than zero"));
    }

    let mut orders = Vec::with_capacity(config.grid.orders_per_side as usize * 2);
    for level in 1..=config.grid.orders_per_side {
        let buy_price = oscillation_level_price(
            center_price,
            config.grid.spacing,
            level,
            &config.grid.spacing_type,
            OrderSide::Buy,
        );
        let sell_price = oscillation_level_price(
            center_price,
            config.grid.spacing,
            level,
            &config.grid.spacing_type,
            OrderSide::Sell,
        );
        let buy_quantity = config.grid.order_amount / buy_price;
        let sell_quantity = config.grid.order_amount / sell_price;
        orders.push(GridOrderPlan {
            client_id: oscillation_client_id(config, &OrderSide::Buy, level),
            price: buy_price,
            quantity: buy_quantity,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
        });
        orders.push(GridOrderPlan {
            client_id: oscillation_client_id(config, &OrderSide::Sell, level),
            price: sell_price,
            quantity: sell_quantity,
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
        });
    }

    Ok(OscillationGridPlan {
        center_price,
        orders,
    })
}

pub fn apply_oscillation_fill(
    config: &OscillationGridCoreConfig,
    state: &mut OscillationGridState,
    fill: OscillationFill,
) -> Result<OscillationFillTransition> {
    if fill.filled_quantity <= 0.0 {
        return Err(anyhow!("filled quantity must be greater than zero"));
    }
    if fill.fill_price <= 0.0 {
        return Err(anyhow!("fill price must be greater than zero"));
    }

    state.total_trades += 1;
    state.last_price = fill.fill_price;

    let (new_price, realized_profit_delta, next_side) = match fill.side {
        OrderSide::Buy => {
            state.position_quantity += fill.filled_quantity;
            state.position_value += fill.filled_quantity * fill.fill_price;
            state
                .buy_orders
                .retain(|order| order.order_id != fill.order_id);
            (
                fill.fill_price * (1.0 - config.grid.spacing),
                0.0,
                OrderSide::Buy,
            )
        }
        OrderSide::Sell => {
            state.position_quantity -= fill.filled_quantity;
            state.position_value -= fill.filled_quantity * fill.fill_price;
            state
                .sell_orders
                .retain(|order| order.order_id != fill.order_id);
            let profit = fill.filled_quantity * config.grid.spacing * fill.fill_price;
            state.total_profit += profit;
            (
                fill.fill_price * (1.0 + config.grid.spacing),
                profit,
                OrderSide::Sell,
            )
        }
    };
    let quantity = config.grid.order_amount / new_price;
    let replenish_order = GridOrderPlan {
        client_id: oscillation_client_id(config, &next_side, state.total_trades),
        price: new_price,
        quantity,
        side: next_side,
        order_type: OrderType::Limit,
    };
    let record = OscillationGridOrder {
        order_id: replenish_order.client_id.clone(),
        client_order_id: replenish_order.client_id.clone(),
        price: replenish_order.price,
        quantity: replenish_order.quantity,
        side: replenish_order.side.clone(),
        status: OscillationOrderStatus::Planned,
        placed_at: Utc::now(),
    };
    match replenish_order.side {
        OrderSide::Buy => state.buy_orders.push(record),
        OrderSide::Sell => state.sell_orders.push(record),
    }
    Ok(OscillationFillTransition {
        replenish_order,
        realized_profit_delta,
    })
}

fn oscillation_level_price(
    center_price: f64,
    spacing: f64,
    level: u32,
    spacing_type: &OscillationSpacingType,
    side: OrderSide,
) -> f64 {
    match (spacing_type, side) {
        (OscillationSpacingType::Arithmetic, OrderSide::Buy) => {
            center_price * (1.0 - spacing * level as f64)
        }
        (OscillationSpacingType::Arithmetic, OrderSide::Sell) => {
            center_price * (1.0 + spacing * level as f64)
        }
        (OscillationSpacingType::Geometric, OrderSide::Buy) => {
            center_price * (1.0 - spacing).powi(level as i32)
        }
        (OscillationSpacingType::Geometric, OrderSide::Sell) => {
            center_price * (1.0 + spacing).powi(level as i32)
        }
    }
}

fn oscillation_client_id(
    config: &OscillationGridCoreConfig,
    side: &OrderSide,
    level_or_seq: u32,
) -> String {
    let side_tag = match side {
        OrderSide::Buy => "B",
        OrderSide::Sell => "S",
    };
    format!(
        "osc_{}_{}_{}_{}",
        safe_id_segment(&config.account.exchange),
        safe_id_segment(&config.symbol),
        side_tag,
        level_or_seq
    )
}
