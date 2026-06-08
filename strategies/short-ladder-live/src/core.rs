use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{MarketType, OrderSide};
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_market_type() -> MarketType {
    MarketType::Futures
}

fn default_log_level() -> String {
    "INFO".to_string()
}

fn default_poll_interval_secs() -> u64 {
    30
}

fn default_decision_interval() -> String {
    "5m".to_string()
}

fn default_kline_limit() -> u32 {
    720
}

fn default_session_start_hour_utc() -> u32 {
    11
}

fn default_session_end_hour_utc() -> u32 {
    22
}

fn default_initial_notional() -> f64 {
    200.0
}

fn default_max_notional() -> f64 {
    2_000.0
}

fn default_layer_weights() -> Vec<f64> {
    vec![1.0, 1.0, 3.0, 5.0]
}

fn default_layer_spacing_atr() -> f64 {
    0.55
}

fn default_take_profit_atr() -> f64 {
    1.2
}

fn default_take_profit_mode() -> LiveTakeProfitMode {
    LiveTakeProfitMode::FixedAtr
}

fn default_trailing_take_profit_activation_atr() -> f64 {
    1.4
}

fn default_trailing_take_profit_distance_atr() -> f64 {
    1.4
}

fn default_stop_loss_atr() -> f64 {
    1.7
}

fn default_breakeven_trigger_atr() -> f64 {
    0.8
}

fn default_breakeven_buffer_bps() -> f64 {
    2.0
}

fn default_max_hold_bars() -> usize {
    48
}

fn default_atr_period() -> usize {
    14
}

fn default_rsi_period() -> usize {
    14
}

fn default_entry_rsi_min() -> f64 {
    52.0
}

fn default_filter_ema() -> usize {
    50
}

fn default_strong_1h_ema_slope_lookback() -> usize {
    3
}

fn default_strong_1h_ema_slope_max_pct() -> f64 {
    -0.03
}

fn default_maker_price_offset_bps() -> f64 {
    1.0
}

fn default_order_cooldown_secs() -> u64 {
    300
}

fn default_take_profit_maker_timeout_secs() -> u64 {
    45
}

fn default_take_profit_maker_poll_secs() -> u64 {
    5
}

fn default_take_profit_maker_timeout_bars() -> u64 {
    0
}

fn default_entry_maker_timeout_secs() -> u64 {
    45
}

fn default_entry_maker_reprice_attempts() -> u32 {
    1
}

fn default_adopt_progress_tolerance_pct() -> f64 {
    0.02
}

fn default_ws_exit_check_interval_ms() -> u64 {
    1_000
}

fn default_trailing_update_interval_secs() -> u64 {
    15
}

fn default_ws_reconnect_delay_secs() -> u64 {
    5
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortLadderLiveCoreConfig {
    pub strategy: StrategyInfo,
    pub account: AccountConfig,
    pub data: DataConfig,
    pub symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub signal: SignalConfig,
    #[serde(default)]
    pub ladder: LadderConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub notifications: NotificationConfig,
}

impl ShortLadderLiveCoreConfig {
    pub fn enabled_symbols(&self) -> impl Iterator<Item = &SymbolConfig> {
        self.symbols.iter().filter(|symbol| symbol.enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTakeProfitMode {
    FixedAtr,
    AtrTrailing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_true")]
    pub dual_position_mode: bool,
    #[serde(default)]
    pub default_leverage: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_decision_interval")]
    pub decision_interval: String,
    #[serde(default = "default_kline_limit")]
    pub kline_limit: u32,
    #[serde(default = "default_session_start_hour_utc")]
    pub session_start_hour_utc: u32,
    #[serde(default = "default_session_end_hour_utc")]
    pub session_end_hour_utc: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_initial_notional")]
    pub initial_notional: f64,
    #[serde(default = "default_max_notional")]
    pub max_notional: f64,
    #[serde(default)]
    pub leverage: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalConfig {
    #[serde(default = "default_atr_period")]
    pub atr_period: usize,
    #[serde(default = "default_rsi_period")]
    pub rsi_period: usize,
    #[serde(default = "default_entry_rsi_min")]
    pub entry_rsi_min: f64,
    #[serde(default = "default_false")]
    pub entry_requires_lower_close: bool,
    #[serde(default = "default_false")]
    pub entry_requires_prior_high_sweep: bool,
    #[serde(default = "default_filter_ema")]
    pub filter_15m_ema: usize,
    #[serde(default = "default_filter_ema")]
    pub filter_1h_ema: usize,
    #[serde(default = "default_true")]
    pub final_layer_requires_strong_1h: bool,
    #[serde(default = "default_strong_1h_ema_slope_lookback")]
    pub strong_1h_ema_slope_lookback: usize,
    #[serde(default = "default_strong_1h_ema_slope_max_pct")]
    pub strong_1h_ema_slope_max_pct: f64,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            atr_period: default_atr_period(),
            rsi_period: default_rsi_period(),
            entry_rsi_min: default_entry_rsi_min(),
            entry_requires_lower_close: default_false(),
            entry_requires_prior_high_sweep: default_false(),
            filter_15m_ema: default_filter_ema(),
            filter_1h_ema: default_filter_ema(),
            final_layer_requires_strong_1h: default_true(),
            strong_1h_ema_slope_lookback: default_strong_1h_ema_slope_lookback(),
            strong_1h_ema_slope_max_pct: default_strong_1h_ema_slope_max_pct(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LadderConfig {
    #[serde(default = "default_layer_weights")]
    pub layer_weights: Vec<f64>,
    #[serde(default = "default_layer_spacing_atr")]
    pub layer_spacing_atr: f64,
    #[serde(default = "default_take_profit_atr")]
    pub take_profit_atr: f64,
    #[serde(default = "default_take_profit_mode")]
    pub take_profit_mode: LiveTakeProfitMode,
    #[serde(default = "default_trailing_take_profit_activation_atr")]
    pub trailing_take_profit_activation_atr: f64,
    #[serde(default = "default_trailing_take_profit_distance_atr")]
    pub trailing_take_profit_distance_atr: f64,
    #[serde(default = "default_stop_loss_atr")]
    pub stop_loss_atr: f64,
    #[serde(default = "default_max_hold_bars")]
    pub max_hold_bars: usize,
    #[serde(default = "default_false")]
    pub breakeven_stop: bool,
    #[serde(default = "default_breakeven_trigger_atr")]
    pub breakeven_trigger_atr: f64,
    #[serde(default = "default_breakeven_buffer_bps")]
    pub breakeven_buffer_bps: f64,
    #[serde(default = "default_adopt_progress_tolerance_pct")]
    pub adopt_progress_tolerance_pct: f64,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            layer_weights: default_layer_weights(),
            layer_spacing_atr: default_layer_spacing_atr(),
            take_profit_atr: default_take_profit_atr(),
            take_profit_mode: default_take_profit_mode(),
            trailing_take_profit_activation_atr: default_trailing_take_profit_activation_atr(),
            trailing_take_profit_distance_atr: default_trailing_take_profit_distance_atr(),
            stop_loss_atr: default_stop_loss_atr(),
            max_hold_bars: default_max_hold_bars(),
            breakeven_stop: default_false(),
            breakeven_trigger_atr: default_breakeven_trigger_atr(),
            breakeven_buffer_bps: default_breakeven_buffer_bps(),
            adopt_progress_tolerance_pct: default_adopt_progress_tolerance_pct(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub channel_id: Option<String>,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            channel_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_true")]
    pub use_post_only_entry: bool,
    #[serde(default = "default_maker_price_offset_bps")]
    pub maker_price_offset_bps: f64,
    #[serde(default = "default_order_cooldown_secs")]
    pub order_cooldown_secs: u64,
    #[serde(default)]
    pub initial_order_taker_fallback_secs: Option<u64>,
    #[serde(default = "default_true")]
    pub close_with_market_order: bool,
    #[serde(default = "default_true")]
    pub take_profit_maker_first: bool,
    #[serde(default = "default_take_profit_maker_timeout_secs")]
    pub take_profit_maker_timeout_secs: u64,
    #[serde(default = "default_take_profit_maker_poll_secs")]
    pub take_profit_maker_poll_secs: u64,
    #[serde(default = "default_take_profit_maker_timeout_bars")]
    pub take_profit_maker_timeout_bars: u64,
    #[serde(default = "default_entry_maker_timeout_secs")]
    pub entry_maker_timeout_secs: u64,
    #[serde(default = "default_entry_maker_reprice_attempts")]
    pub entry_maker_reprice_attempts: u32,
    #[serde(default = "default_true")]
    pub initial_entry_market_after_reprice: bool,
    #[serde(default)]
    pub websocket: WebSocketExitConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            use_post_only_entry: default_true(),
            maker_price_offset_bps: default_maker_price_offset_bps(),
            order_cooldown_secs: default_order_cooldown_secs(),
            initial_order_taker_fallback_secs: None,
            close_with_market_order: default_true(),
            take_profit_maker_first: default_true(),
            take_profit_maker_timeout_secs: default_take_profit_maker_timeout_secs(),
            take_profit_maker_poll_secs: default_take_profit_maker_poll_secs(),
            take_profit_maker_timeout_bars: default_take_profit_maker_timeout_bars(),
            entry_maker_timeout_secs: default_entry_maker_timeout_secs(),
            entry_maker_reprice_attempts: default_entry_maker_reprice_attempts(),
            initial_entry_market_after_reprice: default_true(),
            websocket: WebSocketExitConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketExitConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_ws_exit_check_interval_ms")]
    pub exit_check_interval_ms: u64,
    #[serde(default = "default_trailing_update_interval_secs")]
    pub trailing_update_interval_secs: u64,
    #[serde(default = "default_ws_reconnect_delay_secs")]
    pub reconnect_delay_secs: u64,
}

impl Default for WebSocketExitConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            exit_check_interval_ms: default_ws_exit_check_interval_ms(),
            trailing_update_interval_secs: default_trailing_update_interval_secs(),
            reconnect_delay_secs: default_ws_reconnect_delay_secs(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub step_size: f64,
    pub tick_size: f64,
    pub min_notional: f64,
    pub min_order_size: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveShortPosition {
    pub average_entry_price: f64,
    pub quantity: f64,
    pub current_notional: f64,
    pub filled_layers: usize,
    pub next_layer_index: usize,
    pub last_layer_price: f64,
    pub atr_at_entry: f64,
    pub breakeven_armed: bool,
    pub trailing_take_profit_armed: bool,
    pub best_favorable_price: Option<f64>,
    pub opened_at: DateTime<Utc>,
    pub last_sync_at: DateTime<Utc>,
}

impl LiveShortPosition {
    pub fn unrealized_pnl(&self, reference_price: f64) -> f64 {
        (self.average_entry_price - reference_price) * self.quantity
    }

    pub fn is_losing(&self, reference_price: f64) -> bool {
        self.unrealized_pnl(reference_price) < 0.0
    }

    pub fn update_short_trailing_take_profit(
        &mut self,
        current_price: f64,
        activation_atr: f64,
        distance_atr: f64,
    ) -> Option<f64> {
        if self.atr_at_entry <= 0.0 || activation_atr <= 0.0 || distance_atr <= 0.0 {
            return None;
        }

        let activation_price = self.average_entry_price - activation_atr * self.atr_at_entry;
        if current_price <= activation_price {
            self.trailing_take_profit_armed = true;
            self.best_favorable_price = Some(
                self.best_favorable_price
                    .map(|best| best.min(current_price))
                    .unwrap_or(current_price),
            );
        }

        if !self.trailing_take_profit_armed {
            return None;
        }

        self.best_favorable_price.and_then(|best_price| {
            let stop_price = best_price + distance_atr * self.atr_at_entry;
            (current_price >= stop_price).then_some(stop_price)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingEntryOrder {
    pub order_id: String,
    pub layer_index: usize,
    pub quantity: f64,
    pub notional: f64,
    pub reference_price: f64,
    pub reason: String,
    pub attempts: u32,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingInitialEntry {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub notional: f64,
    pub order_price: f64,
    pub reference_price: f64,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedOrder {
    pub order_id: String,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingExitOrder {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub reason: String,
    pub submitted_at: DateTime<Utc>,
    pub reference_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolState {
    pub symbol: String,
    pub precision: Option<SymbolPrecision>,
    pub short: Option<LiveShortPosition>,
    pub pending_entry: Option<PendingEntryOrder>,
    pub last_order_at: Option<DateTime<Utc>>,
    pub pending_initial_entry: Option<PendingInitialEntry>,
    pub pending_exit: Option<PendingExitOrder>,
    pub last_ws_exit_check_at: Option<DateTime<Utc>>,
    pub last_trailing_update_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl SymbolState {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            precision: None,
            short: None,
            pending_entry: None,
            last_order_at: None,
            pending_initial_entry: None,
            pending_exit: None,
            last_ws_exit_check_at: None,
            last_trailing_update_at: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeState {
    pub symbols: HashMap<String, SymbolState>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdoptedShortProgress {
    pub current_notional: f64,
    pub filled_layers: usize,
    pub next_layer_index: usize,
    pub capped_at_max_notional: bool,
}

pub fn cumulative_layer_notionals(initial_notional: f64, layer_weights: &[f64]) -> Vec<f64> {
    let mut total = 0.0;
    layer_weights
        .iter()
        .map(|weight| {
            total += initial_notional * *weight;
            total
        })
        .collect()
}

pub fn adopt_short_progress(
    quantity: f64,
    average_entry_price: f64,
    initial_notional: f64,
    max_notional: f64,
    layer_weights: &[f64],
    tolerance_pct: f64,
) -> AdoptedShortProgress {
    let current_notional = (quantity.abs() * average_entry_price).max(0.0);
    let cumulative = cumulative_layer_notionals(initial_notional, layer_weights);
    let mut filled_layers = 0_usize;
    let tolerance = tolerance_pct.max(0.0);

    for (index, target) in cumulative.iter().enumerate() {
        if current_notional <= *target * (1.0 + tolerance) {
            filled_layers = index + 1;
            break;
        }
    }

    if filled_layers == 0 && !cumulative.is_empty() {
        filled_layers = cumulative.len();
    }
    if current_notional <= initial_notional * (1.0 + tolerance) {
        filled_layers = filled_layers.max(1).min(cumulative.len().max(1));
    }

    let capped_at_max_notional = current_notional >= max_notional * (1.0 - tolerance);
    if capped_at_max_notional && !cumulative.is_empty() {
        filled_layers = cumulative.len();
    }

    AdoptedShortProgress {
        current_notional,
        filled_layers,
        next_layer_index: filled_layers.min(layer_weights.len()),
        capped_at_max_notional,
    }
}

pub fn infer_short_ladder_last_price(
    average_entry_price: f64,
    atr_spacing: f64,
    layer_notionals: &[f64],
) -> f64 {
    if layer_notionals.len() <= 1 || atr_spacing <= 0.0 || average_entry_price <= 0.0 {
        return average_entry_price;
    }

    let mut low = (average_entry_price - atr_spacing * layer_notionals.len() as f64 * 4.0)
        .max(average_entry_price * 0.05)
        .max(1e-9);
    let high = average_entry_price;
    while implied_average(low, atr_spacing, layer_notionals) > average_entry_price && low > 1e-8 {
        low *= 0.5;
    }

    let mut left = low;
    let mut right = high;
    for _ in 0..80 {
        let mid = (left + right) * 0.5;
        if implied_average(mid, atr_spacing, layer_notionals) <= average_entry_price {
            left = mid;
        } else {
            right = mid;
        }
    }

    left + atr_spacing * (layer_notionals.len() as f64 - 1.0)
}

fn implied_average(first_price: f64, atr_spacing: f64, layer_notionals: &[f64]) -> f64 {
    let total_notional = layer_notionals.iter().sum::<f64>();
    let total_qty = layer_notionals
        .iter()
        .enumerate()
        .map(|(index, notional)| notional / (first_price + atr_spacing * index as f64))
        .sum::<f64>();
    if total_qty <= 0.0 {
        return first_price;
    }
    total_notional / total_qty
}

pub fn should_add_short_layer(
    position: &LiveShortPosition,
    current_price: f64,
    layer_count: usize,
    layer_spacing_atr: f64,
    allow_final_layer: bool,
) -> Option<(usize, f64)> {
    if !position.is_losing(current_price) || position.next_layer_index >= layer_count {
        return None;
    }
    let is_final_layer = position.next_layer_index + 1 == layer_count;
    if is_final_layer && !allow_final_layer {
        return None;
    }
    let trigger_price = position.last_layer_price + layer_spacing_atr * position.atr_at_entry;
    if current_price >= trigger_price {
        Some((position.next_layer_index, trigger_price))
    } else {
        None
    }
}

pub fn capped_layer_notional(
    initial_notional: f64,
    max_notional: f64,
    current_notional: f64,
    layer_weights: &[f64],
    layer_index: usize,
) -> Option<f64> {
    if layer_index >= layer_weights.len() || initial_notional <= 0.0 || max_notional <= 0.0 {
        return None;
    }
    let remaining = max_notional - current_notional.max(0.0);
    if remaining <= 0.0 {
        return None;
    }
    let planned = initial_notional * layer_weights[layer_index].max(0.0);
    let capped = planned.min(remaining);
    (capped > 0.0).then_some(capped)
}

pub fn held_bars_since(
    opened_at: DateTime<Utc>,
    reference_time: DateTime<Utc>,
    interval_secs: u64,
) -> usize {
    if interval_secs == 0 || reference_time <= opened_at {
        return 0;
    }
    let elapsed = reference_time
        .signed_duration_since(opened_at)
        .num_seconds()
        .max(0) as u64;
    (elapsed / interval_secs) as usize
}

pub fn precision_round_down(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    if step <= 0.0 {
        return value;
    }
    (value / step).floor() * step
}

pub fn precision_round_up(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    if step <= 0.0 {
        return value;
    }
    (value / step).ceil() * step
}

pub fn decimal_places_from_step(step: f64) -> usize {
    if step <= 0.0 || !step.is_finite() {
        return 8;
    }
    let mut places = 0_usize;
    let mut scaled = step.abs();
    while places < 12 && scaled.fract().abs() > 1e-12 {
        scaled *= 10.0;
        places += 1;
    }
    places
}

pub fn precision_format(value: f64, step: f64) -> String {
    let places = decimal_places_from_step(step);
    let mut text = format!("{:.*}", places, value);
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text == "-0" || text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

pub fn matches_short_position_side(side: &str) -> bool {
    let normalized = side.trim().to_ascii_uppercase();
    normalized == "SHORT" || normalized == "SELL"
}

pub fn position_params(
    dual_position_mode: bool,
    side: OrderSide,
) -> Option<HashMap<String, String>> {
    if !dual_position_mode {
        return None;
    }
    let mut params = HashMap::new();
    params.insert(
        "positionSide".to_string(),
        match side {
            OrderSide::Buy => "LONG".to_string(),
            OrderSide::Sell => "SHORT".to_string(),
        },
    );
    Some(params)
}

pub fn derive_order_quantity_from_precision(
    precision: &SymbolPrecision,
    notional: f64,
    reference_price: f64,
) -> Option<f64> {
    if notional <= 0.0 || reference_price <= 0.0 {
        return None;
    }
    let mut qty = notional / reference_price;
    qty = precision_round_down(qty, precision.step_size.max(precision.min_order_size));
    if qty < precision.min_order_size {
        qty = precision.min_order_size;
    }
    qty = precision_round_down(qty, precision.step_size);
    if qty <= 0.0 {
        return None;
    }

    let final_notional = qty * reference_price;
    if precision.min_notional > 0.0 && final_notional + 1e-9 < precision.min_notional {
        let min_qty = precision_round_down(
            (precision.min_notional / reference_price).max(precision.min_order_size),
            precision.step_size,
        );
        if min_qty <= 0.0 {
            return None;
        }
        qty = min_qty;
    }
    Some(qty)
}

pub fn maker_sell_price(
    precision: &SymbolPrecision,
    reference_price: f64,
    maker_price_offset_bps: f64,
) -> f64 {
    let price = reference_price * (1.0 + maker_price_offset_bps / 10_000.0);
    precision_round_up(price, precision.tick_size)
}

pub fn maker_buy_price(
    precision: &SymbolPrecision,
    reference_price: f64,
    maker_price_offset_bps: f64,
) -> f64 {
    let price = reference_price * (1.0 - maker_price_offset_bps / 10_000.0);
    precision_round_down(price, precision.tick_size)
}

pub fn order_params_with_precision(
    dual_position_mode: bool,
    side: OrderSide,
    precision: &SymbolPrecision,
    quantity: f64,
    price: Option<f64>,
) -> HashMap<String, String> {
    let mut params = position_params(dual_position_mode, side).unwrap_or_default();
    params.insert(
        "quantity".to_string(),
        precision_format(quantity, precision.step_size),
    );
    if let Some(price) = price {
        params.insert(
            "price".to_string(),
            precision_format(price, precision.tick_size),
        );
    }
    params
}

pub fn should_fallback_initial_entry_to_market(
    use_post_only_entry: bool,
    fallback_secs: Option<u64>,
    layer_index: usize,
) -> bool {
    use_post_only_entry && layer_index == 0 && fallback_secs.unwrap_or(0) > 0
}

pub fn same_pending_initial_order(order: &ObservedOrder, pending: &PendingInitialEntry) -> bool {
    order.order_id == pending.order_id
        || order
            .client_order_id
            .as_deref()
            .zip(pending.client_order_id.as_deref())
            .is_some_and(|(left_id, right_id)| left_id == right_id)
}

pub fn is_short_ladder_exit_order(order: &ObservedOrder) -> bool {
    order
        .client_order_id
        .as_deref()
        .map(|id| id.starts_with("sll_") && (id.contains("_close_") || id.contains("_c_")))
        .unwrap_or(false)
}

pub fn build_short_ladder_client_order_id_at(
    symbol: &str,
    tag: &str,
    level: usize,
    timestamp_millis: i64,
) -> String {
    let symbol_slug = safe_id_segment(symbol);
    let tag_slug = safe_id_segment(tag);
    let timestamp = timestamp_millis.rem_euclid(1_000_000_000);
    let id = format!(
        "sll_{}_{}_{}_{:02}",
        symbol_slug,
        tag_slug,
        timestamp,
        level % 100
    );
    if id.len() <= 36 {
        return id;
    }

    let tag_len = tag_slug.len().min(5);
    let truncated_tag = tag_slug.chars().take(tag_len).collect::<String>();
    let max_symbol_len = 36usize
        .saturating_sub("sll_".len())
        .saturating_sub(truncated_tag.len())
        .saturating_sub(1)
        .saturating_sub(1)
        .saturating_sub(timestamp.to_string().len())
        .saturating_sub(1)
        .saturating_sub(2);
    let truncated_symbol = symbol_slug.chars().take(max_symbol_len).collect::<String>();
    format!(
        "sll_{}_{}_{}_{:02}",
        truncated_symbol,
        truncated_tag,
        timestamp,
        level % 100
    )
}

fn safe_id_segment(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

pub fn layer_notionals_until(
    initial_notional: f64,
    layer_weights: &[f64],
    filled_layers: usize,
) -> Vec<f64> {
    let cumulative_len = cumulative_layer_notionals(initial_notional, layer_weights).len();
    layer_weights
        .iter()
        .take(filled_layers.min(cumulative_len))
        .map(|weight| initial_notional * *weight)
        .collect()
}
