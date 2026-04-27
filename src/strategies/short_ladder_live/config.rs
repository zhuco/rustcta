use serde::{Deserialize, Serialize};

use crate::core::types::MarketType;

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

fn default_adopt_progress_tolerance_pct() -> f64 {
    0.02
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderLiveConfig {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_true")]
    pub dual_position_mode: bool,
    #[serde(default)]
    pub default_leverage: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LadderConfig {
    #[serde(default = "default_layer_weights")]
    pub layer_weights: Vec<f64>,
    #[serde(default = "default_layer_spacing_atr")]
    pub layer_spacing_atr: f64,
    #[serde(default = "default_take_profit_atr")]
    pub take_profit_atr: f64,
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
            stop_loss_atr: default_stop_loss_atr(),
            max_hold_bars: default_max_hold_bars(),
            breakeven_stop: default_false(),
            breakeven_trigger_atr: default_breakeven_trigger_atr(),
            breakeven_buffer_bps: default_breakeven_buffer_bps(),
            adopt_progress_tolerance_pct: default_adopt_progress_tolerance_pct(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_true")]
    pub use_post_only_entry: bool,
    #[serde(default = "default_maker_price_offset_bps")]
    pub maker_price_offset_bps: f64,
    #[serde(default = "default_order_cooldown_secs")]
    pub order_cooldown_secs: u64,
    #[serde(default = "default_true")]
    pub close_with_market_order: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            use_post_only_entry: default_true(),
            maker_price_offset_bps: default_maker_price_offset_bps(),
            order_cooldown_secs: default_order_cooldown_secs(),
            close_with_market_order: default_true(),
        }
    }
}
