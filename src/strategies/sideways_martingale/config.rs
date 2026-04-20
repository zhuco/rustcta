use serde::{Deserialize, Serialize};

use crate::core::types::MarketType;

fn default_true() -> bool {
    true
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
    120
}

fn default_adx_period() -> usize {
    14
}

fn default_adx_threshold() -> f64 {
    20.0
}

fn default_atr_period() -> usize {
    14
}

fn default_atr_ratio_max() -> f64 {
    0.025
}

fn default_range_lookback() -> usize {
    20
}

fn default_range_amplitude_max() -> f64 {
    0.06
}

fn default_single_bar_pause_pct() -> f64 {
    0.03
}

fn default_ema_period() -> usize {
    20
}

fn default_rsi_period() -> usize {
    14
}

fn default_long_rsi_max() -> f64 {
    35.0
}

fn default_short_rsi_min() -> f64 {
    65.0
}

fn default_layer_multipliers() -> Vec<f64> {
    vec![1.0, 1.5, 2.0, 2.5]
}

fn default_adverse_move_pct() -> f64 {
    0.012
}

fn default_take_profit_pct() -> f64 {
    0.006
}

fn default_max_floating_loss_pct() -> f64 {
    0.06
}

fn default_max_consecutive_stopouts() -> u32 {
    2
}

fn default_cooldown_hours() -> i64 {
    24
}

fn default_use_market_orders() -> bool {
    true
}

fn default_entry_cooldown_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidewaysMartingaleConfig {
    pub strategy: StrategyInfo,
    pub account: AccountConfig,
    pub data: DataConfig,
    pub symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub filters: FilterConfig,
    #[serde(default)]
    pub entry: EntryConfig,
    #[serde(default)]
    pub martingale: MartingaleConfig,
    #[serde(default)]
    pub take_profit: TakeProfitConfig,
    #[serde(default)]
    pub risk: RiskConfig,
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
    pub allow_short: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub base_notional: f64,
    #[serde(default)]
    pub leverage: Option<u32>,
    #[serde(default)]
    pub allow_short: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    #[serde(default = "default_adx_period")]
    pub adx_period: usize,
    #[serde(default = "default_adx_threshold")]
    pub adx_threshold: f64,
    #[serde(default = "default_atr_period")]
    pub atr_period: usize,
    #[serde(default = "default_atr_ratio_max")]
    pub atr_ratio_max: f64,
    #[serde(default = "default_range_lookback")]
    pub range_lookback: usize,
    #[serde(default = "default_range_amplitude_max")]
    pub range_amplitude_max: f64,
    #[serde(default = "default_single_bar_pause_pct")]
    pub single_bar_pause_pct: f64,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            adx_period: default_adx_period(),
            adx_threshold: default_adx_threshold(),
            atr_period: default_atr_period(),
            atr_ratio_max: default_atr_ratio_max(),
            range_lookback: default_range_lookback(),
            range_amplitude_max: default_range_amplitude_max(),
            single_bar_pause_pct: default_single_bar_pause_pct(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryConfig {
    #[serde(default = "default_ema_period")]
    pub ema_period: usize,
    #[serde(default = "default_rsi_period")]
    pub rsi_period: usize,
    #[serde(default = "default_long_rsi_max")]
    pub long_rsi_max: f64,
    #[serde(default = "default_short_rsi_min")]
    pub short_rsi_min: f64,
}

impl Default for EntryConfig {
    fn default() -> Self {
        Self {
            ema_period: default_ema_period(),
            rsi_period: default_rsi_period(),
            long_rsi_max: default_long_rsi_max(),
            short_rsi_min: default_short_rsi_min(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MartingaleConfig {
    #[serde(default = "default_layer_multipliers")]
    pub layer_multipliers: Vec<f64>,
    #[serde(default = "default_adverse_move_pct")]
    pub adverse_move_pct: f64,
}

impl Default for MartingaleConfig {
    fn default() -> Self {
        Self {
            layer_multipliers: default_layer_multipliers(),
            adverse_move_pct: default_adverse_move_pct(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TakeProfitConfig {
    #[serde(default = "default_take_profit_pct")]
    pub target_pct: f64,
}

impl Default for TakeProfitConfig {
    fn default() -> Self {
        Self {
            target_pct: default_take_profit_pct(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    #[serde(default = "default_max_floating_loss_pct")]
    pub max_floating_loss_pct: f64,
    #[serde(default = "default_max_consecutive_stopouts")]
    pub max_consecutive_stopouts: u32,
    #[serde(default = "default_cooldown_hours")]
    pub cooldown_hours: i64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_floating_loss_pct: default_max_floating_loss_pct(),
            max_consecutive_stopouts: default_max_consecutive_stopouts(),
            cooldown_hours: default_cooldown_hours(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_use_market_orders")]
    pub use_market_orders: bool,
    #[serde(default = "default_entry_cooldown_secs")]
    pub entry_cooldown_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            use_market_orders: default_use_market_orders(),
            entry_cooldown_secs: default_entry_cooldown_secs(),
        }
    }
}
