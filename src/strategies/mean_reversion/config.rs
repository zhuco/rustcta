use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::types::MarketType;

fn default_true() -> bool {
    true
}

fn default_allow_short() -> bool {
    true
}

fn default_market_type() -> MarketType {
    MarketType::Spot
}

fn default_scan_interval_secs() -> u64 {
    300
}

fn default_min_klines_15m() -> usize {
    400
}

fn default_max_cancel_per_minute() -> u32 {
    20
}

fn default_depth_multiplier() -> f64 {
    3.0
}

fn default_volume_window_minutes() -> u64 {
    10
}

fn default_maker_fee_threshold() -> f64 {
    0.0
}

fn default_base_improve() -> f64 {
    0.001
}

fn default_alpha_atr() -> f64 {
    1.5
}

fn default_beta_sigma() -> f64 {
    1.0
}

fn default_ttl_secs() -> u64 {
    30
}

fn default_relist_delay_ms() -> u64 {
    800
}

fn default_per_trade_notional() -> f64 {
    6.0
}

fn default_per_trade_risk() -> f64 {
    6.0
}

fn default_time_stop_bars() -> u32 {
    12
}

fn default_daily_drawdown() -> f64 {
    0.03
}

fn default_cache_1m() -> usize {
    600
}

fn default_cache_5m() -> usize {
    800
}

fn default_cache_15m() -> usize {
    500
}

fn default_indicator_choppiness() -> Option<ChoppinessConfig> {
    Some(ChoppinessConfig {
        enabled: false,
        lookback: 14,
        threshold: 62.0,
    })
}

fn default_blackout_windows() -> Vec<BlackoutWindow> {
    Vec::new()
}

fn default_symbol_volume_threshold() -> f64 {
    100_000.0
}

fn default_symbol_top_levels() -> usize {
    1
}

fn default_liquidity_window_minutes() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionConfig {
    pub strategy: StrategyInfo,
    pub account: AccountSettings,
    pub data: DataConfig,
    pub symbols: Vec<SymbolConfig>,
    pub indicators: IndicatorConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub liquidity: LiquidityConfig,
    #[serde(default)]
    pub cache: CacheConfig,
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

fn default_log_level() -> String {
    "INFO".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSettings {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_allow_short")]
    pub allow_short: bool,
    #[serde(default)]
    pub max_leverage: Option<u32>,
    #[serde(default)]
    pub trading_hours: Option<TradingHours>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingHours {
    pub timezone: String,
    pub sessions: Vec<SessionWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindow {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_scan_interval_secs")]
    pub scan_interval_secs: u64,
    pub websocket: WebSocketConfig,
    pub rest: RestConfig,
    #[serde(default)]
    pub validation: DataValidationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    #[serde(default)]
    pub url_override: Option<String>,
    #[serde(default)]
    pub ping_interval_secs: Option<u64>,
    #[serde(default)]
    pub reconnect: ReconnectConfig,
    #[serde(default)]
    pub subscriptions: SubscriptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectConfig {
    #[serde(default = "default_reconnect_initial_ms")]
    pub initial_delay_ms: u64,
    #[serde(default = "default_reconnect_max_ms")]
    pub max_delay_ms: u64,
    #[serde(default = "default_reconnect_factor")]
    pub multiplier: f64,
}

fn default_reconnect_initial_ms() -> u64 {
    1_000
}

fn default_reconnect_max_ms() -> u64 {
    60_000
}

fn default_reconnect_factor() -> f64 {
    1.8
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: default_reconnect_initial_ms(),
            max_delay_ms: default_reconnect_max_ms(),
            multiplier: default_reconnect_factor(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    #[serde(default = "default_true")]
    pub kline_1m: bool,
    #[serde(default = "default_true")]
    pub kline_5m: bool,
    #[serde(default = "default_true")]
    pub kline_15m: bool,
    #[serde(default)]
    pub partial_book_depth: Option<u32>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            kline_1m: true,
            kline_5m: true,
            kline_15m: true,
            partial_book_depth: Some(5),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestConfig {
    #[serde(default = "default_min_klines_15m")]
    pub min_klines_15m: usize,
    #[serde(default = "default_history_batch")]
    pub history_batch_size: usize,
    #[serde(default = "default_history_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub backoff: BackoffConfig,
    #[serde(default)]
    pub heartbeat_secs: Option<u64>,
}

fn default_history_batch() -> usize {
    500
}

fn default_history_max_retries() -> u32 {
    6
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    #[serde(default = "default_backoff_base_ms")]
    pub base_delay_ms: u64,
    #[serde(default = "default_backoff_max_ms")]
    pub max_delay_ms: u64,
    #[serde(default = "default_backoff_factor")]
    pub multiplier: f64,
}

fn default_backoff_base_ms() -> u64 {
    500
}

fn default_backoff_max_ms() -> u64 {
    30_000
}

fn default_backoff_factor() -> f64 {
    2.0
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_delay_ms: default_backoff_base_ms(),
            max_delay_ms: default_backoff_max_ms(),
            multiplier: default_backoff_factor(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataValidationConfig {
    #[serde(default = "default_true")]
    pub enable_missing_bar_freeze: bool,
    #[serde(default = "default_true")]
    pub enable_sequence_check: bool,
    #[serde(default)]
    pub max_gap_minutes: Option<u64>,
}

impl Default for DataValidationConfig {
    fn default() -> Self {
        Self {
            enable_missing_bar_freeze: true,
            enable_sequence_check: true,
            max_gap_minutes: Some(45),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_quote_volume_5m: f64,
    #[serde(default = "default_depth_multiplier")]
    pub depth_multiplier: f64,
    #[serde(default = "default_symbol_top_levels")]
    pub depth_levels: usize,
    #[serde(default = "default_true")]
    pub enforce_spread_rule: bool,
    #[serde(default = "default_blackout_windows")]
    pub blackout_windows: Vec<BlackoutWindow>,
    #[serde(default)]
    pub overrides: HashMap<String, f64>,
    #[serde(default)]
    pub allow_short: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlackoutWindow {
    pub reason: String,
    pub start: String,
    pub end: String,
    #[serde(default)]
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorConfig {
    pub bollinger: BollingerConfig,
    pub rsi: RsiConfig,
    pub atr: AtrConfig,
    pub adx: AdxConfig,
    pub bbw: BbwConfig,
    pub slope: SlopeConfig,
    #[serde(default = "default_indicator_choppiness")]
    pub choppiness: Option<ChoppinessConfig>,
    #[serde(default)]
    pub volume: VolumeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BollingerConfig {
    #[serde(default = "default_boll_period")]
    pub period: usize,
    #[serde(default = "default_boll_std")]
    pub std_dev: f64,
    #[serde(default = "default_band_pct_long")]
    pub entry_band_pct_long: f64,
    #[serde(default = "default_band_pct_short")]
    pub entry_band_pct_short: f64,
    #[serde(default = "default_z_long")]
    pub entry_z_long: f64,
    #[serde(default = "default_z_short")]
    pub entry_z_short: f64,
}

fn default_boll_period() -> usize {
    20
}

fn default_boll_std() -> f64 {
    2.0
}

fn default_band_pct_long() -> f64 {
    0.1
}

fn default_band_pct_short() -> f64 {
    0.9
}

fn default_z_long() -> f64 {
    -1.2
}

fn default_z_short() -> f64 {
    1.2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RsiConfig {
    #[serde(default = "default_rsi_period")]
    pub period: usize,
    #[serde(default = "default_rsi_long")]
    pub long_threshold: f64,
    #[serde(default = "default_rsi_short")]
    pub short_threshold: f64,
}

fn default_rsi_period() -> usize {
    14
}

fn default_rsi_long() -> f64 {
    25.0
}

fn default_rsi_short() -> f64 {
    75.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtrConfig {
    #[serde(default = "default_atr_period")]
    pub period: usize,
    #[serde(default = "default_initial_stop_k")]
    pub initial_stop_k: f64,
    #[serde(default = "default_trailing_stop_k")]
    pub trailing_stop_k: f64,
}

fn default_atr_period() -> usize {
    14
}

fn default_initial_stop_k() -> f64 {
    1.4
}

fn default_trailing_stop_k() -> f64 {
    1.2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdxConfig {
    #[serde(default = "default_adx_period")]
    pub period: usize,
    #[serde(default = "default_adx_threshold")]
    pub threshold: f64,
}

fn default_adx_period() -> usize {
    14
}

fn default_adx_threshold() -> f64 {
    20.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BbwConfig {
    #[serde(default = "default_bbw_lookback")]
    pub lookback: usize,
    #[serde(default = "default_bbw_percentile")]
    pub percentile: f64,
}

fn default_bbw_lookback() -> usize {
    400
}

fn default_bbw_percentile() -> f64 {
    0.35
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlopeConfig {
    #[serde(default = "default_slope_lookback")]
    pub lookback: usize,
    #[serde(default = "default_slope_threshold")]
    pub threshold: f64,
}

fn default_slope_lookback() -> usize {
    5
}

fn default_slope_threshold() -> f64 {
    0.15
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChoppinessConfig {
    pub enabled: bool,
    pub lookback: usize,
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VolumeConfig {
    #[serde(default = "default_volume_window_minutes")]
    pub lookback_minutes: u64,
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_quote_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_true")]
    pub post_only: bool,
    #[serde(default = "default_base_improve")]
    pub base_improve: f64,
    #[serde(default = "default_alpha_atr")]
    pub alpha_atr: f64,
    #[serde(default = "default_beta_sigma")]
    pub beta_sigma: f64,
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_max_cancel_per_minute")]
    pub cancel_rate_limit_per_min: u32,
    #[serde(default = "default_relist_delay_ms")]
    pub relist_delay_ms: u64,
    #[serde(default)]
    pub max_relist_attempts: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    #[serde(default = "default_true")]
    pub enable: bool,
    #[serde(default = "default_max_positions")]
    pub max_positions: usize,
    #[serde(default = "default_max_symbol_positions")]
    pub max_positions_per_symbol: usize,
    #[serde(default = "default_max_net_exposure")]
    pub max_net_exposure: f64,
    #[serde(default = "default_per_trade_notional")]
    pub per_trade_notional: f64,
    #[serde(default = "default_per_trade_risk")]
    pub per_trade_risk: f64,
    #[serde(default = "default_time_stop_bars")]
    pub time_stop_bars: u32,
    #[serde(default = "default_daily_drawdown")]
    pub daily_drawdown: f64,
    #[serde(default)]
    pub freeze_on_gap: bool,
}

fn default_max_positions() -> usize {
    10
}

fn default_max_symbol_positions() -> usize {
    1
}

fn default_max_net_exposure() -> f64 {
    1_000.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityConfig {
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_recent_quote_volume: f64,
    #[serde(default = "default_liquidity_window_minutes")]
    pub volume_window_minutes: u64,
    #[serde(default = "default_depth_multiplier")]
    pub depth_multiplier: f64,
    #[serde(default = "default_symbol_top_levels")]
    pub top_levels: usize,
    #[serde(default)]
    pub spread_factor_limit: Option<f64>,
    #[serde(default = "default_maker_fee_threshold")]
    pub maker_fee_threshold: f64,
}

impl Default for LiquidityConfig {
    fn default() -> Self {
        Self {
            min_recent_quote_volume: default_symbol_volume_threshold(),
            volume_window_minutes: default_volume_window_minutes(),
            depth_multiplier: default_depth_multiplier(),
            top_levels: default_symbol_top_levels(),
            spread_factor_limit: None,
            maker_fee_threshold: default_maker_fee_threshold(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_1m")]
    pub max_1m_bars: usize,
    #[serde(default = "default_cache_5m")]
    pub max_5m_bars: usize,
    #[serde(default = "default_cache_15m")]
    pub max_15m_bars: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_1m_bars: default_cache_1m(),
            max_5m_bars: default_cache_5m(),
            max_15m_bars: default_cache_15m(),
        }
    }
}
