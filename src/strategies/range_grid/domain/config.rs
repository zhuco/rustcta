use serde::{Deserialize, Serialize};

/// 自动震荡网格策略的主配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeGridConfig {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub strategy_type: String,
    pub market_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub startup_cancel_all: bool,
    pub shutdown_cancel_all: bool,
    pub thread_per_symbol: bool,
    pub evaluation_interval_secs: u64,
    pub cooldown_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionManagementConfig {
    pub auto_fetch: bool,
    pub write_back: bool,
    pub lock_path: Option<String>,
    pub backup_suffix: Option<String>,
    #[serde(default)]
    pub config_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorConfig {
    pub lower_timeframe: LowerTimeframeConfig,
    pub higher_timeframe: HigherTimeframeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LowerTimeframeConfig {
    pub timeframe: String,
    pub bollinger: BollingerConfig,
    pub rsi: RsiConfig,
    pub atr: AtrConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BollingerConfig {
    pub length: usize,
    pub k: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RsiConfig {
    pub length: usize,
    pub overbought: f64,
    pub oversold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtrConfig {
    pub length: usize,
    pub stop_multiplier: f64,
    pub trail_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChoppinessConfig {
    pub enabled: bool,
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub timezone: String,
    pub sessions: Vec<SessionWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindow {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskControlConfig {
    pub max_leverage: f64,
    pub max_drawdown: f64,
    pub daily_loss_limit: f64,
    pub position_limit_per_symbol: f64,
    pub max_unrealized_loss_per_symbol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub env_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub price_digits: Option<u32>,
    pub amount_digits: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    pub grid_spacing_pct: f64,
    pub levels_per_side: usize,
    pub base_order_notional: f64,
    pub max_position_notional: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeFilterConfig {
    pub min_liquidity_usd: f64,
    pub require_conditions: usize,
    pub adx_max: f64,
    pub bbw_quantile: f64,
    pub slope_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderConfig {
    pub post_only: bool,
    pub tif: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketExitConfig {
    pub use_market_order: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotificationConfig {
    #[serde(default)]
    pub wecom: Option<WeComConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeComConfig {
    pub webhook_url: String,
    #[serde(default)]
    pub mentioned_list: Vec<String>,
    #[serde(default)]
    pub mentioned_mobile_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
