use serde::{Deserialize, Serialize};

/// AS策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASConfig {
    /// 策略基本信息
    pub strategy: ASStrategyInfo,
    /// 账户配置
    pub account: ASAccountConfig,
    /// 交易配置
    pub trading: ASTradingConfig,
    /// AS策略参数
    pub as_params: ASParams,
    /// 风险管理
    pub risk: ASRiskConfig,
    /// 技术指标
    pub indicators: ASIndicatorConfig,
    /// 数据源配置
    pub data_sources: ASDataSourceConfig,
    /// 日志配置
    pub logging: ASLoggingConfig,
    /// 监控配置
    pub monitoring: ASMonitoringConfig,
}

/// 策略基本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASStrategyInfo {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub strategy_type: String,
    pub description: String,
    pub enabled: bool,
}

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASAccountConfig {
    pub account_id: String,
    pub exchange: String,
}

/// 交易配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASTradingConfig {
    pub symbol: String,
    pub market_type: String,
    pub order_size_usdc: f64,
    pub max_inventory: f64,
    pub min_spread_bp: f64,
    pub max_spread_bp: f64,
    pub refresh_interval_secs: u64,
    pub price_precision: usize,
    pub quantity_precision: usize,
    pub order_config: ASOrderConfig,
}

/// 订单配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASOrderConfig {
    pub post_only: bool,
    pub time_in_force: String,
    pub reduce_only: bool,
}

/// AS策略参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASParams {
    pub scalping_frequency: ASScalpingFrequency,
    pub microstructure: ASMicrostructure,
    pub spread_adjustment: ASSpreadAdjustment,
    pub order_management: ASOrderManagement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASScalpingFrequency {
    pub min_interval_ms: u64,
    pub max_orders_per_minute: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASMicrostructure {
    pub orderbook_depth_levels: usize,
    pub bid_ask_imbalance_threshold: f64,
    pub volume_spike_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASSpreadAdjustment {
    pub volatility_multiplier: f64,
    pub inventory_penalty_rate: f64,
    pub market_impact_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASOrderManagement {
    pub max_open_orders_per_side: u32,
    pub order_lifetime_seconds: u64,
    pub partial_fill_threshold: f64,
}

/// 风险配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASRiskConfig {
    pub max_unrealized_loss: f64,
    pub max_daily_loss: f64,
    pub inventory_skew_limit: f64,
    pub stop_loss_pct: f64,
    pub max_position_hours: u64,
    pub emergency_stop: ASEmergencyStop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASEmergencyStop {
    pub max_drawdown_pct: f64,
    pub consecutive_losses: u32,
    pub volatility_threshold: f64,
}

/// 技术指标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASIndicatorConfig {
    pub ema: ASEMAConfig,
    pub rsi: ASRSIConfig,
    pub vwap: ASVWAPConfig,
    pub atr: ASATRConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASEMAConfig {
    pub fast_period: usize,
    pub slow_period: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASRSIConfig {
    pub period: usize,
    pub overbought: f64,
    pub oversold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASVWAPConfig {
    pub period: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASATRConfig {
    pub period: usize,
}

/// 数据源配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASDataSourceConfig {
    pub websocket: ASWebSocketConfig,
    pub rest_api: ASRestAPIConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASWebSocketConfig {
    pub enabled: bool,
    pub streams: Vec<String>,
    pub reconnect_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASRestAPIConfig {
    pub enabled: bool,
    pub refresh_interval: u64,
    pub endpoints: Vec<String>,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASLoggingConfig {
    pub level: String,
    pub enable_trade_log: bool,
    pub enable_performance_log: bool,
}

/// 监控配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASMonitoringConfig {
    pub performance: ASPerformanceConfig,
    pub health_check: ASHealthCheckConfig,
    pub alerts: ASAlertsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASPerformanceConfig {
    pub enable_metrics: bool,
    pub metrics_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASHealthCheckConfig {
    pub enabled: bool,
    pub check_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASAlertsConfig {
    pub enabled: bool,
    pub webhook_url: Option<String>,
    pub email_alerts: bool,
}
