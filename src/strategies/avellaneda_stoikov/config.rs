use serde::{Deserialize, Serialize};

/// Avellaneda-Stoikov策略配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ASConfig {
    pub strategy: StrategyConfig,
    pub account: AccountConfig,
    pub trading: TradingConfig,
    pub as_params: ASParameters,
    pub market_data: MarketDataConfig,
    pub risk: RiskConfig,
    pub performance: PerformanceConfig,
    pub data_sources: DataSourcesConfig,
    pub monitoring: MonitoringConfig,
    pub perpetual: PerpetualConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyConfig {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub strategy_type: String,
    pub description: String,
    pub enabled: bool,
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountConfig {
    pub account_id: String,
    pub exchange: String,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TradingConfig {
    pub symbol: String,
    pub market_type: String,
    pub order_size_usdc: f64,
    pub max_inventory: f64,
    pub min_spread_bp: f64,
    pub max_spread_bp: f64,
    pub refresh_interval_secs: u64,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub order_config: OrderConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderConfig {
    pub post_only: bool,
    pub time_in_force: String,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ASParameters {
    pub risk_aversion: f64,
    pub order_book_intensity: f64,
    pub time_horizon_seconds: u64,
    pub volatility: VolatilityConfig,
    pub inventory_skew: InventorySkewConfig,
    pub spread_adjustment: SpreadAdjustmentConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VolatilityConfig {
    pub lookback_periods: usize,
    pub update_interval: u64,
    pub decay_factor: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InventorySkewConfig {
    pub enabled: bool,
    pub skew_factor: f64,
    pub target_inventory_ratio: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SpreadAdjustmentConfig {
    pub volume_factor: f64,
    pub depth_factor: f64,
    pub pressure_factor: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketDataConfig {
    pub orderbook_levels: usize,
    pub trades_buffer_size: usize,
    pub kline: KlineConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KlineConfig {
    pub interval: String,
    pub history_size: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskConfig {
    pub max_unrealized_loss: f64,
    pub max_daily_loss: f64,
    pub inventory_risk: InventoryRiskConfig,
    pub stop_loss: StopLossConfig,
    pub volatility_limits: VolatilityLimitsConfig,
    pub emergency_stop: EmergencyStopConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InventoryRiskConfig {
    pub max_position_value: f64,
    pub imbalance_threshold: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StopLossConfig {
    pub enabled: bool,
    pub stop_loss_pct: f64,
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VolatilityLimitsConfig {
    pub max_volatility: f64,
    pub min_volatility: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmergencyStopConfig {
    pub consecutive_losses: u32,
    pub max_drawdown_pct: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    pub order_aggregation: OrderAggregationConfig,
    pub smart_cancellation: SmartCancellationConfig,
    pub partial_fill: PartialFillConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderAggregationConfig {
    pub enabled: bool,
    pub min_price_diff_bp: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SmartCancellationConfig {
    pub enabled: bool,
    pub price_drift_threshold_bp: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartialFillConfig {
    pub min_fill_ratio: f64,
    pub immediate_replace: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataSourcesConfig {
    pub websocket: WebSocketConfig,
    pub rest_api: RestApiConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub streams: Vec<String>,
    pub reconnect_interval: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestApiConfig {
    pub enabled: bool,
    pub refresh_interval: u64,
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringConfig {
    pub metrics: MetricsConfig,
    pub health_check: HealthCheckConfig,
    pub trade_logging: TradeLoggingConfig,
    pub alerts: AlertsConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub export_interval: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthCheckConfig {
    pub enabled: bool,
    pub interval: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TradeLoggingConfig {
    pub enabled: bool,
    pub log_fills: bool,
    pub log_quotes: bool,
    pub log_cancellations: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertsConfig {
    pub enabled: bool,
    pub channels: Vec<String>,
    pub thresholds: AlertThresholds,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertThresholds {
    pub inventory_imbalance: f64,
    pub loss_limit: f64,
    pub low_liquidity: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerpetualConfig {
    pub funding_rate: FundingRateConfig,
    pub maintenance: MaintenanceConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FundingRateConfig {
    pub consider_funding: bool,
    pub threshold_pct: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MaintenanceConfig {
    pub stop_before: u64,
    pub resume_after: u64,
}
