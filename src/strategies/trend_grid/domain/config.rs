use serde::{Deserialize, Serialize};

/// 交易配置 - 每个配置独立运行
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub config_id: String,
    pub enabled: bool,
    pub account: AccountConfig,
    pub symbol: String,
    pub grid: GridConfig,
    pub trend_config: TrendIndicatorConfig,
}

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub env_prefix: String,
}

/// 网格配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    pub spacing: f64,
    pub spacing_type: SpacingType,
    pub order_amount: f64,
    pub orders_per_side: u32,
    pub max_position: f64,
}

/// 网格间距类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpacingType {
    #[serde(rename = "arithmetic")]
    Arithmetic,
    #[serde(rename = "geometric")]
    Geometric,
}

/// 趋势指标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendIndicatorConfig {
    pub ma_fast: u32,
    pub ma_slow: u32,
    pub rsi_period: u32,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub timeframe: String,
    pub show_trend_info: bool,
}

/// 趋势调整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAdjustment {
    pub strong_bull_buy_multiplier: f64,  // 强上涨买单倍数 (2.0)
    pub bull_buy_multiplier: f64,         // 弱上涨买单倍数 (1.5)
    pub bear_sell_multiplier: f64,        // 弱下跌卖单倍数 (1.5)
    pub strong_bear_sell_multiplier: f64, // 强下跌卖单倍数 (2.0)
}

/// 策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendGridConfigV2 {
    pub strategy: StrategyInfo,
    pub trading_configs: Vec<TradingConfig>,
    pub trend_adjustment: TrendAdjustment,
    pub batch_settings: BatchSettings,
    pub grid_management: GridManagement,
    pub websocket: WebSocketConfig,
    pub risk_control: RiskControl,
    pub execution: ExecutionConfig,
    pub logging: LoggingConfig,
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
pub struct BatchSettings {
    pub binance_batch_size: u32,
    pub okx_batch_size: u32,
    pub hyperliquid_batch_size: u32,
    pub default_batch_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridManagement {
    pub check_interval: u64,
    pub rebalance_threshold: f64,
    pub cancel_and_replace: bool,
    pub show_grid_status: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub subscribe_order_updates: bool,
    pub subscribe_trade_updates: bool,
    pub subscribe_ticker: bool,
    pub reconnect_on_disconnect: bool,
    pub heartbeat_interval: u64,
    pub log_all_trades: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskControl {
    pub max_leverage: u32,
    pub max_drawdown: f64,
    pub daily_loss_limit: f64,
    pub position_limit_per_symbol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub startup_cancel_all: bool,
    pub shutdown_cancel_all: bool,
    pub thread_per_config: bool,
    pub startup_delay: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: String,
    pub console: bool,
    pub show_pnl: bool,
    pub show_position: bool,
    pub show_trend_changes: bool,
}
