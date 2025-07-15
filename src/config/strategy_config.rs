use crate::utils::symbol::Symbol;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyConfig {
    pub strategies: Vec<Strategy>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Strategy {
    pub name: String,
    pub enabled: bool,
    pub log_level: Option<String>,
    pub exchange: String, // 绑定的交易所账户名称
    pub class_path: String,
    pub params: StrategyParams,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyParams {
    // 网格策略参数
    pub symbol: Option<String>, // 对于资金费率策略，symbol是可选的
    pub spacing: Option<f64>,
    pub order_value: Option<f64>,
    pub grid_levels: Option<i32>,
    pub leverage: Option<i32>,
    pub health_check_interval_seconds: Option<u64>,
    pub uniformity_threshold: Option<f64>,

    // 网格策略价格边界控制
    pub max_price: Option<f64>, // 最高价，触及时停止策略
    pub min_price: Option<f64>, // 最低价，触及时停止策略
    pub close_positions_on_boundary: Option<bool>, // 触及边界时是否平仓，默认false

    // 网格策略全局止损控制
    pub max_loss_usd: Option<f64>, // 最大亏损金额(USD)，超过时停止策略
    pub close_positions_on_stop_loss: Option<bool>, // 触发止损时是否平仓，默认false

    // 网格策略止盈控制
    pub take_profit_usd: Option<f64>, // 止盈目标金额(USD)，达到时停止策略
    pub close_positions_on_take_profit: Option<bool>, // 触发止盈时是否平仓，默认true

    // 资金费率策略参数
    pub position_size_usd: Option<f64>,
    pub rate_threshold: Option<f64>,
    pub open_offset_ms: Option<i64>,
    pub close_offset_ms: Option<i64>,
    pub check_interval_seconds: Option<u64>,

    // AS做市策略参数
    pub order_amount_usdt: Option<f64>,
    pub max_position_usdt: Option<f64>,
    pub spread_percentage: Option<f64>,
    pub stop_loss_percentage: Option<f64>,
    pub refresh_interval_ms: Option<u64>,
    pub max_hold_time_seconds: Option<u64>,

    // Avellaneda-Stoikov做市策略参数
    pub risk_aversion: Option<f64>,
    pub market_impact: Option<f64>,
    pub order_arrival_rate: Option<f64>,
    pub volatility_window: Option<u64>,
    pub min_spread_bps: Option<f64>,

    // 多时间框架做多策略参数
    pub position_size_usdt: Option<f64>,
    pub rsi_period: Option<usize>,
    pub rsi_oversold: Option<f64>,
    pub rsi_overbought: Option<f64>,
    pub bb_period: Option<usize>,
    pub bb_std_dev: Option<f64>,
    pub primary_timeframe: Option<String>,
    pub secondary_timeframe: Option<String>,
    pub tertiary_timeframe: Option<String>,
    pub take_profit_percentage: Option<f64>,
    pub max_hold_time_minutes: Option<u64>,
    pub min_volume_usdt: Option<f64>,
    pub trend_confirmation_bars: Option<usize>,
    pub order_offset_pct: Option<f64>,
    pub order_timeout_minutes: Option<u64>,
    pub allow_multiple_orders: Option<bool>,
}

// 保留旧的配置结构以兼容性
#[derive(Debug, Deserialize, Clone)]
pub struct GridStrategyConfig {
    pub grid_configs: Vec<GridConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GridConfig {
    pub symbol: Symbol,
    pub grid_spacing: f64,       // For arithmetic grid
    pub grid_ratio: Option<f64>, // For geometric grid
    pub grid_num: i32,
    pub order_value: f64,
    pub leverage: Option<i32>,   // 杠杆倍数
    pub health_check_interval_seconds: Option<u64>, // 健康检查间隔秒数
    pub uniformity_threshold: Option<f64>,          // 均匀性阈值

    // 价格边界控制
    pub max_price: Option<f64>, // 最高价，触及时停止策略
    pub min_price: Option<f64>, // 最低价，触及时停止策略
    pub close_positions_on_boundary: Option<bool>, // 触及边界时是否平仓，默认false

    // 全局止损控制
    pub max_loss_usd: Option<f64>, // 最大亏损金额(USD)，超过时停止策略
    pub close_positions_on_stop_loss: Option<bool>, // 触发止损时是否平仓，默认false

    // 止盈控制
    pub take_profit_usd: Option<f64>, // 止盈目标金额(USD)，达到时停止策略
    pub close_positions_on_take_profit: Option<bool>, // 触发止盈时是否平仓，默认true
}
