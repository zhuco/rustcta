use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::config::TradingConfig;
use crate::core::types::{Order, OrderSide, OrderType};
use crate::utils::indicators::TrendStrengthCalculator;

/// 趋势强度
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrendStrength {
    StrongBull,
    Bull,
    Neutral,
    Bear,
    StrongBear,
}

/// 趋势调整请求
#[derive(Debug, Clone)]
pub struct TrendAdjustmentRequest {
    pub amount: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
}

/// 交易配置状态
pub struct ConfigState {
    pub config: TradingConfig,
    pub current_price: f64,
    pub price_precision: u32,
    pub amount_precision: u32,
    pub grid_orders: HashMap<String, Order>,
    pub active_orders: HashMap<String, Order>,
    pub last_trade_price: f64,
    pub last_trade_time: DateTime<Utc>,
    pub trend_strength: TrendStrength,
    pub trend_calculator: TrendStrengthCalculator,
    pub position: f64,
    pub pnl: f64,
    pub trades_count: u64,
    pub ws_client: Option<Box<dyn crate::core::websocket::WebSocketClient>>,
    pub total_buy_volume: f64,
    pub total_sell_volume: f64,
    pub total_buy_amount: f64,
    pub total_sell_amount: f64,
    pub total_fee: f64,
    pub net_position: f64,
    pub avg_buy_price: f64,
    pub avg_sell_price: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub last_grid_check: DateTime<Utc>,
    pub need_grid_reset: bool,
    pub last_trend_check: DateTime<Utc>,
    pub last_trend_strength: TrendStrength,
}
