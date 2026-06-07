use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use rustcta_strategy_sdk::{MarketType, OrderSide};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Kline {
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
}
