use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::BacktestOrderSide;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategySignal {
    pub strategy: String,
    pub symbol: String,
    pub side: BacktestOrderSide,
    pub logical_ts: DateTime<Utc>,
    pub price: f64,
    pub band_percent: f64,
    pub z_score: f64,
    pub rsi: f64,
    pub atr: f64,
    pub reason: String,
}
