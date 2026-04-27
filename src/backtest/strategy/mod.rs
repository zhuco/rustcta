use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::schema::BacktestEvent;
use crate::core::types::OrderSide;

pub mod mean_reversion;
pub mod mtf_trend_factor;
pub mod short_ladder;
pub mod trend_factor;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategySignal {
    pub strategy: String,
    pub symbol: String,
    pub side: OrderSide,
    pub logical_ts: DateTime<Utc>,
    pub price: f64,
    pub band_percent: f64,
    pub z_score: f64,
    pub rsi: f64,
    pub atr: f64,
    pub reason: String,
}

pub trait BacktestStrategy {
    type Summary;

    fn on_event(&mut self, event: &BacktestEvent) -> Result<Vec<StrategySignal>>;

    fn summary(&self) -> Self::Summary;
}
