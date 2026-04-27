use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::types::{Kline, MarketType, Trade};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub interval: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub kline: Kline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub trade: Trade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthDeltaEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookTickerEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkPriceEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub mark_price: f64,
    pub index_price: f64,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub market_type: MarketType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRateEvent {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub funding_rate: f64,
    pub mark_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BacktestEvent {
    DepthDelta(DepthDeltaEvent),
    BookTicker(BookTickerEvent),
    MarkPrice(MarkPriceEvent),
    FundingRate(FundingRateEvent),
    Trade(TradeEvent),
    Kline(KlineEvent),
}

impl BacktestEvent {
    pub fn logical_ts(&self) -> DateTime<Utc> {
        match self {
            Self::DepthDelta(event) => event.logical_ts,
            Self::BookTicker(event) => event.logical_ts,
            Self::MarkPrice(event) => event.logical_ts,
            Self::FundingRate(event) => event.logical_ts,
            Self::Trade(event) => event.logical_ts,
            Self::Kline(event) => event.logical_ts,
        }
    }

    pub fn stable_priority(&self) -> u8 {
        match self {
            Self::DepthDelta(_) => 0,
            Self::BookTicker(_) => 1,
            Self::MarkPrice(_) => 2,
            Self::FundingRate(_) => 3,
            Self::Trade(_) => 4,
            Self::Kline(_) => 5,
        }
    }
}
