use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, OrderBook5, RouteStatus, RouteType, TakerSide,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventMeta {
    pub exchange: ExchangeId,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub recv_ts: DateTime<Utc>,
    pub sequence: Option<u64>,
    pub source_route: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TickerEvent {
    pub meta: EventMeta,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub meta: EventMeta,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPriceSnapshot {
    pub meta: EventMeta,
    pub mark_price: f64,
    pub index_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentStatusEvent {
    pub meta: EventMeta,
    pub trading_enabled: bool,
    pub close_only: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteHeartbeat {
    pub exchange: ExchangeId,
    pub route_type: RouteType,
    pub status: RouteStatus,
    pub recv_ts: DateTime<Utc>,
    pub latency_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub meta: EventMeta,
    pub side: TakerSide,
    pub price: f64,
    pub quantity: f64,
    pub trade_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Ticker(TickerEvent),
    OrderBook(OrderBook5),
    Trade(TradeEvent),
    Funding(FundingSnapshot),
    MarkPrice(MarkPriceSnapshot),
    InstrumentStatus(InstrumentStatusEvent),
    Heartbeat(RouteHeartbeat),
}

impl MarketEvent {
    pub fn recv_ts(&self) -> DateTime<Utc> {
        match self {
            Self::Ticker(event) => event.meta.recv_ts,
            Self::OrderBook(book) => book.recv_ts,
            Self::Trade(event) => event.meta.recv_ts,
            Self::Funding(event) => event.meta.recv_ts,
            Self::MarkPrice(event) => event.meta.recv_ts,
            Self::InstrumentStatus(event) => event.meta.recv_ts,
            Self::Heartbeat(event) => event.recv_ts,
        }
    }
}
