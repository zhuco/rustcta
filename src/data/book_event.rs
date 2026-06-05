use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderBookLevel, OrderBookSnapshot};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BookKey {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
}

impl BookKey {
    pub fn new(exchange: &str, market_type: MarketType, internal_symbol: &str) -> Self {
        Self {
            exchange: exchange.trim().to_ascii_lowercase(),
            market_type,
            internal_symbol: normalize_symbol(internal_symbol),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookEventKind {
    Snapshot,
    Delta,
    TopOfBook,
    Heartbeat,
    Stale,
    Reconnect,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSource {
    Websocket,
    RestFallback,
    Rest,
    Replay,
}

impl BookSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::RestFallback | Self::Rest => "rest_fallback",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookEvent {
    pub event_id: String,
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub event_kind: BookEventKind,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    #[serde(default)]
    pub best_bid: Option<f64>,
    #[serde(default)]
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    #[serde(default)]
    pub latency_ms: Option<i64>,
    #[serde(default)]
    pub sequence: Option<u64>,
    #[serde(default)]
    pub raw_message: Option<String>,
    pub source: BookSource,
}

impl BookEvent {
    pub fn from_snapshot(
        snapshot: OrderBookSnapshot,
        source: BookSource,
        event_kind: BookEventKind,
    ) -> Self {
        let event_id = format!(
            "{}-{}-{}-{}",
            snapshot.exchange.trim().to_ascii_lowercase(),
            snapshot.symbol.trim().to_ascii_uppercase(),
            snapshot
                .sequence
                .map(|sequence| sequence.to_string())
                .unwrap_or_else(|| "none".to_string()),
            snapshot.received_at.timestamp_micros()
        );
        Self {
            event_id,
            exchange: snapshot.exchange.trim().to_ascii_lowercase(),
            market_type: snapshot.market_type,
            internal_symbol: normalize_symbol(&snapshot.symbol),
            exchange_symbol: snapshot.symbol.trim().to_ascii_uppercase(),
            event_kind,
            best_bid: snapshot.best_bid,
            best_ask: snapshot.best_ask,
            bids: snapshot.bids,
            asks: snapshot.asks,
            exchange_timestamp: snapshot.exchange_timestamp,
            local_timestamp: snapshot.received_at,
            received_at: snapshot.received_at,
            latency_ms: snapshot.latency_ms,
            sequence: snapshot.sequence,
            raw_message: None,
            source,
        }
    }

    pub fn key(&self) -> BookKey {
        BookKey::new(&self.exchange, self.market_type, &self.internal_symbol)
    }
}

pub fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}
