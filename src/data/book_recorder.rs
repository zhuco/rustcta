use std::fs;
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::data::{BookEvent, BookEventKind, BookSource, BookUpdateKind};
use crate::exchanges::unified::{MarketType, OrderBookLevel};
use crate::utils::rotating_file;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRecorderConfig {
    pub path: String,
    pub top_of_book_only: bool,
    pub capacity: usize,
}

impl Default for BookRecorderConfig {
    fn default() -> Self {
        Self {
            path: "data/book_events.jsonl".to_string(),
            top_of_book_only: true,
            capacity: 8_192,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRecord {
    pub event_id: String,
    pub event_kind: BookEventKind,
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bids: Vec<OrderBookLevel>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asks: Vec<OrderBookLevel>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    #[serde(default)]
    pub gateway_received_monotonic_ns: Option<u64>,
    #[serde(default)]
    pub strategy_received_monotonic_ns: Option<u64>,
    pub sequence: Option<u64>,
    #[serde(default)]
    pub first_update_id: Option<u64>,
    #[serde(default)]
    pub final_update_id: Option<u64>,
    #[serde(default)]
    pub previous_update_id: Option<u64>,
    #[serde(default)]
    pub checksum: Option<i64>,
    #[serde(default)]
    pub update_kind: BookUpdateKind,
    #[serde(default = "default_tradeable")]
    pub is_tradeable: bool,
    #[serde(default)]
    pub stale_reason: Option<String>,
    pub source: BookSource,
}

impl BookRecord {
    pub fn from_event(event: &BookEvent, top_only: bool) -> Self {
        Self {
            event_id: event.event_id.clone(),
            event_kind: event.event_kind,
            exchange: event.exchange.clone(),
            market_type: event.market_type,
            internal_symbol: event.internal_symbol.clone(),
            exchange_symbol: event.exchange_symbol.clone(),
            best_bid: event.best_bid,
            best_ask: event.best_ask,
            bids: if top_only {
                Vec::new()
            } else {
                event.bids.clone()
            },
            asks: if top_only {
                Vec::new()
            } else {
                event.asks.clone()
            },
            exchange_timestamp: event.exchange_timestamp,
            local_timestamp: event.local_timestamp,
            received_at: event.received_at,
            latency_ms: event.latency_ms,
            gateway_received_monotonic_ns: event.gateway_received_monotonic_ns,
            strategy_received_monotonic_ns: event.strategy_received_monotonic_ns,
            sequence: event.sequence,
            first_update_id: event.first_update_id,
            final_update_id: event.final_update_id,
            previous_update_id: event.previous_update_id,
            checksum: event.checksum,
            update_kind: event.update_kind,
            is_tradeable: event.is_tradeable,
            stale_reason: event.stale_reason.clone(),
            source: event.source,
        }
    }
}

fn default_tradeable() -> bool {
    true
}

#[derive(Clone)]
pub struct BookRecorder {
    tx: mpsc::Sender<BookRecord>,
    dropped_events: Arc<AtomicU64>,
    top_of_book_only: bool,
}

impl BookRecorder {
    pub async fn start(config: BookRecorderConfig) -> Result<Self> {
        if let Some(parent) = Path::new(&config.path).parent() {
            fs::create_dir_all(parent)?;
        }
        let (tx, mut rx) = mpsc::channel::<BookRecord>(config.capacity.max(1));
        let path = config.path.clone();
        tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                if let Err(error) = append_book_record(&path, &record) {
                    log::error!("book recorder write failed: {}", error);
                }
            }
        });
        Ok(Self {
            tx,
            dropped_events: Arc::new(AtomicU64::new(0)),
            top_of_book_only: config.top_of_book_only,
        })
    }

    pub fn try_record_event(&self, event: &BookEvent) {
        let record = BookRecord::from_event(event, self.top_of_book_only);
        if self.tx.try_send(record).is_err() {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }
}

pub fn append_book_record(path: &str, record: &BookRecord) -> std::io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    rotating_file::append_json_line(path, record)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::exchanges::unified::OrderBookSnapshot;

    fn event() -> BookEvent {
        BookEvent::from_snapshot(
            OrderBookSnapshot {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                bids: vec![OrderBookLevel {
                    price: 99.0,
                    quantity: 1.0,
                }],
                asks: vec![OrderBookLevel {
                    price: 100.0,
                    quantity: 1.0,
                }],
                best_bid: Some(99.0),
                best_ask: Some(100.0),
                exchange_timestamp: Some(Utc::now()),
                received_at: Utc::now(),
                latency_ms: Some(1),
                sequence: Some(1),
                is_stale: false,
            },
            BookSource::Websocket,
            BookEventKind::Snapshot,
        )
    }

    #[test]
    fn book_recorder_should_write_jsonl_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("books.jsonl");
        append_book_record(
            path.to_str().unwrap(),
            &BookRecord::from_event(&event(), true),
        )
        .unwrap();

        let raw = std::fs::read_to_string(path).unwrap();
        assert!(raw.contains("\"event_kind\":\"snapshot\""));
        assert!(raw.contains("\"source\":\"websocket\""));
    }

    #[tokio::test]
    async fn book_recorder_should_drop_without_blocking_when_full() {
        let dir = tempdir().unwrap();
        let recorder = BookRecorder::start(BookRecorderConfig {
            path: dir.path().join("books.jsonl").to_string_lossy().to_string(),
            top_of_book_only: true,
            capacity: 1,
        })
        .await
        .unwrap();
        for _ in 0..10_000 {
            recorder.try_record_event(&event());
        }
        assert!(recorder.dropped_events() > 0);
    }
}
