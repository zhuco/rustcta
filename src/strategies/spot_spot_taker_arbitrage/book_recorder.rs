use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::data as shared_data;
use crate::exchanges::unified::OrderBookLevel;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use super::{BookSource, CachedBook};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum BookRecord {
    BookSnapshot {
        timestamp_local: DateTime<Utc>,
        timestamp_exchange: Option<DateTime<Utc>>,
        exchange: String,
        symbol: String,
        bids: Vec<OrderBookLevel>,
        asks: Vec<OrderBookLevel>,
        sequence: Option<u64>,
        source: BookSource,
        latency_ms: Option<i64>,
    },
    TopOfBook {
        timestamp_local: DateTime<Utc>,
        timestamp_exchange: Option<DateTime<Utc>>,
        exchange: String,
        symbol: String,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        sequence: Option<u64>,
        source: BookSource,
        latency_ms: Option<i64>,
    },
}

impl BookRecord {
    pub fn from_cached(book: &CachedBook, top_only: bool) -> Self {
        if top_only {
            Self::TopOfBook {
                timestamp_local: book.local_timestamp,
                timestamp_exchange: book.exchange_timestamp,
                exchange: book.exchange.clone(),
                symbol: book.symbol.clone(),
                best_bid: book.best_bid,
                best_ask: book.best_ask,
                sequence: book.sequence,
                source: book.source,
                latency_ms: book.latency_ms,
            }
        } else {
            Self::BookSnapshot {
                timestamp_local: book.local_timestamp,
                timestamp_exchange: book.exchange_timestamp,
                exchange: book.exchange.clone(),
                symbol: book.symbol.clone(),
                bids: book.bids.clone(),
                asks: book.asks.clone(),
                sequence: book.sequence,
                source: book.source,
                latency_ms: book.latency_ms,
            }
        }
    }

    pub fn timestamp_local(&self) -> DateTime<Utc> {
        match self {
            Self::BookSnapshot {
                timestamp_local, ..
            }
            | Self::TopOfBook {
                timestamp_local, ..
            } => *timestamp_local,
        }
    }

    pub fn into_cached(self) -> CachedBook {
        match self {
            Self::BookSnapshot {
                timestamp_local,
                timestamp_exchange,
                exchange,
                symbol,
                bids,
                asks,
                sequence,
                source,
                latency_ms,
            } => CachedBook {
                exchange,
                symbol,
                best_bid: bids.first().map(|level| level.price),
                best_ask: asks.first().map(|level| level.price),
                bids,
                asks,
                exchange_timestamp: timestamp_exchange,
                local_timestamp: timestamp_local,
                latency_ms,
                sequence,
                source,
                is_stale: false,
            },
            Self::TopOfBook {
                timestamp_local,
                timestamp_exchange,
                exchange,
                symbol,
                best_bid,
                best_ask,
                sequence,
                source,
                latency_ms,
            } => CachedBook {
                exchange,
                symbol,
                bids: best_bid
                    .map(|price| OrderBookLevel {
                        price,
                        quantity: f64::MAX / 4.0,
                    })
                    .into_iter()
                    .collect(),
                asks: best_ask
                    .map(|price| OrderBookLevel {
                        price,
                        quantity: f64::MAX / 4.0,
                    })
                    .into_iter()
                    .collect(),
                best_bid,
                best_ask,
                exchange_timestamp: timestamp_exchange,
                local_timestamp: timestamp_local,
                latency_ms,
                sequence,
                source,
                is_stale: false,
            },
        }
    }
}

#[derive(Clone)]
pub struct BookRecorder {
    inner: crate::data::BookRecorder,
}

impl BookRecorder {
    pub async fn start(path: String) -> anyhow::Result<Self> {
        Ok(Self {
            inner: crate::data::BookRecorder::start(crate::data::BookRecorderConfig {
                path,
                top_of_book_only: true,
                capacity: 8_192,
            })
            .await?,
        })
    }

    pub async fn record(&self, record: BookRecord) {
        let cached = record.into_cached();
        let source = cached.source;
        let event = shared_data::BookEvent::from_snapshot(
            cached.into_snapshot(),
            to_shared_book_source(source),
            shared_data::BookEventKind::Snapshot,
        );
        self.inner.try_record_event(&event);
    }

    pub fn dropped_events(&self) -> u64 {
        self.inner.dropped_events()
    }

    pub fn shared(&self) -> crate::data::BookRecorder {
        self.inner.clone()
    }
}

fn to_shared_book_source(source: BookSource) -> shared_data::BookSource {
    match source {
        BookSource::Websocket => shared_data::BookSource::Websocket,
        BookSource::Rest => shared_data::BookSource::RestFallback,
        BookSource::Replay => shared_data::BookSource::Replay,
    }
}

pub fn append_book_record(path: &str, record: &BookRecord) -> std::io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, record)?;
    writeln!(file)?;
    Ok(())
}
