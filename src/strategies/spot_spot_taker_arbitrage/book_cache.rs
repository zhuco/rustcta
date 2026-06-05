use chrono::Utc;

use crate::data as shared_data;
use crate::exchanges::unified::{MarketType, OrderBookSnapshot};

use super::{BookSource, CachedBook, SpotVenue};

#[derive(Debug, Clone, Default)]
pub struct BookCache {
    inner: shared_data::BookCache,
}

impl BookCache {
    pub async fn update_book(&self, snapshot: OrderBookSnapshot, source: BookSource) {
        self.inner
            .update_from_snapshot(snapshot, source.into())
            .await;
    }

    pub async fn get_book(&self, exchange: &str, symbol: &str) -> Option<CachedBook> {
        self.inner
            .get_book(exchange, MarketType::Spot, symbol)
            .await
            .map(from_market_cached)
    }

    pub async fn get_best_bid_ask(&self, exchange: &str, symbol: &str) -> Option<(f64, f64)> {
        self.inner
            .get_best_bid_ask(exchange, MarketType::Spot, symbol)
            .await
    }

    pub async fn mark_stale(&self, exchange: &str, symbol: &str) {
        self.inner
            .mark_stale(exchange, MarketType::Spot, symbol, "manual stale")
            .await;
    }

    pub async fn mark_exchange_stale(&self, exchange: SpotVenue, symbols: &[String]) {
        self.inner
            .mark_exchange_stale(exchange.as_str(), MarketType::Spot, symbols, "reconnect")
            .await;
    }

    pub async fn is_fresh(&self, exchange: &str, symbol: &str, stale_book_ms: u64) -> bool {
        self.inner
            .is_fresh(exchange, MarketType::Spot, symbol, stale_book_ms)
            .await
    }

    pub async fn get_book_age_ms(&self, exchange: &str, symbol: &str) -> Option<i64> {
        self.inner.age_ms(exchange, MarketType::Spot, symbol).await
    }

    pub async fn get_all_books(&self) -> Vec<CachedBook> {
        self.inner
            .list_books()
            .await
            .into_iter()
            .map(from_market_cached)
            .collect()
    }

    pub fn shared(&self) -> shared_data::BookCache {
        self.inner.clone()
    }
}

impl CachedBook {
    pub fn into_snapshot(self) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: self.exchange,
            market_type: MarketType::Spot,
            symbol: self.symbol,
            bids: self.bids,
            asks: self.asks,
            best_bid: self.best_bid,
            best_ask: self.best_ask,
            exchange_timestamp: self.exchange_timestamp,
            received_at: self.local_timestamp,
            latency_ms: self.latency_ms,
            sequence: self.sequence,
            is_stale: self.is_stale,
        }
    }
}

pub fn cached_book_age_ms(book: &CachedBook) -> i64 {
    Utc::now()
        .signed_duration_since(book.local_timestamp)
        .num_milliseconds()
        .max(0)
}

fn from_market_cached(book: shared_data::CachedBook) -> CachedBook {
    CachedBook {
        exchange: book.exchange,
        symbol: book.internal_symbol,
        bids: book.bids,
        asks: book.asks,
        best_bid: book.best_bid,
        best_ask: book.best_ask,
        exchange_timestamp: book.exchange_timestamp,
        local_timestamp: book.received_at,
        latency_ms: book.latency_ms,
        sequence: book.sequence,
        source: book.source.into(),
        is_stale: book.is_stale,
    }
}

impl From<BookSource> for shared_data::BookSource {
    fn from(value: BookSource) -> Self {
        match value {
            BookSource::Websocket => Self::Websocket,
            BookSource::Rest => Self::RestFallback,
            BookSource::Replay => Self::Replay,
        }
    }
}

impl From<shared_data::BookSource> for BookSource {
    fn from(value: shared_data::BookSource) -> Self {
        match value {
            shared_data::BookSource::Websocket => Self::Websocket,
            shared_data::BookSource::RestFallback | shared_data::BookSource::Rest => Self::Rest,
            shared_data::BookSource::Replay => Self::Replay,
        }
    }
}
