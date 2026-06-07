use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::exchanges::unified::{MarketType, OrderBookLevel, OrderBookSnapshot};

use crate::data::{normalize_symbol, BookEvent, BookEventKind, BookKey, BookSource};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookCacheConfig {
    pub stale_book_ms: u64,
    pub max_book_age_ms: u64,
    pub depth: u16,
    pub log_raw_messages: bool,
}

impl Default for BookCacheConfig {
    fn default() -> Self {
        Self {
            stale_book_ms: 1_000,
            max_book_age_ms: 1_000,
            depth: 5,
            log_raw_messages: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBook {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    pub source: BookSource,
    pub is_stale: bool,
    pub stale_reason: Option<String>,
}

impl CachedBook {
    pub fn into_snapshot(self) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: self.exchange,
            market_type: self.market_type,
            symbol: self.internal_symbol,
            bids: self.bids,
            asks: self.asks,
            best_bid: self.best_bid,
            best_ask: self.best_ask,
            exchange_timestamp: self.exchange_timestamp,
            received_at: self.received_at,
            latency_ms: self.latency_ms,
            sequence: self.sequence,
            is_stale: self.is_stale,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BookCache {
    inner: Arc<RwLock<HashMap<BookKey, CachedBook>>>,
}

impl BookCache {
    pub async fn update_from_event(&self, event: BookEvent) {
        self.update_from_event_ref(&event).await;
    }

    pub async fn update_from_event_ref(&self, event: &BookEvent) {
        let key = event.key();
        if matches!(
            event.event_kind,
            BookEventKind::Stale
                | BookEventKind::Gap
                | BookEventKind::ChecksumMismatch
                | BookEventKind::Reconnect
                | BookEventKind::Error
        ) {
            let reason = event
                .stale_reason
                .clone()
                .unwrap_or_else(|| format!("{:?}", event.event_kind));
            self.mark_stale(
                &event.exchange,
                event.market_type,
                &event.internal_symbol,
                reason,
            )
            .await;
            return;
        }

        let book = CachedBook {
            exchange: event.exchange.clone(),
            market_type: event.market_type,
            internal_symbol: normalize_symbol(&event.internal_symbol),
            exchange_symbol: event.exchange_symbol.clone(),
            bids: event.bids.clone(),
            asks: event.asks.clone(),
            best_bid: event.best_bid,
            best_ask: event.best_ask,
            exchange_timestamp: event.exchange_timestamp,
            local_timestamp: event.local_timestamp,
            received_at: event.received_at,
            latency_ms: event.latency_ms,
            sequence: event.sequence,
            source: event.source,
            is_stale: !event.is_tradeable,
            stale_reason: event.stale_reason.clone(),
        };
        self.inner.write().await.insert(key, book);
    }

    pub async fn update_from_snapshot(&self, snapshot: OrderBookSnapshot, source: BookSource) {
        self.update_from_event(BookEvent::from_snapshot(
            snapshot,
            source,
            BookEventKind::Snapshot,
        ))
        .await;
    }

    pub async fn get_book(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<CachedBook> {
        self.inner
            .read()
            .await
            .get(&BookKey::new(exchange, market_type, symbol))
            .cloned()
    }

    pub async fn get_best_bid_ask(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<(f64, f64)> {
        let book = self.get_book(exchange, market_type, symbol).await?;
        Some((book.best_bid?, book.best_ask?))
    }

    pub async fn mark_stale(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        reason: impl Into<String>,
    ) {
        if let Some(book) =
            self.inner
                .write()
                .await
                .get_mut(&BookKey::new(exchange, market_type, symbol))
        {
            book.is_stale = true;
            book.stale_reason = Some(reason.into());
        }
    }

    pub async fn mark_exchange_stale(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbols: &[String],
        reason: impl Into<String>,
    ) {
        let reason = reason.into();
        for symbol in symbols {
            self.mark_stale(exchange, market_type, symbol, reason.clone())
                .await;
        }
    }

    pub async fn is_fresh(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        stale_ms: u64,
    ) -> bool {
        let Some(book) = self.get_book(exchange, market_type, symbol).await else {
            return false;
        };
        !book.is_stale && cached_book_age_ms(&book) <= stale_ms as i64
    }

    pub async fn age_ms(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<i64> {
        self.get_book(exchange, market_type, symbol)
            .await
            .map(|book| cached_book_age_ms(&book))
    }

    pub async fn list_books(&self) -> Vec<CachedBook> {
        self.inner.read().await.values().cloned().collect()
    }

    pub async fn list_fresh_books(&self, stale_ms: u64) -> Vec<CachedBook> {
        self.inner
            .read()
            .await
            .values()
            .filter(|book| !book.is_stale && cached_book_age_ms(book) <= stale_ms as i64)
            .cloned()
            .collect()
    }

    pub async fn list_stale_books(&self, stale_ms: u64) -> Vec<CachedBook> {
        self.inner
            .read()
            .await
            .values()
            .filter(|book| book.is_stale || cached_book_age_ms(book) > stale_ms as i64)
            .cloned()
            .collect()
    }
}

pub fn cached_book_age_ms(book: &CachedBook) -> i64 {
    Utc::now()
        .signed_duration_since(book.received_at)
        .num_milliseconds()
        .max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(sequence: Option<u64>) -> OrderBookSnapshot {
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
            sequence,
            is_stale: false,
        }
    }

    #[tokio::test]
    async fn book_cache_should_update_and_return_best_bid_ask() {
        let cache = BookCache::default();
        cache
            .update_from_snapshot(snapshot(Some(1)), BookSource::Websocket)
            .await;

        assert_eq!(
            cache
                .get_best_bid_ask("mexc", MarketType::Spot, "BTCUSDT")
                .await,
            Some((99.0, 100.0))
        );
        assert!(
            cache
                .is_fresh("mexc", MarketType::Spot, "BTCUSDT", 1_000)
                .await
        );
    }

    #[tokio::test]
    async fn book_cache_should_mark_stale() {
        let cache = BookCache::default();
        cache
            .update_from_snapshot(snapshot(Some(1)), BookSource::Websocket)
            .await;
        cache
            .mark_stale("mexc", MarketType::Spot, "BTCUSDT", "reconnect")
            .await;

        assert!(
            !cache
                .is_fresh("mexc", MarketType::Spot, "BTCUSDT", 1_000)
                .await
        );
        assert_eq!(cache.list_stale_books(1_000).await.len(), 1);
    }
}
