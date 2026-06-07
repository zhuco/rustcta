//! Order-book data acquisition and cache access.
//!
//! Public market data should flow exchange websocket/REST readers -> `BookEvent`
//! -> `BookCache`/`BookHealth` -> scanner or strategy consumers. Trading
//! decisions must not fetch browser-provided books or place orders from this
//! layer.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderBookLevel};

pub use crate::data::{
    BookCache, BookCacheConfig, BookEvent, BookEventKind, BookHealth, BookKey, BookRecord,
    BookRecorder, BookRecorderConfig, BookSource, BookUpdateKind, CachedBook, ExchangeMarketHealth,
    WebSocketBookManager, WebSocketBookManagerConfig,
};

const BOOK_PRICE_SCALE: f64 = 100_000_000.0;
const BOOK_QTY_SCALE: f64 = 100_000_000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum L2BookStrictness {
    SnapshotOnly,
    BestEffortDelta,
    StrictDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum L2ChecksumMode {
    Disabled,
    TopLevelSum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2BookCapability {
    pub strictness: L2BookStrictness,
    pub supports_sequence: bool,
    pub supports_checksum: bool,
    pub checksum_mode: L2ChecksumMode,
    pub supports_resync_endpoint: bool,
    pub max_depth: Option<u32>,
}

impl L2BookCapability {
    pub fn snapshot_only(max_depth: Option<u32>) -> Self {
        Self {
            strictness: L2BookStrictness::SnapshotOnly,
            supports_sequence: false,
            supports_checksum: false,
            checksum_mode: L2ChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }

    pub fn best_effort_delta(max_depth: Option<u32>) -> Self {
        Self {
            strictness: L2BookStrictness::BestEffortDelta,
            supports_sequence: false,
            supports_checksum: false,
            checksum_mode: L2ChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }

    pub fn strict_delta(max_depth: Option<u32>) -> Self {
        Self {
            strictness: L2BookStrictness::StrictDelta,
            supports_sequence: true,
            supports_checksum: false,
            checksum_mode: L2ChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }
}

impl Default for L2BookCapability {
    fn default() -> Self {
        Self::snapshot_only(None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2BookUpdateStatus {
    Applied,
    DuplicateIgnored,
    StaleNeedsResync,
    Resynced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2BookUpdateOutcome {
    pub status: L2BookUpdateStatus,
    pub is_tradeable: bool,
    pub stale_reason: Option<String>,
    pub sequence: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StrictL2BookState {
    exchange: String,
    market_type: MarketType,
    internal_symbol: String,
    exchange_symbol: String,
    capability: L2BookCapability,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    last_sequence: Option<u64>,
    is_tradeable: bool,
    stale_reason: Option<String>,
    last_received_at: Option<DateTime<Utc>>,
}

impl StrictL2BookState {
    pub fn new(
        exchange: impl Into<String>,
        market_type: MarketType,
        internal_symbol: impl Into<String>,
        exchange_symbol: impl Into<String>,
        capability: L2BookCapability,
    ) -> Self {
        Self {
            exchange: exchange.into().trim().to_ascii_lowercase(),
            market_type,
            internal_symbol: crate::data::normalize_symbol(&internal_symbol.into()),
            exchange_symbol: exchange_symbol.into().trim().to_ascii_uppercase(),
            capability,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_sequence: None,
            is_tradeable: false,
            stale_reason: None,
            last_received_at: None,
        }
    }

    pub fn apply(&mut self, event: &BookEvent) -> L2BookUpdateOutcome {
        self.last_received_at = Some(event.received_at);
        match event.update_kind {
            BookUpdateKind::FullSnapshot => self.apply_snapshot(event, true),
            BookUpdateKind::PartialSnapshot => self.apply_snapshot(event, false),
            BookUpdateKind::Delta => self.apply_delta(event),
        }
    }

    pub fn is_tradeable(&self) -> bool {
        self.is_tradeable
    }

    pub fn needs_resync(&self) -> bool {
        !self.is_tradeable && self.stale_reason.is_some()
    }

    pub fn stale_reason(&self) -> Option<&str> {
        self.stale_reason.as_deref()
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    pub fn to_book_event(&self, source: BookSource) -> Option<BookEvent> {
        let received_at = self.last_received_at?;
        let bids = self.top_bids(self.capability.max_depth.unwrap_or(50) as usize);
        let asks = self.top_asks(self.capability.max_depth.unwrap_or(50) as usize);
        Some(BookEvent {
            event_id: format!(
                "{}-{}-{}-{}",
                self.exchange,
                self.internal_symbol,
                self.last_sequence
                    .map(|sequence| sequence.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                received_at.timestamp_micros()
            ),
            exchange: self.exchange.clone(),
            market_type: self.market_type,
            internal_symbol: self.internal_symbol.clone(),
            exchange_symbol: self.exchange_symbol.clone(),
            event_kind: BookEventKind::Snapshot,
            best_bid: bids.first().map(|level| level.price),
            best_ask: asks.first().map(|level| level.price),
            bids,
            asks,
            exchange_timestamp: None,
            local_timestamp: received_at,
            received_at,
            latency_ms: None,
            gateway_received_monotonic_ns: None,
            strategy_received_monotonic_ns: None,
            sequence: self.last_sequence,
            first_update_id: self.last_sequence,
            final_update_id: self.last_sequence,
            previous_update_id: None,
            checksum: None,
            update_kind: BookUpdateKind::FullSnapshot,
            is_tradeable: self.is_tradeable,
            stale_reason: self.stale_reason.clone(),
            raw_message: None,
            source,
        })
    }

    fn apply_snapshot(&mut self, event: &BookEvent, full_snapshot: bool) -> L2BookUpdateOutcome {
        self.bids.clear();
        self.asks.clear();
        merge_levels(&mut self.bids, &event.bids);
        merge_levels(&mut self.asks, &event.asks);
        self.last_sequence = event.final_update_id.or(event.sequence);

        let tradeable =
            full_snapshot || !matches!(self.capability.strictness, L2BookStrictness::StrictDelta);
        self.is_tradeable = tradeable;
        self.stale_reason =
            (!tradeable).then(|| "partial snapshot cannot resync strict book".into());

        self.outcome(
            if full_snapshot {
                L2BookUpdateStatus::Resynced
            } else {
                L2BookUpdateStatus::Applied
            },
            self.stale_reason.clone(),
        )
    }

    fn apply_delta(&mut self, event: &BookEvent) -> L2BookUpdateOutcome {
        if !matches!(self.capability.strictness, L2BookStrictness::StrictDelta) {
            merge_levels(&mut self.bids, &event.bids);
            merge_levels(&mut self.asks, &event.asks);
            self.last_sequence = event
                .final_update_id
                .or(event.sequence)
                .or(self.last_sequence);
            self.is_tradeable = true;
            self.stale_reason = None;
            return self.outcome(L2BookUpdateStatus::Applied, None);
        }

        let Some(last_sequence) = self.last_sequence else {
            return self.mark_stale("delta before full snapshot");
        };
        let first_update_id = event
            .first_update_id
            .or(event.sequence)
            .unwrap_or(last_sequence + 1);
        let final_update_id = event
            .final_update_id
            .or(event.sequence)
            .unwrap_or(first_update_id);

        if final_update_id <= last_sequence {
            return self.outcome(L2BookUpdateStatus::DuplicateIgnored, None);
        }
        if first_update_id > last_sequence + 1 {
            return self.mark_stale(format!(
                "sequence gap expected={} first_update_id={} final_update_id={}",
                last_sequence + 1,
                first_update_id,
                final_update_id
            ));
        }
        if event
            .previous_update_id
            .is_some_and(|previous| previous != last_sequence)
        {
            return self.mark_stale(format!(
                "previous update mismatch expected={} previous_update_id={}",
                last_sequence,
                event.previous_update_id.unwrap()
            ));
        }

        merge_levels(&mut self.bids, &event.bids);
        merge_levels(&mut self.asks, &event.asks);
        self.last_sequence = Some(final_update_id);

        if self.capability.supports_checksum {
            if let Some(expected_checksum) = event.checksum {
                let actual_checksum = self.top_level_checksum();
                if actual_checksum != expected_checksum {
                    return self.mark_stale(format!(
                        "checksum mismatch expected={} actual={}",
                        expected_checksum, actual_checksum
                    ));
                }
            }
        }

        self.is_tradeable = true;
        self.stale_reason = None;
        self.outcome(L2BookUpdateStatus::Applied, None)
    }

    fn mark_stale(&mut self, reason: impl Into<String>) -> L2BookUpdateOutcome {
        let reason = reason.into();
        self.is_tradeable = false;
        self.stale_reason = Some(reason.clone());
        self.outcome(L2BookUpdateStatus::StaleNeedsResync, Some(reason))
    }

    fn outcome(
        &self,
        status: L2BookUpdateStatus,
        stale_reason: Option<String>,
    ) -> L2BookUpdateOutcome {
        L2BookUpdateOutcome {
            status,
            is_tradeable: self.is_tradeable,
            stale_reason,
            sequence: self.last_sequence,
        }
    }

    fn top_bids(&self, limit: usize) -> Vec<OrderBookLevel> {
        self.bids
            .iter()
            .rev()
            .take(limit)
            .map(|(price, quantity)| level_from_ticks(*price, *quantity))
            .collect()
    }

    fn top_asks(&self, limit: usize) -> Vec<OrderBookLevel> {
        self.asks
            .iter()
            .take(limit)
            .map(|(price, quantity)| level_from_ticks(*price, *quantity))
            .collect()
    }

    fn top_level_checksum(&self) -> i64 {
        match self.capability.checksum_mode {
            L2ChecksumMode::Disabled => 0,
            L2ChecksumMode::TopLevelSum => self
                .top_bids(5)
                .into_iter()
                .chain(self.top_asks(5))
                .map(|level| price_to_tick(level.price) + quantity_to_lot(level.quantity))
                .sum(),
        }
    }
}

fn merge_levels(book: &mut BTreeMap<i64, i64>, levels: &[OrderBookLevel]) {
    for level in levels {
        let price = price_to_tick(level.price);
        let quantity = quantity_to_lot(level.quantity);
        if quantity <= 0 {
            book.remove(&price);
        } else {
            book.insert(price, quantity);
        }
    }
}

fn price_to_tick(price: f64) -> i64 {
    (price * BOOK_PRICE_SCALE).round() as i64
}

fn quantity_to_lot(quantity: f64) -> i64 {
    (quantity * BOOK_QTY_SCALE).round() as i64
}

fn level_from_ticks(price: i64, quantity: i64) -> OrderBookLevel {
    OrderBookLevel {
        price: price as f64 / BOOK_PRICE_SCALE,
        quantity: quantity as f64 / BOOK_QTY_SCALE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(update_kind: BookUpdateKind, sequence: u64) -> BookEvent {
        BookEvent {
            event_id: format!("binance-btcusdt-{sequence}"),
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            event_kind: match update_kind {
                BookUpdateKind::Delta => BookEventKind::Delta,
                _ => BookEventKind::Snapshot,
            },
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
            exchange_timestamp: None,
            local_timestamp: Utc::now(),
            received_at: Utc::now(),
            latency_ms: None,
            gateway_received_monotonic_ns: None,
            strategy_received_monotonic_ns: None,
            sequence: Some(sequence),
            first_update_id: Some(sequence),
            final_update_id: Some(sequence),
            previous_update_id: None,
            checksum: None,
            update_kind,
            is_tradeable: true,
            stale_reason: None,
            raw_message: None,
            source: BookSource::Websocket,
        }
    }

    #[test]
    fn strict_l2_book_should_merge_normal_delta() {
        let mut state = StrictL2BookState::new(
            "binance",
            MarketType::Spot,
            "BTCUSDT",
            "BTCUSDT",
            L2BookCapability::strict_delta(Some(10)),
        );
        assert_eq!(
            state.apply(&event(BookUpdateKind::FullSnapshot, 10)).status,
            L2BookUpdateStatus::Resynced
        );

        let mut delta = event(BookUpdateKind::Delta, 11);
        delta.bids = vec![OrderBookLevel {
            price: 99.5,
            quantity: 2.0,
        }];
        delta.asks = vec![OrderBookLevel {
            price: 100.0,
            quantity: 0.0,
        }];

        let outcome = state.apply(&delta);
        assert_eq!(outcome.status, L2BookUpdateStatus::Applied);
        assert!(outcome.is_tradeable);

        let book = state.to_book_event(BookSource::Websocket).unwrap();
        assert_eq!(book.best_bid, Some(99.5));
        assert_eq!(book.best_ask, None);
        assert!(book.is_tradeable);
    }

    #[test]
    fn strict_l2_book_should_mark_stale_on_sequence_gap() {
        let mut state = StrictL2BookState::new(
            "binance",
            MarketType::Spot,
            "BTCUSDT",
            "BTCUSDT",
            L2BookCapability::strict_delta(Some(10)),
        );
        state.apply(&event(BookUpdateKind::FullSnapshot, 10));

        let outcome = state.apply(&event(BookUpdateKind::Delta, 12));

        assert_eq!(outcome.status, L2BookUpdateStatus::StaleNeedsResync);
        assert!(!outcome.is_tradeable);
        assert!(state.needs_resync());
        assert!(outcome.stale_reason.unwrap().contains("sequence gap"));
    }

    #[test]
    fn strict_l2_book_should_restore_tradeable_after_resync_snapshot() {
        let mut state = StrictL2BookState::new(
            "binance",
            MarketType::Spot,
            "BTCUSDT",
            "BTCUSDT",
            L2BookCapability::strict_delta(Some(10)),
        );
        state.apply(&event(BookUpdateKind::FullSnapshot, 10));
        state.apply(&event(BookUpdateKind::Delta, 12));
        assert!(!state.is_tradeable());

        let outcome = state.apply(&event(BookUpdateKind::FullSnapshot, 13));

        assert_eq!(outcome.status, L2BookUpdateStatus::Resynced);
        assert!(outcome.is_tradeable);
        assert_eq!(state.last_sequence(), Some(13));
    }
}
