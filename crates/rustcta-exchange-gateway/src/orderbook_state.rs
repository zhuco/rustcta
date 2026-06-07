use std::cmp::Ordering;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, OrderBookCapability, OrderBookChecksumMode, OrderBookStrictness,
    ResyncReason, SequenceGap,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, MarketType, OrderBookLevel, OrderBookSnapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBookStateStatus {
    Empty,
    Live,
    NeedsResync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookDeltaLevel {
    pub price: f64,
    pub quantity: f64,
}

impl OrderBookDeltaLevel {
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub bids: Vec<OrderBookDeltaLevel>,
    pub asks: Vec<OrderBookDeltaLevel>,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub previous_sequence: Option<u64>,
    pub checksum: Option<i64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
}

impl OrderBookDelta {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        received_at: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange_id,
            market_type,
            canonical_symbol,
            bids: Vec::new(),
            asks: Vec::new(),
            first_sequence: None,
            last_sequence: None,
            previous_sequence: None,
            checksum: None,
            exchange_timestamp: None,
            received_at,
        }
    }

    pub fn with_sequences(
        mut self,
        first_sequence: Option<u64>,
        last_sequence: Option<u64>,
    ) -> Self {
        self.first_sequence = first_sequence;
        self.last_sequence = last_sequence;
        self
    }

    pub fn with_previous_sequence(mut self, previous_sequence: Option<u64>) -> Self {
        self.previous_sequence = previous_sequence;
        self
    }

    pub fn with_checksum(mut self, checksum: Option<i64>) -> Self {
        self.checksum = checksum;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookResyncRequest {
    pub reason: ResyncReason,
    pub expected_next_sequence: Option<u64>,
    pub received_first_sequence: Option<u64>,
    pub received_last_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderBookApplyOutcome {
    SnapshotApplied { sequence: Option<u64> },
    DeltaApplied { sequence: Option<u64> },
    StaleDeltaIgnored { sequence: Option<u64> },
    ResyncRequired(OrderBookResyncRequest),
}

pub trait OrderBookChecksum {
    fn mode(&self) -> Option<OrderBookChecksumMode> {
        None
    }

    fn checksum(&self, snapshot: &OrderBookSnapshot) -> Result<i64, ExchangeApiError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookStateConfig {
    pub strictness: OrderBookStrictness,
    pub require_sequence: bool,
    pub verify_checksum: bool,
    pub checksum_mode: OrderBookChecksumMode,
}

impl Default for OrderBookStateConfig {
    fn default() -> Self {
        Self::from_capability(&OrderBookCapability::default())
    }
}

impl OrderBookStateConfig {
    pub fn from_capability(capability: &OrderBookCapability) -> Self {
        Self {
            strictness: capability.strictness,
            require_sequence: matches!(capability.strictness, OrderBookStrictness::StrictDelta),
            verify_checksum: capability.supports_checksum
                && capability.checksum_mode != OrderBookChecksumMode::Disabled,
            checksum_mode: capability.checksum_mode,
        }
    }

    pub fn best_effort_delta() -> Self {
        Self {
            strictness: OrderBookStrictness::BestEffortDelta,
            require_sequence: false,
            verify_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
        }
    }

    pub fn strict_delta() -> Self {
        Self {
            strictness: OrderBookStrictness::StrictDelta,
            require_sequence: true,
            verify_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopLevelSumChecksum {
    pub depth: usize,
}

impl TopLevelSumChecksum {
    pub fn new(depth: usize) -> Self {
        Self { depth }
    }
}

impl OrderBookChecksum for TopLevelSumChecksum {
    fn mode(&self) -> Option<OrderBookChecksumMode> {
        Some(OrderBookChecksumMode::TopLevelSum)
    }

    fn checksum(&self, snapshot: &OrderBookSnapshot) -> Result<i64, ExchangeApiError> {
        let bid_sum = snapshot
            .bids
            .iter()
            .take(self.depth)
            .map(|level| scaled_price_quantity(level))
            .try_fold(0_i64, |sum, value| checked_add(sum, value?))?;
        let ask_sum = snapshot
            .asks
            .iter()
            .take(self.depth)
            .map(|level| scaled_price_quantity(level))
            .try_fold(0_i64, |sum, value| checked_add(sum, value?))?;
        checked_add(bid_sum, ask_sum)
    }
}

#[derive(Debug, Clone)]
pub struct OrderBookState {
    exchange_id: ExchangeId,
    market_type: MarketType,
    canonical_symbol: CanonicalSymbol,
    exchange_symbol: Option<String>,
    bids: BTreeMap<PriceKey, f64>,
    asks: BTreeMap<PriceKey, f64>,
    last_sequence: Option<u64>,
    last_exchange_timestamp: Option<DateTime<Utc>>,
    last_received_at: Option<DateTime<Utc>>,
    status: OrderBookStateStatus,
    last_resync_reason: Option<ResyncReason>,
    config: OrderBookStateConfig,
}

impl OrderBookState {
    pub fn new(
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
    ) -> Self {
        Self {
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_sequence: None,
            last_exchange_timestamp: None,
            last_received_at: None,
            status: OrderBookStateStatus::Empty,
            last_resync_reason: None,
            config: OrderBookStateConfig::default(),
        }
    }

    pub fn with_config(mut self, config: OrderBookStateConfig) -> Self {
        self.config = config;
        self
    }

    pub fn from_snapshot(snapshot: OrderBookSnapshot) -> Result<Self, ExchangeApiError> {
        let mut state = Self::new(
            snapshot.exchange_id.clone(),
            snapshot.market_type,
            snapshot.canonical_symbol.clone(),
        );
        state.exchange_symbol = snapshot
            .exchange_symbol
            .as_ref()
            .map(|symbol| symbol.symbol.clone());
        state.apply_snapshot(snapshot)?;
        Ok(state)
    }

    pub fn status(&self) -> OrderBookStateStatus {
        self.status
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    pub fn last_resync_reason(&self) -> Option<ResyncReason> {
        self.last_resync_reason
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: OrderBookSnapshot,
    ) -> Result<OrderBookApplyOutcome, ExchangeApiError> {
        self.ensure_snapshot_scope(&snapshot)?;
        snapshot.validate().map_err(validation_error)?;

        self.bids = levels_to_book(&snapshot.bids)?;
        self.asks = levels_to_book(&snapshot.asks)?;
        self.exchange_symbol = snapshot
            .exchange_symbol
            .as_ref()
            .map(|symbol| symbol.symbol.clone())
            .or_else(|| self.exchange_symbol.clone());
        self.last_sequence = snapshot.sequence;
        self.last_exchange_timestamp = snapshot.exchange_timestamp;
        self.last_received_at = Some(snapshot.received_at);
        self.status = OrderBookStateStatus::Live;
        self.last_resync_reason = None;

        Ok(OrderBookApplyOutcome::SnapshotApplied {
            sequence: self.last_sequence,
        })
    }

    pub fn apply_delta(
        &mut self,
        delta: OrderBookDelta,
        checksum: Option<&dyn OrderBookChecksum>,
    ) -> Result<OrderBookApplyOutcome, ExchangeApiError> {
        self.ensure_delta_scope(&delta)?;
        if self.config.strictness == OrderBookStrictness::SnapshotOnly {
            return Ok(self.require_resync(
                ResyncReason::Manual,
                delta.first_sequence,
                delta.last_sequence,
            ));
        }
        if self.status != OrderBookStateStatus::Live {
            return Ok(self.require_resync(
                ResyncReason::Manual,
                delta.first_sequence,
                delta.last_sequence,
            ));
        }

        if let Some(outcome) = self.sequence_outcome(&delta) {
            return Ok(outcome);
        }
        if self.config.require_sequence
            && delta.previous_sequence.is_none()
            && delta.first_sequence.is_none()
            && delta.last_sequence.is_none()
        {
            return Ok(self.require_resync(ResyncReason::SequenceGap, None, None));
        }

        let original_bids = self.bids.clone();
        let original_asks = self.asks.clone();
        let original_sequence = self.last_sequence;
        let original_exchange_timestamp = self.last_exchange_timestamp;
        let original_received_at = self.last_received_at;

        if let Err(err) = apply_side_updates(&mut self.bids, &delta.bids) {
            self.restore_after_failed_delta(
                original_bids,
                original_asks,
                original_sequence,
                original_exchange_timestamp,
                original_received_at,
                ResyncReason::Manual,
            );
            return Err(err);
        }
        if let Err(err) = apply_side_updates(&mut self.asks, &delta.asks) {
            self.restore_after_failed_delta(
                original_bids,
                original_asks,
                original_sequence,
                original_exchange_timestamp,
                original_received_at,
                ResyncReason::Manual,
            );
            return Err(err);
        }

        self.last_sequence = delta
            .last_sequence
            .or(delta.first_sequence)
            .or(self.last_sequence);
        self.last_exchange_timestamp = delta.exchange_timestamp.or(self.last_exchange_timestamp);
        self.last_received_at = Some(delta.received_at);

        let snapshot = match self.snapshot(delta.received_at) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.restore_after_failed_delta(
                    original_bids,
                    original_asks,
                    original_sequence,
                    original_exchange_timestamp,
                    original_received_at,
                    ResyncReason::Manual,
                );
                return Err(err);
            }
        };

        if self.config.verify_checksum || delta.checksum.is_some() {
            match (delta.checksum, checksum) {
                (Some(expected), Some(checksum)) => {
                    if self.config.verify_checksum
                        && checksum.mode() != Some(self.config.checksum_mode)
                    {
                        self.restore_after_failed_delta(
                            original_bids,
                            original_asks,
                            original_sequence,
                            original_exchange_timestamp,
                            original_received_at,
                            ResyncReason::ChecksumMismatch,
                        );
                        return Ok(self.require_resync(
                            ResyncReason::ChecksumMismatch,
                            delta.first_sequence,
                            delta.last_sequence,
                        ));
                    }
                    let actual = checksum.checksum(&snapshot)?;
                    if actual != expected {
                        self.restore_after_failed_delta(
                            original_bids,
                            original_asks,
                            original_sequence,
                            original_exchange_timestamp,
                            original_received_at,
                            ResyncReason::ChecksumMismatch,
                        );
                        return Ok(self.require_resync(
                            ResyncReason::ChecksumMismatch,
                            delta.first_sequence,
                            delta.last_sequence,
                        ));
                    }
                }
                (None, _) | (Some(_), None) => {
                    self.restore_after_failed_delta(
                        original_bids,
                        original_asks,
                        original_sequence,
                        original_exchange_timestamp,
                        original_received_at,
                        ResyncReason::ChecksumMismatch,
                    );
                    return Ok(self.require_resync(
                        ResyncReason::ChecksumMismatch,
                        delta.first_sequence,
                        delta.last_sequence,
                    ));
                }
            }
        }

        Ok(OrderBookApplyOutcome::DeltaApplied {
            sequence: self.last_sequence,
        })
    }

    pub fn snapshot(
        &self,
        received_at: DateTime<Utc>,
    ) -> Result<OrderBookSnapshot, ExchangeApiError> {
        let bids = book_to_levels(&self.bids, true)?;
        let asks = book_to_levels(&self.asks, false)?;
        let mut snapshot = OrderBookSnapshot::new(
            self.exchange_id.clone(),
            self.market_type,
            self.canonical_symbol.clone(),
            bids,
            asks,
            received_at,
        )
        .map_err(validation_error)?;
        snapshot.sequence = self.last_sequence;
        snapshot.exchange_timestamp = self.last_exchange_timestamp;
        snapshot.is_stale = self.status != OrderBookStateStatus::Live;
        Ok(snapshot)
    }

    pub fn mark_resync_required(&mut self, reason: ResyncReason) {
        self.status = OrderBookStateStatus::NeedsResync;
        self.last_resync_reason = Some(reason);
    }

    fn sequence_outcome(&mut self, delta: &OrderBookDelta) -> Option<OrderBookApplyOutcome> {
        let current = self.last_sequence?;
        if let Some(previous_sequence) = delta.previous_sequence {
            if previous_sequence == current {
                return None;
            }
            if previous_sequence < current {
                return Some(OrderBookApplyOutcome::StaleDeltaIgnored {
                    sequence: self.last_sequence,
                });
            }
            return Some(self.require_resync(
                ResyncReason::SequenceGap,
                delta.first_sequence,
                delta.last_sequence,
            ));
        }

        if let Some(first_sequence) = delta.first_sequence {
            if let Some(last_sequence) = delta.last_sequence {
                if last_sequence <= current {
                    return Some(OrderBookApplyOutcome::StaleDeltaIgnored {
                        sequence: self.last_sequence,
                    });
                }
                if first_sequence > current.saturating_add(1) {
                    return Some(self.require_resync(
                        ResyncReason::SequenceGap,
                        delta.first_sequence,
                        delta.last_sequence,
                    ));
                }
                return None;
            }

            if first_sequence <= current {
                return Some(OrderBookApplyOutcome::StaleDeltaIgnored {
                    sequence: self.last_sequence,
                });
            }
            if first_sequence != current.saturating_add(1) {
                return Some(self.require_resync(
                    ResyncReason::SequenceGap,
                    delta.first_sequence,
                    delta.last_sequence,
                ));
            }
        }

        None
    }

    fn require_resync(
        &mut self,
        reason: ResyncReason,
        received_first_sequence: Option<u64>,
        received_last_sequence: Option<u64>,
    ) -> OrderBookApplyOutcome {
        self.mark_resync_required(reason);
        OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
            reason,
            expected_next_sequence: self
                .last_sequence
                .map(|sequence| sequence.saturating_add(1)),
            received_first_sequence,
            received_last_sequence,
        })
    }

    pub fn sequence_gap(
        &self,
        request: &OrderBookResyncRequest,
        detected_at: DateTime<Utc>,
    ) -> Option<SequenceGap> {
        if request.reason != ResyncReason::SequenceGap {
            return None;
        }
        Some(SequenceGap {
            exchange: self.exchange_id.clone(),
            market_type: self.market_type,
            exchange_symbol: self
                .exchange_symbol
                .clone()
                .unwrap_or_else(|| self.canonical_symbol.as_str().to_string()),
            expected_sequence: request.expected_next_sequence,
            received_sequence: request
                .received_first_sequence
                .or(request.received_last_sequence),
            detected_at,
        })
    }

    fn restore_after_failed_delta(
        &mut self,
        bids: BTreeMap<PriceKey, f64>,
        asks: BTreeMap<PriceKey, f64>,
        last_sequence: Option<u64>,
        last_exchange_timestamp: Option<DateTime<Utc>>,
        last_received_at: Option<DateTime<Utc>>,
        reason: ResyncReason,
    ) {
        self.bids = bids;
        self.asks = asks;
        self.last_sequence = last_sequence;
        self.last_exchange_timestamp = last_exchange_timestamp;
        self.last_received_at = last_received_at;
        self.mark_resync_required(reason);
    }

    fn ensure_snapshot_scope(&self, snapshot: &OrderBookSnapshot) -> Result<(), ExchangeApiError> {
        if snapshot.exchange_id != self.exchange_id
            || snapshot.market_type != self.market_type
            || snapshot.canonical_symbol != self.canonical_symbol
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "order book snapshot scope does not match state".to_string(),
            });
        }
        Ok(())
    }

    fn ensure_delta_scope(&self, delta: &OrderBookDelta) -> Result<(), ExchangeApiError> {
        if delta.exchange_id != self.exchange_id
            || delta.market_type != self.market_type
            || delta.canonical_symbol != self.canonical_symbol
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "order book delta scope does not match state".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PriceKey(f64);

impl Eq for PriceKey {}

impl Ord for PriceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for PriceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn levels_to_book(levels: &[OrderBookLevel]) -> Result<BTreeMap<PriceKey, f64>, ExchangeApiError> {
    let mut book = BTreeMap::new();
    for level in levels {
        validate_delta_level(level.price, level.quantity)?;
        book.insert(PriceKey(level.price), level.quantity);
    }
    Ok(book)
}

fn apply_side_updates(
    book: &mut BTreeMap<PriceKey, f64>,
    levels: &[OrderBookDeltaLevel],
) -> Result<(), ExchangeApiError> {
    for level in levels {
        validate_delta_level(level.price, level.quantity)?;
        if level.quantity == 0.0 {
            book.remove(&PriceKey(level.price));
        } else {
            book.insert(PriceKey(level.price), level.quantity);
        }
    }
    Ok(())
}

fn book_to_levels(
    book: &BTreeMap<PriceKey, f64>,
    descending: bool,
) -> Result<Vec<OrderBookLevel>, ExchangeApiError> {
    let iter: Box<dyn Iterator<Item = (&PriceKey, &f64)> + '_> = if descending {
        Box::new(book.iter().rev())
    } else {
        Box::new(book.iter())
    };

    iter.map(|(price, quantity)| OrderBookLevel::new(price.0, *quantity).map_err(validation_error))
        .collect()
}

fn validate_delta_level(price: f64, quantity: f64) -> Result<(), ExchangeApiError> {
    if !price.is_finite() || price <= 0.0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "order book delta price must be finite and positive".to_string(),
        });
    }
    if !quantity.is_finite() || quantity < 0.0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "order book delta quantity must be finite and non-negative".to_string(),
        });
    }
    Ok(())
}

fn scaled_price_quantity(level: &OrderBookLevel) -> Result<i64, ExchangeApiError> {
    let value = (level.price * level.quantity * 100_000_000.0).round();
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "order book checksum level overflow".to_string(),
        });
    }
    Ok(value as i64)
}

fn checked_add(left: i64, right: i64) -> Result<i64, ExchangeApiError> {
    left.checked_add(right)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "order book checksum overflow".to_string(),
        })
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("binance").expect("exchange")
    }

    fn symbol() -> CanonicalSymbol {
        CanonicalSymbol::new("BTC", "USDT").expect("symbol")
    }

    fn level(price: f64, quantity: f64) -> OrderBookLevel {
        OrderBookLevel::new(price, quantity).expect("level")
    }

    fn snapshot(sequence: u64) -> OrderBookSnapshot {
        let mut snapshot = OrderBookSnapshot::new(
            exchange_id(),
            MarketType::Spot,
            symbol(),
            vec![level(100.0, 1.0), level(99.0, 2.0)],
            vec![level(101.0, 1.5), level(102.0, 2.5)],
            Utc::now(),
        )
        .expect("snapshot");
        snapshot.sequence = Some(sequence);
        snapshot
    }

    fn delta(first_sequence: u64, last_sequence: u64) -> OrderBookDelta {
        OrderBookDelta::new(exchange_id(), MarketType::Spot, symbol(), Utc::now())
            .with_sequences(Some(first_sequence), Some(last_sequence))
    }

    #[test]
    fn orderbook_state_should_apply_snapshot() {
        let mut state = OrderBookState::new(exchange_id(), MarketType::Spot, symbol());

        let outcome = state.apply_snapshot(snapshot(10)).expect("snapshot apply");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::SnapshotApplied { sequence: Some(10) }
        );
        assert_eq!(state.status(), OrderBookStateStatus::Live);
        assert_eq!(
            state
                .snapshot(Utc::now())
                .expect("book")
                .best_bid()
                .unwrap()
                .price,
            100.0
        );
    }

    #[test]
    fn orderbook_state_should_apply_contiguous_delta() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = delta(11, 11);
        delta.bids = vec![
            OrderBookDeltaLevel::new(100.0, 0.0),
            OrderBookDeltaLevel::new(98.5, 3.0),
        ];
        delta.asks = vec![OrderBookDeltaLevel::new(101.0, 2.0)];

        let outcome = state.apply_delta(delta, None).expect("delta apply");
        let book = state.snapshot(Utc::now()).expect("book");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::DeltaApplied { sequence: Some(11) }
        );
        assert_eq!(book.best_bid().unwrap().price, 99.0);
        assert_eq!(book.asks[0], level(101.0, 2.0));
    }

    #[test]
    fn orderbook_state_should_ignore_stale_delta() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = delta(9, 10);
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("delta ignored");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::StaleDeltaIgnored { sequence: Some(10) }
        );
        assert_eq!(state.snapshot(Utc::now()).expect("book").bids.len(), 2);
    }

    #[test]
    fn orderbook_state_should_request_resync_on_sequence_gap() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = delta(12, 12);
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("gap outcome");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(state.last_resync_reason(), Some(ResyncReason::SequenceGap));
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::SequenceGap,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(12),
                received_last_sequence: Some(12),
            })
        );
    }

    #[test]
    fn orderbook_state_should_request_resync_on_checksum_mismatch() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = delta(11, 11).with_checksum(Some(-1));
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];
        let checksum = TopLevelSumChecksum::new(2);

        let outcome = state
            .apply_delta(delta, Some(&checksum))
            .expect("checksum outcome");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(
            state.last_resync_reason(),
            Some(ResyncReason::ChecksumMismatch)
        );
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::ChecksumMismatch,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(11),
                received_last_sequence: Some(11),
            })
        );
        assert_eq!(state.snapshot(Utc::now()).expect("book").bids.len(), 2);
    }

    #[test]
    fn orderbook_state_should_support_previous_sequence_protocols() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = OrderBookDelta::new(exchange_id(), MarketType::Spot, symbol(), Utc::now())
            .with_previous_sequence(Some(10))
            .with_sequences(None, Some(15));
        delta.asks = vec![OrderBookDeltaLevel::new(103.0, 2.0)];

        let outcome = state.apply_delta(delta, None).expect("delta apply");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::DeltaApplied { sequence: Some(15) }
        );
        assert_eq!(state.snapshot(Utc::now()).expect("book").asks.len(), 3);
    }

    #[test]
    fn orderbook_state_should_reject_delta_for_snapshot_only_capability() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::from_capability(
                &OrderBookCapability::snapshot_only(Some(100)),
            ));
        let mut delta = delta(11, 11);
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state
            .apply_delta(delta, None)
            .expect("snapshot-only outcome");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::Manual,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(11),
                received_last_sequence: Some(11),
            })
        );
    }

    #[test]
    fn orderbook_state_should_allow_best_effort_delta_without_sequence() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::best_effort_delta());
        let mut delta = OrderBookDelta::new(exchange_id(), MarketType::Spot, symbol(), Utc::now());
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("best effort delta");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::DeltaApplied { sequence: Some(10) }
        );
        assert_eq!(state.status(), OrderBookStateStatus::Live);
        assert_eq!(state.snapshot(Utc::now()).expect("book").bids.len(), 3);
    }

    #[test]
    fn orderbook_state_should_not_require_sequence_for_best_effort_capability() {
        let mut capability = OrderBookCapability::best_effort_delta(Some(100));
        capability.supports_sequence = true;
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::from_capability(&capability));
        let mut delta = OrderBookDelta::new(exchange_id(), MarketType::Spot, symbol(), Utc::now());
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("best effort delta");

        assert_eq!(
            outcome,
            OrderBookApplyOutcome::DeltaApplied { sequence: Some(10) }
        );
        assert_eq!(state.status(), OrderBookStateStatus::Live);
    }

    #[test]
    fn orderbook_state_should_require_checksum_when_configured() {
        let mut capability = OrderBookCapability::strict_delta(Some(100));
        capability.supports_checksum = true;
        capability.checksum_mode = OrderBookChecksumMode::TopLevelSum;
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::from_capability(&capability));
        let mut delta = delta(11, 11);
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("checksum required");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::ChecksumMismatch,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(11),
                received_last_sequence: Some(11),
            })
        );
    }

    #[test]
    fn orderbook_state_should_reject_unidentified_checksum_hook_when_configured() {
        struct UnidentifiedChecksum;

        impl OrderBookChecksum for UnidentifiedChecksum {
            fn checksum(&self, _snapshot: &OrderBookSnapshot) -> Result<i64, ExchangeApiError> {
                Ok(0)
            }
        }

        let mut capability = OrderBookCapability::strict_delta(Some(100));
        capability.supports_checksum = true;
        capability.checksum_mode = OrderBookChecksumMode::TopLevelSum;
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::from_capability(&capability));
        let mut delta = delta(11, 11).with_checksum(Some(0));
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state
            .apply_delta(delta, Some(&UnidentifiedChecksum))
            .expect("checksum hook mode");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::ChecksumMismatch,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(11),
                received_last_sequence: Some(11),
            })
        );
    }

    #[test]
    fn orderbook_state_should_require_sequence_for_strict_delta() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::strict_delta());
        let mut delta = OrderBookDelta::new(exchange_id(), MarketType::Spot, symbol(), Utc::now());
        delta.bids = vec![OrderBookDeltaLevel::new(98.0, 8.0)];

        let outcome = state.apply_delta(delta, None).expect("strict outcome");

        assert_eq!(state.status(), OrderBookStateStatus::NeedsResync);
        assert_eq!(
            outcome,
            OrderBookApplyOutcome::ResyncRequired(OrderBookResyncRequest {
                reason: ResyncReason::SequenceGap,
                expected_next_sequence: Some(11),
                received_first_sequence: None,
                received_last_sequence: None,
            })
        );
    }

    #[test]
    fn orderbook_state_should_build_sequence_gap_details() {
        let mut state = OrderBookState::from_snapshot(snapshot(10))
            .expect("state")
            .with_config(OrderBookStateConfig::strict_delta());
        let outcome = state.apply_delta(delta(12, 12), None).expect("gap");
        let OrderBookApplyOutcome::ResyncRequired(request) = outcome else {
            panic!("expected resync request");
        };

        let detected_at = Utc::now();
        let gap = state
            .sequence_gap(&request, detected_at)
            .expect("gap details");

        assert_eq!(gap.exchange, exchange_id());
        assert_eq!(gap.market_type, MarketType::Spot);
        assert_eq!(gap.expected_sequence, Some(11));
        assert_eq!(gap.received_sequence, Some(12));
        assert_eq!(gap.detected_at, detected_at);
    }
}
