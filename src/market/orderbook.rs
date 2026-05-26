use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::OrderBook5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceCheck {
    pub previous: Option<u64>,
    pub current: Option<u64>,
    pub gap_detected: bool,
}

impl SequenceCheck {
    pub fn check(previous: Option<u64>, current: Option<u64>) -> Self {
        let gap_detected = match (previous, current) {
            (Some(prev), Some(curr)) => curr <= prev || curr > prev + 1,
            _ => false,
        };

        Self {
            previous,
            current,
            gap_detected,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookFreshness {
    pub recv_ts: DateTime<Utc>,
    pub checked_at: DateTime<Utc>,
    pub max_age_ms: i64,
    pub stale: bool,
}

impl BookFreshness {
    pub fn check(recv_ts: DateTime<Utc>, checked_at: DateTime<Utc>, max_age_ms: i64) -> Self {
        let max_age = Duration::milliseconds(max_age_ms.max(0));
        let stale = checked_at.signed_duration_since(recv_ts) > max_age;

        Self {
            recv_ts,
            checked_at,
            max_age_ms,
            stale,
        }
    }
}

pub fn mark_book_freshness(
    mut book: OrderBook5,
    checked_at: DateTime<Utc>,
    max_age_ms: i64,
) -> OrderBook5 {
    let freshness = BookFreshness::check(book.recv_ts, checked_at, max_age_ms);
    book.quality.stale = freshness.stale;
    book
}

pub fn mark_sequence_gap(
    mut book: OrderBook5,
    previous_sequence: Option<u64>,
) -> (OrderBook5, SequenceCheck) {
    let check = SequenceCheck::check(previous_sequence, book.sequence);
    book.quality.sequence_gap = check.gap_detected;
    (book, check)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::market::{BookLevel, CanonicalSymbol, ExchangeId, ExchangeSymbol};

    fn book(sequence: Option<u64>, recv_ts: DateTime<Utc>) -> OrderBook5 {
        let exchange = ExchangeId::Okx;
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("wif", "usdt"),
            ExchangeSymbol::new(exchange, "WIF-USDT-SWAP"),
            vec![BookLevel::new(99.0, 1.0)],
            vec![BookLevel::new(100.0, 1.0)],
            recv_ts,
            recv_ts,
            sequence,
            None,
        )
    }

    #[test]
    fn freshness_should_mark_stale_book() {
        let now = Utc::now();
        let stale_book = mark_book_freshness(book(Some(1), now - Duration::seconds(2)), now, 500);

        assert!(stale_book.quality.stale);
        assert!(!stale_book.is_usable());
    }

    #[test]
    fn sequence_check_should_detect_gaps() {
        let (book, check) = mark_sequence_gap(book(Some(5), Utc::now()), Some(3));

        assert!(check.gap_detected);
        assert!(book.quality.sequence_gap);
    }

    #[test]
    fn sequence_check_should_accept_next_sequence() {
        let (_, check) = mark_sequence_gap(book(Some(4), Utc::now()), Some(3));

        assert!(!check.gap_detected);
    }
}
