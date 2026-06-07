use chrono::Utc;

use crate::exchanges::unified::{OrderBookLevel, OrderBookSnapshot};

pub fn book_age_ms(book: &OrderBookSnapshot) -> i64 {
    Utc::now()
        .signed_duration_since(book.received_at)
        .num_milliseconds()
        .max(0)
}

pub fn is_book_fresh(book: &OrderBookSnapshot, stale_book_ms: u64, max_latency_ms: u64) -> bool {
    if book.is_stale {
        return false;
    }
    if book_age_ms(book) > stale_book_ms as i64 {
        return false;
    }
    if book
        .latency_ms
        .is_some_and(|latency| latency > max_latency_ms as i64 || latency < -1_000)
    {
        return false;
    }
    true
}

pub fn buy_depth_notional(asks: &[OrderBookLevel], max_notional: f64) -> f64 {
    depth_notional(asks, max_notional)
}

pub fn sell_depth_notional(bids: &[OrderBookLevel], max_notional: f64) -> f64 {
    depth_notional(bids, max_notional)
}

fn depth_notional(levels: &[OrderBookLevel], max_notional: f64) -> f64 {
    let mut remaining = max_notional.max(0.0);
    let mut filled = 0.0;
    for level in levels {
        let level_notional = level.price * level.quantity;
        let take = remaining.min(level_notional);
        filled += take;
        remaining -= take;
        if remaining <= 1e-12 {
            break;
        }
    }
    filled
}
