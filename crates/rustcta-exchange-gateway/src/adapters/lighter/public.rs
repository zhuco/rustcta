#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{PublicStreamKind, PublicStreamSubscription};

pub fn lighter_public_channel(subscription: &PublicStreamSubscription) -> String {
    let market = normalize_market_index(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("order_book/{market}")
        }
        PublicStreamKind::Ticker => format!("ticker/{market}"),
        PublicStreamKind::Trades => format!("trade/{market}"),
        PublicStreamKind::Candles { interval } => format!("candle/{market}/{interval}"),
    }
}

pub fn normalize_market_index(symbol: &str) -> String {
    symbol
        .trim()
        .strip_prefix("market:")
        .unwrap_or(symbol.trim())
        .to_string()
}
