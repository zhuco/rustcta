#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{PublicStreamKind, PublicStreamSubscription};

pub fn grvt_market_data_stream(kind: &PublicStreamKind) -> &'static str {
    match kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "v1.book.s",
        PublicStreamKind::Trades => "v1.trade",
        PublicStreamKind::Ticker => "v1.ticker",
        PublicStreamKind::Candles { .. } => "v1.candle",
    }
}

pub fn grvt_public_feed(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-'], "_");
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("{symbol}@500-1-10")
        }
        PublicStreamKind::Candles { interval } => format!("{symbol}@{interval}"),
        PublicStreamKind::Trades | PublicStreamKind::Ticker => symbol,
    }
}
