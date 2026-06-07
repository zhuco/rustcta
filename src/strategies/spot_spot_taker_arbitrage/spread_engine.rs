pub use rustcta_strategy_spot_spot_arbitrage::{
    calculate_spread, DirectedVenuePair, EventDrivenSpreadEngine, EventDrivenSpreadEngineConfig,
    EventDrivenSpreadResult, SpotBookEvent, SpotBookEventKind, SpreadEstimate,
};

use crate::data::{BookEvent, BookEventKind};
use crate::exchanges::unified::{MarketType, OrderBookSnapshot};

pub fn spot_book_event_from_shared(event: &BookEvent) -> Option<SpotBookEvent> {
    if event.market_type != MarketType::Spot {
        return None;
    }
    Some(SpotBookEvent {
        exchange: event.exchange.clone(),
        symbol: event.internal_symbol.clone(),
        event_kind: spot_book_event_kind(event.event_kind),
        bids: event
            .bids
            .iter()
            .map(
                |level| rustcta_strategy_spot_spot_arbitrage::SpotOrderBookLevel {
                    price: level.price,
                    quantity: level.quantity,
                },
            )
            .collect(),
        asks: event
            .asks
            .iter()
            .map(
                |level| rustcta_strategy_spot_spot_arbitrage::SpotOrderBookLevel {
                    price: level.price,
                    quantity: level.quantity,
                },
            )
            .collect(),
        best_bid: event.best_bid,
        best_ask: event.best_ask,
        exchange_timestamp: event.exchange_timestamp,
        local_timestamp: event.received_at,
        latency_ms: event.latency_ms,
        sequence: event.sequence,
        source: event.source.into(),
        is_tradeable: event.is_tradeable,
        stale_reason: event.stale_reason.clone(),
    })
}

pub fn event_engine_snapshot(
    book: &rustcta_strategy_spot_spot_arbitrage::CachedBook,
) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: book.exchange.clone(),
        market_type: MarketType::Spot,
        symbol: book.symbol.clone(),
        bids: book
            .bids
            .iter()
            .map(|level| crate::exchanges::unified::OrderBookLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|level| crate::exchanges::unified::OrderBookLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect(),
        best_bid: book.best_bid,
        best_ask: book.best_ask,
        exchange_timestamp: book.exchange_timestamp,
        received_at: book.local_timestamp,
        latency_ms: book.latency_ms,
        sequence: book.sequence,
        is_stale: book.is_stale,
    }
}

fn spot_book_event_kind(kind: BookEventKind) -> SpotBookEventKind {
    match kind {
        BookEventKind::Snapshot => SpotBookEventKind::Snapshot,
        BookEventKind::Delta => SpotBookEventKind::Delta,
        BookEventKind::TopOfBook => SpotBookEventKind::TopOfBook,
        BookEventKind::Heartbeat => SpotBookEventKind::Heartbeat,
        BookEventKind::Stale => SpotBookEventKind::Stale,
        BookEventKind::Gap => SpotBookEventKind::Gap,
        BookEventKind::ChecksumMismatch => SpotBookEventKind::ChecksumMismatch,
        BookEventKind::Reconnect => SpotBookEventKind::Reconnect,
        BookEventKind::Error => SpotBookEventKind::Error,
    }
}
