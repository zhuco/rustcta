//! Market-data normalization for detection-only cross-exchange arbitrage.

use std::collections::HashMap;

use super::config::CrossExchangeArbitrageConfig;
use super::types::NormalizedDepthSnapshot;
use crate::exchanges::unified::OrderBookSnapshot;
use crate::market::{BookLevel, CanonicalSymbol, ExchangeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataSubscription {
    pub exchange: ExchangeId,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketDataBookStore {
    books: HashMap<(ExchangeId, CanonicalSymbol), NormalizedDepthSnapshot>,
}

impl MarketDataBookStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, book: NormalizedDepthSnapshot) {
        self.books
            .insert((book.exchange.clone(), book.symbol.clone()), book);
    }

    pub fn get(
        &self,
        exchange: &ExchangeId,
        symbol: &CanonicalSymbol,
    ) -> Option<&NormalizedDepthSnapshot> {
        self.books.get(&(exchange.clone(), symbol.clone()))
    }
}

pub fn build_subscription_plan(
    config: &CrossExchangeArbitrageConfig,
) -> Vec<MarketDataSubscription> {
    config
        .detection
        .exchanges
        .iter()
        .cloned()
        .map(|exchange| MarketDataSubscription {
            symbols: config
                .detection
                .symbols
                .iter()
                .map(|symbol| {
                    spot_exchange_symbol(&exchange, symbol, &config.detection.symbol_mappings)
                })
                .collect(),
            exchange,
        })
        .collect()
}

pub fn normalize_orderbook_snapshot(
    exchange: ExchangeId,
    snapshot: OrderBookSnapshot,
    symbol_mappings: &HashMap<ExchangeId, HashMap<String, String>>,
) -> Option<NormalizedDepthSnapshot> {
    let canonical = normalize_symbol(&snapshot.symbol)
        .or_else(|| mapped_canonical_symbol(&exchange, &snapshot.symbol, symbol_mappings))?;
    Some(NormalizedDepthSnapshot {
        exchange,
        symbol: canonical,
        exchange_symbol: snapshot.symbol,
        bids: snapshot
            .bids
            .into_iter()
            .map(|level| BookLevel::new(level.price, level.quantity))
            .collect(),
        asks: snapshot
            .asks
            .into_iter()
            .map(|level| BookLevel::new(level.price, level.quantity))
            .collect(),
        exchange_timestamp: snapshot.exchange_timestamp,
        received_at: snapshot.received_at,
        sequence: snapshot.sequence,
    })
}

pub fn normalize_symbol(symbol: &str) -> Option<CanonicalSymbol> {
    let value = symbol.trim().to_ascii_uppercase();
    if let Some(symbol) = CanonicalSymbol::parse(&value) {
        return Some(symbol);
    }
    let compact = value.replace(['-', '_', '/'], "");
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = compact.strip_suffix(quote) {
            if !base.is_empty() {
                return Some(CanonicalSymbol::new(base, quote));
            }
        }
    }
    None
}

pub fn spot_exchange_symbol(
    exchange: &ExchangeId,
    symbol: &str,
    symbol_mappings: &HashMap<ExchangeId, HashMap<String, String>>,
) -> String {
    if let Some(mapped) = symbol_mappings
        .get(exchange)
        .and_then(|mappings| mappings.get(symbol))
    {
        return mapped.clone();
    }
    let Some(canonical) = normalize_symbol(symbol) else {
        return symbol.trim().to_ascii_uppercase();
    };
    match exchange {
        ExchangeId::Okx => format!("{}-{}", canonical.base(), canonical.quote()),
        _ => format!("{}{}", canonical.base(), canonical.quote()),
    }
}

fn mapped_canonical_symbol(
    exchange: &ExchangeId,
    exchange_symbol: &str,
    symbol_mappings: &HashMap<ExchangeId, HashMap<String, String>>,
) -> Option<CanonicalSymbol> {
    symbol_mappings.get(exchange).and_then(|mappings| {
        mappings.iter().find_map(|(internal, mapped)| {
            (mapped == exchange_symbol)
                .then(|| normalize_symbol(internal))
                .flatten()
        })
    })
}
