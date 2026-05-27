use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{
    mark_book_freshness, CanonicalSymbol, ExchangeId, InstrumentMeta, MarketFundingSnapshot,
    OrderBook5, RouteHealth, RouteType,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSymbolSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub orderbook: Option<OrderBook5>,
    pub funding: Option<MarketFundingSnapshot>,
    pub instrument: Option<InstrumentMeta>,
    pub route_health: Vec<RouteHealth>,
    pub usable_for_entries: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStateCache {
    stale_quote_ms: i64,
    orderbooks: HashMap<(ExchangeId, CanonicalSymbol), OrderBook5>,
    funding: HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
    instruments: HashMap<(ExchangeId, CanonicalSymbol), InstrumentMeta>,
    route_health: HashMap<(ExchangeId, RouteType, String), RouteHealth>,
}

impl Default for MarketStateCache {
    fn default() -> Self {
        Self::new(1_000)
    }
}

impl MarketStateCache {
    pub fn new(stale_quote_ms: i64) -> Self {
        Self {
            stale_quote_ms,
            orderbooks: HashMap::new(),
            funding: HashMap::new(),
            instruments: HashMap::new(),
            route_health: HashMap::new(),
        }
    }

    pub fn stale_quote_ms(&self) -> i64 {
        self.stale_quote_ms
    }

    pub fn upsert_orderbook(&mut self, book: OrderBook5) -> OrderBook5 {
        self.upsert_orderbook_at(book, Utc::now())
    }

    pub fn upsert_orderbook_at(
        &mut self,
        book: OrderBook5,
        checked_at: DateTime<Utc>,
    ) -> OrderBook5 {
        let key = (book.exchange.clone(), book.canonical_symbol.clone());
        let book = mark_book_freshness(book, checked_at, self.stale_quote_ms);
        self.orderbooks.insert(key, book.clone());
        book
    }

    pub fn orderbook(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&OrderBook5> {
        self.orderbooks
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn upsert_funding(&mut self, snapshot: MarketFundingSnapshot) {
        self.funding.insert(
            (snapshot.exchange.clone(), snapshot.canonical_symbol.clone()),
            snapshot,
        );
    }

    pub fn funding(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&MarketFundingSnapshot> {
        self.funding
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn upsert_instrument(&mut self, instrument: InstrumentMeta) {
        self.instruments.insert(
            (
                instrument.exchange.clone(),
                instrument.canonical_symbol.clone(),
            ),
            instrument,
        );
    }

    pub fn instrument(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&InstrumentMeta> {
        self.instruments
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn upsert_route_health(&mut self, health: RouteHealth) {
        self.route_health.insert(
            (
                health.exchange.clone(),
                health.route_type,
                health.endpoint.clone(),
            ),
            health,
        );
    }

    pub fn route_health_for_exchange(&self, exchange: &ExchangeId) -> Vec<&RouteHealth> {
        self.route_health
            .iter()
            .filter_map(|((venue, _, _), health)| (venue == exchange).then_some(health))
            .collect()
    }

    pub fn snapshots_for_symbol(
        &self,
        canonical_symbol: &CanonicalSymbol,
    ) -> Vec<MarketSymbolSnapshot> {
        let mut exchanges = HashSet::new();
        for (exchange, symbol) in self.orderbooks.keys() {
            if symbol == canonical_symbol {
                exchanges.insert(exchange.clone());
            }
        }
        for (exchange, symbol) in self.funding.keys() {
            if symbol == canonical_symbol {
                exchanges.insert(exchange.clone());
            }
        }
        for (exchange, symbol) in self.instruments.keys() {
            if symbol == canonical_symbol {
                exchanges.insert(exchange.clone());
            }
        }

        let mut snapshots = exchanges
            .into_iter()
            .map(|exchange| {
                let key = (exchange.clone(), canonical_symbol.clone());
                let orderbook = self.orderbooks.get(&key).cloned();
                let funding = self.funding.get(&key).cloned();
                let instrument = self.instruments.get(&key).cloned();
                let route_health = self
                    .route_health_for_exchange(&exchange)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let routes_allow_entries = route_health
                    .iter()
                    .any(|health: &RouteHealth| health.allows_new_entries());
                let usable_for_entries = orderbook
                    .as_ref()
                    .map(OrderBook5::is_usable)
                    .unwrap_or(false)
                    && instrument
                        .as_ref()
                        .map(InstrumentMeta::is_tradeable_usdt_perpetual)
                        .unwrap_or(false)
                    && routes_allow_entries;

                MarketSymbolSnapshot {
                    exchange,
                    canonical_symbol: canonical_symbol.clone(),
                    orderbook,
                    funding,
                    instrument,
                    route_health,
                    usable_for_entries,
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.exchange.as_str().cmp(right.exchange.as_str()));
        snapshots
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::market::{BookLevel, ContractType, ExchangeSymbol, InstrumentStatus, RouteStatus};

    fn book(sequence: Option<u64>, recv_ts: DateTime<Utc>) -> OrderBook5 {
        let exchange = ExchangeId::Binance;
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            vec![BookLevel::new(99.0, 1.0)],
            vec![BookLevel::new(100.0, 1.0)],
            recv_ts,
            recv_ts,
            sequence,
            Some("primary".to_string()),
        )
    }

    fn instrument() -> InstrumentMeta {
        InstrumentMeta::new(
            ExchangeId::Binance,
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            "BTC",
            "USDT",
            "USDT",
            ContractType::LinearPerpetual,
            1.0,
            0.1,
            0.001,
            0.001,
            5.0,
            1,
            3,
            InstrumentStatus::Trading,
        )
    }

    #[test]
    fn cache_should_update_book_and_mark_freshness_without_strict_sequence_gap() {
        let now = Utc::now();
        let mut cache = MarketStateCache::new(500);

        let first = cache.upsert_orderbook_at(book(Some(1), now), now);
        let second = cache.upsert_orderbook_at(book(Some(3), now - Duration::seconds(2)), now);

        assert!(!first.quality.sequence_gap);
        assert!(!second.quality.sequence_gap);
        assert!(second.quality.stale);
        assert!(!second.is_usable());
    }

    #[test]
    fn cache_should_build_strategy_independent_symbol_snapshots() {
        let now = Utc::now();
        let mut cache = MarketStateCache::new(500);
        let canonical = CanonicalSymbol::new("btc", "usdt");
        let mut health = RouteHealth::new(ExchangeId::Binance, RouteType::MarketWs, "primary");
        health.status = RouteStatus::Healthy;

        cache.upsert_instrument(instrument());
        cache.upsert_orderbook_at(book(Some(1), now), now);
        cache.upsert_funding(MarketFundingSnapshot::new(
            ExchangeId::Binance,
            canonical.clone(),
            Some(ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")),
            0.0001,
            None,
            now,
        ));
        cache.upsert_route_health(health);

        let snapshots = cache.snapshots_for_symbol(&canonical);

        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].orderbook.is_some());
        assert!(snapshots[0].funding.is_some());
        assert!(snapshots[0].instrument.is_some());
        assert!(snapshots[0].usable_for_entries);
    }
}
