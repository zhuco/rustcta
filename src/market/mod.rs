//! Shared market-data contracts for cross-exchange arbitrage.
//!
//! Phase 0 keeps these types pure and side-effect free so exchange adapters,
//! strategy code, and execution code can compile against the same contract.

pub mod adapter;
pub mod cache;
pub mod event;
pub mod funding;
pub mod health;
pub mod instrument;
pub mod orderbook;
pub mod precision;
pub mod registry;
pub mod routing;
pub mod symbol;
pub mod trade;
pub mod vwap;
pub mod ws_supervisor;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub use adapter::*;
pub use cache::*;
pub use event::*;
pub use funding::*;
pub use health::*;
pub use instrument::*;
pub use orderbook::*;
pub use precision::*;
pub use registry::*;
pub use routing::*;
pub use symbol::*;
pub use trade::*;
pub use vwap::*;
pub use ws_supervisor::*;

pub const ORDER_BOOK5_DEPTH: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExchangeId {
    Binance,
    Okx,
    Bitget,
    Gate,
    Other(String),
}

impl Serialize for ExchangeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ExchangeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value.as_str()))
    }
}

impl ExchangeId {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Binance => "binance",
            Self::Okx => "okx",
            Self::Bitget => "bitget",
            Self::Gate => "gate",
            Self::Other(exchange) => exchange.as_str(),
        }
    }
}

impl From<&str> for ExchangeId {
    fn from(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance" => Self::Binance,
            "okx" => Self::Okx,
            "bitget" => Self::Bitget,
            "gate" | "gateio" | "gate.io" => Self::Gate,
            other => Self::Other(other.to_string()),
        }
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalSymbol {
    base: String,
    quote: String,
}

impl Serialize for CanonicalSymbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_pair())
    }
}

impl<'de> Deserialize<'de> for CanonicalSymbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid canonical symbol: {value}")))
    }
}

impl CanonicalSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into().trim().to_ascii_uppercase(),
            quote: quote.into().trim().to_ascii_uppercase(),
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let (base, quote) = value.split_once('/')?;
        let symbol = Self::new(base, quote);
        if symbol.base.is_empty() || symbol.quote.is_empty() {
            return None;
        }
        Some(symbol)
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn quote(&self) -> &str {
        &self.quote
    }

    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

impl fmt::Display for CanonicalSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExchangeSymbol {
    pub exchange: ExchangeId,
    pub symbol: String,
}

impl ExchangeSymbol {
    pub fn new(exchange: ExchangeId, symbol: impl Into<String>) -> Self {
        Self {
            exchange,
            symbol: symbol.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl BookLevel {
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }

    pub fn is_valid(&self) -> bool {
        self.price.is_finite()
            && self.quantity.is_finite()
            && self.price > 0.0
            && self.quantity > 0.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookQuality {
    pub has_bids: bool,
    pub has_asks: bool,
    pub crossed: bool,
    pub has_invalid_levels: bool,
    pub stale: bool,
    pub sequence_gap: bool,
}

impl BookQuality {
    pub fn healthy() -> Self {
        Self {
            has_bids: true,
            has_asks: true,
            crossed: false,
            has_invalid_levels: false,
            stale: false,
            sequence_gap: false,
        }
    }

    pub fn is_usable(&self) -> bool {
        self.has_bids
            && self.has_asks
            && !self.crossed
            && !self.has_invalid_levels
            && !self.stale
            && !self.sequence_gap
    }
}

impl Default for BookQuality {
    fn default() -> Self {
        Self::healthy()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteStatus {
    Healthy,
    Degraded,
    CloseOnly,
    Offline,
}

impl RouteStatus {
    pub fn allows_new_entries(self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn allows_closes(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded | Self::CloseOnly)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Observe,
    Simulation,
    Shadow,
    LiveSmall,
    LiveScaled,
}

impl RuntimeMode {
    pub fn allows_live_orders(self) -> bool {
        matches!(self, Self::LiveSmall | Self::LiveScaled)
    }
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::Simulation
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBook5 {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub exchange_ts: DateTime<Utc>,
    pub recv_ts: DateTime<Utc>,
    pub sequence: Option<u64>,
    pub source_route: Option<String>,
    pub quality: BookQuality,
}

impl OrderBook5 {
    pub fn new(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        exchange_ts: DateTime<Utc>,
        recv_ts: DateTime<Utc>,
        sequence: Option<u64>,
        source_route: Option<String>,
    ) -> Self {
        let bids = bids.into_iter().take(ORDER_BOOK5_DEPTH).collect::<Vec<_>>();
        let asks = asks.into_iter().take(ORDER_BOOK5_DEPTH).collect::<Vec<_>>();
        let quality = Self::evaluate_quality(&bids, &asks);

        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            bids,
            asks,
            exchange_ts,
            recv_ts,
            sequence,
            source_route,
            quality,
        }
    }

    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids.first().copied()
    }

    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks.first().copied()
    }

    pub fn is_usable(&self) -> bool {
        self.quality.is_usable()
    }

    pub fn mark_stale(mut self) -> Self {
        self.quality.stale = true;
        self
    }

    fn evaluate_quality(bids: &[BookLevel], asks: &[BookLevel]) -> BookQuality {
        let has_bids = !bids.is_empty();
        let has_asks = !asks.is_empty();
        let has_invalid_levels = bids
            .iter()
            .chain(asks.iter())
            .any(|level| !level.is_valid());
        let crossed = match (bids.first(), asks.first()) {
            (Some(best_bid), Some(best_ask)) => best_bid.price >= best_ask.price,
            _ => false,
        };

        BookQuality {
            has_bids,
            has_asks,
            crossed,
            has_invalid_levels,
            stale: false,
            sequence_gap: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_book(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBook5 {
        let exchange = ExchangeId::Binance;
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            bids,
            asks,
            Utc::now(),
            Utc::now(),
            Some(1),
            Some("primary".to_string()),
        )
    }

    #[test]
    fn market_contracts_should_parse_canonical_symbol() {
        let symbol = CanonicalSymbol::parse("btc/usdt").expect("valid canonical symbol");

        assert_eq!(symbol.base(), "BTC");
        assert_eq!(symbol.quote(), "USDT");
        assert_eq!(symbol.to_string(), "BTC/USDT");
        assert!(CanonicalSymbol::parse("BTCUSDT").is_none());
    }

    #[test]
    fn market_contracts_should_reject_crossed_orderbook() {
        let book = test_book(
            vec![BookLevel::new(101.0, 1.0)],
            vec![BookLevel::new(100.0, 1.0)],
        );

        assert!(book.quality.crossed);
        assert!(!book.is_usable());
    }

    #[test]
    fn market_contracts_should_reject_invalid_levels() {
        let book = test_book(
            vec![BookLevel::new(99.0, 0.0)],
            vec![BookLevel::new(100.0, 1.0)],
        );

        assert!(book.quality.has_invalid_levels);
        assert!(!book.is_usable());
    }

    #[test]
    fn market_contracts_should_keep_top_five_levels() {
        let bids = (0..10)
            .map(|i| BookLevel::new(100.0 - i as f64, 1.0))
            .collect::<Vec<_>>();
        let asks = (0..10)
            .map(|i| BookLevel::new(101.0 + i as f64, 1.0))
            .collect::<Vec<_>>();
        let book = test_book(bids, asks);

        assert_eq!(book.bids.len(), ORDER_BOOK5_DEPTH);
        assert_eq!(book.asks.len(), ORDER_BOOK5_DEPTH);
        assert!(book.is_usable());
    }

    #[test]
    fn market_contracts_should_encode_route_and_runtime_safety() {
        assert!(RouteStatus::Healthy.allows_new_entries());
        assert!(!RouteStatus::Degraded.allows_new_entries());
        assert!(RouteStatus::CloseOnly.allows_closes());
        assert!(!RouteStatus::Offline.allows_closes());

        assert_eq!(RuntimeMode::default(), RuntimeMode::Simulation);
        assert!(!RuntimeMode::Shadow.allows_live_orders());
        assert!(RuntimeMode::LiveSmall.allows_live_orders());
    }
}
