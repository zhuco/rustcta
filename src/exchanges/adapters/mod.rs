//! Exchange-specific market adapter skeletons for cross-exchange arbitrage.
//!
//! This module is intentionally side-effect free. It declares capabilities,
//! naming conventions, and small parsing helpers without opening sockets,
//! calling REST APIs, or placing orders.

use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};
use serde::{Deserialize, Serialize};

pub mod binance_market;
pub mod bitget_market;
pub mod gate_market;
pub mod okx_market;
pub mod private_perp;
pub mod trading;

pub use binance_market::BinanceMarketAdapter;
pub use bitget_market::BitgetMarketAdapter;
pub use gate_market::GateMarketAdapter;
pub use okx_market::OkxMarketAdapter;
pub use private_perp::*;
pub use trading::{private_trading_support_for, ExchangeTradingAdapter, PrivateTradingSupport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCapabilities {
    pub supports_orderbook5: bool,
    pub supports_trades: bool,
    pub supports_funding: bool,
    pub supports_mark_price: bool,
    pub supports_sequence: bool,
}

impl MarketCapabilities {
    pub const fn new(
        supports_orderbook5: bool,
        supports_trades: bool,
        supports_funding: bool,
        supports_mark_price: bool,
        supports_sequence: bool,
    ) -> Self {
        Self {
            supports_orderbook5,
            supports_trades,
            supports_funding,
            supports_mark_price,
            supports_sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExchangeMarketAdapterInfo {
    pub exchange: ExchangeId,
    pub name: &'static str,
    pub venue_symbol_example: &'static str,
    pub capabilities: MarketCapabilities,
    pub protocol_notes: &'static [&'static str],
}

pub trait MarketAdapterInfo {
    fn info(&self) -> ExchangeMarketAdapterInfo;

    fn exchange(&self) -> ExchangeId {
        self.info().exchange
    }

    fn capabilities(&self) -> MarketCapabilities {
        self.info().capabilities
    }

    fn supports_orderbook5(&self) -> bool {
        self.capabilities().supports_orderbook5
    }

    fn supports_trades(&self) -> bool {
        self.capabilities().supports_trades
    }

    fn supports_funding(&self) -> bool {
        self.capabilities().supports_funding
    }

    fn supports_mark_price(&self) -> bool {
        self.capabilities().supports_mark_price
    }

    fn supports_sequence(&self) -> bool {
        self.capabilities().supports_sequence
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol;
}

pub fn compact_usdt_symbol(canonical: &CanonicalSymbol) -> String {
    format!("{}{}", canonical.base(), canonical.quote())
}

pub fn dashed_swap_symbol(canonical: &CanonicalSymbol) -> String {
    format!("{}-{}-SWAP", canonical.base(), canonical.quote())
}

pub fn underscored_symbol(canonical: &CanonicalSymbol) -> String {
    format!("{}_{}", canonical.base(), canonical.quote())
}

pub fn compact_symbol_to_canonical(symbol: &str) -> Option<CanonicalSymbol> {
    let normalized = symbol
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "")
        .replace("SWAP", "");
    let base = normalized.strip_suffix("USDT")?;
    (!base.is_empty()).then(|| CanonicalSymbol::new(base, "USDT"))
}

pub fn okx_symbol_to_canonical(symbol: &str) -> Option<CanonicalSymbol> {
    let mut parts = symbol.split('-');
    let base = parts.next()?;
    let quote = parts.next()?;
    (quote.eq_ignore_ascii_case("USDT")).then(|| CanonicalSymbol::new(base, quote))
}

pub fn parse_level_pair(level: &[serde_json::Value]) -> Option<crate::market::BookLevel> {
    let price = level.first().and_then(parse_json_f64)?;
    let quantity = level.get(1).and_then(parse_json_f64)?;
    Some(crate::market::BookLevel::new(price, quantity))
}

pub fn parse_json_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

pub fn parse_json_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

pub fn datetime_from_millis(
    ms: Option<u64>,
    fallback: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    ms.and_then(|value| chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value as i64))
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btc_usdt() -> CanonicalSymbol {
        CanonicalSymbol::new("btc", "usdt")
    }

    #[test]
    fn adapters_should_normalize_symbols_for_supported_exchanges() {
        let symbol = btc_usdt();

        assert_eq!(
            BinanceMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTCUSDT"
        );
        assert_eq!(
            OkxMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTC-USDT-SWAP"
        );
        assert_eq!(
            BitgetMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTCUSDT"
        );
        assert_eq!(
            GateMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTC_USDT"
        );
    }

    #[test]
    fn adapters_should_expose_expected_capabilities() {
        assert!(BinanceMarketAdapter.supports_orderbook5());
        assert!(BinanceMarketAdapter.supports_sequence());
        assert!(OkxMarketAdapter.supports_orderbook5());
        assert!(!OkxMarketAdapter.supports_sequence());
        assert!(BitgetMarketAdapter.supports_funding());
        assert!(!BitgetMarketAdapter.supports_sequence());
        assert!(GateMarketAdapter.supports_mark_price());
        assert!(!GateMarketAdapter.supports_sequence());
    }
}
