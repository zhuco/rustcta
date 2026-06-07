//! Exchange-specific market adapter skeletons for cross-exchange arbitrage.
//!
//! This module is intentionally side-effect free. It declares capabilities,
//! naming conventions, and small parsing helpers without opening sockets,
//! calling REST APIs, or placing orders.

use crate::market::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketDataAdapter, PublicBookProfileKind,
};
use serde::{Deserialize, Serialize};

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;
pub mod htx;
pub mod kraken;
pub mod mexc;
pub mod okx;
pub mod toobit;

pub use binance::BinanceMarketAdapter;
pub use bitget::BitgetMarketAdapter;
pub use bybit::BybitMarketAdapter;
pub use gate::GateMarketAdapter;
pub use htx::HtxMarketAdapter;
pub use kraken::KrakenMarketAdapter;
pub use mexc::MexcMarketAdapter;
pub use okx::OkxMarketAdapter;
pub use toobit::ToobitMarketAdapter;

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

pub fn dashed_symbol(canonical: &CanonicalSymbol) -> String {
    format!("{}-{}", canonical.base(), canonical.quote())
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
        assert_eq!(
            BybitMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTCUSDT"
        );
        assert_eq!(
            MexcMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTC_USDT"
        );
        assert_eq!(
            HtxMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTC-USDT"
        );
        assert_eq!(
            KrakenMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "PF_XBTUSDT"
        );
        assert_eq!(
            ToobitMarketAdapter.to_exchange_symbol(&symbol).symbol,
            "BTC-SWAP-USDT"
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
        assert!(BybitMarketAdapter.supports_sequence());
        assert!(MexcMarketAdapter.supports_orderbook5());
        assert!(HtxMarketAdapter.supports_funding());
        assert!(KrakenMarketAdapter.supports_funding());
        assert!(ToobitMarketAdapter.supports_mark_price());
        assert!(!ToobitMarketAdapter.supports_sequence());
        assert!(ToobitMarketAdapter.supports_funding());
    }

    #[test]
    fn public_book_profile_should_select_expected_fastest_channels() {
        let compact = vec![ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")];
        let dashed_swap = vec![ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP")];
        let underscored = vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")];
        let htx = vec![ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")];
        let kraken = vec![ExchangeSymbol::new(ExchangeId::Kraken, "PF_XBTUSDT")];
        let toobit = vec![ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT")];

        assert_profile(
            BinanceMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestL1,
            ),
            PublicBookProfileKind::FastestL1,
            "bookTicker",
            1,
        );
        assert_profile(
            BinanceMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "depth5@100ms",
            5,
        );
        assert_profile(
            ToobitMarketAdapter.build_public_ws_subscriptions_for_profile(
                &toobit,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "depth",
            20,
        );
        assert_profile(
            OkxMarketAdapter.build_public_ws_subscriptions_for_profile(
                &dashed_swap,
                PublicBookProfileKind::FastestL1,
            ),
            PublicBookProfileKind::FastestL1,
            "bbo-tbt",
            1,
        );
        assert_profile(
            OkxMarketAdapter.build_public_ws_subscriptions_for_profile(
                &dashed_swap,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "books5",
            5,
        );
        assert_profile(
            BitgetMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestL1,
            ),
            PublicBookProfileKind::FastestL1,
            "books1",
            1,
        );
        assert_profile(
            BitgetMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "books5",
            5,
        );
        assert_profile(
            BybitMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestL1,
            ),
            PublicBookProfileKind::FastestL1,
            "orderbook.1",
            1,
        );
        assert_profile(
            BybitMarketAdapter.build_public_ws_subscriptions_for_profile(
                &compact,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "orderbook.50",
            50,
        );
        assert_profile(
            GateMarketAdapter.build_public_ws_subscriptions_for_profile(
                &underscored,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "futures.order_book_update",
            20,
        );
        assert_profile(
            MexcMarketAdapter.build_public_ws_subscriptions_for_profile(
                &underscored,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "sub.depth.full",
            5,
        );
        assert_profile(
            HtxMarketAdapter.build_public_ws_subscriptions_for_profile(
                &htx,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "depth.size_20.high_freq",
            20,
        );
        assert_profile(
            KrakenMarketAdapter.build_public_ws_subscriptions_for_profile(
                &kraken,
                PublicBookProfileKind::FastestDepth,
            ),
            PublicBookProfileKind::FastestDepth,
            "book",
            10,
        );
    }

    fn assert_profile(
        subscriptions: Vec<crate::market::WsSubscription>,
        profile: PublicBookProfileKind,
        channel: &str,
        depth: u16,
    ) {
        assert!(!subscriptions.is_empty());
        assert_eq!(subscriptions[0].profile, profile);
        assert_eq!(subscriptions[0].channel, channel);
        assert_eq!(subscriptions[0].depth, depth);
    }
}
