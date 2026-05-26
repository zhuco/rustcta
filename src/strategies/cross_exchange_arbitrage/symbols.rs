//! Symbol helpers for strategy-level universe normalization.

use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolMapping {
    pub canonical: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
}

pub fn parse_usdt_perp_symbol(value: &str) -> Option<CanonicalSymbol> {
    let normalized = value
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "")
        .replace('/', "");
    let base = normalized.strip_suffix("USDT")?;
    if base.is_empty() {
        return None;
    }
    Some(CanonicalSymbol::new(base, "USDT"))
}

pub fn exchange_symbol(exchange: ExchangeId, canonical: &CanonicalSymbol) -> ExchangeSymbol {
    let symbol = match exchange {
        ExchangeId::Binance => format!("{}{}", canonical.base(), canonical.quote()),
        ExchangeId::Okx => format!("{}-{}-SWAP", canonical.base(), canonical.quote()),
        ExchangeId::Bitget => format!("{}{}UMCBL", canonical.base(), canonical.quote()),
        ExchangeId::Gate => format!("{}_{}", canonical.base(), canonical.quote()),
        ExchangeId::Other(_) => canonical.as_pair(),
    };
    ExchangeSymbol::new(exchange, symbol)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_exchange_arbitrage_symbols_should_parse_usdt_perp() {
        assert_eq!(
            parse_usdt_perp_symbol("wif_usdt").unwrap(),
            CanonicalSymbol::new("WIF", "USDT")
        );
        assert!(parse_usdt_perp_symbol("BTCUSDC").is_none());
    }
}
