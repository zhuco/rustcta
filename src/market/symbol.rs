use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{
    filter_tradeable_usdt_perpetuals, CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta,
};

pub fn exchange_symbol_for(
    exchange: &ExchangeId,
    canonical_symbol: &CanonicalSymbol,
) -> ExchangeSymbol {
    let symbol = match exchange {
        ExchangeId::Binance | ExchangeId::Bitget | ExchangeId::Bybit | ExchangeId::CoinEx => {
            format!("{}{}", canonical_symbol.base(), canonical_symbol.quote())
        }
        ExchangeId::Okx => format!(
            "{}-{}-SWAP",
            canonical_symbol.base(),
            canonical_symbol.quote()
        ),
        ExchangeId::Gate | ExchangeId::Mexc => {
            format!("{}_{}", canonical_symbol.base(), canonical_symbol.quote())
        }
        ExchangeId::Htx | ExchangeId::KuCoin => {
            format!("{}-{}", canonical_symbol.base(), canonical_symbol.quote())
        }
        ExchangeId::Kraken => format!("PF_{}{}", canonical_symbol.base(), canonical_symbol.quote()),
        ExchangeId::Toobit => format!(
            "{}-SWAP-{}",
            canonical_symbol.base(),
            canonical_symbol.quote()
        ),
        ExchangeId::Other(_) => canonical_symbol.as_pair(),
    };
    ExchangeSymbol::new(exchange.clone(), symbol)
}

pub fn canonical_from_exchange_symbol(
    exchange: &ExchangeId,
    exchange_symbol: &str,
) -> Option<CanonicalSymbol> {
    let value = exchange_symbol.trim().to_ascii_uppercase();
    match exchange {
        ExchangeId::Okx => parse_delimited_symbol(&value, '-'),
        ExchangeId::Gate | ExchangeId::Mexc => parse_delimited_symbol(&value, '_'),
        ExchangeId::Htx | ExchangeId::KuCoin => parse_delimited_symbol(&value, '-'),
        ExchangeId::Kraken => parse_kraken_usdt_symbol(&value),
        ExchangeId::Toobit => parse_toobit_usdt_symbol(&value),
        ExchangeId::Binance
        | ExchangeId::Bitget
        | ExchangeId::Bybit
        | ExchangeId::CoinEx
        | ExchangeId::Other(_) => parse_compact_usdt_symbol(&value)
            .or_else(|| parse_delimited_symbol(&value, '_'))
            .or_else(|| parse_delimited_symbol(&value, '-')),
    }
}

fn parse_toobit_usdt_symbol(value: &str) -> Option<CanonicalSymbol> {
    let compact = value.replace("-SWAP-", "").replace("SWAP", "");
    parse_compact_usdt_symbol(&compact).or_else(|| parse_delimited_symbol(value, '-'))
}

fn parse_kraken_usdt_symbol(value: &str) -> Option<CanonicalSymbol> {
    let compact = value
        .trim_start_matches("PF_")
        .replace(['/', '-', '_'], "")
        .replace("XBT", "BTC");
    parse_compact_usdt_symbol(&compact)
}

pub fn parse_compact_usdt_symbol(value: &str) -> Option<CanonicalSymbol> {
    let normalized = value
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "")
        .replace("PERP", "")
        .replace("SWAP", "");
    let usdt_end = normalized.find("USDT").map(|index| index + 4)?;
    let compact = &normalized[..usdt_end];
    let base = compact.strip_suffix("USDT")?;
    (!base.is_empty()).then(|| CanonicalSymbol::new(base, "USDT"))
}

fn parse_delimited_symbol(value: &str, delimiter: char) -> Option<CanonicalSymbol> {
    let mut parts = value.split(delimiter).filter(|part| !part.is_empty());
    let base = parts.next()?;
    let quote = parts.next()?;
    quote
        .eq_ignore_ascii_case("USDT")
        .then(|| CanonicalSymbol::new(base, quote))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolMapping {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub instrument: InstrumentMeta,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolMapper {
    by_canonical: HashMap<(ExchangeId, CanonicalSymbol), ExchangeSymbol>,
    by_exchange_symbol: HashMap<ExchangeSymbol, CanonicalSymbol>,
    instruments: HashMap<(ExchangeId, CanonicalSymbol), InstrumentMeta>,
}

impl SymbolMapper {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_instruments(instruments: impl IntoIterator<Item = InstrumentMeta>) -> Self {
        let mut mapper = Self::new();
        mapper.register_instruments(instruments);
        mapper
    }

    pub fn register_instruments(&mut self, instruments: impl IntoIterator<Item = InstrumentMeta>) {
        for instrument in instruments {
            self.register_instrument(instrument);
        }
    }

    pub fn register_instrument(&mut self, instrument: InstrumentMeta) {
        let key = (
            instrument.exchange.clone(),
            instrument.canonical_symbol.clone(),
        );
        self.by_exchange_symbol.insert(
            instrument.exchange_symbol.clone(),
            instrument.canonical_symbol.clone(),
        );
        self.by_canonical
            .insert(key.clone(), instrument.exchange_symbol.clone());
        self.instruments.insert(key, instrument);
    }

    pub fn to_exchange_symbol(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&ExchangeSymbol> {
        self.by_canonical
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn to_canonical_symbol(
        &self,
        exchange_symbol: &ExchangeSymbol,
    ) -> Option<&CanonicalSymbol> {
        self.by_exchange_symbol.get(exchange_symbol)
    }

    pub fn instrument(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&InstrumentMeta> {
        self.instruments
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn instruments_for_exchange(&self, exchange: &ExchangeId) -> Vec<&InstrumentMeta> {
        self.instruments
            .iter()
            .filter_map(|((venue, _), instrument)| (venue == exchange).then_some(instrument))
            .collect()
    }

    pub fn tradeable_usdt_perpetuals(&self) -> Vec<&InstrumentMeta> {
        self.instruments
            .values()
            .filter(|instrument| instrument.is_tradeable_usdt_perpetual())
            .collect()
    }

    pub fn canonical_symbols(&self) -> HashSet<CanonicalSymbol> {
        self.instruments
            .keys()
            .map(|(_, symbol)| symbol.clone())
            .collect()
    }

    pub fn mappings(&self) -> Vec<SymbolMapping> {
        self.instruments
            .values()
            .cloned()
            .map(|instrument| SymbolMapping {
                exchange: instrument.exchange.clone(),
                canonical_symbol: instrument.canonical_symbol.clone(),
                exchange_symbol: instrument.exchange_symbol.clone(),
                instrument,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolRegistry {
    mapper: SymbolMapper,
}

impl SymbolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_instruments(instruments: impl IntoIterator<Item = InstrumentMeta>) -> Self {
        Self {
            mapper: SymbolMapper::from_instruments(instruments),
        }
    }

    pub fn from_tradeable_usdt_perpetuals(
        instruments: impl IntoIterator<Item = InstrumentMeta>,
    ) -> Self {
        Self::from_instruments(filter_tradeable_usdt_perpetuals(instruments))
    }

    pub fn register_instrument(&mut self, instrument: InstrumentMeta) {
        self.mapper.register_instrument(instrument);
    }

    pub fn mapper(&self) -> &SymbolMapper {
        &self.mapper
    }

    pub fn to_exchange_symbol(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&ExchangeSymbol> {
        self.mapper.to_exchange_symbol(exchange, canonical_symbol)
    }

    pub fn to_canonical_symbol(
        &self,
        exchange_symbol: &ExchangeSymbol,
    ) -> Option<&CanonicalSymbol> {
        self.mapper.to_canonical_symbol(exchange_symbol)
    }

    pub fn tradeable_usdt_perpetuals(&self) -> Vec<&InstrumentMeta> {
        self.mapper.tradeable_usdt_perpetuals()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{ContractType, InstrumentStatus};

    fn instrument(exchange: ExchangeId, canonical: &str, exchange_symbol: &str) -> InstrumentMeta {
        let canonical_symbol = CanonicalSymbol::parse(canonical).expect("valid symbol");
        InstrumentMeta::new(
            exchange.clone(),
            canonical_symbol.clone(),
            ExchangeSymbol::new(exchange, exchange_symbol),
            canonical_symbol.base(),
            canonical_symbol.quote(),
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
    fn symbol_should_round_trip_canonical_and_exchange_symbols() {
        let registry = SymbolRegistry::from_instruments(vec![instrument(
            ExchangeId::Okx,
            "BTC/USDT",
            "BTC-USDT-SWAP",
        )]);
        let canonical = CanonicalSymbol::new("btc", "usdt");
        let exchange_symbol = ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP");

        assert_eq!(
            registry
                .to_exchange_symbol(&ExchangeId::Okx, &canonical)
                .expect("mapped"),
            &exchange_symbol
        );
        assert_eq!(
            registry
                .to_canonical_symbol(&exchange_symbol)
                .expect("mapped"),
            &canonical
        );
    }

    #[test]
    fn symbol_should_parse_common_exchange_formats() {
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Binance, "BTCUSDT").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Okx, "BTC-USDT-SWAP").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Gate, "BTC_USDT").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Bitget, "BTCUSDT_UMCBL").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Bybit, "BTCUSDT").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Mexc, "BTC_USDT").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(
            canonical_from_exchange_symbol(&ExchangeId::Htx, "BTC-USDT").unwrap(),
            CanonicalSymbol::new("BTC", "USDT")
        );
    }

    #[test]
    fn symbol_should_format_exchange_symbols() {
        let canonical = CanonicalSymbol::new("ETH", "USDT");

        assert_eq!(
            exchange_symbol_for(&ExchangeId::Binance, &canonical).symbol,
            "ETHUSDT"
        );
        assert_eq!(
            exchange_symbol_for(&ExchangeId::Okx, &canonical).symbol,
            "ETH-USDT-SWAP"
        );
        assert_eq!(
            exchange_symbol_for(&ExchangeId::Gate, &canonical).symbol,
            "ETH_USDT"
        );
        assert_eq!(
            exchange_symbol_for(&ExchangeId::Bybit, &canonical).symbol,
            "ETHUSDT"
        );
        assert_eq!(
            exchange_symbol_for(&ExchangeId::Mexc, &canonical).symbol,
            "ETH_USDT"
        );
        assert_eq!(
            exchange_symbol_for(&ExchangeId::Htx, &canonical).symbol,
            "ETH-USDT"
        );
    }
}
