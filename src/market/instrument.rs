use serde::{Deserialize, Serialize};

use super::{CanonicalSymbol, ExchangeId, ExchangeSymbol};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContractType {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    DeliveryFuture,
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentStatus {
    Trading,
    CloseOnly,
    Paused,
    Delisted,
    Unknown,
}

impl InstrumentStatus {
    pub fn allows_new_entries(self) -> bool {
        matches!(self, Self::Trading)
    }

    pub fn allows_closes(self) -> bool {
        matches!(self, Self::Trading | Self::CloseOnly)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub base: String,
    pub quote: String,
    pub settle_asset: String,
    pub contract_type: ContractType,
    pub contract_size: f64,
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_qty: f64,
    pub min_notional: f64,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub status: InstrumentStatus,
    pub supports_hedge_mode: bool,
    pub supports_reduce_only: bool,
    pub supports_post_only: bool,
    pub supports_ioc: bool,
}

impl InstrumentMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        base: impl Into<String>,
        quote: impl Into<String>,
        settle_asset: impl Into<String>,
        contract_type: ContractType,
        contract_size: f64,
        price_tick: f64,
        quantity_step: f64,
        min_qty: f64,
        min_notional: f64,
        price_precision: u32,
        quantity_precision: u32,
        status: InstrumentStatus,
    ) -> Self {
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            base: base.into().trim().to_ascii_uppercase(),
            quote: quote.into().trim().to_ascii_uppercase(),
            settle_asset: settle_asset.into().trim().to_ascii_uppercase(),
            contract_type,
            contract_size,
            price_tick,
            quantity_step,
            min_qty,
            min_notional,
            price_precision,
            quantity_precision,
            status,
            supports_hedge_mode: false,
            supports_reduce_only: true,
            supports_post_only: true,
            supports_ioc: true,
        }
    }

    pub fn with_order_capabilities(
        mut self,
        supports_hedge_mode: bool,
        supports_reduce_only: bool,
        supports_post_only: bool,
        supports_ioc: bool,
    ) -> Self {
        self.supports_hedge_mode = supports_hedge_mode;
        self.supports_reduce_only = supports_reduce_only;
        self.supports_post_only = supports_post_only;
        self.supports_ioc = supports_ioc;
        self
    }

    pub fn is_usdt_perpetual(&self) -> bool {
        self.quote == "USDT"
            && self.settle_asset == "USDT"
            && matches!(self.contract_type, ContractType::LinearPerpetual)
    }

    pub fn is_tradeable_usdt_perpetual(&self) -> bool {
        self.is_usdt_perpetual() && self.status.allows_new_entries()
    }

    pub fn has_valid_lot_rules(&self) -> bool {
        self.contract_size.is_finite()
            && self.contract_size > 0.0
            && self.price_tick.is_finite()
            && self.price_tick > 0.0
            && self.quantity_step.is_finite()
            && self.quantity_step > 0.0
            && self.min_qty.is_finite()
            && self.min_qty >= 0.0
            && self.min_notional.is_finite()
            && self.min_notional >= 0.0
    }
}

pub fn filter_tradeable_usdt_perpetuals(
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Vec<InstrumentMeta> {
    instruments
        .into_iter()
        .filter(InstrumentMeta::is_tradeable_usdt_perpetual)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument(
        exchange: ExchangeId,
        canonical: &str,
        exchange_symbol: &str,
        quote: &str,
        settle: &str,
        contract_type: ContractType,
        status: InstrumentStatus,
    ) -> InstrumentMeta {
        let canonical_symbol = CanonicalSymbol::parse(canonical).expect("valid symbol");
        InstrumentMeta::new(
            exchange.clone(),
            canonical_symbol.clone(),
            ExchangeSymbol::new(exchange, exchange_symbol),
            canonical_symbol.base(),
            quote,
            settle,
            contract_type,
            1.0,
            0.1,
            0.001,
            0.001,
            5.0,
            1,
            3,
            status,
        )
    }

    #[test]
    fn instrument_should_filter_tradeable_usdt_perpetuals() {
        let instruments = vec![
            instrument(
                ExchangeId::Binance,
                "BTC/USDT",
                "BTCUSDT",
                "USDT",
                "USDT",
                ContractType::LinearPerpetual,
                InstrumentStatus::Trading,
            ),
            instrument(
                ExchangeId::Okx,
                "BTC/USDC",
                "BTC-USDC-SWAP",
                "USDC",
                "USDC",
                ContractType::LinearPerpetual,
                InstrumentStatus::Trading,
            ),
            instrument(
                ExchangeId::Gate,
                "ETH/USDT",
                "ETH_USDT",
                "USDT",
                "USDT",
                ContractType::LinearPerpetual,
                InstrumentStatus::Paused,
            ),
        ];

        let filtered = filter_tradeable_usdt_perpetuals(instruments);

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].canonical_symbol,
            CanonicalSymbol::new("btc", "usdt")
        );
        assert!(filtered[0].has_valid_lot_rules());
    }
}
