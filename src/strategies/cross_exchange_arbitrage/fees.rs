//! Fee model for maker/taker entry, close, and emergency close estimates.

use super::config::FeeConfig;
use crate::market::ExchangeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeFeeRates {
    pub maker: f64,
    pub taker: f64,
}

impl ExchangeFeeRates {
    pub fn rate(self, role: FeeRole) -> f64 {
        match role {
            FeeRole::Maker => self.maker,
            FeeRole::Taker => self.taker,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub maker_entry_fee: f64,
    pub taker_hedge_fee: f64,
    pub maker_close_fee: f64,
    pub taker_close_fee: f64,
    pub emergency_close_fee: f64,
}

impl FeeBreakdown {
    pub fn open_fee(&self) -> f64 {
        self.maker_entry_fee + self.taker_hedge_fee
    }

    pub fn normal_close_fee(&self) -> f64 {
        self.maker_close_fee + self.taker_close_fee
    }

    pub fn total_normal_fee(&self) -> f64 {
        self.open_fee() + self.normal_close_fee()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    default: ExchangeFeeRates,
    per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
}

impl FeeModel {
    pub fn new(
        default: ExchangeFeeRates,
        per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
    ) -> Self {
        Self {
            default,
            per_exchange,
        }
    }

    pub fn from_config(config: &FeeConfig) -> Self {
        Self {
            default: ExchangeFeeRates {
                maker: config.default_maker_fee_rate,
                taker: config.default_taker_fee_rate,
            },
            per_exchange: config.per_exchange.clone(),
        }
    }

    pub fn rates_for(&self, exchange: &ExchangeId) -> ExchangeFeeRates {
        self.per_exchange
            .get(exchange)
            .copied()
            .unwrap_or(self.default)
    }

    pub fn rate(&self, exchange: &ExchangeId, role: FeeRole) -> f64 {
        self.rates_for(exchange).rate(role)
    }

    pub fn fee_amount(&self, exchange: &ExchangeId, role: FeeRole, notional_usdt: f64) -> f64 {
        notional_usdt * self.rate(exchange, role)
    }

    pub fn estimate_maker_taker_round_trip(
        &self,
        maker_exchange: &ExchangeId,
        taker_exchange: &ExchangeId,
        notional_usdt: f64,
    ) -> FeeBreakdown {
        FeeBreakdown {
            maker_entry_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_hedge_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            maker_close_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_close_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            emergency_close_fee: self.fee_amount(maker_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
        }
    }
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::from_config(&FeeConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_exchange_arbitrage_fee_model_should_allow_negative_maker_fee() {
        let mut rates = HashMap::new();
        rates.insert(
            ExchangeId::Binance,
            ExchangeFeeRates {
                maker: -0.0001,
                taker: 0.0005,
            },
        );
        let model = FeeModel::new(
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            rates,
        );

        assert_eq!(
            model.fee_amount(&ExchangeId::Binance, FeeRole::Maker, 100.0),
            -0.01
        );
    }
}
