use serde::{Deserialize, Serialize};

use super::InstrumentMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoundingMode {
    Floor,
    Ceil,
    Nearest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrecisionViolation {
    InvalidQuantity,
    InvalidPrice,
    BelowMinQuantity,
    BelowMinNotional,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedOrderInput {
    pub quantity: f64,
    pub price: Option<f64>,
    pub notional: Option<f64>,
    pub violations: Vec<PrecisionViolation>,
}

impl NormalizedOrderInput {
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }
}

pub fn round_to_step(value: f64, step: f64, mode: RoundingMode) -> f64 {
    if !value.is_finite() || !step.is_finite() || step <= 0.0 {
        return value;
    }

    let scaled = value / step;
    let rounded = match mode {
        RoundingMode::Floor => scaled.floor(),
        RoundingMode::Ceil => scaled.ceil(),
        RoundingMode::Nearest => scaled.round(),
    };
    normalize_float(rounded * step)
}

pub fn decimal_places(step: f64) -> u32 {
    if !step.is_finite() || step <= 0.0 {
        return 0;
    }

    let text = format!("{step:.12}");
    text.trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
}

impl InstrumentMeta {
    pub fn quantize_price(&self, price: f64, mode: RoundingMode) -> f64 {
        round_to_step(price, self.price_tick, mode)
    }

    pub fn quantize_quantity(&self, quantity: f64, mode: RoundingMode) -> f64 {
        round_to_step(quantity, self.quantity_step, mode)
    }

    pub fn normalize_order_input(
        &self,
        quantity: f64,
        price: Option<f64>,
        quantity_mode: RoundingMode,
        price_mode: RoundingMode,
    ) -> NormalizedOrderInput {
        let quantity = self.quantize_quantity(quantity, quantity_mode);
        let price = price.map(|price| self.quantize_price(price, price_mode));
        let contract_size = if self.contract_size.is_finite() && self.contract_size > 0.0 {
            self.contract_size
        } else {
            1.0
        };
        let notional = price.map(|price| price * quantity * contract_size);
        let mut violations = Vec::new();

        if !quantity.is_finite() || quantity <= 0.0 {
            violations.push(PrecisionViolation::InvalidQuantity);
        }
        if price
            .map(|price| !price.is_finite() || price <= 0.0)
            .unwrap_or(false)
        {
            violations.push(PrecisionViolation::InvalidPrice);
        }
        if self.min_qty > 0.0 && quantity < self.min_qty {
            violations.push(PrecisionViolation::BelowMinQuantity);
        }
        if let Some(notional) = notional {
            if self.min_notional > 0.0 && notional < self.min_notional {
                violations.push(PrecisionViolation::BelowMinNotional);
            }
        }

        NormalizedOrderInput {
            quantity,
            price,
            notional,
            violations,
        }
    }
}

fn normalize_float(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let text = format!("{value:.12}");
    text.trim_end_matches('0')
        .trim_end_matches('.')
        .parse()
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{
        CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentStatus,
    };

    fn instrument() -> InstrumentMeta {
        InstrumentMeta::new(
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
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
    fn precision_should_round_to_exchange_steps() {
        assert_eq!(round_to_step(1.2349, 0.01, RoundingMode::Floor), 1.23);
        assert_eq!(round_to_step(1.2301, 0.01, RoundingMode::Ceil), 1.24);
        assert_eq!(round_to_step(1.235, 0.01, RoundingMode::Nearest), 1.24);
    }

    #[test]
    fn precision_should_normalize_order_input_and_validate_min_notional() {
        let normalized = instrument().normalize_order_input(
            0.00149,
            Some(65000.04),
            RoundingMode::Floor,
            RoundingMode::Nearest,
        );

        assert_eq!(normalized.quantity, 0.001);
        assert_eq!(normalized.price, Some(65000.0));
        assert!(normalized.is_valid());
    }
}
