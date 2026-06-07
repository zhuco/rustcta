//! Per-exchange notional sizing and precision normalization.

use crate::market::{InstrumentMeta, NormalizedOrderInput, RoundingMode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeLegSize {
    pub target_notional_usdt: f64,
    pub reference_price: f64,
    pub raw_quantity: f64,
    pub raw_base_quantity: f64,
    pub normalized_quantity: f64,
    pub normalized_base_quantity: f64,
    pub normalized_price: Option<f64>,
    pub normalized_notional_usdt: Option<f64>,
    pub min_notional_usdt: f64,
    pub valid: bool,
    pub violations: Vec<String>,
}

impl ExchangeLegSize {
    pub fn from_normalized(
        target_notional_usdt: f64,
        reference_price: f64,
        raw_quantity: f64,
        raw_base_quantity: f64,
        normalized_base_quantity: f64,
        min_notional_usdt: f64,
        normalized: NormalizedOrderInput,
    ) -> Self {
        Self {
            target_notional_usdt,
            reference_price,
            raw_quantity,
            raw_base_quantity,
            normalized_quantity: normalized.quantity,
            normalized_base_quantity,
            normalized_price: normalized.price,
            normalized_notional_usdt: normalized.notional,
            min_notional_usdt,
            valid: normalized.is_valid(),
            violations: normalized
                .violations
                .into_iter()
                .map(|violation| format!("{violation:?}"))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SizedHedgePair {
    pub maker: ExchangeLegSize,
    pub taker: ExchangeLegSize,
    pub executable: bool,
    pub reject_reasons: Vec<String>,
}

pub fn size_exchange_leg_for_notional(
    instrument: &InstrumentMeta,
    target_notional_usdt: f64,
    reference_price: f64,
    price: Option<f64>,
) -> ExchangeLegSize {
    if target_notional_usdt <= 0.0 || !target_notional_usdt.is_finite() {
        return invalid_leg(
            target_notional_usdt,
            reference_price,
            "InvalidTargetNotional",
        );
    }
    if reference_price <= 0.0 || !reference_price.is_finite() {
        return invalid_leg(
            target_notional_usdt,
            reference_price,
            "InvalidReferencePrice",
        );
    }

    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let raw_base_quantity = target_notional_usdt / reference_price;
    let raw_quantity = raw_base_quantity / contract_size;
    let validation_price = Some(price.unwrap_or(reference_price));
    let mut normalized = instrument.normalize_order_input(
        raw_quantity,
        validation_price,
        RoundingMode::Floor,
        RoundingMode::Nearest,
    );
    if !normalized.is_valid() && instrument.min_notional <= target_notional_usdt {
        let min_qty_candidate = raw_quantity.max(instrument.min_qty);
        normalized = instrument.normalize_order_input(
            min_qty_candidate,
            validation_price,
            RoundingMode::Ceil,
            RoundingMode::Nearest,
        );
    }
    if price.is_none() {
        normalized.price = None;
    }

    ExchangeLegSize::from_normalized(
        target_notional_usdt,
        reference_price,
        raw_quantity,
        raw_base_quantity,
        normalized.quantity * contract_size,
        instrument.min_notional,
        normalized,
    )
}

pub fn size_exchange_leg_for_base_quantity(
    instrument: &InstrumentMeta,
    target_notional_usdt: f64,
    reference_price: f64,
    base_quantity: f64,
    price: Option<f64>,
) -> ExchangeLegSize {
    if target_notional_usdt <= 0.0 || !target_notional_usdt.is_finite() {
        return invalid_leg(
            target_notional_usdt,
            reference_price,
            "InvalidTargetNotional",
        );
    }
    if reference_price <= 0.0 || !reference_price.is_finite() {
        return invalid_leg(
            target_notional_usdt,
            reference_price,
            "InvalidReferencePrice",
        );
    }
    if base_quantity <= 0.0 || !base_quantity.is_finite() {
        return invalid_leg(target_notional_usdt, reference_price, "InvalidBaseQuantity");
    }

    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let raw_quantity = base_quantity / contract_size;
    let validation_price = Some(price.unwrap_or(reference_price));
    let mut normalized = instrument.normalize_order_input(
        raw_quantity,
        validation_price,
        RoundingMode::Floor,
        RoundingMode::Nearest,
    );
    if price.is_none() {
        normalized.price = None;
    }

    ExchangeLegSize::from_normalized(
        target_notional_usdt,
        reference_price,
        raw_quantity,
        base_quantity,
        normalized.quantity * contract_size,
        instrument.min_notional,
        normalized,
    )
}

pub fn size_hedge_pair_for_notional(
    maker_instrument: &InstrumentMeta,
    maker_reference_price: f64,
    maker_limit_price: Option<f64>,
    taker_instrument: &InstrumentMeta,
    taker_reference_price: f64,
    target_notional_usdt: f64,
) -> SizedHedgePair {
    let maker = size_exchange_leg_for_notional(
        maker_instrument,
        target_notional_usdt,
        maker_reference_price,
        maker_limit_price,
    );
    let taker = size_exchange_leg_for_notional(
        taker_instrument,
        target_notional_usdt,
        taker_reference_price,
        None,
    );
    if maker.valid && taker.valid {
        let common_base_quantity = maker
            .normalized_base_quantity
            .min(taker.normalized_base_quantity)
            .max(min_valid_base_quantity(
                maker_instrument,
                maker_reference_price,
                maker_limit_price,
            ))
            .max(min_valid_base_quantity(
                taker_instrument,
                taker_reference_price,
                None,
            ));
        let common_base_quantity = common_base_quantity_on_both_quantity_grids(
            maker_instrument,
            taker_instrument,
            common_base_quantity,
        );
        let maker = size_exchange_leg_for_base_quantity(
            maker_instrument,
            target_notional_usdt,
            maker_reference_price,
            common_base_quantity,
            maker_limit_price,
        );
        let taker = size_exchange_leg_for_base_quantity(
            taker_instrument,
            target_notional_usdt,
            taker_reference_price,
            common_base_quantity,
            None,
        );
        return sized_pair_from_legs(maker, taker);
    }

    sized_pair_from_legs(maker, taker)
}

fn sized_pair_from_legs(maker: ExchangeLegSize, taker: ExchangeLegSize) -> SizedHedgePair {
    let mut reject_reasons = Vec::new();

    if !maker.valid {
        reject_reasons.push("MakerLegPrecisionInvalid".to_string());
    }
    if !taker.valid {
        reject_reasons.push("TakerLegPrecisionInvalid".to_string());
    }

    SizedHedgePair {
        maker,
        taker,
        executable: reject_reasons.is_empty(),
        reject_reasons,
    }
}

fn common_base_quantity_on_both_quantity_grids(
    maker_instrument: &InstrumentMeta,
    taker_instrument: &InstrumentMeta,
    base_quantity: f64,
) -> f64 {
    ceil_base_quantity(maker_instrument, base_quantity)
        .max(ceil_base_quantity(taker_instrument, base_quantity))
}

fn ceil_base_quantity(instrument: &InstrumentMeta, base_quantity: f64) -> f64 {
    if base_quantity <= 0.0 || !base_quantity.is_finite() {
        return base_quantity;
    }
    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let raw_quantity = base_quantity / contract_size;
    instrument.quantize_quantity(raw_quantity, RoundingMode::Ceil) * contract_size
}

fn min_valid_base_quantity(
    instrument: &InstrumentMeta,
    reference_price: f64,
    price: Option<f64>,
) -> f64 {
    let validation_price = price.unwrap_or(reference_price);
    if validation_price <= 0.0 || !validation_price.is_finite() {
        return 0.0;
    }

    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let min_base_from_qty = if instrument.min_qty.is_finite() && instrument.min_qty > 0.0 {
        instrument.min_qty * contract_size
    } else {
        0.0
    };
    let min_base_from_notional =
        if instrument.min_notional.is_finite() && instrument.min_notional > 0.0 {
            instrument.min_notional / validation_price
        } else {
            0.0
        };
    let min_raw_quantity = min_base_from_qty.max(min_base_from_notional) / contract_size;
    if min_raw_quantity <= 0.0 || !min_raw_quantity.is_finite() {
        return 0.0;
    }

    instrument.quantize_quantity(min_raw_quantity, RoundingMode::Ceil) * contract_size
}

fn invalid_leg(
    target_notional_usdt: f64,
    reference_price: f64,
    violation: impl Into<String>,
) -> ExchangeLegSize {
    ExchangeLegSize {
        target_notional_usdt,
        reference_price,
        raw_quantity: 0.0,
        raw_base_quantity: 0.0,
        normalized_quantity: 0.0,
        normalized_base_quantity: 0.0,
        normalized_price: None,
        normalized_notional_usdt: None,
        min_notional_usdt: 0.0,
        valid: false,
        violations: vec![violation.into()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{
        CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentStatus,
    };

    fn instrument(
        exchange: ExchangeId,
        exchange_symbol: &str,
        contract_size: f64,
        price_tick: f64,
        quantity_step: f64,
        min_qty: f64,
        min_notional: f64,
    ) -> InstrumentMeta {
        InstrumentMeta::new(
            exchange.clone(),
            CanonicalSymbol::new("ARB", "USDT"),
            ExchangeSymbol::new(exchange, exchange_symbol),
            "ARB",
            "USDT",
            "USDT",
            ContractType::LinearPerpetual,
            contract_size,
            price_tick,
            quantity_step,
            min_qty,
            min_notional,
            5,
            3,
            InstrumentStatus::Trading,
        )
    }

    #[test]
    fn sizing_should_convert_each_exchange_leg_to_about_100_usdt() {
        let binance = instrument(ExchangeId::Binance, "ARBUSDT", 1.0, 0.0001, 0.1, 0.1, 5.0);
        let okx = instrument(
            ExchangeId::Okx,
            "ARB-USDT-SWAP",
            10.0,
            0.00001,
            0.1,
            0.1,
            5.0,
        );

        let pair =
            size_hedge_pair_for_notional(&binance, 0.1081, Some(0.1081), &okx, 0.1080, 100.0);

        assert!(pair.executable, "{:?}", pair.reject_reasons);
        assert_eq!(pair.maker.normalized_quantity, 925.0);
        assert_eq!(pair.maker.normalized_base_quantity, 925.0);
        assert_eq!(pair.maker.normalized_price, Some(0.1081));
        assert!(pair.maker.normalized_notional_usdt.unwrap() <= 100.0);
        assert!(pair.maker.normalized_notional_usdt.unwrap() >= 99.0);
        assert_eq!(pair.taker.normalized_quantity, 92.5);
        assert_eq!(pair.taker.normalized_base_quantity, 925.0);
        assert!(pair.taker.normalized_notional_usdt.unwrap() >= 99.0);
    }

    #[test]
    fn sizing_should_expose_base_quantity_for_execution() {
        let gate = instrument(ExchangeId::Gate, "SPCX_USDT", 0.01, 0.01, 1.0, 1.0, 0.0);
        let leg = size_exchange_leg_for_notional(&gate, 10.0, 192.15, Some(192.15));

        assert!(leg.valid, "{:?}", leg.violations);
        assert_eq!(leg.normalized_quantity, 5.0);
        assert!((leg.normalized_base_quantity - 0.05).abs() < 1e-12);
        assert!(leg.normalized_notional_usdt.unwrap() <= 10.0);
    }

    #[test]
    fn sizing_should_use_common_base_quantity_for_maker_taker_pair() {
        let bitget = instrument(ExchangeId::Bitget, "SMCIUSDT", 1.0, 0.01, 0.01, 0.01, 5.0);
        let gate = instrument(ExchangeId::Gate, "SMCI_USDT", 1.0, 0.01, 0.1, 0.1, 0.0);

        let pair = size_hedge_pair_for_notional(&bitget, 46.622, Some(46.622), &gate, 47.11, 10.0);

        assert!(pair.executable, "{:?}", pair.reject_reasons);
        assert_eq!(pair.maker.normalized_base_quantity, 0.2);
        assert_eq!(pair.taker.normalized_base_quantity, 0.2);
    }

    #[test]
    fn sizing_should_raise_common_base_quantity_to_exchange_min_notional() {
        let bitget = instrument(ExchangeId::Bitget, "SMCIUSDT", 1.0, 0.01, 0.01, 0.01, 5.0);
        let gate = instrument(ExchangeId::Gate, "SMCI_USDT", 1.0, 0.01, 0.1, 0.1, 0.0);

        let pair = size_hedge_pair_for_notional(&bitget, 46.622, Some(46.622), &gate, 47.11, 6.0);

        assert!(pair.executable, "{:?}", pair.reject_reasons);
        assert_eq!(pair.maker.normalized_base_quantity, 0.2);
        assert_eq!(pair.taker.normalized_base_quantity, 0.2);
        assert!(pair.maker.normalized_notional_usdt.unwrap() >= 5.0);
        assert!(pair.taker.normalized_notional_usdt.unwrap() >= 5.0);
    }

    #[test]
    fn sizing_should_keep_five_usdt_leg_valid_when_flooring_would_drop_below_min_notional() {
        let bitget = instrument(ExchangeId::Bitget, "ALTUSDT", 1.0, 0.000001, 1.0, 1.0, 5.0);
        let binance = instrument(ExchangeId::Binance, "ALTUSDT", 1.0, 0.000001, 1.0, 1.0, 5.0);

        let pair = size_hedge_pair_for_notional(
            &bitget,
            0.005563,
            Some(0.005563),
            &binance,
            0.005573,
            5.0,
        );

        assert!(pair.executable, "{:?}", pair.reject_reasons);
        assert_eq!(pair.maker.normalized_base_quantity, 899.0);
        assert_eq!(pair.taker.normalized_base_quantity, 899.0);
        assert!(pair.maker.normalized_notional_usdt.unwrap() >= 5.0);
        assert!(pair.taker.normalized_notional_usdt.unwrap() >= 5.0);
    }

    #[test]
    fn sizing_should_reject_when_100_usdt_is_below_exchange_minimum() {
        let instrument = instrument(ExchangeId::Gate, "ARB_USDT", 1.0, 0.0001, 1.0, 1.0, 150.0);

        let leg = size_exchange_leg_for_notional(&instrument, 100.0, 0.108, Some(0.108));

        assert!(!leg.valid);
        assert!(leg
            .violations
            .iter()
            .any(|violation| violation == "BelowMinNotional"));
    }
}
