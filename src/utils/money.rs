use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;

pub type Money = Decimal;
pub type Price = Decimal;
pub type Quantity = Decimal;

pub fn parse_decimal(value: &str, field: &'static str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value.trim())
        .map_err(|_| anyhow::anyhow!("invalid decimal field {field}: {value}"))
}

pub fn decimal_from_f64(value: f64, field: &'static str) -> anyhow::Result<Decimal> {
    Decimal::from_f64_retain(value)
        .ok_or_else(|| anyhow::anyhow!("invalid finite decimal field {field}: {value}"))
}

pub fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}

pub fn decimal_to_f64_option(value: Decimal) -> Option<f64> {
    value.to_f64()
}

pub fn notional(price: Price, quantity: Quantity) -> Money {
    price * quantity
}

pub fn fee_amount(notional: Money, fee_bps: Decimal) -> Money {
    notional.max(Decimal::ZERO) * fee_bps / Decimal::from(10_000)
}

pub fn multiply_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    let left = decimal_from_f64(left, left_field).ok()?;
    let right = decimal_from_f64(right, right_field).ok()?;
    decimal_to_f64_option(left * right)
}

pub fn add_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    let left = decimal_from_f64(left, left_field).ok()?;
    let right = decimal_from_f64(right, right_field).ok()?;
    decimal_to_f64_option(left + right)
}

pub fn subtract_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    let left = decimal_from_f64(left, left_field).ok()?;
    let right = decimal_from_f64(right, right_field).ok()?;
    decimal_to_f64_option(left - right)
}

pub fn divide_f64(
    numerator: f64,
    denominator: f64,
    numerator_field: &'static str,
    denominator_field: &'static str,
) -> Option<f64> {
    if denominator == 0.0 {
        return None;
    }
    let numerator = decimal_from_f64(numerator, numerator_field).ok()?;
    let denominator = decimal_from_f64(denominator, denominator_field).ok()?;
    decimal_to_f64_option(numerator / denominator)
}

pub fn notional_f64(price: f64, quantity: f64) -> Option<f64> {
    let price = decimal_from_f64(price, "price").ok()?;
    let quantity = decimal_from_f64(quantity, "quantity").ok()?;
    decimal_to_f64_option(notional(price, quantity))
}

pub fn notional_with_contract_f64(price: f64, quantity: f64, contract_size: f64) -> Option<f64> {
    let price = decimal_from_f64(price, "price").ok()?;
    let quantity = decimal_from_f64(quantity, "quantity").ok()?;
    let contract_size = decimal_from_f64(contract_size, "contract_size").ok()?;
    decimal_to_f64_option(price * quantity * contract_size)
}

pub fn fee_amount_f64(notional: f64, fee_bps: f64) -> Option<f64> {
    let notional = decimal_from_f64(notional, "notional").ok()?;
    let fee_bps = decimal_from_f64(fee_bps, "fee_bps").ok()?;
    decimal_to_f64_option(fee_amount(notional, fee_bps))
}
