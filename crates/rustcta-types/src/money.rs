use thiserror::Error;

pub type Money = f64;
pub type Price = f64;
pub type Quantity = f64;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MoneyError {
    #[error("invalid decimal field {field}: {value}")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("invalid finite decimal field {field}: {value}")]
    InvalidFiniteDecimal { field: &'static str, value: String },
}

pub fn parse_decimal(value: &str, field: &'static str) -> Result<f64, MoneyError> {
    decimal_from_f64(
        value
            .trim()
            .parse::<f64>()
            .map_err(|_| MoneyError::InvalidDecimal {
                field,
                value: value.to_string(),
            })?,
        field,
    )
}

pub fn decimal_from_f64(value: f64, field: &'static str) -> Result<f64, MoneyError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(MoneyError::InvalidFiniteDecimal {
            field,
            value: value.to_string(),
        })
    }
}

pub fn decimal_to_f64(value: f64) -> f64 {
    value
}

pub fn decimal_to_f64_option(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

pub fn notional(price: Price, quantity: Quantity) -> Money {
    price * quantity
}

pub fn fee_amount(notional: Money, fee_bps: f64) -> Money {
    notional.max(0.0) * fee_bps / 10_000.0
}

pub fn multiply_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    Some(decimal_from_f64(left, left_field).ok()? * decimal_from_f64(right, right_field).ok()?)
        .filter(|value| value.is_finite())
}

pub fn add_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    Some(decimal_from_f64(left, left_field).ok()? + decimal_from_f64(right, right_field).ok()?)
        .filter(|value| value.is_finite())
}

pub fn subtract_f64(
    left: f64,
    right: f64,
    left_field: &'static str,
    right_field: &'static str,
) -> Option<f64> {
    Some(decimal_from_f64(left, left_field).ok()? - decimal_from_f64(right, right_field).ok()?)
        .filter(|value| value.is_finite())
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
    Some(
        decimal_from_f64(numerator, numerator_field).ok()?
            / decimal_from_f64(denominator, denominator_field).ok()?,
    )
    .filter(|value| value.is_finite())
}

pub fn notional_f64(price: f64, quantity: f64) -> Option<f64> {
    multiply_f64(price, quantity, "price", "quantity")
}

pub fn notional_with_contract_f64(price: f64, quantity: f64, contract_size: f64) -> Option<f64> {
    let value = decimal_from_f64(price, "price").ok()?
        * decimal_from_f64(quantity, "quantity").ok()?
        * decimal_from_f64(contract_size, "contract_size").ok()?;
    value.is_finite().then_some(value)
}

pub fn fee_amount_f64(notional: f64, fee_bps: f64) -> Option<f64> {
    let value = fee_amount(
        decimal_from_f64(notional, "notional").ok()?,
        decimal_from_f64(fee_bps, "fee_bps").ok()?,
    );
    value.is_finite().then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_helpers_should_parse_and_calculate_values() {
        let price = parse_decimal("123.45", "price").unwrap();
        let quantity = parse_decimal("2", "quantity").unwrap();
        let value = notional(price, quantity);

        assert_eq!(value, 246.9);
        assert_eq!(
            fee_amount(value, parse_decimal("10", "fee_bps").unwrap()),
            0.2469
        );
    }

    #[test]
    fn money_f64_helpers_should_reject_non_finite_values() {
        assert_eq!(notional_f64(100.0, 0.25), Some(25.0));
        assert_eq!(fee_amount_f64(25.0, 10.0), Some(0.025));
        assert_eq!(notional_f64(f64::NAN, 1.0), None);
        assert_eq!(divide_f64(1.0, 0.0, "numerator", "denominator"), None);
    }
}
