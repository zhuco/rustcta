#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

use super::parser::normalize_product_code;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitflyerRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn place_limit_order_spec(
    product_code: &str,
    side: &str,
    price: &str,
    size: &str,
    client_order_acceptance_id: Option<&str>,
) -> ExchangeApiResult<BitflyerRequestSpec> {
    let mut body = json!({
        "product_code": normalize_product_code(product_code)?,
        "child_order_type": "LIMIT",
        "side": normalize_side(side)?,
        "price": decimal_number(price)?,
        "size": decimal_number(size)?,
    });
    if let Some(client_id) = client_order_acceptance_id.filter(|value| !value.trim().is_empty()) {
        body["minute_to_expire"] = json!(43200);
        body["time_in_force"] = json!("GTC");
        body["client_order_acceptance_id"] = json!(client_id);
    }
    spec("POST", "/v1/me/sendchildorder", body)
}

pub fn cancel_order_spec(
    product_code: &str,
    child_order_acceptance_id: &str,
) -> ExchangeApiResult<BitflyerRequestSpec> {
    if child_order_acceptance_id.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitflyer cancel requires child_order_acceptance_id".to_string(),
        });
    }
    spec(
        "POST",
        "/v1/me/cancelchildorder",
        json!({
            "product_code": normalize_product_code(product_code)?,
            "child_order_acceptance_id": child_order_acceptance_id,
        }),
    )
}

pub fn balances_spec() -> BitflyerRequestSpec {
    BitflyerRequestSpec {
        method: "GET".to_string(),
        path: "/v1/me/getbalance".to_string(),
        body: String::new(),
    }
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BitflyerRequestSpec> {
    Ok(BitflyerRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok("BUY"),
        "SELL" => Ok("SELL"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitflyer side {side}"),
        }),
    }
}

fn decimal_number(value: &str) -> ExchangeApiResult<Value> {
    let number = value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid bitflyer decimal {value}: {error}"),
        })?;
    Ok(json!(number))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{balances_spec, cancel_order_spec, place_limit_order_spec};

    #[test]
    fn bitflyer_request_specs_should_match_fixtures() {
        let place = place_limit_order_spec(
            "BTC_JPY",
            "BUY",
            "30000",
            "0.1",
            Some("JRF20260608-000000-000001"),
        )
        .expect("place spec");
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitflyer/request_specs/place_limit_order.json"
        ))
        .expect("place fixture");
        assert_eq!(place.method, expected["method"].as_str().unwrap());
        assert_eq!(place.path, expected["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&place.body).unwrap(),
            expected["body"]
        );

        let cancel =
            cancel_order_spec("BTC_JPY", "JRF20260608-000000-000001").expect("cancel spec");
        let expected_cancel: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitflyer/request_specs/cancel_order.json"
        ))
        .expect("cancel fixture");
        assert_eq!(cancel.method, expected_cancel["method"].as_str().unwrap());
        assert_eq!(cancel.path, expected_cancel["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&cancel.body).unwrap(),
            expected_cancel["body"]
        );

        let balances = balances_spec();
        assert_eq!(balances.method, "GET");
        assert!(balances.body.is_empty());
    }
}
