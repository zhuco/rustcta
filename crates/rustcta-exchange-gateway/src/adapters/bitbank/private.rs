#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

use super::parser::normalize_bitbank_pair;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbankRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn assets_spec() -> BitbankRequestSpec {
    BitbankRequestSpec {
        method: "GET".to_string(),
        path: "/v1/user/assets".to_string(),
        body: String::new(),
    }
}

pub fn place_limit_order_spec(
    pair: &str,
    side: &str,
    price: &str,
    amount: &str,
    post_only: bool,
) -> ExchangeApiResult<BitbankRequestSpec> {
    spec(
        "POST",
        "/v1/user/spot/order",
        json!({
            "pair": normalize_bitbank_pair(pair)?,
            "side": normalize_side(side)?,
            "type": "limit",
            "price": price,
            "amount": amount,
            "post_only": post_only,
        }),
    )
}

pub fn cancel_order_spec(pair: &str, order_id: u64) -> ExchangeApiResult<BitbankRequestSpec> {
    spec(
        "POST",
        "/v1/user/spot/cancel_order",
        json!({
            "pair": normalize_bitbank_pair(pair)?,
            "order_id": order_id,
        }),
    )
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BitbankRequestSpec> {
    Ok(BitbankRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitbank side {side}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{assets_spec, cancel_order_spec, place_limit_order_spec};

    #[test]
    fn bitbank_request_specs_should_match_fixtures() {
        let place =
            place_limit_order_spec("btc_jpy", "buy", "3000000", "0.01", true).expect("place");
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/place_limit_order.json"
        ))
        .expect("fixture");
        assert_eq!(place.method, expected["method"].as_str().unwrap());
        assert_eq!(place.path, expected["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&place.body).unwrap(),
            expected["body"]
        );

        let cancel = cancel_order_spec("btc_jpy", 1).expect("cancel");
        let expected_cancel: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/cancel_order.json"
        ))
        .expect("cancel fixture");
        assert_eq!(cancel.method, expected_cancel["method"].as_str().unwrap());
        assert_eq!(cancel.path, expected_cancel["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&cancel.body).unwrap(),
            expected_cancel["body"]
        );

        assert_eq!(assets_spec().path, "/v1/user/assets");
    }
}
