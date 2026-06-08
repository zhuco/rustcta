#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_order_ack(payload: &str) -> ExchangeApiResult<(bool, Option<String>)> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid OX.FUN private WS order payload: {error}"),
        })?;
    let submitted = value
        .get("submitted")
        .and_then(Value::as_bool)
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "OX.FUN private order ack missing submitted".to_string(),
        })?;
    let order_id = value
        .get("data")
        .and_then(|data| data.get("orderId"))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        });
    Ok((submitted, order_id))
}
