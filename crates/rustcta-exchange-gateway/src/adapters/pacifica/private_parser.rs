#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_order_response(payload: &str) -> ExchangeApiResult<Option<u64>> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid Pacifica order response: {error}"),
        })?;
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(ExchangeApiError::InvalidRequest {
            message: value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Pacifica order response failed")
                .to_string(),
        });
    }
    Ok(value
        .get("data")
        .and_then(|data| data.get("order_id"))
        .and_then(Value::as_u64))
}
