#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances = value
        .get("payload")
        .and_then(|payload| payload.get("balances"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso balance fixture missing payload.balances".to_string(),
        })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("currency").and_then(Value::as_str))
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = value
        .get("payload")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso open orders fixture missing payload array".to_string(),
        })?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("oid").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = value
        .get("payload")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso fills fixture missing payload array".to_string(),
        })?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("tid").and_then(Value::as_i64))
        .map(|id| id.to_string())
        .collect())
}
