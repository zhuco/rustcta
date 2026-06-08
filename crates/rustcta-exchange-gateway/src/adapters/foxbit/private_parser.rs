#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances = value
        .get("data")
        .or_else(|| value.get("balances"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit balance fixture missing balances array".to_string(),
        })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("currency_symbol").or_else(|| row.get("currency")))
        .filter_map(Value::as_str)
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = value
        .get("data")
        .or_else(|| value.get("orders"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit open orders fixture missing orders array".to_string(),
        })?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("order_id")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = value
        .get("data")
        .or_else(|| value.get("fills"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit fills fixture missing fills array".to_string(),
        })?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("trade_id")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}
