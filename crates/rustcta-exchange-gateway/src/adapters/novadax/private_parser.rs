#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances =
        unwrap_data(value)
            .as_array()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "novadax balance fixture missing data array".to_string(),
            })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("currency").or_else(|| row.get("asset")))
        .filter_map(Value::as_str)
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = unwrap_data(value)
        .get("items")
        .or_else(|| unwrap_data(value).get("list"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "novadax open orders fixture missing items array".to_string(),
        })?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("orderId")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = unwrap_data(value)
        .get("items")
        .or_else(|| unwrap_data(value).get("list"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "novadax fills fixture missing items array".to_string(),
        })?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("tradeId")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}

fn unwrap_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}
