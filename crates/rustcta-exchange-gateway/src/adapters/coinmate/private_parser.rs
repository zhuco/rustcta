#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use super::parser::value_as_string;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate balances fixture missing data array".to_string(),
        })?;
    Ok(rows
        .iter()
        .filter_map(|row| row.get("currency").and_then(value_as_string))
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    parse_ids(value, &["id", "orderId"])
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    parse_ids(value, &["transactionId", "id"])
}

fn parse_ids(value: &Value, fields: &[&str]) -> ExchangeApiResult<Vec<String>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate private fixture missing data array".to_string(),
        })?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            fields
                .iter()
                .find_map(|field| row.get(*field).and_then(value_as_string))
        })
        .collect())
}
