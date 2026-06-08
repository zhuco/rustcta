#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct OxfunMarketSpec {
    pub market_code: String,
    pub market_type: String,
    pub base: Option<String>,
    pub counter: Option<String>,
    pub tick_size: Option<String>,
    pub quantity_increment: Option<String>,
    pub margin_currency: Option<String>,
    pub contract_value_currency: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OxfunDepthEvent {
    pub table: String,
    pub market_code: String,
    pub action: Option<String>,
    pub sequence: Option<u64>,
    pub checksum: Option<u64>,
    pub timestamp_ms: Option<i64>,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

pub fn parse_market_specs(payload: &str) -> ExchangeApiResult<Vec<OxfunMarketSpec>> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid OX.FUN market payload: {error}"),
        })?;
    let rows = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::Serialization {
            message: "OX.FUN market payload missing data array".to_string(),
        }
    })?;
    rows.iter()
        .map(|row| {
            Ok(OxfunMarketSpec {
                market_code: string_field(row, "marketCode")?,
                market_type: string_field(row, "type")?,
                base: optional_string(row, "base"),
                counter: optional_string(row, "counter"),
                tick_size: optional_string(row, "tickSize"),
                quantity_increment: optional_string(row, "qtyIncrement"),
                margin_currency: optional_string(row, "marginCurrency"),
                contract_value_currency: optional_string(row, "contractValCurrency"),
            })
        })
        .collect()
}

pub fn parse_depth_event(payload: &str) -> ExchangeApiResult<OxfunDepthEvent> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid OX.FUN depth payload: {error}"),
        })?;
    let data = depth_data_row(&value)?;
    Ok(OxfunDepthEvent {
        table: string_field(&value, "table")?,
        market_code: string_field(data, "marketCode")?,
        action: optional_string(data, "action"),
        sequence: data
            .get("seqNum")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| data.get("seqNum").and_then(Value::as_u64)),
        checksum: data
            .get("checksum")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| data.get("checksum").and_then(Value::as_u64)),
        timestamp_ms: data
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .or_else(|| data.get("timestamp").and_then(Value::as_i64)),
        bids: parse_levels(data.get("bids"))?,
        asks: parse_levels(data.get("asks"))?,
    })
}

fn depth_data_row(value: &Value) -> ExchangeApiResult<&Value> {
    let data = value
        .get("data")
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "OX.FUN depth payload missing data".to_string(),
        })?;
    if let Some(rows) = data.as_array() {
        return rows.first().ok_or_else(|| ExchangeApiError::Serialization {
            message: "OX.FUN depth payload data array is empty".to_string(),
        });
    }
    if data.is_object() {
        return Ok(data);
    }
    Err(ExchangeApiError::Serialization {
        message: "OX.FUN depth payload data must be object or array".to_string(),
    })
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<(String, String)>> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let pair = row
                .as_array()
                .ok_or_else(|| ExchangeApiError::Serialization {
                    message: "OX.FUN depth level must be an array".to_string(),
                })?;
            let price = pair.first().and_then(value_as_string).ok_or_else(|| {
                ExchangeApiError::Serialization {
                    message: "OX.FUN depth level missing price".to_string(),
                }
            })?;
            let quantity = pair.get(1).and_then(value_as_string).ok_or_else(|| {
                ExchangeApiError::Serialization {
                    message: "OX.FUN depth level missing quantity".to_string(),
                }
            })?;
            Ok((price, quantity))
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    optional_string(value, field).ok_or_else(|| ExchangeApiError::Serialization {
        message: format!("OX.FUN payload missing {field}"),
    })
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(value_as_string)
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
