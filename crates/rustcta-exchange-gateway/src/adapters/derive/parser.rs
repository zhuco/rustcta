#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeriveInstrumentMetadata {
    pub instrument_name: String,
    pub instrument_type: String,
    pub currency: String,
    pub settlement_currency: String,
    pub tick_size: String,
    pub amount_step: String,
    pub expiry: Option<String>,
    pub strike: Option<String>,
    pub option_type: Option<String>,
}

pub fn parse_instrument_metadata(value: &Value) -> Vec<DeriveInstrumentMetadata> {
    let rows = value
        .get("result")
        .and_then(|result| result.get("instruments"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array);
    rows.into_iter()
        .flatten()
        .filter_map(|instrument| {
            Some(DeriveInstrumentMetadata {
                instrument_name: text(instrument, "instrument_name")
                    .or_else(|| text(instrument, "name"))?,
                instrument_type: text(instrument, "instrument_type")
                    .or_else(|| text(instrument, "type"))?,
                currency: text(instrument, "currency")?,
                settlement_currency: text(instrument, "settlement_currency")
                    .or_else(|| text(instrument, "settlement_asset"))?,
                tick_size: text(instrument, "tick_size")?,
                amount_step: text(instrument, "amount_step")
                    .or_else(|| text(instrument, "step_size"))?,
                expiry: text(instrument, "expiry"),
                strike: text(instrument, "strike"),
                option_type: text(instrument, "option_type"),
            })
        })
        .collect()
}

pub fn parse_subaccount_position_names(value: &Value) -> Vec<String> {
    value
        .get("result")
        .and_then(|result| result.get("positions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|position| text(position, "instrument_name"))
        .collect()
}

fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
