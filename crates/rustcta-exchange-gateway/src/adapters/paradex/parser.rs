#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParadexMarketMetadata {
    pub symbol: String,
    pub market_kind: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub settlement_currency: String,
    pub tick_size: String,
    pub step_size: String,
    pub expiry: Option<String>,
    pub strike_price: Option<String>,
    pub option_kind: Option<String>,
}

pub fn parse_market_metadata(value: &Value) -> Vec<ParadexMarketMetadata> {
    value
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|market| {
            Some(ParadexMarketMetadata {
                symbol: text(market, "symbol")?,
                market_kind: text(market, "market_kind").or_else(|| text(market, "asset_kind"))?,
                base_currency: text(market, "base_currency")?,
                quote_currency: text(market, "quote_currency")?,
                settlement_currency: text(market, "settlement_currency")
                    .or_else(|| text(market, "settle_currency"))?,
                tick_size: text(market, "tick_size").or_else(|| text(market, "price_tick_size"))?,
                step_size: text(market, "step_size")
                    .or_else(|| text(market, "order_size_increment"))?,
                expiry: text(market, "expiry").or_else(|| text(market, "expiry_at")),
                strike_price: text(market, "strike_price"),
                option_kind: text(market, "option_kind").or_else(|| text(market, "option_type")),
            })
        })
        .collect()
}

pub fn parse_position_symbols(value: &Value) -> Vec<String> {
    value
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|position| text(position, "market").or_else(|| text(position, "symbol")))
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
