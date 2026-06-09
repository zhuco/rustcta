#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TapbitTicker {
    pub symbol: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub last_price: String,
    pub bid_price: Option<String>,
    pub ask_price: Option<String>,
    pub mark_price: Option<String>,
    pub high_24h: Option<String>,
    pub low_24h: Option<String>,
    pub volume_24h: Option<String>,
    pub quote_volume_24h: Option<String>,
    pub change_24h: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TapbitPublicTrade {
    pub symbol: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub side: Option<String>,
    pub price: String,
    pub quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TapbitKline {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub turnover: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TapbitFundingRate {
    pub symbol: String,
    pub funding_rate: String,
}

pub fn parse_spot_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    data_array(exchange_id, value, "tapbit spot symbols missing data")?
        .iter()
        .map(|row| parse_spot_symbol_rule(exchange_id, row))
        .collect()
}

pub fn parse_swap_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    data_array(exchange_id, value, "tapbit swap symbols missing data")?
        .iter()
        .map(|row| parse_swap_symbol_rule(exchange_id, row))
        .collect()
}

fn parse_spot_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "trade_pair_name")?.to_ascii_uppercase();
    let base_asset = value
        .get("base_asset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| raw_symbol.split_once('/').map(|(base, _)| base.to_string()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "tapbit spot base asset missing", value))?;
    let quote_asset = value
        .get("quote_asset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| {
            raw_symbol
                .split_once('/')
                .map(|(_, quote)| quote.to_string())
        })
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "tapbit spot quote asset missing",
                value,
            )
        })?;
    let price_precision = integer_precision(value.get("price_precision"));
    let quantity_precision = integer_precision(value.get("amount_precision"));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol_scope(
            exchange_id,
            MarketType::Spot,
            &base_asset,
            &quote_asset,
            raw_symbol,
        )?,
        base_asset,
        quote_asset,
        price_increment: price_precision.map(step_from_precision),
        quantity_increment: quantity_precision.map(step_from_precision),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_amount")),
        max_quantity: None,
        min_notional: string_or_number(value.get("min_notional")),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_swap_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "contract_code")?.to_ascii_uppercase();
    let base_asset = raw_symbol
        .strip_suffix("-SWAP")
        .filter(|base| !base.is_empty())
        .unwrap_or(raw_symbol.as_str())
        .to_string();
    let quote_asset = "USDT".to_string();
    let price_precision = integer_precision(value.get("price_precision"));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol_scope(
            exchange_id,
            MarketType::Perpetual,
            &base_asset,
            &quote_asset,
            raw_symbol,
        )?,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("min_price_change")),
        quantity_increment: Some("1".to_string()),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_amount")),
        max_quantity: string_or_number(value.get("max_amount")),
        min_notional: None,
        max_notional: None,
        price_precision,
        quantity_precision: Some(0),
        supports_market_orders: false,
        supports_limit_orders: false,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_spot_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    parse_orderbook_snapshot(exchange_id, MarketType::Spot, symbol, value)
}

pub fn parse_swap_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    parse_orderbook_snapshot(exchange_id, MarketType::Perpetual, symbol, value)
}

fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "tapbit order book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("version")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_i64)
        .and_then(|value| u64::try_from(value).ok());
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    levels
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "tapbit book missing levels",
                &Value::Null,
            )
        })?
        .iter()
        .map(|level| {
            let values = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "tapbit book level must be array",
                    level,
                )
            })?;
            let price = values
                .first()
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = values
                .get(1)
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::FeeRateSnapshot>> {
    let rules = parse_spot_symbol_rules(exchange_id, value)?;
    Ok(requested_symbols
        .iter()
        .filter_map(|symbol| {
            rules
                .iter()
                .find(|rule| {
                    rule.symbol.exchange_symbol.symbol.eq_ignore_ascii_case(
                        &normalize_spot_symbol_lossy(&symbol.exchange_symbol.symbol),
                    )
                })
                .map(|rule| rustcta_exchange_api::FeeRateSnapshot {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    symbol: symbol.clone(),
                    maker_rate: field_for_symbol_fee(
                        value,
                        &rule.symbol.exchange_symbol.symbol,
                        "maker_fee_rate",
                    )
                    .unwrap_or_else(|| "0".to_string()),
                    taker_rate: field_for_symbol_fee(
                        value,
                        &rule.symbol.exchange_symbol.symbol,
                        "taker_fee_rate",
                    )
                    .unwrap_or_else(|| "0".to_string()),
                    source: Some("tapbit.spot.trade_pair_list".to_string()),
                    updated_at: Utc::now(),
                })
        })
        .collect())
}

fn field_for_symbol_fee(value: &Value, symbol: &str, field: &str) -> Option<String> {
    value
        .get("data")
        .and_then(Value::as_array)?
        .iter()
        .find(|row| {
            row.get("trade_pair_name")
                .and_then(Value::as_str)
                .is_some_and(|raw| raw.eq_ignore_ascii_case(symbol))
        })?
        .get(field)
        .and_then(|value| string_or_number(Some(value)))
}

pub fn parse_server_time(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<i64> {
    let data = value.get("data").unwrap_or(value);
    data.get("timestamp")
        .or(Some(data))
        .and_then(value_as_i64)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "tapbit server time missing timestamp",
                value,
            )
        })
}

pub fn parse_ticker(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<TapbitTicker> {
    let data = value.get("data").unwrap_or(value);
    let last_price = string_or_number(
        data.get("last_price")
            .or_else(|| data.get("lastPrice"))
            .or_else(|| data.get("last")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "tapbit ticker missing last price",
            value,
        )
    })?;
    Ok(TapbitTicker {
        symbol: value_as_string(
            data.get("trade_pair_name")
                .or_else(|| data.get("contract_code"))
                .or_else(|| data.get("symbol")),
        ),
        timestamp_ms: data.get("timestamp").and_then(value_as_i64),
        last_price,
        bid_price: string_or_number(
            data.get("highest_bid")
                .or_else(|| data.get("highest_bid_price"))
                .or_else(|| data.get("bestBidPrice")),
        ),
        ask_price: string_or_number(
            data.get("lowest_ask")
                .or_else(|| data.get("lowest_ask_price"))
                .or_else(|| data.get("bestAskPrice")),
        ),
        mark_price: string_or_number(data.get("mark_price").or_else(|| data.get("markPrice"))),
        high_24h: string_or_number(
            data.get("highest_price_24h")
                .or_else(|| data.get("high24h"))
                .or_else(|| data.get("highestPrice24h")),
        ),
        low_24h: string_or_number(
            data.get("lowest_price_24h")
                .or_else(|| data.get("low24h"))
                .or_else(|| data.get("lowestPrice24h")),
        ),
        volume_24h: string_or_number(data.get("volume24h").or_else(|| data.get("volume_24h"))),
        quote_volume_24h: string_or_number(data.get("amount24h").or_else(|| data.get("turnover"))),
        change_24h: string_or_number(data.get("chg24h")),
    })
}

pub fn parse_spot_public_trades(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<TapbitPublicTrade>> {
    data_array(
        exchange_id,
        value,
        "tapbit spot trades response missing data",
    )?
    .iter()
    .map(|row| {
        let values = row.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "tapbit spot trade row must be array",
                row,
            )
        })?;
        Ok(TapbitPublicTrade {
            symbol: Some(required_array_string(exchange_id, values, 0, row)?),
            price: required_array_string(exchange_id, values, 1, row)?,
            quantity: required_array_string(exchange_id, values, 2, row)?,
            side: Some(required_array_string(exchange_id, values, 3, row)?),
            timestamp_ms: values.get(4).and_then(value_as_i64),
        })
    })
    .collect()
}

pub fn parse_swap_public_trades(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<Vec<TapbitPublicTrade>> {
    data_array(
        exchange_id,
        value,
        "tapbit swap trades response missing data",
    )?
    .iter()
    .map(|row| {
        let values = row.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "tapbit swap trade row must be array",
                row,
            )
        })?;
        Ok(TapbitPublicTrade {
            symbol: Some(symbol.to_string()),
            price: required_array_string(exchange_id, values, 0, row)?,
            side: Some(required_array_string(exchange_id, values, 1, row)?),
            quantity: required_array_string(exchange_id, values, 2, row)?,
            timestamp_ms: values.get(3).and_then(value_as_i64),
        })
    })
    .collect()
}

pub fn parse_klines(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<TapbitKline>> {
    data_array(exchange_id, value, "tapbit klines response missing data")?
        .iter()
        .map(|row| {
            let values = row.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "tapbit kline row must be array", row)
            })?;
            let open_time_ms = values.first().and_then(value_as_i64).ok_or_else(|| {
                parse_error(exchange_id.clone(), "tapbit kline missing time", row)
            })?;
            Ok(TapbitKline {
                open_time_ms,
                open: required_array_string(exchange_id, values, 1, row)?,
                high: required_array_string(exchange_id, values, 2, row)?,
                low: required_array_string(exchange_id, values, 3, row)?,
                close: required_array_string(exchange_id, values, 4, row)?,
                volume: required_array_string(exchange_id, values, 5, row)?,
                turnover: values
                    .get(6)
                    .and_then(|value| string_or_number(Some(value))),
            })
        })
        .collect()
}

pub fn parse_funding_rate(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<TapbitFundingRate> {
    let data = value.get("data").unwrap_or(value);
    let funding_rate = string_or_number(
        data.get("funding_rate").or_else(|| data.get("fundingRate")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "tapbit funding rate missing", value))?;
    Ok(TapbitFundingRate {
        symbol: normalize_swap_symbol(symbol)?,
        funding_rate,
    })
}

pub fn normalize_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_spot_symbol_lossy(symbol);
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tapbit symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_spot_symbol_lossy(symbol: &str) -> String {
    let normalized = symbol.trim().replace(['_', '-'], "/").to_ascii_uppercase();
    if normalized.contains('/') {
        return normalized;
    }
    normalized
        .strip_suffix("USDT")
        .filter(|base| !base.is_empty())
        .map(|base| format!("{base}/USDT"))
        .unwrap_or(normalized)
}

pub fn normalize_swap_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tapbit swap symbol must not be empty".to_string(),
        });
    }
    if normalized.ends_with("-SWAP") {
        Ok(normalized)
    } else if let Some(base) = normalized.strip_suffix("-USDT") {
        Ok(format!("{base}-SWAP"))
    } else if let Some(base) = normalized.strip_suffix("USDT") {
        Ok(format!("{base}-SWAP"))
    } else {
        Ok(format!("{normalized}-SWAP"))
    }
}

pub fn swap_depth_instrument(symbol: &str) -> ExchangeApiResult<String> {
    Ok(normalize_swap_symbol(symbol)?
        .strip_suffix("-SWAP")
        .unwrap_or(symbol)
        .to_string())
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=50 => 50,
        51..=100 => 100,
        _ => 200,
    }
}

fn required_array_string(
    exchange_id: &ExchangeId,
    values: &[Value],
    index: usize,
    raw: &Value,
) -> ExchangeApiResult<String> {
    values
        .get(index)
        .and_then(|value| string_or_number(Some(value)))
        .ok_or_else(|| parse_error(exchange_id.clone(), "tapbit array field missing", raw))
}

pub(super) fn data_array<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    message: &str,
) -> ExchangeApiResult<&'a Vec<Value>> {
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| parse_error(exchange_id.clone(), message, value))
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("tapbit response missing field {field}"),
            value,
        )
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Tapbit decimal value {value}: {error}"),
        })
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    base_asset: &str,
    quote_asset: &str,
    exchange_symbol: String,
) -> ExchangeApiResult<SymbolScope> {
    let canonical_symbol =
        CanonicalSymbol::new(base_asset, quote_asset).map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn integer_precision(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|value| value as u32),
        _ => None,
    })
}

fn step_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat((precision - 1) as usize))
    }
}
