#![allow(dead_code)]

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
pub struct LBankTicker {
    pub symbol: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub latest: String,
    pub high: Option<String>,
    pub low: Option<String>,
    pub change_percent: Option<String>,
    pub turnover: Option<String>,
    pub volume: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LBankBookTicker {
    pub symbol: String,
    pub bid_price: String,
    pub bid_quantity: Option<String>,
    pub ask_price: String,
    pub ask_quantity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LBankPublicTrade {
    pub trade_id: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub side: Option<String>,
    pub price: String,
    pub quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LBankKline {
    pub open_time_s: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

pub fn parse_spot_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let data = data_array(exchange_id, value, "spot accuracy response missing data")?;
    data.iter()
        .map(|item| parse_spot_symbol_rule(exchange_id, item))
        .collect()
}

pub fn parse_contract_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "contract instrument missing data",
                value,
            )
        })?;
    rows.iter()
        .map(|item| parse_contract_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_spot_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_lowercase();
    let (base_asset, quote_asset) = split_lbank_symbol(exchange_id, &raw_symbol)?;
    let price_precision = integer_precision(value.get("priceAccuracy"));
    let quantity_precision = integer_precision(value.get("quantityAccuracy"));
    let price_increment = price_precision.map(step_from_precision);
    let quantity_increment = quantity_precision.map(step_from_precision);
    let symbol = symbol_scope(
        exchange_id,
        MarketType::Spot,
        &base_asset,
        &quote_asset,
        raw_symbol,
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment,
        quantity_increment,
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minTranQua")),
        max_quantity: None,
        min_notional: string_or_number(value.get("minOrderAmount")),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_contract_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "baseCurrency")
        .map(str::to_ascii_uppercase)
        .or_else(|_| infer_contract_base(&raw_symbol))?;
    let quote_asset = value
        .get("priceCurrency")
        .or_else(|| value.get("clearCurrency"))
        .and_then(Value::as_str)
        .unwrap_or("USDT")
        .to_ascii_uppercase();
    let symbol = symbol_scope(
        exchange_id,
        MarketType::Perpetual,
        &base_asset,
        &quote_asset,
        raw_symbol,
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("priceTick")),
        quantity_increment: string_or_number(value.get("volumeTick")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minOrderVolume")),
        max_quantity: string_or_number(value.get("maxOrderVolume")),
        min_notional: string_or_number(value.get("minOrderCost")),
        max_notional: None,
        price_precision: precision_from_step(value.get("priceTick").and_then(number_from_value)),
        quantity_precision: precision_from_step(
            value.get("volumeTick").and_then(number_from_value),
        ),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_spot_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    parse_orderbook_snapshot(exchange_id, MarketType::Spot, symbol, data)
}

pub fn parse_contract_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    parse_orderbook_snapshot(exchange_id, MarketType::Perpetual, symbol, data)
}

pub fn parse_spot_server_time(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<i64> {
    value
        .get("data")
        .or_else(|| value.get("timestamp"))
        .or_else(|| value.get("ts"))
        .or(Some(value))
        .and_then(value_as_i64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "server time missing timestamp", value))
}

pub fn parse_spot_currency_pairs(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<String>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "currency pairs missing array", value))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_ascii_lowercase)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid currency pair", item))
        })
        .collect()
}

pub fn parse_spot_ticker(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<LBankTicker> {
    let data = value.get("data").unwrap_or(value);
    let ticker = data
        .get("ticker")
        .or_else(|| data.get("data"))
        .unwrap_or(data);
    let latest = string_or_number(
        ticker
            .get("latest")
            .or_else(|| ticker.get("price"))
            .or_else(|| ticker.get("last")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "ticker missing latest price", value))?;
    Ok(LBankTicker {
        symbol: data
            .get("symbol")
            .or_else(|| ticker.get("symbol"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase),
        timestamp_ms: data
            .get("timestamp")
            .or_else(|| ticker.get("timestamp"))
            .or_else(|| data.get("ts"))
            .and_then(value_as_i64),
        latest,
        high: string_or_number(ticker.get("high")),
        low: string_or_number(ticker.get("low")),
        change_percent: string_or_number(ticker.get("change")),
        turnover: string_or_number(ticker.get("turnover")),
        volume: string_or_number(ticker.get("vol").or_else(|| ticker.get("volume"))),
    })
}

pub fn parse_spot_book_ticker(
    exchange_id: &ExchangeId,
    requested_symbol: &str,
    value: &Value,
) -> ExchangeApiResult<LBankBookTicker> {
    let data = value.get("data").unwrap_or(value);
    let bid_price = string_or_number(data.get("bidPrice").or_else(|| data.get("bid_price")))
        .ok_or_else(|| parse_error(exchange_id.clone(), "book ticker missing bid price", value))?;
    let ask_price = string_or_number(data.get("askPrice").or_else(|| data.get("ask_price")))
        .ok_or_else(|| parse_error(exchange_id.clone(), "book ticker missing ask price", value))?;
    Ok(LBankBookTicker {
        symbol: data
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or(requested_symbol)
            .to_ascii_lowercase(),
        bid_price,
        bid_quantity: string_or_number(data.get("bidQty").or_else(|| data.get("bid_qty"))),
        ask_price,
        ask_quantity: string_or_number(data.get("askQty").or_else(|| data.get("ask_qty"))),
    })
}

pub fn parse_spot_public_trades(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<LBankPublicTrade>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "public trades missing array", value))?;
    rows.iter()
        .map(|row| {
            Ok(LBankPublicTrade {
                trade_id: value_as_string(row.get("tid").or_else(|| row.get("id"))),
                timestamp_ms: row
                    .get("date_ms")
                    .or_else(|| row.get("time"))
                    .or_else(|| row.get("timestamp"))
                    .and_then(value_as_i64),
                side: row
                    .get("type")
                    .or_else(|| row.get("side"))
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase),
                price: string_or_number(row.get("price")).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "public trade missing price", row)
                })?,
                quantity: string_or_number(row.get("amount").or_else(|| row.get("qty")))
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "public trade missing amount", row)
                    })?,
            })
        })
        .collect()
}

pub fn parse_spot_klines(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<LBankKline>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "klines missing array", value))?;
    rows.iter()
        .map(|row| {
            let array = row.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "kline row must be an array", row)
            })?;
            if array.len() < 6 {
                return Err(parse_error(
                    exchange_id.clone(),
                    "kline row has fewer than 6 fields",
                    row,
                ));
            }
            Ok(LBankKline {
                open_time_s: array
                    .first()
                    .and_then(value_as_i64)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing time", row))?,
                open: string_or_number(array.get(1))
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing open", row))?,
                high: string_or_number(array.get(2))
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing high", row))?,
                low: string_or_number(array.get(3))
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing low", row))?,
                close: string_or_number(array.get(4))
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing close", row))?,
                volume: string_or_number(array.get(5))
                    .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing volume", row))?,
            })
        })
        .collect()
}

fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LBank order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing bid/ask levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("volume")
                .or_else(|| level.get("quantity"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub fn normalize_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_contract_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_spot_depth(depth: u32) -> u32 {
    depth.clamp(1, 200)
}

pub fn normalize_contract_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
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
            &format!("missing field {field}"),
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
            message: format!("invalid LBank decimal value {value}: {error}"),
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

fn split_lbank_symbol(
    exchange_id: &ExchangeId,
    symbol: &str,
) -> ExchangeApiResult<(String, String)> {
    let (base, quote) = symbol.split_once('_').ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "LBank spot symbol must contain '_'",
            &Value::String(symbol.to_string()),
        )
    })?;
    Ok((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
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

fn precision_from_step(step: Option<f64>) -> Option<u32> {
    let step = step?;
    if step <= 0.0 {
        return None;
    }
    let text = format!("{step:.16}");
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
}

fn infer_contract_base(symbol: &str) -> ExchangeApiResult<String> {
    symbol
        .strip_suffix("USDT")
        .filter(|base| !base.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer LBank contract base asset from {symbol}"),
        })
}
