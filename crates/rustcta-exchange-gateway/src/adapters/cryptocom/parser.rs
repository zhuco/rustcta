#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, SchemaVersion,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComTicker24h {
    pub symbol: SymbolScope,
    pub last_price: Option<String>,
    pub bid_price: Option<String>,
    pub ask_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub base_volume: Option<String>,
    pub quote_volume: Option<String>,
    pub open_interest: Option<String>,
    pub price_change: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub match_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
    pub trade_time_ns: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComCandle {
    pub symbol: SymbolScope,
    pub interval: String,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComValuationPoint {
    pub instrument_name: String,
    pub valuation_type: String,
    pub value: String,
    pub timestamp: DateTime<Utc>,
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "instrument response is not an array",
                value,
            )
        })?;
    instruments
        .iter()
        .filter(|item| infer_market_type(item).is_some())
        .map(|item| parse_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")
        .or_else(|_| required_str(exchange_id, value, "instrument_name"))?
        .to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "base_ccy")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quote_ccy")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let market_type = infer_market_type(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "unsupported cryptocom instrument type",
            value,
        )
    })?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let tradable = value
        .get("tradable")
        .or_else(|| value.get("trading_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("price_tick_size")),
        quantity_increment: string_or_number(value.get("qty_tick_size")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_quantity")),
        max_quantity: string_or_number(value.get("max_quantity")),
        min_notional: string_or_number(value.get("min_notional")),
        max_notional: None,
        price_precision: integer_from_value(value.get("quote_decimals")),
        quantity_precision: integer_from_value(value.get("quantity_decimals")),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let book = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cryptocom order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = book
        .get("u")
        .or_else(|| book.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("t")
        .or_else(|| book.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_ticker_24h(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CryptoComTicker24h> {
    let ticker = result_data(exchange_id, value)?
        .first()
        .ok_or_else(|| parse_error(exchange_id.clone(), "ticker response is empty", value))?;
    Ok(CryptoComTicker24h {
        symbol,
        last_price: string_or_number(ticker.get("a")),
        bid_price: string_or_number(ticker.get("b")),
        ask_price: string_or_number(ticker.get("k")),
        high_price: string_or_number(ticker.get("h")),
        low_price: string_or_number(ticker.get("l")),
        base_volume: string_or_number(ticker.get("v")),
        quote_volume: string_or_number(ticker.get("vv")),
        open_interest: string_or_number(ticker.get("oi")),
        price_change: string_or_number(ticker.get("c")),
        timestamp: ticker
            .get("t")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    })
}

pub fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<CryptoComPublicTrade>> {
    result_data(exchange_id, value)?
        .iter()
        .map(|trade| {
            let side = match trade.get("s").and_then(Value::as_str) {
                Some("BUY") => OrderSide::Buy,
                Some("SELL") => OrderSide::Sell,
                _ => {
                    return Err(parse_error(
                        exchange_id.clone(),
                        "trade response missing side",
                        trade,
                    ));
                }
            };
            let price = string_or_number(trade.get("p")).ok_or_else(|| {
                parse_error(exchange_id.clone(), "trade response missing price", trade)
            })?;
            let quantity = string_or_number(trade.get("q")).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "trade response missing quantity",
                    trade,
                )
            })?;
            let traded_at = trade
                .get("t")
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "trade response missing timestamp",
                        trade,
                    )
                })?;
            Ok(CryptoComPublicTrade {
                symbol: symbol.clone(),
                trade_id: value_as_string(trade.get("d")),
                match_id: value_as_string(trade.get("m")),
                side,
                price,
                quantity,
                traded_at,
                trade_time_ns: value_as_string(trade.get("tn")),
            })
        })
        .collect()
}

pub fn parse_candles(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    interval: &str,
    value: &Value,
) -> ExchangeApiResult<Vec<CryptoComCandle>> {
    result_data(exchange_id, value)?
        .iter()
        .map(|candle| {
            let opened_at = candle
                .get("t")
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "candle response missing timestamp",
                        candle,
                    )
                })?;
            Ok(CryptoComCandle {
                symbol: symbol.clone(),
                interval: interval.to_string(),
                opened_at,
                open: string_or_number(candle.get("o")).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "candle response missing open", candle)
                })?,
                high: string_or_number(candle.get("h")).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "candle response missing high", candle)
                })?,
                low: string_or_number(candle.get("l")).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "candle response missing low", candle)
                })?,
                close: string_or_number(candle.get("c")).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "candle response missing close", candle)
                })?,
                volume: string_or_number(candle.get("v")),
            })
        })
        .collect()
}

pub fn parse_valuations(
    exchange_id: &ExchangeId,
    instrument_name: &str,
    valuation_type: &str,
    value: &Value,
) -> ExchangeApiResult<Vec<CryptoComValuationPoint>> {
    result_data(exchange_id, value)?
        .iter()
        .map(|point| {
            let timestamp = point
                .get("t")
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "valuation response missing timestamp",
                        point,
                    )
                })?;
            Ok(CryptoComValuationPoint {
                instrument_name: value
                    .get("instrument_name")
                    .and_then(Value::as_str)
                    .unwrap_or(instrument_name)
                    .to_string(),
                valuation_type: valuation_type.to_string(),
                value: string_or_number(point.get("v")).ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "valuation response missing value",
                        point,
                    )
                })?,
                timestamp,
            })
        })
        .collect()
}

pub fn normalize_cryptocom_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let raw = symbol.symbol_text();
    let normalized = raw.trim().replace('/', "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') || normalized.contains('-') {
        return Ok(normalized);
    }
    if let Some(canonical) = &symbol.canonical_symbol {
        return Ok(format!(
            "{}_{}",
            canonical.base_asset(),
            canonical.quote_asset()
        ));
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=10 => 10,
        _ => 50,
    }
}

fn infer_market_type(value: &Value) -> Option<MarketType> {
    value
        .get("inst_type")
        .or_else(|| value.get("instrument_type"))
        .and_then(Value::as_str)
        .map(|text| {
            let normalized = text.trim().to_ascii_uppercase();
            if normalized.contains("PERP") || normalized.contains("FUTURE") {
                Some(MarketType::Perpetual)
            } else if normalized == "SPOT" || normalized == "CCY_PAIR" {
                Some(MarketType::Spot)
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            let is_perp = value
                .get("symbol")
                .or_else(|| value.get("instrument_name"))
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.to_ascii_uppercase().contains("PERP"));
            if is_perp {
                Some(MarketType::Perpetual)
            } else if value
                .get("symbol")
                .or_else(|| value.get("instrument_name"))
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.contains('_'))
            {
                Some(MarketType::Spot)
            } else {
                None
            }
        })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book level", level)
            })?;
            let price = array
                .first()
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn result_data<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<&'a Vec<Value>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "response data is not an array", value))
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
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
        _ => None,
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}

trait SymbolText {
    fn symbol_text(&self) -> &str;
}

impl SymbolText for SymbolScope {
    fn symbol_text(&self) -> &str {
        &self.exchange_symbol.symbol
    }
}
