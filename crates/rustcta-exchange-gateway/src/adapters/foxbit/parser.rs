#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

const KNOWN_QUOTES: &[&str] = &["BRL", "USDT", "USD", "BTC", "ETH"];

pub fn foxbit_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_lowercase()
}

pub fn foxbit_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = symbol.trim().replace(['-', '_'], "/").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('/') {
        if !base.is_empty() && !quote.is_empty() {
            return Ok((base.to_string(), quote.to_string()));
        }
    }
    let compact = normalized.replace('/', "");
    for quote in KNOWN_QUOTES {
        if compact.ends_with(quote) && compact.len() > quote.len() {
            return Ok((
                compact[..compact.len() - quote.len()].to_string(),
                (*quote).to_string(),
            ));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("foxbit symbol {symbol} must include a supported quote"),
    })
}

pub fn ensure_foxbit_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "foxbit.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn normalize_depth(depth: Option<u32>) -> ExchangeApiResult<usize> {
    match depth {
        Some(0) => Err(ExchangeApiError::InvalidRequest {
            message: "foxbit order book depth must be greater than zero".to_string(),
        }),
        Some(depth) => Ok(depth.min(300) as usize),
        None => Ok(300),
    }
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("markets"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "Foxbit markets missing array", value))?;
    let mut rules = Vec::new();
    for row in rows {
        if !is_active(row) {
            continue;
        }
        let native_symbol = text(row.get("symbol").or_else(|| row.get("market_symbol")))
            .ok_or_else(|| parse_error(exchange_id.clone(), "missing symbol", row))?;
        let normalized_symbol = foxbit_symbol(&native_symbol);
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| foxbit_symbol(&symbol.exchange_symbol.symbol) == normalized_symbol)
        {
            continue;
        }
        let (fallback_base, fallback_quote) = foxbit_canonical_pair(&normalized_symbol)?;
        let base_asset = asset_symbol(row.get("base")).unwrap_or(fallback_base);
        let quote_asset = asset_symbol(row.get("quote")).unwrap_or(fallback_quote);
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                normalized_symbol,
            )
            .map_err(validation_error)?,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_increment: text(
                row.get("price_increment")
                    .or_else(|| row.get("quote_increment"))
                    .or_else(|| row.get("tick_size")),
            ),
            quantity_increment: text(
                row.get("quantity_increment")
                    .or_else(|| row.get("base_increment"))
                    .or_else(|| row.get("step_size")),
            ),
            min_price: text(row.get("price_min").or_else(|| row.get("min_price"))),
            max_price: None,
            min_quantity: text(
                row.get("quantity_min")
                    .or_else(|| row.get("min_quantity"))
                    .or_else(|| row.get("min_order_quantity")),
            ),
            max_quantity: None,
            min_notional: text(
                row.get("amount_min")
                    .or_else(|| row.get("min_amount"))
                    .or_else(|| row.get("notional_min")),
            ),
            max_notional: None,
            price_precision: integer(row.get("price_precision")),
            quantity_precision: integer(row.get("quantity_precision")),
            supports_market_orders: supports_order_type(row, "MARKET"),
            supports_limit_orders: supports_order_type(row, "LIMIT"),
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_order_book_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: usize,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value.get("data").unwrap_or(value);
    let mut bids = levels(exchange_id, book.get("bids"))?;
    let mut asks = levels(exchange_id, book.get("asks"))?;
    bids.truncate(depth);
    asks.truncate(depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "foxbit order book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = book.get("sequence_id").and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_order_ack_id(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("id")
        .or_else(|| value.get("order_id"))
        .or_else(|| value.get("client_order_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit order fixture missing id".to_string(),
        })
}

fn levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let values = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Foxbit orderbook missing level array",
            &Value::Null,
        )
    })?;
    values
        .iter()
        .map(|level| {
            let (price, quantity) = if let Some(array) = level.as_array() {
                (
                    array.first().and_then(value_as_f64),
                    array.get(1).and_then(value_as_f64),
                )
            } else {
                (
                    level.get("price").and_then(value_as_f64),
                    level
                        .get("quantity")
                        .or_else(|| level.get("qty"))
                        .or_else(|| level.get("amount"))
                        .and_then(value_as_f64),
                )
            };
            OrderBookLevel::new(
                price.ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Foxbit level price", level)
                })?,
                quantity.ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Foxbit level quantity", level)
                })?,
            )
            .map_err(validation_error)
        })
        .collect()
}

fn asset_symbol(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.to_ascii_uppercase()),
        Value::Object(map) => text(map.get("symbol").or_else(|| map.get("code")))
            .map(|value| value.to_ascii_uppercase()),
        _ => None,
    }
}

fn is_active(value: &Value) -> bool {
    value
        .get("is_active")
        .or_else(|| value.get("enabled"))
        .or_else(|| value.get("active"))
        .map(|status| match status {
            Value::Bool(status) => *status,
            Value::Number(number) => number.as_i64() != Some(0),
            Value::String(text) => !matches!(text.as_str(), "0" | "false" | "False" | "inactive"),
            _ => true,
        })
        .unwrap_or(true)
}

fn supports_order_type(value: &Value, expected: &str) -> bool {
    let Some(types) = value
        .get("order_types")
        .or_else(|| value.get("order_type"))
        .or_else(|| value.get("supported_order_types"))
    else {
        return matches!(expected, "LIMIT" | "MARKET");
    };
    if let Some(array) = types.as_array() {
        return array.iter().any(|item| {
            item.as_str()
                .is_some_and(|text| text.eq_ignore_ascii_case(expected))
        });
    }
    types
        .as_str()
        .is_some_and(|text| text.to_ascii_uppercase().contains(expected))
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn integer(value: Option<&Value>) -> Option<u32> {
    match value? {
        Value::Number(value) => value.as_u64().and_then(|value| value.try_into().ok()),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
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

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
