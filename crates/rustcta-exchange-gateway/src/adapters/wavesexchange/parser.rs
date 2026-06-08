use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, OrderBookLevel,
    OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let pair_id = symbol.exchange_symbol.symbol.clone();
    let restrictions = value
        .get("restrictions")
        .and_then(|restrictions| {
            restrictions.get(&pair_id).or_else(|| {
                restrictions
                    .as_object()
                    .and_then(|object| object.values().next())
            })
        })
        .filter(|value| !value.is_null());
    let matching_rules = value.get("matchingRules");
    let canonical = canonical_from_scope(exchange_id, &symbol)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset: canonical.base_asset().to_string(),
        quote_asset: canonical.quote_asset().to_string(),
        price_increment: decimal_path(matching_rules, &["tickSize"])
            .or_else(|| decimal_path(restrictions, &["step-price"])),
        quantity_increment: decimal_path(restrictions, &["step-amount"]),
        min_price: decimal_path(restrictions, &["min-price"]),
        max_price: decimal_path(restrictions, &["max-price"]),
        min_quantity: decimal_path(restrictions, &["min-amount"]),
        max_quantity: decimal_path(restrictions, &["max-amount"]),
        min_notional: None,
        max_notional: None,
        price_precision: decimal_path(matching_rules, &["tickSize"])
            .or_else(|| decimal_path(restrictions, &["step-price"]))
            .and_then(|value| precision_hint(&value)),
        quantity_precision: decimal_path(restrictions, &["step-amount"])
            .and_then(|value| precision_hint(&value)),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol = canonical_from_scope(exchange_id, &symbol)?;
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
    snapshot.sequence = value.get("lastOffset").and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

pub fn split_pair(exchange_symbol: &ExchangeSymbol) -> ExchangeApiResult<(&str, &str)> {
    exchange_symbol
        .symbol
        .split_once('-')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!(
                "wavesexchange symbol must be amountAsset-priceAsset, got {}",
                exchange_symbol.symbol
            ),
        })
}

fn canonical_from_scope(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
) -> ExchangeApiResult<CanonicalSymbol> {
    symbol.canonical_symbol.clone().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "WavesExchange symbol rules/order book require canonical_symbol because asset ids are not names",
            &Value::Null,
        )
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "WavesExchange order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let (price, quantity) = if let Some(values) = level.as_array() {
                (
                    values.first().and_then(value_as_f64),
                    values.get(1).and_then(value_as_f64),
                )
            } else {
                (
                    level.get("price").and_then(value_as_f64),
                    level
                        .get("amount")
                        .or_else(|| level.get("quantity"))
                        .and_then(value_as_f64),
                )
            };
            let price = price
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = quantity
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level amount", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn decimal_path(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut cursor = value?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn timestamp_millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

fn precision_hint(value: &str) -> Option<u32> {
    if let Ok(number) = value.parse::<u32>() {
        return Some(number);
    }
    value
        .trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
