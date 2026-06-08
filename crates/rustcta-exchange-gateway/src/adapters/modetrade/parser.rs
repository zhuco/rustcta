use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ModeTrade public info missing rows",
                value,
            )
        })?;
    rows.iter()
        .filter(|row| {
            row.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.starts_with("PERP_"))
        })
        .map(|row| parse_market(exchange_id, row))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("symbol")
                .and_then(Value::as_str)
                .and_then(|symbol| canonical_from_orderly_symbol(symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "ModeTrade order book requires canonical_symbol or response symbol"
                .to_string(),
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
    snapshot.sequence = data
        .get("seq")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let canonical_symbol = canonical_from_orderly_symbol(symbol_text)?;
    let base_asset = value
        .get("base")
        .or_else(|| value.get("base_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote")
        .or_else(|| value.get("quote_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            symbol_text,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["quote_tick"])
        .or_else(|| decimal_path(value, &["price_tick"]))
        .or_else(|| decimal_path(value, &["tick_size"]));
    let quantity_increment = decimal_path(value, &["base_tick"])
        .or_else(|| decimal_path(value, &["size_tick"]))
        .or_else(|| decimal_path(value, &["lot_size"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(value, &["base_min"]),
        max_quantity: decimal_path(value, &["base_max"]),
        min_notional: decimal_path(value, &["min_notional"])
            .or_else(|| decimal_path(value, &["quote_min"])),
        max_notional: decimal_path(value, &["quote_max"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn canonical_from_orderly_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let parts = symbol.trim().split('_').collect::<Vec<_>>();
    if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("PERP") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer ModeTrade canonical symbol from {symbol}"),
        });
    }
    CanonicalSymbol::new(parts[1], parts[2]).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ModeTrade order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(values) => {
                let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(object) => {
                let price = object
                    .get("price")
                    .or_else(|| object.get("p"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level price", level)
                    })?;
                let quantity = object
                    .get("quantity")
                    .or_else(|| object.get("size"))
                    .or_else(|| object.get("q"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "ModeTrade order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "ModeTrade response success=false",
        value,
    ))
}

fn decimal_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
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

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("ModeTrade public info missing {field}"),
            value,
        )
    })
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
