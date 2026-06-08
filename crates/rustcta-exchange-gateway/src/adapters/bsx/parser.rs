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
    ensure_no_error(exchange_id, value)?;
    let products = value
        .as_array()
        .or_else(|| value.get("products").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| data.get("products"))
                .and_then(Value::as_array)
        })
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BSX products response missing products",
                value,
            )
        })?;
    products
        .iter()
        .filter(|product| is_perpetual_product(product))
        .map(|product| parse_product(exchange_id, product))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_no_error(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("product_id")
                .or_else(|| data.get("product"))
                .or_else(|| data.get("symbol"))
                .and_then(Value::as_str)
                .and_then(|product_id| canonical_from_product_id(product_id).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "BSX order book requires canonical_symbol or response product_id".to_string(),
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
        .get("sequence")
        .or_else(|| data.get("seq"))
        .or_else(|| data.get("gsn"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("time"))
        .and_then(value_as_i128)
        .and_then(timestamp_any);
    Ok(snapshot)
}

fn parse_product(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let product_id = required_str(exchange_id, value, "product_id")
        .or_else(|_| required_str(exchange_id, value, "id"))
        .or_else(|_| required_str(exchange_id, value, "symbol"))?;
    let canonical_symbol = canonical_from_product_id(product_id)?;
    let base_asset = value
        .get("base_asset")
        .or_else(|| value.get("base"))
        .or_else(|| value.get("underlying_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote_asset")
        .or_else(|| value.get("quote"))
        .or_else(|| value.get("settlement_asset"))
        .or_else(|| value.get("collateral_asset"))
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
            product_id,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["tick_size"])
        .or_else(|| decimal_path(value, &["price_increment"]))
        .or_else(|| decimal_path(value, &["price_tick"]));
    let quantity_increment = decimal_path(value, &["step_size"])
        .or_else(|| decimal_path(value, &["size_increment"]))
        .or_else(|| decimal_path(value, &["quantity_increment"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: decimal_path(value, &["min_price"]),
        max_price: decimal_path(value, &["max_price"]),
        min_quantity: decimal_path(value, &["min_size"])
            .or_else(|| decimal_path(value, &["min_quantity"])),
        max_quantity: decimal_path(value, &["max_size"])
            .or_else(|| decimal_path(value, &["max_quantity"])),
        min_notional: decimal_path(value, &["min_notional"]),
        max_notional: decimal_path(value, &["max_notional"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn is_perpetual_product(value: &Value) -> bool {
    let product_type = value
        .get("product_type")
        .or_else(|| value.get("type"))
        .or_else(|| value.get("market_type"))
        .and_then(Value::as_str);
    product_type
        .map(|product_type| {
            let product_type = product_type.to_ascii_lowercase();
            product_type.contains("perp") || product_type.contains("future")
        })
        .unwrap_or_else(|| {
            value
                .get("product_id")
                .or_else(|| value.get("id"))
                .or_else(|| value.get("symbol"))
                .and_then(Value::as_str)
                .is_some_and(|product_id| product_id.to_ascii_uppercase().ends_with("-PERP"))
        })
}

fn canonical_from_product_id(product_id: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let normalized = product_id.trim().to_ascii_uppercase();
    if let Some(base) = normalized.strip_suffix("-PERP") {
        return CanonicalSymbol::new(base, "USDC").map_err(validation_error);
    }
    let parts = normalized.split('-').collect::<Vec<_>>();
    if parts.len() >= 2 {
        return CanonicalSymbol::new(parts[0], parts[1]).map_err(validation_error);
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot infer BSX canonical symbol from {product_id}"),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BSX order book missing price levels",
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
                "BSX order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_no_error(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value.get("error").is_none() && value.get("errors").is_none() {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "BSX response contains error",
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

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn value_as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Number(number) => number.as_i64().map(i128::from),
        Value::String(text) => text.parse::<i128>().ok(),
        _ => None,
    }
}

fn timestamp_any(raw: i128) -> Option<DateTime<Utc>> {
    let millis = if raw > 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw > 10_000_000_000 {
        raw
    } else {
        raw * 1_000
    };
    Utc.timestamp_millis_opt(millis.try_into().ok()?).single()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("BSX response missing {field}"),
            value,
        )
    })
}

fn precision_hint(decimal: &str) -> Option<u32> {
    decimal
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        format!("{message}: {value}"),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
