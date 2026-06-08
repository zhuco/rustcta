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
    let markets = market_values(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Zeta Markets symbols response missing market list",
            value,
        )
    })?;
    markets
        .into_iter()
        .filter_map(|market| parse_market(exchange_id, market).transpose())
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_success(exchange_id, value)?;
    let data = value
        .get("data")
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("bid")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("ask")))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("ticker_id")
                .or_else(|| data.get("market"))
                .or_else(|| data.get("symbol"))
                .and_then(Value::as_str)
                .and_then(|symbol| canonical_from_zeta_symbol(symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Zeta Markets order book requires canonical_symbol or response ticker_id"
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
        .get("slot")
        .or_else(|| data.get("sequence"))
        .or_else(|| data.get("seq"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

fn market_values(value: &Value) -> Option<Vec<&Value>> {
    if let Some(array) = value.as_array() {
        return Some(array.iter().collect());
    }
    for key in ["symbols", "markets", "contracts", "data", "result"] {
        if let Some(child) = value.get(key) {
            if let Some(array) = child.as_array() {
                return Some(array.iter().collect());
            }
            if let Some(object) = child.as_object() {
                return Some(object.values().collect());
            }
        }
    }
    value.as_object().map(|object| object.values().collect())
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<Option<SymbolRules>> {
    let symbol_text = match value {
        Value::String(symbol) => symbol.trim(),
        Value::Object(object) => object
            .get("ticker_id")
            .or_else(|| object.get("market"))
            .or_else(|| object.get("symbol"))
            .or_else(|| object.get("asset"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim(),
        _ => "",
    };
    if symbol_text.is_empty() {
        return Ok(None);
    }
    let canonical_symbol = canonical_from_zeta_symbol(symbol_text)?;
    let object = value.as_object();
    let base_asset = object
        .and_then(|object| {
            object
                .get("base")
                .or_else(|| object.get("base_asset"))
                .or_else(|| object.get("underlying"))
                .and_then(Value::as_str)
        })
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = object
        .and_then(|object| {
            object
                .get("quote")
                .or_else(|| object.get("quote_asset"))
                .and_then(Value::as_str)
        })
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let exchange_symbol = zeta_exchange_symbol(symbol_text, &canonical_symbol);
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["tick_size"])
        .or_else(|| decimal_path(value, &["price_increment"]))
        .or_else(|| decimal_path(value, &["min_price_increment"]))
        .or_else(|| Some("0.0001".to_string()));
    let quantity_increment = decimal_path(value, &["lot_size"])
        .or_else(|| decimal_path(value, &["quantity_increment"]))
        .or_else(|| decimal_path(value, &["min_trade_tick_size"]))
        .or_else(|| Some("0.001".to_string()));
    Ok(Some(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(value, &["min_quantity"]),
        max_quantity: decimal_path(value, &["max_quantity"]),
        min_notional: decimal_path(value, &["min_notional"]),
        max_notional: decimal_path(value, &["max_notional"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    }))
}

fn canonical_from_zeta_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    let base = trimmed
        .strip_suffix("-PERP")
        .or_else(|| trimmed.strip_suffix("_PERP"))
        .or_else(|| trimmed.strip_prefix("PERP_"))
        .unwrap_or(trimmed.as_str())
        .split(['-', '_', '/'])
        .next()
        .unwrap_or(trimmed.as_str());
    if base.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Zeta Markets canonical symbol from {symbol}"),
        });
    }
    CanonicalSymbol::new(base, "USDC").map_err(validation_error)
}

fn zeta_exchange_symbol(symbol: &str, canonical_symbol: &CanonicalSymbol) -> String {
    let trimmed = symbol.trim().to_ascii_uppercase();
    if trimmed.ends_with("-PERP") || trimmed.ends_with("_PERP") || trimmed.starts_with("PERP_") {
        trimmed
    } else {
        format!("{}-PERP", canonical_symbol.base_asset())
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Zeta Markets order book missing price levels",
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
                "Zeta Markets order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .or_else(|| value.get("ok"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "Zeta Markets response success=false",
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
