use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let data = value.get("data").unwrap_or(value);
    if let Some(items) = data.as_array() {
        return items
            .iter()
            .map(|item| {
                if let Some(symbol) = item.as_str() {
                    parse_symbol_rule(
                        exchange_id,
                        &serde_json::json!({
                            "symbol": symbol,
                            "base_currency": fallback_base(symbol),
                            "quote_currency": fallback_quote(symbol),
                        }),
                    )
                } else {
                    parse_symbol_rule(exchange_id, item)
                }
            })
            .collect();
    }
    Ok(vec![parse_symbol_rule(exchange_id, data)?])
}

pub fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let normalized_symbol = normalize_gemini_symbol(symbol_text)?;
    let base_asset = value
        .get("base_currency")
        .or_else(|| value.get("base"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| fallback_base(&normalized_symbol));
    let quote_asset = value
        .get("quote_currency")
        .or_else(|| value.get("quote"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| fallback_quote(&normalized_symbol));
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalized_symbol,
        )
        .map_err(validation_error)?,
    };
    let tick_size = string_or_number(
        value
            .get("quote_increment")
            .or_else(|| value.get("tick_size")),
    );
    let quantity_increment = string_or_number(
        value
            .get("tick_size")
            .or_else(|| value.get("quantity_increment")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: tick_size.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_order_size")),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: tick_size.as_deref().and_then(precision_from_step),
        quantity_precision: quantity_increment.as_deref().and_then(precision_from_step),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gemini order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = book
        .get("timestampms")
        .or_else(|| book.get("timestamp_ms"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_gemini_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "gemini symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=10 => 10,
        11..=50 => 50,
        _ => 100,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "gemini order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = level
                .get("price")
                .and_then(number_from_value)
                .or_else(|| level.as_array()?.first().and_then(number_from_value))
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("amount")
                .or_else(|| level.get("quantity"))
                .and_then(number_from_value)
                .or_else(|| level.as_array()?.get(1).and_then(number_from_value))
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("gemini response missing field {field}"),
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
    string_or_number(value)
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn fallback_base(symbol: &str) -> String {
    let normalized = symbol.to_ascii_uppercase();
    split_compact_symbol(&normalized)
        .map(|(base, _)| base)
        .unwrap_or(normalized)
}

fn fallback_quote(symbol: &str) -> String {
    let normalized = symbol.to_ascii_uppercase();
    split_compact_symbol(&normalized)
        .map(|(_, quote)| quote)
        .unwrap_or_else(|| "USD".to_string())
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 8] = ["USDT", "USDC", "GUSD", "USD", "EUR", "GBP", "BTC", "ETH"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn precision_from_step(step: &str) -> Option<u32> {
    let mantissa = step.trim().split(['e', 'E']).next().unwrap_or(step);
    Some(
        mantissa
            .trim_end_matches('0')
            .trim_end_matches('.')
            .split('.')
            .nth(1)
            .map(|fraction| fraction.len() as u32)
            .unwrap_or(0),
    )
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
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
