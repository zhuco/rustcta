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
    let symbols = value
        .as_array()
        .or_else(|| value.get("data").and_then(Value::as_array))
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols response missing data", value))?;
    symbols
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "baseCurrency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteCurrency")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
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
    let price_increment = string_or_number(
        value
            .get("tickSize")
            .or_else(|| value.get("priceIncrement")),
    );
    let quantity_increment =
        string_or_number(value.get("lotSize").or_else(|| value.get("baseIncrement")));
    let tradable = value
        .get("enableTrading")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("minOrderQty")
                .or_else(|| value.get("baseMinSize")),
        ),
        max_quantity: string_or_number(
            value
                .get("maxOrderQty")
                .or_else(|| value.get("baseMaxSize")),
        ),
        min_notional: string_or_number(
            value
                .get("maxRiskLimit")
                .or_else(|| value.get("quoteMinSize")),
        ),
        max_notional: None,
        price_precision: precision_from_step(price_increment.as_deref()),
        quantity_precision: precision_from_step(quantity_increment.as_deref()),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: tradable,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
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
                message: "kucoinfutures order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("sequence")
        .or_else(|| data.get("sequenceStart"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("time")
        .or_else(|| data.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_kucoinfutures_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(trimmed.replace(['/', '_', '-'], "").to_ascii_uppercase())
}

pub fn orderbook_endpoint(depth: u32) -> &'static str {
    if depth <= 20 {
        "/api/v1/level2/snapshot"
    } else {
        "/api/v1/level2/snapshot"
    }
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=20 => 20,
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

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid KuCoin decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?.trim();
    if step.is_empty() {
        return None;
    }
    let mantissa = step.split(['e', 'E']).next().unwrap_or(step);
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

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field).and_then(value_as_i64))
        .find_map(kucoinfutures_timestamp_to_datetime)
}

fn kucoinfutures_timestamp_to_datetime(raw: i64) -> Option<DateTime<Utc>> {
    let millis = if raw > 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw > 10_000_000_000 {
        raw
    } else {
        raw * 1_000
    };
    DateTime::<Utc>::from_timestamp_millis(millis)
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
