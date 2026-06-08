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
    let markets = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bithumb market list response is not an array",
            value,
        )
    })?;
    markets
        .iter()
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let market = required_str(exchange_id, value, "market")?;
    let normalized = normalize_bithumb_symbol(market)?;
    let (quote_asset, base_asset) = split_bithumb_market(&normalized)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    };
    let is_active = value
        .get("market_state")
        .or_else(|| value.get("state"))
        .and_then(Value::as_str)
        .map(|state| state.eq_ignore_ascii_case("active"))
        .unwrap_or(true)
        && !value
            .get("market_warning")
            .and_then(Value::as_str)
            .is_some_and(|warning| warning.eq_ignore_ascii_case("caution"));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: None,
        quantity_increment: None,
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: is_active,
        supports_limit_orders: is_active,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(value);
    let units = book
        .get("orderbook_units")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bithumb order book missing orderbook_units",
                book,
            )
        })?;
    let bids = units
        .iter()
        .map(|unit| parse_level(exchange_id, unit, "bid_price", "bid_size"))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let asks = units
        .iter()
        .map(|unit| parse_level(exchange_id, unit, "ask_price", "ask_size"))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bithumb order book request requires canonical_symbol".to_string(),
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
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bithumb_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let symbol = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if symbol.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bithumb symbol must not be empty".to_string(),
        });
    }
    if symbol.contains('-') {
        return Ok(symbol);
    }
    for quote in ["KRW", "BTC", "USDT"] {
        if let Some(base) = symbol.strip_suffix(quote).filter(|base| !base.is_empty()) {
            return Ok(format!("{quote}-{base}"));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot normalize Bithumb symbol {symbol}"),
    })
}

pub(super) fn split_bithumb_market(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let mut parts = symbol.split('-');
    let quote = parts.next().filter(|part| !part.is_empty());
    let base = parts.next().filter(|part| !part.is_empty());
    if parts.next().is_some() || quote.is_none() || base.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid Bithumb market symbol {symbol}"),
        });
    }
    Ok((quote.unwrap().to_string(), base.unwrap().to_string()))
}

fn parse_level(
    exchange_id: &ExchangeId,
    value: &Value,
    price_field: &str,
    size_field: &str,
) -> ExchangeApiResult<OrderBookLevel> {
    let price = decimal_value_to_f64(value.get(price_field))?.ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Bithumb order book missing {price_field}"),
            value,
        )
    })?;
    let quantity = decimal_value_to_f64(value.get(size_field))?.ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Bithumb order book missing {size_field}"),
            value,
        )
    })?;
    OrderBookLevel::new(price, quantity).map_err(validation_error)
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Bithumb response missing field {field}"),
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
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| match value {
            Value::String(text) => {
                text.parse::<f64>()
                    .map_err(|error| ExchangeApiError::Serialization {
                        message: format!("invalid Bithumb decimal {text}: {error}"),
                    })
            }
            Value::Number(number) => {
                number
                    .as_f64()
                    .ok_or_else(|| ExchangeApiError::Serialization {
                        message: format!("invalid Bithumb number {number}"),
                    })
            }
            _ => Err(ExchangeApiError::Serialization {
                message: format!("invalid Bithumb decimal value {value}"),
            }),
        })
        .transpose()
}

pub(super) fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    for field in fields {
        if let Some(timestamp) = value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
        {
            return Some(timestamp);
        }
        if let Some(timestamp) = value
            .get(*field)
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|timestamp| timestamp.with_timezone(&Utc))
        {
            return Some(timestamp);
        }
    }
    None
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn bithumb_symbol_scope(
    exchange_id: &ExchangeId,
    symbol: &str,
) -> ExchangeApiResult<rustcta_exchange_api::SymbolScope> {
    let normalized = normalize_bithumb_symbol(symbol)?;
    let (quote, base) = split_bithumb_market(&normalized)?;
    Ok(rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(&base, &quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

pub(super) fn exchange_error(
    exchange_id: ExchangeId,
    class: ExchangeErrorClass,
    message: &str,
    code: Option<String>,
    raw: Option<Value>,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange_id, class, message, Utc::now());
    error.code = code;
    error.raw = raw;
    ExchangeApiError::Exchange(error)
}

pub(super) fn schema_version() -> SchemaVersion {
    SchemaVersion::current()
}
