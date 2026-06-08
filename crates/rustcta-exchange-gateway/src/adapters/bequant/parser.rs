use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let requested = requested_symbols
        .iter()
        .map(|symbol| normalize_bequant_symbol(&symbol.exchange_symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let symbols = value
        .as_object()
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbol response missing object", value))?;
    symbols
        .iter()
        .filter(|(symbol, spec)| {
            (requested.is_empty() || requested.contains(&symbol.to_ascii_uppercase()))
                && spec
                    .get("type")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.eq_ignore_ascii_case("spot"))
        })
        .map(|(symbol, spec)| parse_symbol_rule(exchange_id, symbol, spec))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let base_asset = required_str(exchange_id, value, "base_currency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quote_currency")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = normalize_bequant_symbol(symbol)?;
    let price_increment = string_or_number(value.get("tick_size"));
    let quantity_increment = string_or_number(value.get("quantity_increment"));
    let take_rate = decimal_value_to_f64(value.get("take_rate"))?.unwrap_or(0.0);
    let make_rate = decimal_value_to_f64(value.get("make_rate"))?.unwrap_or(0.0);
    let fee_known = take_rate >= 0.0 && make_rate >= 0.0;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("working")
        .to_ascii_lowercase();
    let trading_enabled = matches!(status.as_str(), "working" | "online" | "active");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: quantity_increment.clone(),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_increment(price_increment.as_deref()),
        quantity_precision: precision_from_increment(quantity_increment.as_deref()),
        supports_market_orders: trading_enabled,
        supports_limit_orders: trading_enabled,
        supports_post_only: trading_enabled && fee_known,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bid").or_else(|| value.get("bids")))?;
    let asks = parse_levels(exchange_id, value.get("ask").or_else(|| value.get("asks")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bequant order book request requires canonical_symbol".to_string(),
            })?;
    let timestamp = first_timestamp(value, &["timestamp", "time"]).unwrap_or_else(Utc::now);
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        timestamp,
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = value
        .get("sequence")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()));
    Ok(snapshot)
}

pub fn normalize_bequant_symbol(symbol: &str) -> ExchangeApiResult<String> {
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

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
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
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bequant decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
            .map(|time| time.with_timezone(&Utc))
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn precision_from_increment(increment: Option<&str>) -> Option<u32> {
    let text = increment?.trim().trim_end_matches('0');
    text.split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
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
