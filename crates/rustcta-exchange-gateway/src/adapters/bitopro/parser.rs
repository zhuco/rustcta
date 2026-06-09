use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn normalize_bitopro_pair(value: &str) -> ExchangeApiResult<String> {
    let pair = value.trim().replace(['/', '-'], "_").to_ascii_lowercase();
    if pair.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitopro pair must not be empty".to_string(),
        });
    }
    Ok(pair)
}

pub fn bitopro_ws_pair(value: &str) -> ExchangeApiResult<String> {
    Ok(normalize_bitopro_pair(value)?.to_ascii_uppercase())
}

pub fn canonical_from_pair(pair: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let pair = normalize_bitopro_pair(pair)?;
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("bitopro pair {pair} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id, "trading-pairs data array", value))?;
    rows.iter()
        .map(|row| {
            let pair = normalize_bitopro_pair(required_str(exchange_id, row, "pair")?)?;
            let base_asset = required_str(exchange_id, row, "base")?.to_ascii_uppercase();
            let quote_asset = required_str(exchange_id, row, "quote")?.to_ascii_uppercase();
            let canonical_symbol =
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
            let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
                .map_err(validation_error)?;
            let price_precision = integer_field(row, "quotePrecision");
            let quantity_precision = integer_field(row, "amountPrecision")
                .or_else(|| integer_field(row, "basePrecision"));
            let maintain = row
                .get("maintain")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical_symbol),
                    exchange_symbol,
                },
                base_asset,
                quote_asset,
                price_increment: price_precision.map(increment_from_precision),
                quantity_increment: quantity_precision.map(increment_from_precision),
                min_price: None,
                max_price: None,
                min_quantity: string_or_number(row.get("minLimitBaseAmount")),
                max_quantity: string_or_number(row.get("maxLimitBaseAmount")),
                min_notional: string_or_number(row.get("minMarketBuyQuoteAmount")),
                max_notional: None,
                price_precision,
                quantity_precision,
                supports_market_orders: !maintain,
                supports_limit_orders: !maintain,
                supports_post_only: !maintain,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitopro order book request requires canonical_symbol".to_string(),
            })?;
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
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
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .and_then(value_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
        .or_else(|| {
            data.get("datetime")
                .and_then(Value::as_str)
                .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
                .map(|value| value.with_timezone(&Utc))
        });
    Ok(snapshot)
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    match depth.unwrap_or(5) {
        0 | 1 => 1,
        2..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=30 => 30,
        _ => 50,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "order book levels", &Value::Null))?;
    rows.iter()
        .map(|row| {
            let (price, quantity) = if let Some(level) = row.as_array() {
                (level.first(), level.get(1))
            } else {
                (row.get("price"), row.get("amount"))
            };
            let price = price
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level price", row))?;
            let quantity = quantity
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level amount", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id, &format!("missing field {field}"), value))
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

pub(super) fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

pub(super) fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn integer_field(value: &Value, field: &str) -> Option<u32> {
    value.get(field).and_then(|value| {
        value
            .as_u64()
            .map(|value| value as u32)
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

pub(super) fn parse_error(
    exchange_id: &ExchangeId,
    message: &str,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("bitopro parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
