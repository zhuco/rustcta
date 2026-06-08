use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

const IDR_SUFFIX: &str = "_idr";

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let pairs = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Indodax pairs response missing array",
            value,
        )
    })?;
    pairs
        .iter()
        .filter(|pair| {
            pair.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.to_ascii_lowercase().ends_with(IDR_SUFFIX))
        })
        .map(|pair| parse_symbol_rule(exchange_id, pair))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_indodax_symbol(required_str(exchange_id, value, "id")?)?;
    let base_asset = value
        .get("traded_currency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_indodax_symbol(&exchange_symbol).map(|(base, _)| base))
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let quote_asset = value
        .get("base_currency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_indodax_symbol(&exchange_symbol).map(|(_, quote)| quote))
        .unwrap_or_else(|| "IDR".to_string());
    let price_precision = value
        .get("price_precision")
        .and_then(value_as_u32)
        .or_else(|| value.get("price_round").and_then(value_as_u32));
    let quantity_precision = value
        .get("volume_precision")
        .and_then(value_as_u32)
        .or_else(|| {
            value.get("trade_min_traded_currency").and_then(|raw| {
                string_or_number(Some(raw)).map(|text| precision_from_decimal(&text))
            })
        });
    let min_quantity = value
        .get("trade_min_traded_currency")
        .and_then(|raw| string_or_number(Some(raw)));
    let min_notional = value
        .get("trade_min_base_currency")
        .and_then(|raw| string_or_number(Some(raw)));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                exchange_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: min_quantity.clone(),
        min_price: None,
        max_price: None,
        min_quantity,
        max_quantity: None,
        min_notional,
        max_notional: None,
        price_precision,
        quantity_precision,
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
    stale_book_ms: u64,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("buy").or_else(|| value.get("bids")))?;
    let asks = parse_levels(exchange_id, value.get("sell").or_else(|| value.get("asks")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "indodax order book request requires canonical_symbol".to_string(),
            })?;
    let received_at = Utc::now();
    let exchange_timestamp = value
        .get("server_time")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .map(|timestamp| {
            if timestamp > 9_999_999_999 {
                timestamp
            } else {
                timestamp * 1_000
            }
        })
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        received_at,
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = exchange_timestamp;
    snapshot.is_stale = exchange_timestamp
        .map(|ts| received_at.signed_duration_since(ts).num_milliseconds() > stale_book_ms as i64)
        .unwrap_or(false);
    Ok(snapshot)
}

pub fn normalize_indodax_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace('-', "_").to_ascii_lowercase();
    if normalized.is_empty() || !normalized.contains('_') {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid Indodax symbol {symbol}"),
        });
    }
    if !normalized.ends_with(IDR_SUFFIX) {
        return Err(ExchangeApiError::Unsupported {
            operation: "indodax.non_idr_market",
        });
    }
    Ok(normalized)
}

pub(super) fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    exchange_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = normalize_indodax_symbol(exchange_symbol)?;
    let (base, quote) = split_indodax_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "IDR".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    })
}

pub(super) fn split_indodax_symbol(symbol: &str) -> Option<(String, String)> {
    let (base, quote) = symbol.split_once('_')?;
    Some((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
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
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

pub(super) fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    for field in fields {
        if let Some(timestamp) = value.get(*field).and_then(value_as_i64) {
            let millis = if timestamp > 9_999_999_999 {
                timestamp
            } else {
                timestamp * 1_000
            };
            if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(millis) {
                return Some(dt);
            }
        }
        if let Some(text) = value.get(*field).and_then(Value::as_str) {
            if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
                return Some(dt.with_timezone(&Utc));
            }
        }
    }
    None
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message,
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Indodax order book missing levels",
            value.unwrap_or(&Value::Null),
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let item = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Indodax order book level is not array",
                    level,
                )
            })?;
            let price = decimal_as_f64(item.first()).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Indodax order book level missing price",
                    level,
                )
            })?;
            let quantity = decimal_as_f64(item.get(1)).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Indodax order book level missing quantity",
                    level,
                )
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn precision_from_decimal(value: &str) -> u32 {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
