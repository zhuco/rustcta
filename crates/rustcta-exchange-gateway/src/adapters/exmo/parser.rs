use chrono::{DateTime, TimeZone, Utc};
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
    let pairs = value.as_object().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "EXMO pair_settings response must be an object",
            value,
        )
    })?;
    pairs
        .iter()
        .map(|(pair, settings)| parse_symbol_rule(exchange_id, pair, settings))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    pair: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let (base_asset, quote_asset) = split_pair(pair)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_exmo_exchange_symbol(pair)?,
        )
        .map_err(validation_error)?,
    };
    let price_precision = value
        .get("price_precision")
        .and_then(value_as_u32)
        .or_else(|| precision_from_decimal(string_or_number(value.get("min_price")).as_deref()));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: None,
        min_price: string_or_number(value.get("min_price")),
        max_price: string_or_number(value.get("max_price")),
        min_quantity: string_or_number(value.get("min_quantity")),
        max_quantity: string_or_number(value.get("max_quantity")),
        min_notional: string_or_number(value.get("min_amount")),
        max_notional: string_or_number(value.get("max_amount")),
        price_precision,
        quantity_precision: None,
        supports_market_orders: true,
        supports_limit_orders: true,
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
    let pair = normalize_exmo_exchange_symbol(&symbol.exchange_symbol.symbol)?;
    let book = value.get(&pair).unwrap_or(value);
    let bids = parse_levels(
        exchange_id,
        book.get("bid").or_else(|| book.get("bids")),
        true,
    )?;
    let asks = parse_levels(
        exchange_id,
        book.get("ask").or_else(|| book.get("asks")),
        false,
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "EXMO order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = first_timestamp(book, &["updated", "date", "timestamp"]);
    Ok(snapshot)
}

pub fn normalize_exmo_exchange_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "EXMO symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') {
        let (base, quote) = split_pair(&normalized)?;
        return Ok(format!("{base}_{quote}"));
    }
    for quote in ["USDT", "USD", "EUR", "UAH", "PLN", "BTC", "ETH"] {
        if let Some(base) = normalized
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
        {
            return Ok(format!("{base}_{quote}"));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot infer EXMO pair from {symbol}"),
    })
}

pub fn split_pair(pair: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = pair.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    let (base, quote) =
        normalized
            .split_once('_')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("EXMO pair {pair} must use BASE_QUOTE form"),
            })?;
    if base.is_empty() || quote.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("EXMO pair {pair} has empty base or quote"),
        });
    }
    Ok((base.to_string(), quote.to_string()))
}

pub(super) fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    bids: bool,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "EXMO order book response missing levels",
            &Value::Null,
        )
    })?;
    let mut parsed = levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid EXMO level price", level)
                })?;
                let quantity = array.get(1).and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid EXMO level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level.get("price").and_then(decimal_as_f64).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid EXMO level price", level)
            })?;
            let quantity = level
                .get("quantity")
                .or_else(|| level.get("qty"))
                .and_then(decimal_as_f64)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid EXMO level quantity", level)
                })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    if bids {
        parsed.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        parsed.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(parsed)
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid EXMO decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        let value = value.get(*field)?;
        if let Some(millis) = value.as_i64().filter(|millis| *millis > 10_000_000_000) {
            return DateTime::<Utc>::from_timestamp_millis(millis);
        }
        let seconds = value.as_i64().or_else(|| value.as_str()?.parse().ok())?;
        Utc.timestamp_opt(seconds, 0).single()
    })
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn decimal_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn precision_from_decimal(value: Option<&str>) -> Option<u32> {
    let value = value?;
    let decimals = value.split_once('.')?.1.trim_end_matches('0');
    Some(decimals.len() as u32)
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

#[allow(dead_code)]
pub(super) fn schema_version() -> SchemaVersion {
    SchemaVersion::current()
}
