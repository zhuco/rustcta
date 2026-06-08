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
    let markets = value
        .get("markets")
        .or_else(|| value.get("data").and_then(|data| data.get("markets")))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "markets response missing data", value))?;
    markets
        .iter()
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let base_asset = required_str(exchange_id, value, "coin")?.to_ascii_uppercase();
    let quote_asset = value
        .get("market")
        .and_then(Value::as_str)
        .unwrap_or("AUD")
        .to_ascii_uppercase();
    if quote_asset != "AUD" {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinspot.non_aud_market",
        });
    }
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol = format!("{base_asset}/{quote_asset}");
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status.eq_ignore_ascii_case("online"))
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                exchange_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("price_tick"))
            .or(Some("0.00000001".to_string())),
        quantity_increment: string_or_number(value.get("amount_tick"))
            .or(Some("0.00000001".to_string())),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minimum_amount")),
        max_quantity: None,
        min_notional: string_or_number(value.get("minimum_total")),
        max_notional: None,
        price_precision: Some(8),
        quantity_precision: Some(8),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let buys = value
        .get("buyorders")
        .or_else(|| value.get("buy"))
        .or_else(|| value.get("bids"));
    let sells = value
        .get("sellorders")
        .or_else(|| value.get("sell"))
        .or_else(|| value.get("asks"));
    let mut bids = parse_levels(exchange_id, buys)?;
    let mut asks = parse_levels(exchange_id, sells)?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinspot order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = first_timestamp(value, &["timestamp", "time"]);
    Ok(snapshot)
}

pub fn normalize_coinspot_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    let (base, quote) = if let Some((base, quote)) = upper.split_once('/') {
        (base.to_string(), quote.to_string())
    } else if let Some(base) = upper.strip_suffix("AUD") {
        (base.to_string(), "AUD".to_string())
    } else {
        (upper, "AUD".to_string())
    };
    if base.is_empty() || quote.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid CoinSpot symbol {symbol}"),
        });
    }
    if quote != "AUD" {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinspot.non_aud_market",
        });
    }
    Ok((base.to_ascii_lowercase(), "aud".to_string()))
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(20).clamp(1, 100)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinSpot order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("rate")
                .or_else(|| level.get("price"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("amount")
                .or_else(|| level.get("quantity"))
                .or_else(|| level.get("volume"))
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

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid CoinSpot decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(timestamp_value_to_datetime)
}

fn timestamp_value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        if number > 1_000_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(number);
        }
        return Utc.timestamp_opt(number, 0).single();
    }
    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
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

pub(super) fn schema_version() -> SchemaVersion {
    SchemaVersion::current()
}
