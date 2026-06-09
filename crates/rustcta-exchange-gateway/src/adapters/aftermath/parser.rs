use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PositionSourceBoundaryAudit {
    pub boundary: String,
    pub required_sources: Vec<String>,
    pub position_fields: Vec<String>,
    pub reconciliation_required: Vec<String>,
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Aftermath markets response must be an array",
            value,
        )
    })?;
    markets
        .iter()
        .filter(|market| is_perpetual_market(market))
        .map(|market| parse_market(exchange_id, market))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            value
                .get("symbol")
                .and_then(Value::as_str)
                .and_then(|symbol| canonical_from_ccxt_symbol(symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Aftermath order book requires canonical_symbol or response symbol"
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
    snapshot.sequence = value.get("nonce").and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

#[cfg(test)]
pub(super) fn parse_position_source_boundary(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<PositionSourceBoundaryAudit> {
    require_text(exchange_id, value, "exchange", "aftermath")?;
    require_text(exchange_id, value, "operation", "get_positions")?;
    require_bool(exchange_id, value, "runtime_enabled", false)?;

    let fixture_policy = value.get("fixture_policy").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Aftermath position source boundary missing fixture_policy",
            value,
        )
    })?;
    require_bool(exchange_id, fixture_policy, "live_call_allowed", false)?;

    Ok(PositionSourceBoundaryAudit {
        boundary: required_str(exchange_id, value, "boundary")?.to_string(),
        required_sources: required_string_array(exchange_id, value, &["source", "requires"])?,
        position_fields: required_string_array(exchange_id, value, &["position_fields"])?,
        reconciliation_required: required_string_array(
            exchange_id,
            value,
            &["reconciliation_required"],
        )?,
    })
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let market_id = required_str(exchange_id, value, "id")?.to_string();
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let canonical_symbol = canonical_from_ccxt_symbol(symbol_text)?;
    let base_asset = value
        .get("base")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, market_id)
            .map_err(validation_error)?,
    };
    let precision = value.get("precision");
    let limits = value.get("limits");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: decimal_path(precision, &["price"]),
        quantity_increment: decimal_path(precision, &["amount"]),
        min_price: decimal_path(limits, &["price", "min"]),
        max_price: decimal_path(limits, &["price", "max"]),
        min_quantity: decimal_path(limits, &["amount", "min"]),
        max_quantity: decimal_path(limits, &["amount", "max"]),
        min_notional: decimal_path(limits, &["cost", "min"]),
        max_notional: decimal_path(limits, &["cost", "max"]),
        price_precision: decimal_path(precision, &["price"])
            .and_then(|value| precision_hint(&value)),
        quantity_precision: decimal_path(precision, &["amount"])
            .and_then(|value| precision_hint(&value)),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn is_perpetual_market(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|market_type| market_type.eq_ignore_ascii_case("swap"))
        || value.get("swap").and_then(Value::as_bool).unwrap_or(false)
}

fn canonical_from_ccxt_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let pair = symbol
        .split_once(':')
        .map(|(pair, _)| pair)
        .unwrap_or(symbol)
        .trim();
    let (base, quote) = pair
        .split_once('/')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Aftermath canonical symbol from {symbol}"),
        })?;
    CanonicalSymbol::new(base, quote).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Aftermath order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let values = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Aftermath order book level must be a [price, amount] array",
                    level,
                )
            })?;
            let price = values
                .first()
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = values
                .get(1)
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level amount", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn decimal_path(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut cursor = value?;
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

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("Aftermath market missing {field}"),
            value,
        )
    })
}

#[cfg(test)]
fn require_text(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
    expected: &str,
) -> ExchangeApiResult<()> {
    let actual = required_str(exchange_id, value, field)?;
    if actual == expected {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        format!("Aftermath source boundary {field} expected {expected}, got {actual}"),
        value,
    ))
}

#[cfg(test)]
fn require_bool(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
    expected: bool,
) -> ExchangeApiResult<()> {
    match value.get(field).and_then(Value::as_bool) {
        Some(actual) if actual == expected => Ok(()),
        _ => Err(parse_error(
            exchange_id.clone(),
            format!("Aftermath source boundary {field} expected {expected}"),
            value,
        )),
    }
}

#[cfg(test)]
fn required_string_array(
    exchange_id: &ExchangeId,
    value: &Value,
    path: &[&str],
) -> ExchangeApiResult<Vec<String>> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!("Aftermath source boundary missing {}", path.join(".")),
                value,
            )
        })?;
    }
    let values = cursor.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!(
                "Aftermath source boundary {} must be an array",
                path.join(".")
            ),
            cursor,
        )
    })?;
    let parsed = values
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!(
                    "Aftermath source boundary {} must contain strings",
                    path.join(".")
                ),
                cursor,
            )
        })?;
    if parsed.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            format!(
                "Aftermath source boundary {} must not be empty",
                path.join(".")
            ),
            cursor,
        ));
    }
    Ok(parsed)
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
