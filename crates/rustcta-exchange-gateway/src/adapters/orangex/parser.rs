use chrono::{DateTime, Utc};
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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OrangeX get_instruments response is not an array",
            value,
        )
    })?;
    instruments
        .iter()
        .filter(|instrument| instrument_matches_market(instrument, market_type))
        .map(|instrument| parse_symbol_rule(exchange_id, market_type, instrument))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_orangex_symbol(
        required_str(exchange_id, value, "instrument_name")?,
        market_type,
    )?;
    let (base_asset, quote_asset) = split_orangex_symbol(&exchange_symbol)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let active = value
        .get("is_active")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tick_size")),
        quantity_increment: string_or_number(
            value
                .get("instr_multiple")
                .or_else(|| value.get("quantity_tick_size"))
                .or_else(|| value.get("step_size")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("min_qty")
                .or_else(|| value.get("min_trade_amount")),
        ),
        max_quantity: string_or_number(value.get("max_qty")),
        min_notional: string_or_number(value.get("min_notional")),
        max_notional: None,
        price_precision: precision_from_step_value(value.get("tick_size")),
        quantity_precision: precision_from_step_value(
            value
                .get("instr_multiple")
                .or_else(|| value.get("quantity_tick_size"))
                .or_else(|| value.get("step_size")),
        ),
        supports_market_orders: active,
        supports_limit_orders: active,
        supports_post_only: active,
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "orangex order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value.get("version").and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_orangex_symbol(
    symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let normalized = match market_type {
        MarketType::Spot => normalized
            .strip_suffix("-SPOT")
            .unwrap_or(&normalized)
            .to_string(),
        MarketType::Perpetual => {
            if normalized.ends_with("-PERPETUAL") {
                normalized
            } else {
                format!("{normalized}-PERPETUAL")
            }
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.unsupported_market_type",
            });
        }
    };
    split_orangex_symbol(&normalized)?;
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
}

fn instrument_matches_market(value: &Value, market_type: MarketType) -> bool {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let instrument_name = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    match market_type {
        MarketType::Spot => kind == "spot" || instrument_name.ends_with("-SPOT"),
        MarketType::Perpetual => {
            instrument_name.ends_with("-PERPETUAL")
                || (kind == "future" && instrument_name.contains("-USDT-"))
        }
        _ => false,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OrangeX order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let level = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "invalid OrangeX order book level",
                    level,
                )
            })?;
            let price = number_from_value(level.first()).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid level price", &Value::Null)
            })?;
            let quantity = number_from_value(level.get(1)).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid level quantity", &Value::Null)
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub(super) fn split_orangex_instrument(
    symbol: &str,
) -> ExchangeApiResult<(MarketType, String, String)> {
    let market_type = if symbol.trim().to_ascii_uppercase().ends_with("-PERPETUAL") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    };
    let (base, quote) = split_orangex_symbol(symbol)?;
    Ok((market_type, base, quote))
}

fn split_orangex_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let symbol = symbol.trim().to_ascii_uppercase();
    let without_suffix = symbol
        .strip_suffix("-SPOT")
        .or_else(|| symbol.strip_suffix("-MARGIN"))
        .or_else(|| symbol.strip_suffix("-PERPETUAL"))
        .unwrap_or(&symbol);
    let mut parts = without_suffix.split('-');
    let base = parts.next().unwrap_or_default();
    let quote = parts.next().unwrap_or_default();
    if base.is_empty() || quote.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot normalize OrangeX symbol {symbol}"),
        });
    }
    Ok((base.to_string(), quote.to_string()))
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("OrangeX response missing field {field}"),
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

pub(super) fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn parse_millis(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn precision_from_step_value(value: Option<&Value>) -> Option<u32> {
    let text = string_or_number(value)?;
    let trimmed = text.trim();
    let (_, decimals) = trimmed.split_once('.')?;
    Some(decimals.trim_end_matches('0').len() as u32)
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

pub(super) fn market_currency(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("SPOT"),
        MarketType::Perpetual => Ok("USDT"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "orangex.unsupported_market_type",
        }),
    }
}

pub(super) fn market_kind(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("spot"),
        MarketType::Perpetual => Ok("perpetual"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "orangex.unsupported_market_type",
        }),
    }
}
