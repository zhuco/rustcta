use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::{Map, Value};

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let pairs = pairs_object(value)?;
    pairs
        .iter()
        .map(|(symbol, data)| parse_symbol_rule(exchange_id, symbol, data))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let base_asset =
        required_str_any(value, &["pair_base", "base_currency", "base"])?.to_ascii_uppercase();
    let quote_asset =
        required_str_any(value, &["pair_2", "quote_currency", "quote"])?.to_ascii_uppercase();
    let active = bool_or_default(value.get("active"), true)
        && bool_or_default(value.get("verified"), true)
        && bool_or_default(value.get("is_public"), true);
    let normalized_symbol = normalize_hollaex_symbol_text(symbol)?;
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
                normalized_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("increment_price")),
        quantity_increment: string_or_number(value.get("increment_size")),
        min_price: string_or_number(value.get("min_price")),
        max_price: string_or_number(value.get("max_price")),
        min_quantity: string_or_number(value.get("min_size")),
        max_quantity: string_or_number(value.get("max_size")),
        min_notional: None,
        max_notional: None,
        price_precision: value
            .get("increment_price")
            .and_then(|value| string_or_number(Some(value)))
            .map(|value| precision_from_decimal(&value)),
        quantity_precision: value
            .get("increment_size")
            .and_then(|value| string_or_number(Some(value)))
            .map(|value| precision_from_decimal(&value)),
        supports_market_orders: active,
        supports_limit_orders: active,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    normalized_symbol: &str,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value
        .get(normalized_symbol)
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| data.get(normalized_symbol))
        })
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let mut bids = parse_levels(exchange_id, book.get("bids"))?;
    let mut asks = parse_levels(exchange_id, book.get("asks"))?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hollaex order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = book.get("timestamp").and_then(parse_datetime_value);
    Ok(snapshot)
}

pub fn normalize_hollaex_symbol(scope: &SymbolScope) -> ExchangeApiResult<String> {
    if scope.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "hollaex.non_spot_market_type",
        });
    }
    if let Some(canonical_symbol) = &scope.canonical_symbol {
        return Ok(format!(
            "{}-{}",
            canonical_symbol.base_asset(),
            canonical_symbol.quote_asset()
        )
        .to_ascii_lowercase());
    }
    normalize_hollaex_symbol_text(&scope.exchange_symbol.symbol)
}

pub(super) fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    exchange_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = normalize_hollaex_symbol_text(exchange_symbol)?;
    let (base, quote) =
        split_symbol_guess(&exchange_symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer canonical HollaEx symbol from {exchange_symbol}"),
        })?;
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

pub(super) fn normalize_hollaex_symbol_text(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '_'], "-").to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "HollaEx symbol must not be empty".to_string(),
        });
    }
    if normalized.ends_with("-perp") {
        return Err(ExchangeApiError::Unsupported {
            operation: "hollaex.derivatives_not_documented_for_demo_profile",
        });
    }
    if normalized.contains('-') {
        Ok(normalized)
    } else if let Some((base, quote)) = split_symbol_guess(&normalized) {
        Ok(format!("{base}-{quote}"))
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!("HollaEx symbol must include base/quote separator: {symbol}"),
        })
    }
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(10).clamp(1, 10)
}

pub(super) fn split_symbol_guess(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('-') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
        }
    }
    for quote in ["usdt", "usdc", "btc", "eth", "usd", "eur", "xht"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
            }
        }
    }
    None
}

fn pairs_object(value: &Value) -> ExchangeApiResult<&Map<String, Value>> {
    value
        .get("pairs")
        .or_else(|| value.get("data").and_then(|data| data.get("pairs")))
        .and_then(Value::as_object)
        .ok_or_else(|| {
            parse_error(
                ExchangeId::new("hollaex").unwrap_or_else(|_| ExchangeId::new("unknown").unwrap()),
                "HollaEx constants response missing pairs object",
                value,
            )
        })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "HollaEx order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "HollaEx order book level is not an array",
                    level,
                )
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

fn required_str_any<'a>(value: &'a Value, fields: &[&str]) -> ExchangeApiResult<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("HollaEx constants pair missing one of {fields:?}"),
        })
}

fn bool_or_default(value: Option<&Value>, default: bool) -> bool {
    value.and_then(Value::as_bool).unwrap_or(default)
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub(super) fn parse_datetime_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(timestamp) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        let millis = if timestamp > 9_999_999_999 {
            timestamp
        } else {
            timestamp * 1_000
        };
        return DateTime::<Utc>::from_timestamp_millis(millis);
    }
    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
}

fn precision_from_decimal(value: &str) -> u32 {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
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
