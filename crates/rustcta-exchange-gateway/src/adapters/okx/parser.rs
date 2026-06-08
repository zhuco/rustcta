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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX instruments response is not an array",
            value,
        )
    })?;
    instruments
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, market_type, value))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "instId")?.to_ascii_uppercase();
    let (base_asset, quote_asset) = if market_type == MarketType::Spot {
        (
            required_str(exchange_id, value, "baseCcy")?.to_ascii_uppercase(),
            required_str(exchange_id, value, "quoteCcy")?.to_ascii_uppercase(),
        )
    } else {
        let (fallback_base, fallback_quote) =
            split_okx_inst_id(&exchange_symbol).unwrap_or_default();
        let base_asset = value
            .get("baseCcy")
            .or_else(|| value.get("ctValCcy"))
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback_base);
        let quote_asset = value
            .get("quoteCcy")
            .or_else(|| value.get("settleCcy"))
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback_quote);
        if base_asset.is_empty() || quote_asset.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("OKX instrument missing base/quote assets: {value}"),
            });
        }
        (base_asset, quote_asset)
    };
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let tradable = value
        .get("state")
        .and_then(Value::as_str)
        .is_none_or(|state| state.eq_ignore_ascii_case("live"));
    let price_increment = string_or_number(value.get("tickSz"));
    let quantity_increment = string_or_number(value.get("lotSz"));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minSz")),
        max_quantity: string_or_number(value.get("maxLmtSz"))
            .or_else(|| string_or_number(value.get("maxMktSz"))),
        min_notional: string_or_number(value.get("minLmtAmt"))
            .or_else(|| string_or_number(value.get("minMktAmt"))),
        max_notional: None,
        price_precision: precision_from_step(price_increment.as_deref()),
        quantity_precision: precision_from_step(quantity_increment.as_deref()),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(value);
    let bids = parse_levels(exchange_id, payload.get("bids"))?;
    let asks = parse_levels(exchange_id, payload.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = payload
        .get("ts")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_okx_symbol(symbol: &str) -> ExchangeApiResult<String> {
    normalize_okx_symbol_for_market(symbol, MarketType::Spot)
}

pub fn normalize_okx_symbol_for_market(
    symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let normalized_key = trimmed.replace(['-', '/', '_'], "").to_ascii_uppercase();
    let upper = trimmed.replace(['/', '_'], "-").to_ascii_uppercase();
    if market_type == MarketType::Perpetual && upper.ends_with("-SWAP") {
        return Ok(upper);
    }
    if upper.contains('-') {
        if market_type == MarketType::Perpetual {
            return Ok(format!("{upper}-SWAP"));
        }
        return Ok(upper);
    }
    let normalized =
        split_compact_symbol(&normalized_key).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer OKX instId from {symbol}"),
        })?;
    if market_type == MarketType::Perpetual {
        Ok(format!("{normalized}-SWAP"))
    } else {
        Ok(normalized)
    }
}

pub fn okx_inst_type(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("SPOT"),
        MarketType::Perpetual => Ok("SWAP"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "okx.unsupported_market_type",
        }),
    }
}

pub fn okx_td_mode(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Perpetual => "cross",
        _ => "cash",
    }
}

pub fn split_okx_inst_id(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('-').filter(|part| !part.is_empty());
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    Some((base, quote))
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        _ => 400,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "OKX order book level is not an array",
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

fn split_compact_symbol(symbol: &str) -> Option<String> {
    const QUOTES: [&str; 6] = ["USDT", "USDC", "BTC", "ETH", "EUR", "USD"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| format!("{base}-{quote}"))
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

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?.trim();
    if step.is_empty() {
        return None;
    }
    let normalized = step.trim_end_matches('0').trim_end_matches('.').to_string();
    Some(
        normalized
            .split('.')
            .nth(1)
            .map(|fraction| fraction.len() as u32)
            .unwrap_or(0),
    )
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
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
