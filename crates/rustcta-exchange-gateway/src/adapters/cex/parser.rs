use chrono::{DateTime, TimeZone, Utc};
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
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let pairs = value
        .get("data")
        .and_then(|data| data.get("pairs"))
        .or_else(|| value.get("pairs"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "currency_limits missing pairs", value))?;
    pairs
        .iter()
        .map(|pair| parse_symbol_rule(exchange_id, pair))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let base_asset = required_str(exchange_id, value, "symbol1")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "symbol2")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol_text = format!("{base_asset}/{quote_asset}");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                exchange_symbol_text,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: None,
        quantity_increment: None,
        min_price: string_or_number(value.get("minPrice")),
        max_price: string_or_number(value.get("maxPrice")),
        min_quantity: string_or_number(value.get("minLotSize")),
        max_quantity: string_or_number(value.get("maxLotSize")),
        min_notional: string_or_number(value.get("minLotSizeS2")),
        max_notional: None,
        price_precision: None,
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
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    if let Some(depth) = depth {
        let depth = depth.clamp(1, 100) as usize;
        bids.truncate(depth);
        asks.truncate(depth);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cex order book requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("timestamp_ms")
        .and_then(timestamp_value_to_datetime)
        .or_else(|| value.get("timestamp").and_then(timestamp_value_to_datetime));
    Ok(snapshot)
}

pub fn normalize_cex_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cex symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed
        .replace(':', "/")
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    let (base, quote) = if let Some((base, quote)) = normalized.split_once('/') {
        (base.to_string(), quote.to_string())
    } else {
        split_suffix_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer CEX.IO base/quote from symbol {symbol}"),
        })?
    };
    if base.is_empty() || quote.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid CEX.IO symbol {symbol}"),
        });
    }
    Ok((base, quote))
}

pub fn cex_pair_string(symbol: &str) -> ExchangeApiResult<String> {
    let (base, quote) = normalize_cex_symbol(symbol)?;
    Ok(format!("{base}:{quote}"))
}

fn split_suffix_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in [
        "USDT", "USDC", "USD", "EUR", "GBP", "BTC", "ETH", "BCH", "LTC",
    ] {
        let Some(base) = symbol.strip_suffix(quote) else {
            continue;
        };
        if !base.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    None
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level
                .as_array()
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level", level))?;
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

fn required_str<'a>(
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

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn timestamp_value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        if number > 1_000_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(number);
        }
        return Utc.timestamp_opt(number, 0).single();
    }
    DateTime::parse_from_rfc3339(value.as_str()?)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message,
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
