#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn cryptomus_symbol(symbol: &SymbolScope) -> String {
    normalize_cryptomus_symbol(&symbol.exchange_symbol.symbol)
}

pub fn normalize_cryptomus_symbol(symbol: &str) -> String {
    symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase()
}

pub fn parse_cryptomus_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = items_array(value)?;
    let requested_symbols = requested.iter().map(cryptomus_symbol).collect::<Vec<_>>();
    let mut rules = Vec::new();
    for entry in markets {
        let raw_symbol = string_field(entry, "symbol")
            .or_else(|_| string_field(entry, "currency_pair"))
            .or_else(|_| string_field(entry, "currencyPair"))?;
        let normalized_symbol = normalize_cryptomus_symbol(&raw_symbol);
        if !requested_symbols.is_empty() && !requested_symbols.contains(&normalized_symbol) {
            continue;
        }
        let (base, quote) = symbol_assets(entry, &normalized_symbol)?;
        let base_prec = precision(entry.get("basePrec").or_else(|| entry.get("base_prec")));
        let quote_prec = precision(entry.get("quotePrec").or_else(|| entry.get("quote_prec")));
        let canonical = CanonicalSymbol::new(&base, &quote)
            .map_err(|error| invalid(format!("invalid cryptomus canonical symbol: {error}")))?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized_symbol)
                .map_err(|error| invalid(format!("invalid cryptomus exchange symbol: {error}")))?;
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical),
            exchange_symbol,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset: base,
            quote_asset: quote,
            price_increment: quote_prec.map(decimal_increment),
            quantity_increment: base_prec.map(decimal_increment),
            min_price: None,
            max_price: None,
            min_quantity: optional_string(entry, "baseMinSize")
                .or_else(|| optional_string(entry, "base_min_size")),
            max_quantity: optional_string(entry, "baseMaxSize")
                .or_else(|| optional_string(entry, "base_max_size")),
            min_notional: optional_string(entry, "quoteMinSize")
                .or_else(|| optional_string(entry, "quote_min_size")),
            max_notional: optional_string(entry, "quoteMaxSize")
                .or_else(|| optional_string(entry, "quote_max_size")),
            price_precision: quote_prec,
            quantity_precision: base_prec,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_cryptomus_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_levels(data.get("bids"))?;
    let asks = parse_levels(data.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("cryptomus orderbook requires canonical symbol"))?;
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(|error| invalid(format!("invalid cryptomus orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.exchange_timestamp = data.get("timestamp").and_then(parse_timestamp);
    Ok(snapshot)
}

pub(super) fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let raw_symbol = value
        .get("symbol")
        .or_else(|| value.get("market"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("cryptomus payload missing symbol"))?;
    let normalized = normalize_cryptomus_symbol(raw_symbol);
    let (base, quote) = split_symbol(&normalized)
        .ok_or_else(|| invalid(format!("cannot infer Cryptomus symbol from {raw_symbol}")))?;
    let canonical = CanonicalSymbol::new(&base, &quote)
        .map_err(|error| invalid(format!("invalid cryptomus canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
        .map_err(|error| invalid(format!("invalid cryptomus exchange symbol: {error}")))?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

pub(super) fn items_array(value: &Value) -> ExchangeApiResult<&Vec<Value>> {
    value
        .get("result")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| invalid("cryptomus response missing result array"))
}

pub(super) fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|field_value| {
        field_value
            .as_str()
            .map(str::to_string)
            .or_else(|| field_value.as_f64().map(|number| number.to_string()))
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
            value
                .as_f64()
                .or_else(|| value.as_str()?.parse::<f64>().ok())
                .ok_or_else(|| invalid(format!("cryptomus invalid decimal value {value}")))
        })
        .transpose()
}

pub(super) fn parse_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(integer) = value.as_i64() {
        if integer > 1_000_000_000_000 {
            return Utc.timestamp_millis_opt(integer).single();
        }
        return Utc.timestamp_opt(integer, 0).single();
    }
    let text = value.as_str()?;
    if let Ok(float_seconds) = text.parse::<f64>() {
        let seconds = float_seconds.trunc() as i64;
        let nanos = ((float_seconds.fract()) * 1_000_000_000.0).round() as u32;
        return Utc.timestamp_opt(seconds, nanos).single();
    }
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|datetime| DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc))
}

pub(super) fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    invalid(error.to_string())
}

fn symbol_assets(value: &Value, normalized_symbol: &str) -> ExchangeApiResult<(String, String)> {
    let base = value
        .get("baseCurrency")
        .or_else(|| value.get("base_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    let quote = value
        .get("quoteCurrency")
        .or_else(|| value.get("quote_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    match (base, quote) {
        (Some(base), Some(quote)) => Ok((base, quote)),
        _ => split_symbol(normalized_symbol).ok_or_else(|| {
            invalid(format!(
                "cannot infer Cryptomus symbol from {normalized_symbol}"
            ))
        }),
    }
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn precision(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
        _ => None,
    })
}

fn decimal_increment(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("cryptomus orderbook levels must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let (price, quantity) = if let Some(array) = level.as_array() {
                (array.first(), array.get(1))
            } else {
                (level.get("price"), level.get("quantity"))
            };
            let price = decimal_value_to_f64(price)?.ok_or_else(|| invalid("missing price"))?;
            let quantity =
                decimal_value_to_f64(quantity)?.ok_or_else(|| invalid("missing quantity"))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("cryptomus missing string field {field}")))
}
