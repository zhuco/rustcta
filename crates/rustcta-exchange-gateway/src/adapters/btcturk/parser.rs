#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn btcturk_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn parse_btcturk_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let symbols = value
        .get("data")
        .and_then(|data| data.get("symbols"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("btcturk exchangeinfo missing data.symbols"))?;
    let mut rules = Vec::new();
    for entry in symbols {
        let raw_symbol = string_field(entry, "name")
            .or_else(|_| string_field(entry, "symbol"))
            .or_else(|_| string_field(entry, "pair"))?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| btcturk_symbol(symbol) == raw_symbol.to_ascii_uppercase())
        {
            continue;
        }
        let base =
            string_field(entry, "numerator").or_else(|_| string_field(entry, "numeratorSymbol"))?;
        let quote = string_field(entry, "denominator")
            .or_else(|_| string_field(entry, "denominatorSymbol"))?;
        let scope = symbol_scope(exchange_id.clone(), &base, &quote, &raw_symbol)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base.to_ascii_uppercase(),
            quote_asset: quote.to_ascii_uppercase(),
            price_increment: decimal_increment(entry.get("denominatorScale")),
            quantity_increment: decimal_increment(entry.get("numeratorScale")),
            min_price: optional_string(entry, "minLimitOrderPrice"),
            max_price: optional_string(entry, "maxLimitOrderPrice"),
            min_quantity: optional_string(entry, "minAmount"),
            max_quantity: optional_string(entry, "maxAmount"),
            min_notional: optional_string(entry, "minExchangeValue"),
            max_notional: None,
            price_precision: entry
                .get("denominatorScale")
                .and_then(Value::as_u64)
                .map(|value| value as u32),
            quantity_precision: entry
                .get("numeratorScale")
                .and_then(Value::as_u64)
                .map(|value| value as u32),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_btcturk_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("data")
        .ok_or_else(|| invalid("btcturk orderbook missing data"))?;
    let bids = parse_levels(data.get("bids"))?;
    let asks = parse_levels(data.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("btcturk orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid btcturk orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .and_then(Value::as_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(snapshot)
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
        .map_err(|error| invalid(format!("invalid btcturk canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, raw_symbol)
        .map_err(|error| invalid(format!("invalid btcturk exchange symbol: {error}")))?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("btcturk orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let tuple = level
                .as_array()
                .ok_or_else(|| invalid("btcturk orderbook level must be [price, quantity]"))?;
            let price = parse_f64(tuple.first(), "price")?;
            let quantity = parse_f64(tuple.get(1), "quantity")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid btcturk level: {error}")))
        })
        .collect()
}

fn decimal_increment(value: Option<&Value>) -> Option<String> {
    let scale = value.and_then(Value::as_u64)?;
    if scale == 0 {
        return Some("1".to_string());
    }
    Some(format!(
        "0.{}1",
        "0".repeat(scale.saturating_sub(1) as usize)
    ))
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("btcturk missing string field {field}")))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|field_value| {
        field_value
            .as_str()
            .map(str::to_string)
            .or_else(|| field_value.as_f64().map(|number| number.to_string()))
    })
}

fn parse_f64(value: Option<&Value>, field: &str) -> ExchangeApiResult<f64> {
    value
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .or_else(|| value.as_f64())
        })
        .ok_or_else(|| invalid(format!("btcturk invalid numeric {field}")))
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
