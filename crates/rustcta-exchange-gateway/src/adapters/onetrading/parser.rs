#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn onetrading_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-'], "_")
        .to_ascii_uppercase()
}

pub fn parse_onetrading_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = value
        .as_array()
        .ok_or_else(|| invalid("onetrading instruments response must be an array"))?;
    let mut rules = Vec::new();
    for entry in instruments {
        let raw_symbol = string_field(entry, "id")?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| onetrading_symbol(symbol) == raw_symbol.to_ascii_uppercase())
        {
            continue;
        }
        if string_field(entry, "type")? != "SPOT" {
            continue;
        }
        if entry
            .get("state")
            .and_then(Value::as_str)
            .is_some_and(|state| state != "ACTIVE")
        {
            continue;
        }
        let base = nested_string_field(entry, "base", "code")?.to_ascii_uppercase();
        let quote = nested_string_field(entry, "quote", "code")?.to_ascii_uppercase();
        let scope = symbol_scope(
            exchange_id.clone(),
            MarketType::Spot,
            &base,
            &quote,
            &raw_symbol,
        )?;
        let price_precision = u32_field(entry, "market_precision");
        let quantity_precision = u32_field(entry, "amount_precision");
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base,
            quote_asset: quote,
            price_increment: precision_increment(price_precision),
            quantity_increment: precision_increment(quantity_precision),
            min_price: optional_string(entry, "min_price"),
            max_price: optional_string(entry, "max_price"),
            min_quantity: optional_string(entry, "min_size"),
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision,
            quantity_precision,
            supports_market_orders: false,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_onetrading_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(value.get("bids"))?;
    let asks = parse_levels(value.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("onetrading orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        symbol.market_type,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid onetrading orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.exchange_timestamp = value
        .get("time")
        .and_then(Value::as_i64)
        .and_then(|nanos| Utc.timestamp_micros(nanos / 1_000).single());
    Ok(snapshot)
}

fn symbol_scope(
    exchange_id: ExchangeId,
    market_type: MarketType,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
        .map_err(|error| invalid(format!("invalid onetrading canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
        .map_err(|error| invalid(format!("invalid onetrading exchange symbol: {error}")))?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("onetrading orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let price = parse_level_number(level, 0, "price")?;
            let quantity = parse_level_number(level, 1, "amount")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid onetrading level: {error}")))
        })
        .collect()
}

fn parse_level_number(level: &Value, index: usize, field: &str) -> ExchangeApiResult<f64> {
    if let Some(array) = level.as_array() {
        return parse_f64(array.get(index), field);
    }
    parse_f64(
        level
            .get(field)
            .or_else(|| (field == "amount").then(|| level.get("size")).flatten()),
        field,
    )
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("onetrading missing string field {field}")))
}

fn nested_string_field(value: &Value, parent: &str, field: &str) -> ExchangeApiResult<String> {
    value
        .get(parent)
        .and_then(|parent_value| parent_value.get(field))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("onetrading missing string field {parent}.{field}")))
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
        .ok_or_else(|| invalid(format!("onetrading invalid numeric {field}")))
}

fn u32_field(value: &Value, field: &str) -> Option<u32> {
    value
        .get(field)
        .and_then(|field_value| field_value.as_u64())
        .and_then(|number| u32::try_from(number).ok())
}

fn precision_increment(precision: Option<u32>) -> Option<String> {
    let precision = precision?;
    if precision == 0 {
        return Some("1".to_string());
    }
    Some(format!(
        "0.{}1",
        "0".repeat(precision.saturating_sub(1) as usize)
    ))
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
