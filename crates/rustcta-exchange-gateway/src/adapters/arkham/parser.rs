#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn arkham_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-'], "_")
        .to_ascii_uppercase()
}

pub fn parse_arkham_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let symbols = value
        .as_array()
        .ok_or_else(|| invalid("arkham pairs response must be an array"))?;
    let mut rules = Vec::new();
    for entry in symbols {
        let raw_symbol = string_field(entry, "symbol")?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| arkham_symbol(symbol) == raw_symbol.to_ascii_uppercase())
        {
            continue;
        }
        if entry
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != "listed")
        {
            continue;
        }
        let market_type = market_type(entry)?;
        let base_raw = string_field(entry, "baseSymbol")?;
        let base = base_raw.trim_end_matches(".P").to_ascii_uppercase();
        let quote = string_field(entry, "quoteSymbol")?.to_ascii_uppercase();
        let scope = symbol_scope(exchange_id.clone(), market_type, &base, &quote, &raw_symbol)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base,
            quote_asset: quote,
            price_increment: optional_string(entry, "minTickPrice"),
            quantity_increment: optional_string(entry, "minLotSize")
                .or_else(|| optional_string(entry, "minSize")),
            min_price: optional_string(entry, "minPrice"),
            max_price: optional_string(entry, "maxPrice"),
            min_quantity: optional_string(entry, "minSize"),
            max_quantity: optional_string(entry, "maxSize"),
            min_notional: optional_string(entry, "minNotional"),
            max_notional: None,
            price_precision: decimal_places(entry, "minTickPrice"),
            quantity_precision: decimal_places(entry, "minLotSize")
                .or_else(|| decimal_places(entry, "minSize")),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: market_type == MarketType::Perpetual,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_arkham_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(value.get("bids"))?;
    let asks = parse_levels(value.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("arkham orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        symbol.market_type,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid arkham orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.exchange_timestamp = value
        .get("lastTime")
        .and_then(|value| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .or_else(|| value.get("lastTime").and_then(Value::as_i64))
        .and_then(|micros| Utc.timestamp_micros(micros).single());
    Ok(snapshot)
}

fn market_type(entry: &Value) -> ExchangeApiResult<MarketType> {
    match string_field(entry, "pairType")?.as_str() {
        "spot" => Ok(MarketType::Spot),
        "perpetual" => Ok(MarketType::Perpetual),
        other => Err(invalid(format!("unsupported arkham pairType {other}"))),
    }
}

fn symbol_scope(
    exchange_id: ExchangeId,
    market_type: MarketType,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
        .map_err(|error| invalid(format!("invalid arkham canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
        .map_err(|error| invalid(format!("invalid arkham exchange symbol: {error}")))?;
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
        .ok_or_else(|| invalid("arkham orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let price = parse_f64(level.get("price"), "price")?;
            let quantity = parse_f64(level.get("size"), "size")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid arkham level: {error}")))
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("arkham missing string field {field}")))
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
        .ok_or_else(|| invalid(format!("arkham invalid numeric {field}")))
}

fn decimal_places(value: &Value, field: &str) -> Option<u32> {
    let text = optional_string(value, field)?;
    let decimals = text.split_once('.')?.1.trim_end_matches('0').len();
    Some(decimals as u32)
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
