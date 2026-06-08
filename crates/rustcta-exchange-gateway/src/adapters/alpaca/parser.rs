use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, SymbolRules,
    EXCHANGE_API_SCHEMA_VERSION,
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
    let assets = value.as_array().ok_or_else(|| {
        decode_error(
            exchange_id,
            "alpaca assets response must be an array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for asset in assets {
        let asset_class = asset
            .get("asset_class")
            .or_else(|| asset.get("class"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !asset_class.eq_ignore_ascii_case("crypto") {
            continue;
        }
        let tradable = asset
            .get("tradable")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if !tradable {
            continue;
        }
        let symbol = string_field(exchange_id, asset, "symbol")?;
        let normalized = normalize_alpaca_symbol(symbol)?;
        let canonical = canonical_from_alpaca_symbol(&normalized)?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized.clone())
                .map_err(validation_error)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: rustcta_exchange_api::SymbolScope {
                exchange: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: Some(canonical.clone()),
                exchange_symbol,
            },
            base_asset: canonical.base_asset().to_string(),
            quote_asset: canonical.quote_asset().to_string(),
            price_increment: optional_string(asset, "price_increment"),
            quantity_increment: optional_string(asset, "min_trade_increment")
                .or_else(|| optional_string(asset, "min_order_size")),
            min_price: None,
            max_price: None,
            min_quantity: optional_string(asset, "min_order_size"),
            max_quantity: None,
            min_notional: None,
            max_notional: Some("200000".to_string()),
            price_precision: None,
            quantity_precision: None,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let requested_symbol = normalize_alpaca_symbol(&symbol.exchange_symbol.symbol)?;
    let orderbooks = value
        .get("orderbooks")
        .and_then(Value::as_object)
        .ok_or_else(|| decode_error(exchange_id, "alpaca orderbooks object missing", value))?;
    let book = orderbooks
        .get(&requested_symbol)
        .or_else(|| orderbooks.get(&legacy_alpaca_symbol(&requested_symbol)))
        .ok_or_else(|| {
            decode_error(
                exchange_id,
                format!("alpaca orderbook missing symbol {requested_symbol}"),
                value,
            )
        })?;
    let mut bids = parse_levels(exchange_id, book, "b")?;
    let mut asks = parse_levels(exchange_id, book, "a")?;
    bids.sort_by(|left, right| {
        right
            .price
            .partial_cmp(&left.price)
            .unwrap_or(Ordering::Equal)
    });
    asks.sort_by(|left, right| {
        left.price
            .partial_cmp(&right.price)
            .unwrap_or(Ordering::Equal)
    });
    let canonical = symbol
        .canonical_symbol
        .clone()
        .unwrap_or(canonical_from_alpaca_symbol(&requested_symbol)?);
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = book.get("t").and_then(Value::as_str).and_then(parse_time);
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
        order_book: snapshot,
    })
}

pub fn normalize_alpaca_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let raw = symbol.trim().to_ascii_uppercase().replace('-', "/");
    if raw.contains('/') {
        let canonical = CanonicalSymbol::parse(&raw).map_err(validation_error)?;
        return Ok(canonical.to_string());
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if raw.len() > quote.len() && raw.ends_with(quote) {
            let base = &raw[..raw.len() - quote.len()];
            return Ok(CanonicalSymbol::new(base, quote)
                .map_err(validation_error)?
                .to_string());
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("alpaca crypto symbol must be BASE/QUOTE or legacy BASEQUOTE: {symbol}"),
    })
}

pub fn legacy_alpaca_symbol(symbol: &str) -> String {
    symbol.replace('/', "")
}

pub fn canonical_from_alpaca_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    CanonicalSymbol::parse(normalize_alpaca_symbol(symbol)?).map_err(validation_error)
}

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub fn decode_error(
    exchange_id: &ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        message,
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub fn exchange_error(
    exchange_id: &ExchangeId,
    class: ExchangeErrorClass,
    message: impl Into<String>,
    raw: Option<Value>,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange_id.clone(), class, message, Utc::now());
    error.raw = raw;
    ExchangeApiError::Exchange(error)
}

pub fn string_field<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &'static str,
) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| decode_error(exchange_id, format!("alpaca missing field {field}"), value))
}

pub fn optional_string(value: &Value, field: &'static str) -> Option<String> {
    value.get(field).and_then(|item| match item {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub fn f64_field(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &'static str,
) -> ExchangeApiResult<f64> {
    let raw = value.get(field).ok_or_else(|| {
        decode_error(
            exchange_id,
            format!("alpaca missing numeric field {field}"),
            value,
        )
    })?;
    f64_from_value(raw).ok_or_else(|| {
        decode_error(
            exchange_id,
            format!("alpaca invalid numeric field {field}"),
            value,
        )
    })
}

pub fn optional_f64(value: &Value, field: &'static str) -> Option<f64> {
    value.get(field).and_then(f64_from_value)
}

pub fn f64_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

pub fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn parse_levels(
    exchange_id: &ExchangeId,
    book: &Value,
    field: &'static str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = book.get(field).and_then(Value::as_array).ok_or_else(|| {
        decode_error(
            exchange_id,
            format!("alpaca orderbook missing {field}"),
            book,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = f64_field(exchange_id, level, "p")?;
            let size = f64_field(exchange_id, level, "s")?;
            OrderBookLevel::new(price, size).map_err(validation_error)
        })
        .collect()
}

pub fn empty_order_state(
    exchange_id: &ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
    status: rustcta_types::OrderStatus,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: rustcta_types::OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::None),
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: None,
        status,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

pub fn schema_version() -> SchemaVersion {
    SchemaVersion::current()
}
