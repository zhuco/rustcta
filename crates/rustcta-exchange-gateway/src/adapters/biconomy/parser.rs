use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

use crate::adapters::response_metadata;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    rows(value)
        .iter()
        .map(|row| {
            let symbol_text = required_text(exchange_id, row, &["symbol", "s"])?;
            let (base_asset, quote_asset) = infer_assets(row, &symbol_text);
            let symbol = symbol_scope(
                exchange_id,
                MarketType::Spot,
                &symbol_text,
                &base_asset,
                &quote_asset,
            )?;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                base_asset,
                quote_asset,
                price_increment: text(row.get("tickSize").or_else(|| row.get("priceTick"))),
                quantity_increment: text(row.get("stepSize").or_else(|| row.get("quantityTick"))),
                min_price: text(row.get("minPrice")),
                max_price: text(row.get("maxPrice")),
                min_quantity: text(row.get("minQty").or_else(|| row.get("minQuantity"))),
                max_quantity: text(row.get("maxQty").or_else(|| row.get("maxQuantity"))),
                min_notional: text(row.get("minNotional")),
                max_notional: None,
                price_precision: row.get("pricePrecision").and_then(value_as_u32),
                quantity_precision: row.get("quantityPrecision").and_then(value_as_u32),
                supports_market_orders: true,
                supports_limit_orders: true,
                supports_post_only: true,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_order_book(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = data.get("bids").and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Biconomy order book missing bids",
            value,
        )
    })?;
    let asks = data.get("asks").and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Biconomy order book missing asks",
            value,
        )
    })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "biconomy order book requires canonical_symbol".to_string(),
            })?;
    let snapshot = OrderBookSnapshot {
        schema_version: rustcta_types::SchemaVersion::current(),
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol.clone()),
        bids: levels(bids)?,
        asks: levels(asks)?,
        sequence: data
            .get("lastUpdateId")
            .or_else(|| data.get("sequence"))
            .and_then(value_as_u64),
        exchange_timestamp: timestamp(data, &["timestamp", "ts", "time"]),
        received_at: Utc::now(),
        is_stale: false,
    };
    snapshot.validate().map_err(validation_error)?;
    Ok(snapshot)
}

pub fn parse_orderbook_response(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    request_id: Option<String>,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), request_id),
        order_book: parse_order_book(exchange_id, symbol, value)?,
    })
}

pub fn normalize_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    if symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "biconomy.non_spot_market",
        });
    }
    let symbol_text = symbol.exchange_symbol.symbol.trim();
    if !symbol_text.is_empty() {
        return Ok(symbol_text.replace('_', "").to_ascii_uppercase());
    }
    if let Some(canonical) = &symbol.canonical_symbol {
        return Ok(
            format!("{}{}", canonical.base_asset(), canonical.quote_asset()).to_ascii_uppercase(),
        );
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "biconomy symbol requires exchange_symbol or canonical_symbol".to_string(),
    })
}

pub fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
    base_asset: &str,
    quote_asset: &str,
) -> ExchangeApiResult<SymbolScope> {
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: CanonicalSymbol::new(base_asset, quote_asset).ok(),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

pub fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<String> {
    fields
        .iter()
        .find_map(|field| text(value.get(*field)))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                &format!("missing field {}", fields[0]),
                value,
            )
        })
}

pub fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

pub fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub fn timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

pub fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
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

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn rows(value: &Value) -> &[Value] {
    value
        .get("symbols")
        .or_else(|| value.get("list"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn infer_assets(row: &Value, symbol: &str) -> (String, String) {
    let base = text(row.get("baseAsset").or_else(|| row.get("base")))
        .unwrap_or_else(|| split_symbol(symbol).0);
    let quote = text(row.get("quoteAsset").or_else(|| row.get("quote")))
        .unwrap_or_else(|| split_symbol(symbol).1);
    (base.to_ascii_uppercase(), quote.to_ascii_uppercase())
}

fn split_symbol(symbol: &str) -> (String, String) {
    for quote in ["USDT", "USDC", "BTC", "ETH", "USD"] {
        if symbol.to_ascii_uppercase().ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return (base.to_string(), quote.to_string());
        }
    }
    (symbol.to_string(), "USDT".to_string())
}

fn levels(values: &[Value]) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    values
        .iter()
        .filter_map(|value| {
            let (price, quantity) = if let Some(row) = value.as_array() {
                (
                    row.first().and_then(value_as_f64),
                    row.get(1).and_then(value_as_f64),
                )
            } else {
                (
                    value.get("price").and_then(value_as_f64),
                    value
                        .get("qty")
                        .or_else(|| value.get("quantity"))
                        .and_then(value_as_f64),
                )
            };
            Some((price?, quantity?))
        })
        .map(|(price, quantity)| OrderBookLevel::new(price, quantity).map_err(validation_error))
        .collect()
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}
