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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    rows(value)
        .iter()
        .map(|row| {
            let symbol_text = required_text(
                exchange_id,
                row,
                &["symbol", "contract_code", "contract_code_display"],
            )?;
            let base_asset = text(
                row.get("base-currency")
                    .or_else(|| row.get("symbol_partition")),
            )
            .or_else(|| text(row.get("base_currency")))
            .unwrap_or_else(|| split_symbol(&symbol_text).0);
            let quote_asset = text(row.get("quote-currency"))
                .or_else(|| text(row.get("quote_currency")))
                .unwrap_or_else(|| {
                    if market_type == MarketType::Perpetual {
                        "USDT".to_string()
                    } else {
                        split_symbol(&symbol_text).1
                    }
                });
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol_scope(
                    exchange_id,
                    market_type,
                    &symbol_text,
                    &base_asset,
                    &quote_asset,
                )?,
                base_asset: base_asset.to_ascii_uppercase(),
                quote_asset: quote_asset.to_ascii_uppercase(),
                price_increment: text(
                    row.get("price-precision")
                        .or_else(|| row.get("price_tick"))
                        .or_else(|| row.get("price_tick_size")),
                ),
                quantity_increment: text(
                    row.get("amount-precision")
                        .or_else(|| row.get("contract_size"))
                        .or_else(|| row.get("contract_code")),
                ),
                min_price: None,
                max_price: None,
                min_quantity: text(
                    row.get("min-order-amt")
                        .or_else(|| row.get("contract_size")),
                ),
                max_quantity: text(row.get("max-order-amt")),
                min_notional: text(row.get("min-order-value")),
                max_notional: None,
                price_precision: row.get("price-precision").and_then(value_as_u32),
                quantity_precision: row.get("amount-precision").and_then(value_as_u32),
                supports_market_orders: true,
                supports_limit_orders: true,
                supports_post_only: true,
                supports_reduce_only: market_type == MarketType::Perpetual,
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
    let data = value.get("tick").unwrap_or(value);
    let bids = data
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "HTX order book missing bids", value))?;
    let asks = data
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "HTX order book missing asks", value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "htx order book requires canonical_symbol".to_string(),
            })?;
    let snapshot = OrderBookSnapshot {
        schema_version: SchemaVersion::current(),
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol.clone()),
        bids: levels(bids)?,
        asks: levels(asks)?,
        sequence: data
            .get("version")
            .or_else(|| data.get("seqNum"))
            .or_else(|| data.get("seqId"))
            .and_then(value_as_u64),
        exchange_timestamp: timestamp(data, &["ts", "mrid"]),
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
    if !matches!(symbol.market_type, MarketType::Spot | MarketType::Perpetual) {
        return Err(ExchangeApiError::Unsupported {
            operation: "htx.unsupported_market_type",
        });
    }
    let symbol_text = symbol.exchange_symbol.symbol.trim();
    if !symbol_text.is_empty() {
        return Ok(if symbol.market_type == MarketType::Spot {
            symbol_text.replace('-', "").to_ascii_lowercase()
        } else {
            symbol_text.to_ascii_uppercase()
        });
    }
    if let Some(canonical) = &symbol.canonical_symbol {
        return Ok(if symbol.market_type == MarketType::Spot {
            format!("{}{}", canonical.base_asset(), canonical.quote_asset()).to_ascii_lowercase()
        } else {
            format!("{}-{}", canonical.base_asset(), canonical.quote_asset()).to_ascii_uppercase()
        });
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "htx symbol requires exchange_symbol or canonical_symbol".to_string(),
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

pub fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
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
        .ok_or_else(|| parse_error(exchange_id.clone(), "HTX row missing symbol", value))
}

pub fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

pub fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
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
    if let Some(values) = value.as_array() {
        return values.as_slice();
    }
    value
        .get("symbols")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn split_symbol(symbol: &str) -> (String, String) {
    let upper = symbol.to_ascii_uppercase().replace('-', "");
    for quote in ["USDT", "USDC", "BTC", "ETH", "USD"] {
        if upper.ends_with(quote) && upper.len() > quote.len() {
            return (
                upper[..upper.len() - quote.len()].to_string(),
                quote.to_string(),
            );
        }
    }
    (upper, "USDT".to_string())
}

fn levels(values: &[Value]) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    values
        .iter()
        .filter_map(|value| {
            let row = value.as_array()?;
            Some((
                row.first().and_then(value_as_f64)?,
                row.get(1).and_then(value_as_f64)?,
            ))
        })
        .map(|(price, quantity)| OrderBookLevel::new(price, quantity).map_err(validation_error))
        .collect()
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value.as_u64().and_then(|value| u32::try_from(value).ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
