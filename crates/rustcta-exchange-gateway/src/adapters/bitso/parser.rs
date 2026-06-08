#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

const FIAT_QUOTES: &[&str] = &["MXN", "BRL", "ARS", "COP", "CLP", "PEN", "USD"];

pub fn bitso_book(symbol: &str) -> String {
    symbol.trim().replace(['/', '-'], "_").to_ascii_lowercase()
}

pub fn bitso_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('_') {
        return Ok((base.to_string(), quote.to_string()));
    }
    for quote in FIAT_QUOTES {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            return Ok((
                normalized[..normalized.len() - quote.len()].to_string(),
                (*quote).to_string(),
            ));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("bitso symbol {symbol} must include a fiat quote separator"),
    })
}

pub fn ensure_bitso_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitso.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn parse_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize, Option<String>)> {
    let payload = value.get("payload").unwrap_or(value);
    let bids = payload
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso order_book fixture missing bids".to_string(),
        })?;
    let asks = payload
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso order_book fixture missing asks".to_string(),
        })?;
    Ok((
        bids.len(),
        asks.len(),
        payload
            .get("sequence")
            .and_then(|value| value.as_str().or_else(|| value.as_u64().map(|_| "")))
            .map(|value| {
                if value.is_empty() {
                    payload["sequence"].to_string()
                } else {
                    value.to_string()
                }
            }),
    ))
}

pub fn parse_order_ack_id(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("payload")
        .and_then(|payload| payload.get("oid"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso place order fixture missing payload.oid".to_string(),
        })
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("payload")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitso available_books missing array",
                value,
            )
        })?;
    let mut rules = Vec::new();
    for row in rows {
        let book = required_text(exchange_id, row, "book")?;
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| bitso_book(&symbol.exchange_symbol.symbol) == book)
        {
            continue;
        }
        let (base_asset, quote_asset) = bitso_canonical_pair(&book)?;
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, book)
                .map_err(validation_error)?,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_increment: None,
            quantity_increment: None,
            min_price: text(row.get("minimum_price")),
            max_price: text(row.get("maximum_price")),
            min_quantity: text(row.get("minimum_amount")),
            max_quantity: text(row.get("maximum_amount")),
            min_notional: text(row.get("minimum_value")),
            max_notional: text(row.get("maximum_value")),
            price_precision: None,
            quantity_precision: None,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_order_book_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("payload").unwrap_or(value);
    let bids = payload
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Bitso order book missing bids", value))?;
    let asks = payload
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Bitso order book missing asks", value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitso order book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        levels(exchange_id, bids)?,
        levels(exchange_id, asks)?,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = payload.get("sequence").and_then(value_as_u64);
    Ok(snapshot)
}

fn levels(exchange_id: &ExchangeId, values: &[Value]) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    values
        .iter()
        .map(|level| {
            let (price, quantity) = if let Some(array) = level.as_array() {
                (
                    array.first().and_then(value_as_f64),
                    array.get(1).and_then(value_as_f64),
                )
            } else {
                (
                    level
                        .get("price")
                        .or_else(|| level.get("r"))
                        .and_then(value_as_f64),
                    level
                        .get("amount")
                        .or_else(|| level.get("a"))
                        .and_then(value_as_f64),
                )
            };
            OrderBookLevel::new(
                price.ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?,
                quantity.ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?,
            )
            .map_err(validation_error)
        })
        .collect()
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
) -> ExchangeApiResult<String> {
    text(value.get(field)).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {field}"),
            value,
        )
    })
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
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

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
