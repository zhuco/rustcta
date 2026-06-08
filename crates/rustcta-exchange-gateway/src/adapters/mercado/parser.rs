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

const FIAT_QUOTES: &[&str] = &["BRL", "MXN", "ARS", "USD"];

pub fn mercado_symbol(symbol: &str) -> String {
    symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase()
}

pub fn mercado_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = mercado_symbol(symbol);
    if let Some((base, quote)) = normalized.split_once('-') {
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
        message: format!("mercado symbol {symbol} must include a fiat quote separator"),
    })
}

pub fn ensure_mercado_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "mercado.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn parse_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let bids = value
        .get("bids")
        .or_else(|| value.get("buy"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado orderbook fixture missing bids".to_string(),
        })?;
    let asks = value
        .get("asks")
        .or_else(|| value.get("sell"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado orderbook fixture missing asks".to_string(),
        })?;
    Ok((bids.len(), asks.len()))
}

pub fn parse_order_ack_id(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("id")
        .or_else(|| value.get("orderId"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado order fixture missing id".to_string(),
        })
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("symbols")
        .or_else(|| value.get("data"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "Mercado symbols missing array", value))?;
    let mut rules = Vec::new();
    for row in rows {
        let native_symbol = text(
            row.get("symbol")
                .or_else(|| row.get("instrument"))
                .or_else(|| row.get("id")),
        )
        .ok_or_else(|| parse_error(exchange_id.clone(), "missing symbol", row))?;
        let normalized_symbol = mercado_symbol(&native_symbol);
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| mercado_symbol(&symbol.exchange_symbol.symbol) == normalized_symbol)
        {
            continue;
        }
        let (fallback_base, fallback_quote) = mercado_canonical_pair(&normalized_symbol)?;
        let base_asset = text(row.get("base")).unwrap_or(fallback_base);
        let quote_asset = text(row.get("quote")).unwrap_or(fallback_quote);
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                normalized_symbol,
            )
            .map_err(validation_error)?,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_increment: text(row.get("priceIncrement").or_else(|| row.get("tickSize"))),
            quantity_increment: text(row.get("quantityIncrement").or_else(|| row.get("stepSize"))),
            min_price: None,
            max_price: None,
            min_quantity: text(row.get("minOrderQty")),
            max_quantity: text(row.get("maxOrderQty")),
            min_notional: text(row.get("minOrderCost")),
            max_notional: None,
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
    let bids = value
        .get("bids")
        .or_else(|| value.get("buy"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Mercado orderbook missing bids", value))?;
    let asks = value
        .get("asks")
        .or_else(|| value.get("sell"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Mercado orderbook missing asks", value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mercado order book requires canonical_symbol".to_string(),
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
                    level.get("price").and_then(value_as_f64),
                    level
                        .get("qty")
                        .or_else(|| level.get("amount"))
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
