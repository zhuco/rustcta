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

const FIAT_QUOTES: &[&str] = &["EUR", "CZK"];

pub fn coinmate_pair(symbol: &str) -> String {
    symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase()
}

pub fn coinmate_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = coinmate_pair(symbol);
    if let Some((base, quote)) = normalized.split_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Ok((base.to_string(), quote.to_string()));
        }
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
        message: format!("coinmate symbol {symbol} must include EUR/CZK quote separator"),
    })
}

pub fn ensure_coinmate_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmate.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Coinmate tradingPairs missing array",
                value,
            )
        })?;
    let mut rules = Vec::new();
    for row in rows {
        let pair = required_text(exchange_id, row, "name")?;
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| coinmate_pair(&symbol.exchange_symbol.symbol) == pair)
        {
            continue;
        }
        let base_asset = text(row.get("firstCurrency"))
            .unwrap_or_else(|| {
                coinmate_canonical_pair(&pair)
                    .map(|pair| pair.0)
                    .unwrap_or_default()
            })
            .to_ascii_uppercase();
        let quote_asset = text(row.get("secondCurrency"))
            .unwrap_or_else(|| {
                coinmate_canonical_pair(&pair)
                    .map(|pair| pair.1)
                    .unwrap_or_default()
            })
            .to_ascii_uppercase();
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
                .map_err(validation_error)?,
        };
        let price_precision = row.get("priceDecimals").and_then(value_as_u32);
        let quantity_precision = row.get("lotDecimals").and_then(value_as_u32);
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_increment: price_precision.map(decimal_increment),
            quantity_increment: quantity_precision.map(decimal_increment),
            min_price: None,
            max_price: None,
            min_quantity: text(row.get("minAmount")),
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision,
            quantity_precision,
            supports_market_orders: false,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let book = value.get("data").unwrap_or(value);
    let bids =
        first_array(book, &["bids", "Bids"]).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate order book fixture missing bids".to_string(),
        })?;
    let asks =
        first_array(book, &["asks", "Asks"]).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate order book fixture missing asks".to_string(),
        })?;
    Ok((bids.len(), asks.len()))
}

pub fn parse_order_ack_id(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("data")
        .unwrap_or(value)
        .get("id")
        .or_else(|| value.get("orderId"))
        .and_then(value_as_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate order ack fixture missing id".to_string(),
        })
}

pub fn parse_order_book_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value.get("data").unwrap_or(value);
    let bids = first_array(book, &["bids", "Bids"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Coinmate order book missing bids",
            value,
        )
    })?;
    let asks = first_array(book, &["asks", "Asks"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Coinmate order book missing asks",
            value,
        )
    })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinmate order book requires canonical_symbol".to_string(),
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
                    level
                        .get("price")
                        .or_else(|| level.get("Price"))
                        .and_then(value_as_f64),
                    level
                        .get("amount")
                        .or_else(|| level.get("Amount"))
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

fn first_array<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Vec<Value>> {
    keys.iter().find_map(|key| value.get(*key)?.as_array())
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

pub(super) fn text(value: Option<&Value>) -> Option<String> {
    value.and_then(value_as_string)
}

pub(super) fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| value.try_into().ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn decimal_increment(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
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
