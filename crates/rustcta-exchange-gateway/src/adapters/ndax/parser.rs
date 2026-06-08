#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

const KNOWN_QUOTES: &[&str] = &["CAD", "USD", "USDT", "BTC", "ETH"];

pub fn ndax_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn ndax_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    for separator in ['/', '-', '_'] {
        if let Some((base, quote)) = trimmed.split_once(separator) {
            if !base.is_empty() && !quote.is_empty() {
                return Ok((base.to_string(), quote.to_string()));
            }
        }
    }
    let normalized = ndax_symbol(symbol);
    for quote in KNOWN_QUOTES {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            return Ok((
                normalized[..normalized.len() - quote.len()].to_string(),
                (*quote).to_string(),
            ));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("NDAX symbol {symbol} must include a known quote asset"),
    })
}

pub fn ensure_ndax_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "NDAX GetInstruments fixture missing array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for row in rows {
        let symbol_text = required_text(exchange_id, row, &["symbol", "Symbol"])?;
        let exchange_symbol = ndax_symbol(&symbol_text);
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| ndax_symbol(&symbol.exchange_symbol.symbol) == exchange_symbol)
        {
            continue;
        }
        let (fallback_base, fallback_quote) = ndax_canonical_pair(&exchange_symbol)?;
        let base_asset = text_any(row, &["product1Symbol", "Product1Symbol"])
            .unwrap_or(fallback_base)
            .to_ascii_uppercase();
        let quote_asset = text_any(row, &["product2Symbol", "Product2Symbol"])
            .unwrap_or(fallback_quote)
            .to_ascii_uppercase();
        let status = value_as_i64_any(row, &["sessionStatus", "SessionStatus"]).unwrap_or(1);
        if status != 1 {
            continue;
        }
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                &exchange_symbol,
            )
            .map_err(validation_error)?,
        };
        let price_increment = decimal_text_any(row, &["priceIncrement", "PriceIncrement"]);
        let quantity_increment = decimal_text_any(row, &["quantityIncrement", "QuantityIncrement"]);
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_precision: price_increment.as_deref().and_then(decimal_precision),
            quantity_precision: quantity_increment.as_deref().and_then(decimal_precision),
            price_increment,
            quantity_increment,
            min_price: None,
            max_price: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            supports_market_orders: false,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_order_book_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "NDAX GetL2Snapshot fixture missing array",
            value,
        )
    })?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for row in rows {
        let side = l2_side(row)
            .ok_or_else(|| parse_error(exchange_id.clone(), "NDAX L2 row missing side", row))?;
        let level = OrderBookLevel::new(
            l2_price(row).ok_or_else(|| {
                parse_error(exchange_id.clone(), "NDAX L2 row missing price", row)
            })?,
            l2_quantity(row).ok_or_else(|| {
                parse_error(exchange_id.clone(), "NDAX L2 row missing quantity", row)
            })?,
        )
        .map_err(validation_error)?;
        match side {
            L2Side::Bid => bids.push(level),
            L2Side::Ask => asks.push(level),
        }
    }
    if let Some(depth) = depth.map(|value| value as usize) {
        bids.truncate(depth);
        asks.truncate(depth);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "NDAX order book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    Ok(snapshot)
}

pub fn parse_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let rows = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "NDAX L2 fixture missing array".to_string(),
        })?;
    let mut bids = 0;
    let mut asks = 0;
    for row in rows {
        match l2_side(row) {
            Some(L2Side::Bid) => bids += 1,
            Some(L2Side::Ask) => asks += 1,
            None => {}
        }
    }
    Ok((bids, asks))
}

#[derive(Clone, Copy)]
enum L2Side {
    Bid,
    Ask,
}

fn l2_side(value: &Value) -> Option<L2Side> {
    if let Some(side) = value_as_i64_any(value, &["side", "Side"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(9))
            .and_then(value_as_i64)
    }) {
        return match side {
            0 => Some(L2Side::Bid),
            1 => Some(L2Side::Ask),
            _ => None,
        };
    }
    match text_any(value, &["side", "Side"])?
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "bid" | "bids" => Some(L2Side::Bid),
        "sell" | "ask" | "asks" => Some(L2Side::Ask),
        _ => None,
    }
}

fn l2_price(value: &Value) -> Option<f64> {
    value_as_f64_any(value, &["price", "Price"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(4))
            .and_then(value_as_f64)
    })
}

fn l2_quantity(value: &Value) -> Option<f64> {
    value_as_f64_any(value, &["quantity", "Quantity"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(8))
            .and_then(value_as_f64)
    })
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    keys: &[&str],
) -> ExchangeApiResult<String> {
    text_any(value, keys).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {}", keys.join("/")),
            value,
        )
    })
}

fn text_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| text(value.get(*key)))
        .filter(|value| !value.trim().is_empty())
}

fn decimal_text_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| text(Some(value)).or_else(|| value_as_f64(value).map(trim_float)))
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64_any(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(value_as_i64)
}

fn value_as_f64_any(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(value_as_f64)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn trim_float(value: f64) -> String {
    let formatted = format!("{value:.12}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn decimal_precision(value: &str) -> Option<u32> {
    value
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
        .filter(|precision| *precision > 0)
        .or(Some(0))
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
