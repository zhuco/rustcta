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

pub fn parse_lighter_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    market_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Lighter markets missing order_books",
                value,
            )
        })?
        .iter()
        .filter(|item| {
            item.get("market_type")
                .or_else(|| item.get("type"))
                .or_else(|| item.get("filter"))
                .and_then(Value::as_str)
                .is_none_or(|kind| {
                    matches!(kind.to_ascii_lowercase().as_str(), "perp" | "perpetual")
                })
        })
        .map(|item| parse_lighter_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_lighter_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let market_id = string_or_number(
        value
            .get("market_id")
            .or_else(|| value.get("market_index"))
            .or_else(|| value.get("id")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "Lighter market missing id", value))?;
    let symbol = string_or_number(value.get("symbol").or_else(|| value.get("name")))
        .unwrap_or_else(|| format!("market:{market_id}"));
    let (fallback_base, fallback_quote) =
        split_lighter_symbol(&symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USD".to_string()));
    let base_asset = string_or_number(value.get("base").or_else(|| value.get("base_asset")))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(fallback_base);
    let quote_asset = string_or_number(value.get("quote").or_else(|| value.get("quote_asset")))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| matches!(status.to_ascii_lowercase().as_str(), "active" | "open"))
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                format!("market:{market_id}"),
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("price_tick")
                .or_else(|| value.get("tick_size"))
                .or_else(|| value.get("min_price_increment")),
        ),
        quantity_increment: string_or_number(
            value
                .get("size_tick")
                .or_else(|| value.get("step_size"))
                .or_else(|| value.get("min_base_amount_increment")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("min_base_amount")
                .or_else(|| value.get("min_quantity")),
        ),
        max_quantity: string_or_number(
            value
                .get("max_base_amount")
                .or_else(|| value.get("max_quantity")),
        ),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(
            value
                .get("price_tick")
                .or_else(|| value.get("tick_size"))
                .or_else(|| value.get("min_price_increment")),
        )
        .and_then(decimal_precision),
        quantity_precision: string_or_number(
            value
                .get("size_tick")
                .or_else(|| value.get("step_size"))
                .or_else(|| value.get("min_base_amount_increment")),
        )
        .and_then(decimal_precision),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_lighter_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value
        .get("order_book")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Lighter order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = book.get("nonce").and_then(Value::as_u64);
    Ok(snapshot)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderBookFrameMeta {
    pub channel: Option<String>,
    pub offset: Option<u64>,
    pub begin_nonce: Option<u64>,
    pub nonce: Option<u64>,
    pub checksum: Option<i64>,
}

impl LighterOrderBookFrameMeta {
    pub fn is_continuous_after(&self, previous_nonce: u64) -> bool {
        self.begin_nonce == Some(previous_nonce)
    }

    pub fn requires_resubscribe_after(&self, previous_nonce: u64) -> bool {
        !self.is_continuous_after(previous_nonce)
    }
}

pub fn parse_lighter_order_book_frame_meta(payload: &Value) -> LighterOrderBookFrameMeta {
    let book = payload.get("order_book").unwrap_or(payload);
    LighterOrderBookFrameMeta {
        channel: payload
            .get("channel")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        offset: payload
            .get("offset")
            .or_else(|| book.get("offset"))
            .and_then(Value::as_u64),
        begin_nonce: book.get("begin_nonce").and_then(Value::as_u64),
        nonce: book.get("nonce").and_then(Value::as_u64),
        checksum: book.get("checksum").and_then(Value::as_i64),
    }
}

fn market_items(value: &Value) -> Option<&[Value]> {
    value
        .get("order_books")
        .or_else(|| value.get("markets"))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
        .as_array()
        .map(Vec::as_slice)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Lighter order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("size")
                .or_else(|| level.get("quantity"))
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn split_lighter_symbol(symbol: &str) -> Option<(String, String)> {
    let cleaned = symbol.trim().replace("-PERP", "");
    let mut parts = cleaned
        .split(['-', '/', '_'])
        .filter(|part| !part.is_empty() && !part.eq_ignore_ascii_case("PERP"));
    Some((
        parts.next()?.to_ascii_uppercase(),
        parts.next().unwrap_or("USD").to_ascii_uppercase(),
    ))
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
}

fn decimal_as_f64(value: &Value) -> Option<f64> {
    string_or_number(Some(value))?.parse().ok()
}

fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim().trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
