#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_grvt_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    data_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "GRVT instruments missing result",
                value,
            )
        })?
        .iter()
        .filter(|item| {
            item.get("instrument_type")
                .or_else(|| item.get("kind"))
                .and_then(Value::as_str)
                .is_none_or(|kind| {
                    matches!(
                        kind.to_ascii_uppercase().as_str(),
                        "PERPETUAL" | "FUTURE" | "CALL" | "PUT" | "OPTION"
                    )
                })
        })
        .map(|item| parse_grvt_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_grvt_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let symbol = required_str(
        exchange_id,
        value,
        &["instrument", "instrument_name", "symbol"],
    )?;
    let market_type = match value
        .get("instrument_type")
        .or_else(|| value.get("kind"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("CALL" | "PUT" | "OPTION") => MarketType::Option,
        _ => MarketType::Perpetual,
    };
    let (fallback_base, fallback_quote) =
        split_grvt_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = string_or_number(
        value
            .get("base")
            .or_else(|| value.get("base_asset"))
            .or_else(|| value.get("baseCurrency")),
    )
    .map(|value| value.to_ascii_uppercase())
    .unwrap_or(fallback_base);
    let quote_asset = string_or_number(
        value
            .get("quote")
            .or_else(|| value.get("quote_asset"))
            .or_else(|| value.get("quoteCurrency"))
            .or_else(|| value.get("settlement_asset")),
    )
    .map(|value| value.to_ascii_uppercase())
    .unwrap_or(fallback_quote);
    let tradable = value
        .get("is_active")
        .and_then(Value::as_bool)
        .or_else(|| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(|status| matches!(status.to_ascii_uppercase().as_str(), "ACTIVE" | "LIVE"))
        })
        .unwrap_or(true);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("tick_size")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.get("min_price_increment")),
        ),
        quantity_increment: string_or_number(
            value
                .get("lot_size")
                .or_else(|| value.get("quantity_increment"))
                .or_else(|| value.get("min_size_increment")),
        ),
        min_price: string_or_number(value.get("min_price")),
        max_price: string_or_number(value.get("max_price")),
        min_quantity: string_or_number(value.get("min_size").or_else(|| value.get("min_quantity"))),
        max_quantity: string_or_number(value.get("max_size").or_else(|| value.get("max_quantity"))),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(
            value
                .get("tick_size")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.get("min_price_increment")),
        )
        .and_then(decimal_precision),
        quantity_precision: string_or_number(
            value
                .get("lot_size")
                .or_else(|| value.get("quantity_increment"))
                .or_else(|| value.get("min_size_increment")),
        )
        .and_then(decimal_precision),
        supports_market_orders: tradable && market_type == MarketType::Perpetual,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_grvt_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "GRVT order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = data
        .get("sequence_number")
        .or_else(|| data.get("seq"))
        .and_then(Value::as_u64);
    snapshot.exchange_timestamp = data
        .get("event_time")
        .or_else(|| data.get("eventTime"))
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc));
    Ok(snapshot)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrvtBookFrameMeta {
    pub stream: Option<String>,
    pub sequence_number: Option<u64>,
    pub event_time: Option<String>,
    pub snapshot: bool,
}

pub fn parse_grvt_book_frame_meta(payload: &Value) -> GrvtBookFrameMeta {
    let sequence_number = payload
        .get("sequence_number")
        .or_else(|| payload.get("seq"))
        .and_then(Value::as_u64);
    GrvtBookFrameMeta {
        stream: payload
            .get("stream")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        sequence_number,
        event_time: payload
            .get("event_time")
            .or_else(|| payload.get("eventTime"))
            .and_then(Value::as_str)
            .map(ToString::to_string),
        snapshot: sequence_number == Some(0),
    }
}

fn data_payload(value: &Value) -> &Value {
    value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn data_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.as_array().map(Vec::as_slice)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "GRVT order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "GRVT level must be array", level)
            })?;
            let price = array
                .first()
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    fields: &[&str],
) -> ExchangeApiResult<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!("GRVT payload missing one of {fields:?}"),
                value,
            )
        })
}

fn split_grvt_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol
        .trim()
        .split(['_', '/', '-'])
        .filter(|part| !part.is_empty());
    Some((
        parts.next()?.to_ascii_uppercase(),
        parts.next()?.to_ascii_uppercase(),
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
