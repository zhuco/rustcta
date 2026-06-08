use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiResult, HeartbeatDirection, HeartbeatPolicy, OrderBookResponse, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel};
use serde_json::{json, Value};

use super::parser::{
    number_from_value, parse_datetime_value, split_symbol_guess, validation_error,
};
use super::signing::{sign_hs256_hex, ws_hs256_payload};

pub fn public_orderbook_subscribe_payload(
    symbols: &[String],
    depth: u32,
    speed: &str,
    request_id: u64,
) -> Value {
    json!({
        "method": "subscribe",
        "ch": format!("orderbook/D{}/{speed}", depth.clamp(5, 20)),
        "params": {
            "symbols": symbols,
        },
        "id": request_id,
    })
}

pub fn public_orderbook_unsubscribe_payload(
    symbols: &[String],
    depth: u32,
    speed: &str,
    request_id: u64,
) -> Value {
    json!({
        "method": "unsubscribe",
        "ch": format!("orderbook/D{}/{speed}", depth.clamp(5, 20)),
        "params": {
            "symbols": symbols,
        },
        "id": request_id,
    })
}

pub fn trading_login_hs256_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<Value> {
    let payload = ws_hs256_payload(timestamp_ms, Some(window_ms));
    let signature = sign_hs256_hex(api_secret, &payload)?;
    Ok(json!({
        "method": "login",
        "params": {
            "type": "HS256",
            "api_key": api_key,
            "timestamp": timestamp_ms,
            "window": window_ms,
            "signature": signature,
        },
    }))
}

pub fn heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        direction: HeartbeatDirection::ServerPing,
        ping_interval_ms: 30_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 90_000,
        requires_pong_payload_echo: false,
    }
}

pub fn parse_public_orderbook_notification(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderBookResponse>> {
    let data = value
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: "FMFW.io WS order book notification missing data object".to_string(),
        })?;
    data.iter()
        .map(|(symbol, book)| parse_ws_book(exchange_id, symbol, book))
        .collect()
}

fn parse_ws_book(
    exchange_id: &ExchangeId,
    exchange_symbol: &str,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let (base, quote) = split_symbol_guess(exchange_symbol).ok_or_else(|| {
        rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: format!("cannot infer FMFW.io WS symbol {exchange_symbol}"),
        }
    })?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol.to_string(),
        )
        .map_err(validation_error)?,
    };
    let asks = parse_ws_levels(value.get("a"))?;
    let bids = parse_ws_levels(value.get("b"))?;
    let mut snapshot = rustcta_types::OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        symbol
            .canonical_symbol
            .clone()
            .expect("canonical symbol just constructed"),
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = value.get("t").and_then(parse_datetime_value);
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: rustcta_exchange_api::ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order_book: snapshot,
    })
}

fn parse_ws_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: "FMFW.io WS order book missing levels".to_string(),
        }
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "FMFW.io WS order book level is not an array".to_string(),
                }
            })?;
            let price = array.first().and_then(number_from_value).ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "FMFW.io WS order book level has invalid price".to_string(),
                }
            })?;
            let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "FMFW.io WS order book level has invalid quantity".to_string(),
                }
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}
