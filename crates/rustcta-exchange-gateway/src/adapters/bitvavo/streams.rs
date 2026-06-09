#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, ExchangeSymbol};
use serde_json::{json, Value};

use super::parser::{parse_error, parse_orderbook_snapshot};
use super::signing::sign_ws_auth;

pub fn public_subscribe_payload(channel: &str, symbols: &[ExchangeSymbol]) -> Value {
    json!({
        "action": "subscribe",
        "channels": [{
            "name": channel,
            "markets": symbols.iter().map(|symbol| symbol.symbol.clone()).collect::<Vec<_>>()
        }]
    })
}

pub fn public_subscribe_payload_for_subscription(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = public_channel_name(&subscription.kind)?;
    Ok(public_subscribe_payload(
        channel,
        std::slice::from_ref(&subscription.symbol.exchange_symbol),
    ))
}

fn public_channel_name(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok("book"),
        PublicStreamKind::Trades => Ok("trades"),
        PublicStreamKind::Ticker => Ok("ticker"),
        PublicStreamKind::Candles { .. } => Ok("candles"),
    }
}

pub fn private_auth_payload(api_key: &str, api_secret: &str, timestamp: &str) -> Value {
    json!({
        "action": "authenticate",
        "key": api_key,
        "signature": sign_ws_auth(api_secret, timestamp),
        "timestamp": timestamp.parse::<i64>().unwrap_or_default(),
        "window": 10000
    })
}

pub fn private_account_subscribe_payload() -> Value {
    json!({
        "action": "subscribe",
        "channels": [{ "name": "account" }]
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum BitvavoPublicStreamMessage {
    OrderBook(OrderBookResponse),
    SubscriptionAck { event: String },
}

pub fn bitvavo_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BitvavoPublicStreamMessage> {
    if is_subscription_ack(value) {
        return Ok(BitvavoPublicStreamMessage::SubscriptionAck {
            event: value
                .get("event")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }
    let event = value
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event == "book" || (value.get("nonce").is_some() && value.get("bids").is_some()) {
        return Ok(BitvavoPublicStreamMessage::OrderBook(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
            order_book: parse_orderbook_snapshot(exchange_id, symbol, value)?,
        }));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported bitvavo public stream message",
        value,
    ))
}

pub fn validate_book_nonce(
    previous_nonce: Option<u64>,
    received_nonce: u64,
) -> ExchangeApiResult<()> {
    if let Some(previous_nonce) = previous_nonce {
        let expected =
            previous_nonce
                .checked_add(1)
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitvavo book nonce overflow".to_string(),
                })?;
        if received_nonce != expected {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "bitvavo book nonce gap: expected {expected}, received {received_nonce}"
                ),
            });
        }
    }
    Ok(())
}

fn is_subscription_ack(value: &Value) -> bool {
    matches!(
        value.get("event").and_then(Value::as_str),
        Some("subscribed") | Some("authenticated")
    )
}
