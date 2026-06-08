use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::config::MyOkxGatewayConfig;
use super::signing::okx_style_signature;
use crate::adapters::ensure_exchange_api_schema;

pub fn myokx_public_ws_session(
    config: &MyOkxGatewayConfig,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let payload = myokx_public_subscribe_payload(subscription)?;
    Ok(json!({
        "mode": "subscribe",
        "url": config.public_ws_url,
        "payload": payload,
        "heartbeat": myokx_heartbeat_policy(),
        "resync": "rest_order_book_snapshot"
    }))
}

pub fn myokx_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    ensure_myokx_exchange(subscription.symbol.exchange.as_str())?;
    let channel = public_channel(subscription);
    let inst_id = crate::adapters::okx::parser::normalize_okx_symbol(
        &subscription.symbol.exchange_symbol.symbol,
    )?;
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "channel": channel,
            "instId": inst_id
        }]
    }))
}

pub fn myokx_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    ensure_myokx_exchange(subscription.symbol.exchange.as_str())?;
    let channel = public_channel(subscription);
    let inst_id = crate::adapters::okx::parser::normalize_okx_symbol(
        &subscription.symbol.exchange_symbol.symbol,
    )?;
    Ok(json!({
        "op": "unsubscribe",
        "args": [{
            "channel": channel,
            "instId": inst_id
        }]
    }))
}

pub fn myokx_private_login_payload(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    timestamp: &str,
) -> Value {
    json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp,
            "sign": okx_style_signature(api_secret, timestamp, "GET", "/users/self/verify", "")
        }]
    })
}

pub fn myokx_private_subscribe_payload(subscription: &PrivateStreamSubscription) -> Value {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "orders",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "account",
        PrivateStreamKind::Positions => "positions",
    };
    json!({
        "op": "subscribe",
        "args": [{
            "channel": channel
        }]
    })
}

pub fn myokx_heartbeat_policy() -> Value {
    json!({
        "mode": "server_ping_client_pong",
        "server_ping": "ping",
        "client_pong": "pong",
        "idle_timeout_ms": 30000,
        "recommended_client_ping_interval_ms": 25000
    })
}

pub fn myokx_pong_payload() -> Value {
    Value::String("pong".to_string())
}

fn public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            "books".to_string()
        }
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "tickers".to_string(),
        PublicStreamKind::Candles { interval } => format!("candle{interval}"),
    }
}

fn ensure_myokx_exchange(exchange: &str) -> ExchangeApiResult<()> {
    if exchange == "myokx" {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("myokx stream payload cannot serve exchange {exchange}"),
    })
}
