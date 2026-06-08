use rustcta_exchange_api::{
    ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::parse_orderbook_snapshot;
use crate::adapters::ensure_exchange_api_schema;
use crate::adapters::okx::parser::normalize_okx_symbol;

pub fn okxus_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    okxus_public_subscription_payload("subscribe", subscription)
}

pub fn okxus_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    okxus_public_subscription_payload("unsubscribe", subscription)
}

pub fn okxus_private_login_payload(
    api_key: &str,
    passphrase: &str,
    timestamp_seconds: &str,
    signature: &str,
) -> Value {
    json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp_seconds,
            "sign": signature
        }]
    })
}

pub fn okxus_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "orders",
        PrivateStreamKind::Balances => "account",
        PrivateStreamKind::Positions => "positions",
        PrivateStreamKind::Account => "account",
    };
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "channel": channel,
            "instType": "SPOT"
        }]
    }))
}

pub fn okxus_heartbeat_policy() -> Value {
    json!({
        "mode": "server_ping_client_pong",
        "client_pong": "pong",
        "timeout_ms": 30000,
        "resync": "rest_order_book_snapshot"
    })
}

pub fn parse_okxus_orderbook_ws_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    parse_orderbook_snapshot(exchange_id, symbol, &data)
}

fn okxus_public_subscription_payload(
    op: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            "books5".to_string()
        }
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "tickers".to_string(),
        PublicStreamKind::Candles { interval } => format!("candle{interval}"),
    };
    Ok(json!({
        "op": op,
        "args": [{
            "channel": channel,
            "instId": normalize_okx_symbol(&subscription.symbol.exchange_symbol.symbol)?
        }]
    }))
}
