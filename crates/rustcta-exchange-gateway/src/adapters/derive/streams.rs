#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::DeriveGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl DeriveGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Ok(format!(
            "derive:{}:{}",
            self.config.ws_url,
            derive_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_streams_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: "derive.private_streams_require_session_key",
            });
        }
        Ok(format!(
            "derive:{}:{}",
            self.config.ws_url,
            derive_private_channel(&subscription.kind)?
        ))
    }
}

pub fn derive_private_stream_capabilities(_enabled: bool) -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn derive_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe",
        "params": { "channels": [derive_public_channel(subscription)] }
    })
}

pub fn derive_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "unsubscribe",
        "params": { "channels": [derive_public_channel(subscription)] }
    })
}

pub fn derive_private_subscribe_payload(kind: &PrivateStreamKind) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe",
        "params": { "channels": [derive_private_channel(kind)?] }
    }))
}

pub fn derive_private_unsubscribe_payload(kind: &PrivateStreamKind) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "unsubscribe",
        "params": { "channels": [derive_private_channel(kind)?] }
    }))
}

pub fn derive_login_payload(wallet: &str, session_key: &str, signature: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/login",
        "params": {
            "wallet": wallet,
            "session_key": session_key,
            "signature": signature
        }
    })
}

pub fn derive_ping_payload() -> Value {
    json!({ "jsonrpc": "2.0", "id": 1, "method": "public/ping" })
}

pub fn derive_heartbeat_policy_ms() -> (i64, i64, i64) {
    (30_000, 10_000, 60_000)
}

pub fn derive_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_string();
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("orderbook.{symbol}.100")
        }
        PublicStreamKind::Trades => format!("trades.{symbol}"),
        PublicStreamKind::Ticker => format!("ticker.{symbol}.100ms"),
        PublicStreamKind::Candles { interval } => format!("ticker.{symbol}.{interval}"),
    }
}

pub fn derive_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders => Ok("private.orders"),
        PrivateStreamKind::Fills => Ok("private.trades"),
        PrivateStreamKind::Balances => Ok("private.balances"),
        PrivateStreamKind::Positions => Ok("private.positions"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "derive.private_stream_kind",
        }),
    }
}
