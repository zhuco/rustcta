#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::signing::sign_ws_login;
use super::OxfunGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const OXFUN_WS_PING_INTERVAL_MS: i64 = 20_000;
const OXFUN_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const OXFUN_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl OxfunGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "oxfun.public_streams_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "oxfun.private_stream_runtime_unverified",
        })
    }
}

pub fn oxfun_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn oxfun_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "subscribe",
        "tag": subscription.context.request_id.clone().unwrap_or_else(|| "rustcta".to_string()),
        "args": [oxfun_public_channel(subscription)],
    })
}

pub fn oxfun_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "unsubscribe",
        "tag": subscription.context.request_id.clone().unwrap_or_else(|| "rustcta".to_string()),
        "args": [oxfun_public_channel(subscription)],
    })
}

pub fn oxfun_private_login_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "login",
        "tag": "login",
        "data": {
            "apiKey": api_key,
            "timestamp": timestamp_ms,
            "signature": sign_ws_login(api_secret, timestamp_ms)?,
        }
    }))
}

pub fn oxfun_ping_payload() -> Value {
    Value::String(oxfun_ping_text().to_string())
}

pub fn oxfun_ping_text() -> &'static str {
    "ping"
}

pub fn oxfun_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        OXFUN_WS_PING_INTERVAL_MS,
        OXFUN_WS_PONG_TIMEOUT_MS,
        OXFUN_WS_STALE_MESSAGE_MS,
    )
}

pub fn oxfun_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = normalize_ws_symbol(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::Trades => format!("trade:{symbol}"),
        PublicStreamKind::Ticker => format!("ticker:{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("depthUpdate:{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("depth:{symbol}"),
        PublicStreamKind::Candles { interval } => format!("candle{interval}:{symbol}"),
    }
}

fn normalize_ws_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace('/', "-")
        .replace('_', "-")
        .to_ascii_uppercase()
}
