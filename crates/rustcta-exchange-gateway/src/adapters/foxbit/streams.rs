#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::foxbit_symbol;
use super::signing::{foxbit_hmac_sha256_hex, foxbit_private_ws_login_prehash};
use super::FoxbitGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const FOXBIT_WS_PING_INTERVAL_MS: i64 = 20_000;
const FOXBIT_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const FOXBIT_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl FoxbitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "foxbit.public_streams_disabled",
            });
        }
        Ok(format!(
            "foxbit:{}:{}",
            self.config.public_ws_url,
            foxbit_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "foxbit.private_streams_rest_reconciliation_fallback",
        })
    }
}

pub fn foxbit_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn foxbit_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "params": [{
            "channel": foxbit_public_channel(subscription),
            "market_symbol": foxbit_symbol(&subscription.symbol.exchange_symbol.symbol),
        }]
    })
}

pub fn foxbit_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "unsubscribe",
        "params": [{
            "channel": foxbit_public_channel(subscription),
            "market_symbol": foxbit_symbol(&subscription.symbol.exchange_symbol.symbol),
        }]
    })
}

pub fn foxbit_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbook-1000",
        PublicStreamKind::Candles { .. } => "candles",
    }
}

pub fn foxbit_public_ping_payload() -> Value {
    json!({
        "type": "message",
        "params": [{
            "channel": "ping"
        }]
    })
}

pub fn foxbit_private_login_payload(api_key: &str, api_secret: &str, timestamp_ms: &str) -> Value {
    let signature =
        foxbit_hmac_sha256_hex(api_secret, &foxbit_private_ws_login_prehash(timestamp_ms));
    json!({
        "type": "login",
        "params": {
            "api_key": api_key,
            "timestamp": timestamp_ms,
            "signature": signature
        }
    })
}

pub fn parse_orderbook_snapshot_payload(value: &Value) -> ExchangeApiResult<(u64, usize, usize)> {
    let event = value.get("params").unwrap_or(value);
    let sequence = event
        .get("sequence_id")
        .or_else(|| event.get("last_sequence_id"))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit ws orderbook fixture missing sequence".to_string(),
        })?;
    let bids = event
        .get("bids")
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit ws orderbook fixture missing bids".to_string(),
        })?;
    let asks = event
        .get("asks")
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit ws orderbook fixture missing asks".to_string(),
        })?;
    Ok((sequence, bids, asks))
}

pub fn foxbit_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        FOXBIT_WS_PING_INTERVAL_MS,
        FOXBIT_WS_PONG_TIMEOUT_MS,
        FOXBIT_WS_STALE_MESSAGE_MS,
    )
}
