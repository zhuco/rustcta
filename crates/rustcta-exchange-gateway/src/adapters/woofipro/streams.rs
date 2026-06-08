#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::WoofiproGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const WOOFIPRO_WS_PING_INTERVAL_MS: i64 = 30_000;
const WOOFIPRO_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const WOOFIPRO_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl WoofiproGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "woofipro.public_stream_runtime_spec_only",
            });
        }
        Ok(format!(
            "woofipro:{}:{}",
            self.config.public_ws_url,
            woofipro_public_subscribe_payload(&self.config.broker_id, &subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "woofipro.private_streams_require_orderly_account_auth",
        })
    }
}

pub fn woofipro_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn woofipro_public_subscribe_payload(
    broker_id: &str,
    subscription: &PublicStreamSubscription,
) -> Value {
    let topic = woofipro_public_topic(broker_id, subscription);
    json!({
        "id": format!("woofipro-{topic}"),
        "event": "subscribe",
        "topic": topic,
    })
}

pub fn woofipro_public_unsubscribe_payload(
    broker_id: &str,
    subscription: &PublicStreamSubscription,
) -> Value {
    let topic = woofipro_public_topic(broker_id, subscription);
    json!({
        "id": format!("woofipro-{topic}"),
        "event": "unsubscribe",
        "topic": topic,
    })
}

pub fn woofipro_private_auth_payload(
    orderly_key: &str,
    timestamp_ms: i64,
    signature: &str,
) -> Value {
    json!({
        "id": "woofipro-auth",
        "event": "auth",
        "params": {
            "orderly_key": orderly_key,
            "timestamp": timestamp_ms,
            "sign": signature,
        }
    })
}

pub fn woofipro_ping_payload() -> Value {
    json!({ "event": "ping" })
}

pub fn woofipro_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        WOOFIPRO_WS_PING_INTERVAL_MS,
        WOOFIPRO_WS_PONG_TIMEOUT_MS,
        WOOFIPRO_WS_STALE_MESSAGE_MS,
    )
}

pub fn woofipro_public_topic(broker_id: &str, subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_ascii_uppercase();
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta => format!("{symbol}@orderbookupdate"),
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@orderbook"),
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Ticker => format!("{}${symbol}@ticker", broker_id.trim()),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    }
}
