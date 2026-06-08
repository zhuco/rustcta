#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::ApexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const APEX_WS_PING_INTERVAL_MS: i64 = 15_000;
const APEX_WS_PONG_TIMEOUT_MS: i64 = 15_000;
const APEX_WS_STALE_MESSAGE_MS: i64 = 45_000;

impl ApexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.public_streams_disabled",
            });
        }
        Ok(format!(
            "apex:{}:{}",
            self.config.public_ws_url,
            apex_public_topic(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams || !self.config.api_key_auth_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.private_streams_require_api_key_login",
            });
        }
        Ok(format!("apex:{}", self.config.private_ws_url))
    }
}

pub fn apex_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn apex_public_subscribe_payload(topic: &str) -> Value {
    json!({
        "op": "subscribe",
        "args": [topic],
    })
}

pub fn apex_ping_payload(timestamp_ms: i64) -> Value {
    json!({
        "op": "ping",
        "args": [timestamp_ms.to_string()],
    })
}

pub fn apex_private_login_payload(
    api_key: &str,
    passphrase: &str,
    timestamp: &str,
    signature: &str,
) -> Value {
    json!({
        "op": "login",
        "args": [json!({
            "type": "login",
            "topics": ["ws_zk_accounts_v3"],
            "httpMethod": "GET",
            "requestPath": "/ws/accounts",
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp,
            "signature": signature,
        }).to_string()],
    })
}

pub fn apex_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        APEX_WS_PING_INTERVAL_MS,
        APEX_WS_PONG_TIMEOUT_MS,
        APEX_WS_STALE_MESSAGE_MS,
    )
}

fn apex_public_topic(subscription: &PublicStreamSubscription) -> String {
    let symbol = normalize_symbol(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::Trades => format!("recentlyTrade.H.{symbol}"),
        PublicStreamKind::Ticker => format!("instrumentInfo.H.{symbol}"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("orderBook200.H.{symbol}")
        }
        PublicStreamKind::Candles { interval } => format!("candle.{interval}.{symbol}"),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '/', '_'], "")
        .to_ascii_uppercase()
}
