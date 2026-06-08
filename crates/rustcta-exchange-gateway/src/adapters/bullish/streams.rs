#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::BullishGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const BULLISH_WS_PING_INTERVAL_MS: i64 = 240_000;
const BULLISH_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const BULLISH_WS_STALE_MESSAGE_MS: i64 = 300_000;

impl BullishGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bullish.public_streams_disabled",
            });
        }
        Ok(format!(
            "bullish:{}:{}",
            self.config.public_ws_url,
            bullish_public_topic(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams || self.config.jwt_token.is_none() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bullish.private_streams_require_jwt_cookie_auth",
            });
        }
        Ok(format!("bullish:{}", self.config.private_ws_url))
    }
}

pub fn bullish_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn bullish_subscribe_payload(topic: &str, symbol: Option<&str>, id: &str) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("topic".to_string(), json!(topic));
    if let Some(symbol) = symbol {
        params.insert("symbol".to_string(), json!(normalize_symbol(symbol)));
    }
    json!({
        "jsonrpc": "2.0",
        "type": "command",
        "method": "subscribe",
        "params": params,
        "id": id,
    })
}

pub fn bullish_keepalive_payload(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "type": "command",
        "method": "keepalivePing",
        "params": {},
        "id": id,
    })
}

pub fn bullish_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        BULLISH_WS_PING_INTERVAL_MS,
        BULLISH_WS_PONG_TIMEOUT_MS,
        BULLISH_WS_STALE_MESSAGE_MS,
    )
}

fn bullish_public_topic(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "tick".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "l2Orderbook".to_string()
        }
        PublicStreamKind::Candles { interval } => format!("candles:{interval}"),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '/', '_'], "")
        .to_ascii_uppercase()
}
