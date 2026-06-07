#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::BitkanGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const BITKAN_WS_PING_INTERVAL_MS: i64 = 30_000;
const BITKAN_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const BITKAN_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl BitkanGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitkan.public_streams_unverified",
            });
        }
        Ok(format!(
            "bitkan:{}:{}",
            self.config.public_ws_url,
            bitkan_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitkan.private_streams_unverified",
        })
    }
}

pub fn bitkan_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn bitkan_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "subscribe",
        "channel": bitkan_public_channel(subscription),
    })
}

pub fn bitkan_ping_payload() -> Value {
    json!({ "op": "ping" })
}

pub fn bitkan_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        BITKAN_WS_PING_INTERVAL_MS,
        BITKAN_WS_PONG_TIMEOUT_MS,
        BITKAN_WS_STALE_MESSAGE_MS,
    )
}

pub fn bitkan_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = normalize_ws_symbol(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::Trades => format!("trades:{symbol}"),
        PublicStreamKind::Ticker => format!("ticker:{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("depth:{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("orderbook:{symbol}"),
        PublicStreamKind::Candles { interval } => format!("kline:{interval}:{symbol}"),
    }
}

fn normalize_ws_symbol(symbol: &str) -> String {
    symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase()
}
