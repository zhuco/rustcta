#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::novadax_symbol;
use super::NovadaxGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const NOVADAX_WS_PING_INTERVAL_MS: i64 = 30_000;
const NOVADAX_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const NOVADAX_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl NovadaxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "novadax.public_streams_disabled",
            });
        }
        Ok(format!(
            "novadax:{}:{}",
            self.config.public_ws_url,
            novadax_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "novadax.private_streams_not_promoted",
        })
    }
}

pub fn novadax_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn novadax_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "symbol": novadax_symbol(&subscription.symbol.exchange_symbol.symbol),
        "channel": novadax_public_channel(subscription),
        "transport": "socket_io"
    })
}

pub fn novadax_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = novadax_symbol(&subscription.symbol.exchange_symbol.symbol);
    let suffix = match &subscription.kind {
        PublicStreamKind::Trades => "TRADE",
        PublicStreamKind::Ticker => "TICKER",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "DEPTH",
        PublicStreamKind::Candles { .. } => "KLINE",
    };
    format!("MARKET.{symbol}.{suffix}")
}

pub fn novadax_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        NOVADAX_WS_PING_INTERVAL_MS,
        NOVADAX_WS_PONG_TIMEOUT_MS,
        NOVADAX_WS_STALE_MESSAGE_MS,
    )
}
