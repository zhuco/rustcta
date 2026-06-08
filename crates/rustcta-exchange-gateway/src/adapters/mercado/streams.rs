#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::mercado_symbol;
use super::MercadoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const MERCADO_WS_PING_INTERVAL_MS: i64 = 30_000;
const MERCADO_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const MERCADO_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl MercadoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.public_streams_disabled",
            });
        }
        Ok(format!(
            "mercado:{}:{}",
            self.config.public_ws_url,
            mercado_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "mercado.private_streams_not_promoted",
        })
    }
}

pub fn mercado_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn mercado_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "symbol": mercado_symbol(&subscription.symbol.exchange_symbol.symbol),
        "channel": mercado_public_channel(subscription),
    })
}

pub fn mercado_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbook",
        PublicStreamKind::Candles { .. } => "candles",
    }
}

pub fn mercado_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        MERCADO_WS_PING_INTERVAL_MS,
        MERCADO_WS_PONG_TIMEOUT_MS,
        MERCADO_WS_STALE_MESSAGE_MS,
    )
}
