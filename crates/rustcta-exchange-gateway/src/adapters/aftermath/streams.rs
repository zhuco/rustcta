#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::AftermathGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const AFTERMATH_WS_PING_INTERVAL_MS: i64 = 30_000;
const AFTERMATH_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const AFTERMATH_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl AftermathGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "aftermath.public_stream_runtime_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "aftermath.private_streams_require_sui_account_auth",
        })
    }
}

pub fn aftermath_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn aftermath_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "action": "subscribe",
        "subscriptionType": subscription_type(subscription),
    })
}

pub fn aftermath_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "action": "unsubscribe",
        "subscriptionType": subscription_type(subscription),
    })
}

pub fn aftermath_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        AFTERMATH_WS_PING_INTERVAL_MS,
        AFTERMATH_WS_PONG_TIMEOUT_MS,
        AFTERMATH_WS_STALE_MESSAGE_MS,
    )
}

fn subscription_type(subscription: &PublicStreamSubscription) -> Value {
    let market_id = subscription.symbol.exchange_symbol.symbol.clone();
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            json!({ "orderbook": { "marketId": market_id } })
        }
        PublicStreamKind::Trades => json!({ "marketOrders": { "marketId": market_id } }),
        PublicStreamKind::Ticker => json!({ "market": { "marketId": market_id } }),
        PublicStreamKind::Candles { interval } => json!({
            "marketCandles": {
                "marketId": market_id,
                "interval": interval,
            }
        }),
    }
}
