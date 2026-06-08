#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::public::lighter_public_channel;
use super::transport::lighter_reconnect_policy_ms;
use super::LighterGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl LighterGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let _ = lighter_public_channel(&subscription);
        Err(ExchangeApiError::Unsupported {
            operation: "lighter.public_stream_session_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "lighter.private_stream_session_spec_only",
        })
    }
}

pub fn lighter_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn lighter_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "channel": lighter_public_channel(subscription)
    })
}

pub fn lighter_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "unsubscribe",
        "channel": lighter_public_channel(subscription)
    })
}

pub fn lighter_keepalive_payload() -> Value {
    json!({ "type": "ping" })
}

pub fn lighter_stream_reconnect_policy_ms() -> (i64, i64, i64) {
    lighter_reconnect_policy_ms()
}
