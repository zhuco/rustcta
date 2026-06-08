#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::public::cod3x_public_channel;
use super::transport::cod3x_reconnect_policy_ms;
use super::Cod3xGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl Cod3xGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let _ = cod3x_public_channel(&subscription);
        Err(ExchangeApiError::Unsupported {
            operation: "cod3x.public_stream_unverified",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "cod3x.private_stream_unverified",
        })
    }
}

pub fn cod3x_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn cod3x_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "channel": cod3x_public_channel(subscription),
        "support": "unsupported_unverified"
    })
}

pub fn cod3x_keepalive_payload() -> Value {
    json!({ "type": "ping" })
}

pub fn cod3x_stream_reconnect_policy_ms() -> (i64, i64, i64) {
    cod3x_reconnect_policy_ms()
}
