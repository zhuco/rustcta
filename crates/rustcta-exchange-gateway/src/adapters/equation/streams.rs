#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::public::equation_public_channel;
use super::transport::equation_reconnect_policy_ms;
use super::EquationGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl EquationGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let _ = equation_public_channel(&subscription);
        Err(ExchangeApiError::Unsupported {
            operation: "equation.public_stream_unverified",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "equation.private_stream_unverified",
        })
    }
}

pub fn equation_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn equation_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "subscribe",
        "channel": equation_public_channel(subscription),
        "support": "unsupported_unverified"
    })
}

pub fn equation_keepalive_payload() -> Value {
    json!({ "type": "ping" })
}

pub fn equation_stream_reconnect_policy_ms() -> (i64, i64, i64) {
    equation_reconnect_policy_ms()
}
