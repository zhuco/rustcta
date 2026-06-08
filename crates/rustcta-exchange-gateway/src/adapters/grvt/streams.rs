#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::public::{grvt_market_data_stream, grvt_public_feed};
use super::transport::grvt_reconnect_policy_ms;
use super::GrvtGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl GrvtGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let _ = grvt_public_feed(&subscription);
        Err(ExchangeApiError::Unsupported {
            operation: "grvt.public_stream_session_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "grvt.private_stream_session_spec_only",
        })
    }
}

pub fn grvt_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn grvt_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "stream": grvt_market_data_stream(&subscription.kind),
        "feed": [grvt_public_feed(subscription)],
        "method": "subscribe",
        "is_full": true
    })
}

pub fn grvt_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "stream": grvt_market_data_stream(&subscription.kind),
        "feed": [grvt_public_feed(subscription)],
        "method": "unsubscribe",
        "is_full": true
    })
}

pub fn grvt_ping_payload() -> Value {
    json!({ "method": "ping" })
}

pub fn grvt_stream_reconnect_policy_ms() -> (i64, i64, i64) {
    grvt_reconnect_policy_ms()
}
