use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::BsxGatewayAdapter;

impl BsxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bsx.public_stream_runtime_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Err(ExchangeApiError::Unsupported {
            operation: "bsx.private_streams_require_auth_lifecycle_audit",
        })
    }
}

pub fn bsx_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION)
}

pub fn bsx_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "subscribe",
        "channel": bsx_public_channel(subscription),
        "product_id": subscription.symbol.exchange_symbol.symbol
    })
}

pub fn bsx_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "unsubscribe",
        "channel": bsx_public_channel(subscription),
        "product_id": subscription.symbol.exchange_symbol.symbol
    })
}

pub fn bsx_private_auth_payload(api_key: &str, timestamp_ns: i128, signature: &str) -> Value {
    json!({
        "op": "auth",
        "args": {
            "key": api_key,
            "timestamp": timestamp_ns.to_string(),
            "signature": signature
        }
    })
}

pub fn bsx_ping_payload() -> Value {
    json!({ "op": "ping" })
}

pub fn bsx_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 45_000, 60_000)
}

pub fn bsx_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "book",
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => "candles",
    }
}
