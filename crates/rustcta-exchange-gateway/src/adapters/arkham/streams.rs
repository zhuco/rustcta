#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::arkham_symbol;
use super::ArkhamGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl ArkhamGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "arkham.public_streams_disabled",
            });
        }
        Ok(format!(
            "arkham:{}:{}",
            self.config.public_ws_url,
            arkham_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "arkham.private_streams_use_rest_reconciliation_until_ws_auth_is_verified",
        })
    }
}

pub fn arkham_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn arkham_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "subscribe",
        "channel": arkham_public_channel(subscription),
        "symbol": arkham_symbol(&subscription.symbol)
    })
}

pub fn arkham_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "op": "unsubscribe",
        "channel": arkham_public_channel(subscription),
        "symbol": arkham_symbol(&subscription.symbol)
    })
}

pub fn arkham_private_auth_payload(api_key: &str, expires_us: i64, signature: &str) -> Value {
    json!({
        "op": "auth",
        "headers": {
            "Arkham-Api-Key": api_key,
            "Arkham-Expires": expires_us.to_string(),
            "Arkham-Signature": signature,
            "Arkham-Broker-Id": "1001"
        }
    })
}

pub fn arkham_ping_payload() -> Value {
    json!({ "op": "ping" })
}

pub fn arkham_reconnect_policy_ms() -> (u64, u64, u64) {
    (30_000, 45_000, 60_000)
}

fn arkham_public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "book".to_string()
        }
        PublicStreamKind::Candles { interval } => format!("candles:{interval}"),
    }
}
