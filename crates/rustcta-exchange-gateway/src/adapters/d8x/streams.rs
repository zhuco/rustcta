#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::D8xGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const D8X_WS_PING_INTERVAL_MS: i64 = 30_000;
const D8X_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const D8X_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl D8xGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "d8x.public_stream_runtime_unverified",
            });
        }
        let Some(public_ws_url) = &self.config.public_ws_url else {
            return Err(ExchangeApiError::Unsupported {
                operation: "d8x.public_ws_url_unconfigured",
            });
        };
        Ok(format!(
            "d8x:{}:{}",
            public_ws_url,
            d8x_public_subscribe_payload(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "d8x.private_streams_require_wallet_contract_audit",
        })
    }
}

pub fn d8x_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn d8x_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let channel = d8x_public_channel(subscription);
    json!({
        "type": "subscribe",
        "channel": channel,
        "chain_id": 1101,
    })
}

pub fn d8x_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let channel = d8x_public_channel(subscription);
    json!({
        "type": "unsubscribe",
        "channel": channel,
        "chain_id": 1101,
    })
}

pub fn d8x_private_auth_payload(wallet_address: &str, signature: &str) -> Value {
    json!({
        "type": "auth",
        "wallet": wallet_address,
        "signature": signature,
        "scheme": "eip191_or_eip712_unverified"
    })
}

pub fn d8x_ping_payload() -> Value {
    json!({ "type": "ping" })
}

pub fn d8x_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        D8X_WS_PING_INTERVAL_MS,
        D8X_WS_PONG_TIMEOUT_MS,
        D8X_WS_STALE_MESSAGE_MS,
    )
}

pub fn d8x_public_channel(subscription: &PublicStreamSubscription) -> String {
    let ticker = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_ascii_lowercase();
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("orderbook:{ticker}")
        }
        PublicStreamKind::Trades => format!("trades:{ticker}"),
        PublicStreamKind::Ticker => format!("ticker:{ticker}"),
        PublicStreamKind::Candles { interval } => format!("candles:{ticker}:{interval}"),
    }
}
