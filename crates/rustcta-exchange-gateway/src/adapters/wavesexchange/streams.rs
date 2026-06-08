#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::WavesExchangeGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const WAVESEXCHANGE_WS_PING_INTERVAL_MS: i64 = 30_000;
const WAVESEXCHANGE_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const WAVESEXCHANGE_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl WavesExchangeGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "wavesexchange.public_stream_runtime_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "wavesexchange.private_streams_require_waves_account_auth",
        })
    }
}

pub fn wavesexchange_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn wavesexchange_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let depth = match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => 100,
        _ => 10,
    };
    json!({
        "T": subscription_type(subscription),
        "S": subscription.symbol.exchange_symbol.symbol,
        "d": depth,
    })
}

pub fn wavesexchange_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "T": unsubscribe_type(subscription),
        "S": subscription.symbol.exchange_symbol.symbol,
    })
}

pub fn wavesexchange_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        WAVESEXCHANGE_WS_PING_INTERVAL_MS,
        WAVESEXCHANGE_WS_PONG_TIMEOUT_MS,
        WAVESEXCHANGE_WS_STALE_MESSAGE_MS,
    )
}

fn subscription_type(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "obs",
        PublicStreamKind::Trades => "ts",
        PublicStreamKind::Ticker => "ms",
        PublicStreamKind::Candles { .. } => "cs",
    }
}

fn unsubscribe_type(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "obu",
        PublicStreamKind::Trades => "tu",
        PublicStreamKind::Ticker => "mu",
        PublicStreamKind::Candles { .. } => "cu",
    }
}
