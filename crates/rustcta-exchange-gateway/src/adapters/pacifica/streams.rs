#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::PacificaGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const PACIFICA_WS_PING_INTERVAL_MS: i64 = 30_000;
const PACIFICA_WS_PONG_TIMEOUT_MS: i64 = 60_000;
const PACIFICA_WS_STALE_MESSAGE_MS: i64 = 60_000;
const PACIFICA_WS_FORCE_RECONNECT_MS: i64 = 24 * 60 * 60 * 1000;

impl PacificaGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "pacifica.public_streams_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "pacifica.private_stream_runtime_unverified",
        })
    }
}

pub fn pacifica_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn pacifica_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "method": "subscribe",
        "params": {
            "source": pacifica_public_source(subscription),
            "symbol": normalize_symbol(&subscription.symbol.exchange_symbol.symbol),
        }
    })
}

pub fn pacifica_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "method": "unsubscribe",
        "params": {
            "source": pacifica_public_source(subscription),
            "symbol": normalize_symbol(&subscription.symbol.exchange_symbol.symbol),
        }
    })
}

pub fn pacifica_private_subscribe_payload(source: &str, account: &str) -> Value {
    json!({
        "method": "subscribe",
        "params": {
            "source": source,
            "account": account,
        }
    })
}

pub fn pacifica_ping_payload() -> Value {
    json!({ "method": "ping" })
}

pub fn pacifica_reconnect_policy_ms() -> (i64, i64, i64, i64) {
    (
        PACIFICA_WS_PING_INTERVAL_MS,
        PACIFICA_WS_PONG_TIMEOUT_MS,
        PACIFICA_WS_STALE_MESSAGE_MS,
        PACIFICA_WS_FORCE_RECONNECT_MS,
    )
}

pub fn pacifica_public_source(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "prices",
        PublicStreamKind::OrderBookDelta => "book",
        PublicStreamKind::OrderBookSnapshot => "book",
        PublicStreamKind::Candles { .. } => "candle",
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .split(['-', '/', '_'])
        .next()
        .unwrap_or(symbol)
        .to_ascii_uppercase()
}
