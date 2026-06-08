#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::parse_orderbook_snapshot;
use super::ApolloxDexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const APOLLOX_WS_PING_INTERVAL_MS: i64 = 180_000;
const APOLLOX_WS_PONG_TIMEOUT_MS: i64 = 600_000;
const APOLLOX_WS_CONNECTION_TTL_MS: i64 = 86_400_000;

impl ApolloxDexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "apollox_dex.public_stream_runtime_spec_only",
            });
        }
        Ok(format!(
            "apollox_dex:{}:{}",
            self.config.public_ws_url,
            apollox_public_subscribe_payload(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "apollox_dex.private_stream_requires_listen_key_runtime_audit",
        })
    }
}

pub fn apollox_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn apollox_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let stream = apollox_public_stream_name(subscription);
    json!({
        "method": "SUBSCRIBE",
        "params": [stream],
        "id": 1
    })
}

pub fn apollox_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let stream = apollox_public_stream_name(subscription);
    json!({
        "method": "UNSUBSCRIBE",
        "params": [stream],
        "id": 1
    })
}

pub fn apollox_public_stream_name(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_ascii_lowercase();
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta => format!("{symbol}@depth"),
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@depth20"),
        PublicStreamKind::Trades => format!("{symbol}@aggTrade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    }
}

pub fn apollox_private_listen_key_path() -> &'static str {
    "/fapi/v1/listenKey"
}

pub fn apollox_parse_public_depth_event(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    parse_orderbook_snapshot(exchange_id, symbol, data)
}

pub fn apollox_ws_policy_ms() -> (i64, i64, i64) {
    (
        APOLLOX_WS_PING_INTERVAL_MS,
        APOLLOX_WS_PONG_TIMEOUT_MS,
        APOLLOX_WS_CONNECTION_TTL_MS,
    )
}
