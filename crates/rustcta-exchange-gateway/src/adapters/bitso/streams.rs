#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::bitso_book;
use super::BitsoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const BITSO_WS_PING_INTERVAL_MS: i64 = 30_000;
const BITSO_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const BITSO_WS_STALE_MESSAGE_MS: i64 = 60_000;
const BITSO_ORDER_BOOK_DEPTH_PER_SIDE: u32 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitsoPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub protocol: &'static str,
    pub snapshot_channel: &'static str,
    pub delta_channel: &'static str,
    pub snapshot_depth_per_side: u32,
    pub fixed_update_interval_ms: Option<u64>,
    pub delta_sequence_field: &'static str,
    pub sequence_continuity: &'static str,
    pub checksum: Option<&'static str>,
    pub rest_snapshot_endpoint: &'static str,
    pub resync: &'static str,
}

impl BitsoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.public_streams_disabled",
            });
        }
        Ok(format!(
            "bitso:{}:{}",
            self.config.public_ws_url,
            bitso_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitso.private_streams_not_promoted",
        })
    }
}

pub fn bitso_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn bitso_public_order_book_ws_policy() -> BitsoPublicOrderBookWsPolicy {
    BitsoPublicOrderBookWsPolicy {
        url: "wss://ws.bitso.com",
        protocol: "json_websocket",
        snapshot_channel: "orders",
        delta_channel: "diff-orders",
        snapshot_depth_per_side: BITSO_ORDER_BOOK_DEPTH_PER_SIDE,
        fixed_update_interval_ms: None,
        delta_sequence_field: "sequence",
        sequence_continuity: "diff-orders sequence must advance by one; any gap or regression requires REST order_book rebuild",
        checksum: None,
        rest_snapshot_endpoint: "GET /api/v3/order_book?book={book}",
        resync: "fetch REST order_book snapshot, buffer/replay diff-orders with sequence greater than snapshot sequence, and rebuild on reconnect or sequence gap",
    }
}

pub fn bitso_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "action": "subscribe",
        "book": bitso_book(&subscription.symbol.exchange_symbol.symbol),
        "type": bitso_public_channel(subscription),
    })
}

pub fn bitso_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::OrderBookDelta => "diff-orders",
        PublicStreamKind::OrderBookSnapshot => "orders",
        PublicStreamKind::Candles { .. } => "trades",
    }
}

pub fn bitso_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        BITSO_WS_PING_INTERVAL_MS,
        BITSO_WS_PONG_TIMEOUT_MS,
        BITSO_WS_STALE_MESSAGE_MS,
    )
}

pub fn bitso_diff_orders_sequence(value: &Value) -> ExchangeApiResult<u64> {
    let sequence = value
        .get("sequence")
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso diff-orders message missing sequence".to_string(),
        })?;
    if let Some(sequence) = sequence.as_u64() {
        return Ok(sequence);
    }
    sequence
        .as_str()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso diff-orders sequence must be an integer".to_string(),
        })?
        .parse::<u64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid bitso diff-orders sequence: {error}"),
        })
}

pub fn bitso_diff_orders_sequence_is_contiguous(previous: Option<u64>, next: u64) -> bool {
    match previous {
        Some(previous) => previous.checked_add(1) == Some(next),
        None => true,
    }
}
