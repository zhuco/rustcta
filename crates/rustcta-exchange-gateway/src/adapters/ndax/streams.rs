#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::ndax_symbol;
use super::transport::ndax_gateway_call;
use super::NdaxGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const NDAX_WS_PING_INTERVAL_MS: i64 = 30_000;
const NDAX_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const NDAX_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl NdaxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "ndax.public_streams_disabled_scan_only",
            });
        }
        Ok(format!(
            "ndax:{}:{}",
            self.config.ws_url,
            ndax_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "ndax.private_streams_auth_payload_only",
        })
    }
}

pub fn ndax_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn ndax_public_subscribe_payload(
    request_id: i64,
    oms_id: i64,
    subscription: &PublicStreamSubscription,
    depth: u32,
) -> Value {
    let payload = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => json!({
            "OMSId": oms_id,
            "Symbol": ndax_symbol(&subscription.symbol.exchange_symbol.symbol),
            "Depth": depth
        }),
        PublicStreamKind::Trades => json!({
            "OMSId": oms_id,
            "Symbol": ndax_symbol(&subscription.symbol.exchange_symbol.symbol)
        }),
        PublicStreamKind::Ticker | PublicStreamKind::Candles { .. } => json!({
            "OMSId": oms_id,
            "Symbol": ndax_symbol(&subscription.symbol.exchange_symbol.symbol)
        }),
    };
    ndax_gateway_call(request_id, ndax_public_channel(subscription), payload)
}

pub fn ndax_unsubscribe_payload(
    request_id: i64,
    channel: &str,
    oms_id: i64,
    symbol: &str,
) -> Value {
    ndax_gateway_call(
        request_id,
        channel,
        json!({
            "OMSId": oms_id,
            "Symbol": ndax_symbol(symbol)
        }),
    )
}

pub fn ndax_private_auth_payload(
    request_id: i64,
    api_key: &str,
    user_id: &str,
    nonce: &str,
    signature: &str,
) -> Value {
    ndax_gateway_call(
        request_id,
        "AuthenticateUser",
        json!({
            "APIKey": api_key,
            "UserId": user_id,
            "Nonce": nonce,
            "Signature": signature
        }),
    )
}

pub fn ndax_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "SubscribeLevel2",
        PublicStreamKind::Trades => "SubscribeTrades",
        PublicStreamKind::Ticker => "SubscribeTicker",
        PublicStreamKind::Candles { .. } => "SubscribeTrades",
    }
}

pub fn ndax_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        NDAX_WS_PING_INTERVAL_MS,
        NDAX_WS_PONG_TIMEOUT_MS,
        NDAX_WS_STALE_MESSAGE_MS,
    )
}

pub fn parse_ws_function_name(value: &Value) -> Option<String> {
    value
        .get("n")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub fn parse_ws_level2_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let payload = super::transport::decode_gateway_payload(value)?;
    super::parser::parse_order_book_shape(&payload)
}
