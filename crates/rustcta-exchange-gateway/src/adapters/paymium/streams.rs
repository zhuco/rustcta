#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::parser::normalize_paymium_symbol;
use super::PaymiumGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const PAYMIUM_PUBLIC_SOCKET_PATH: &str = "/ws/socket.io";
pub const PAYMIUM_PUBLIC_SOCKET_PROTOCOL: &str = "socket.io_v1.3";
pub const PAYMIUM_PUBLIC_SOCKET_CONNECT_PACKET: &str = "1::";
pub const PAYMIUM_PUBLIC_STREAM_EVENT: &str = "stream";
pub const PAYMIUM_PUBLIC_BOOK_STREAM: &str = "bids_asks";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymiumPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub path: &'static str,
    pub protocol: &'static str,
    pub stream_event: &'static str,
    pub book_stream: &'static str,
    pub interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub rest_snapshot_endpoint: &'static str,
    pub resync: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymiumPublicSocketSubscriptionSpec {
    pub url: String,
    pub path: &'static str,
    pub protocol: &'static str,
    pub connect_packet: &'static str,
    pub event: &'static str,
    pub stream: &'static str,
    pub subscribe_payload: Option<Value>,
    pub unsubscribe_payload: Option<Value>,
}

impl PaymiumGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        normalize_paymium_symbol(&subscription.symbol.exchange_symbol.symbol)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.public_socketio_runtime_unverified",
            });
        }
        let spec = paymium_public_socket_subscription_spec(
            &subscription,
            &self.config.websocket_public_url,
        )?;
        Ok(format!(
            "paymium:{}:{}:{}:{}",
            spec.url, spec.path, spec.event, spec.stream
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        Err(ExchangeApiError::Unsupported {
            operation: "paymium.user_socket_requires_channel_id_from_private_rest",
        })
    }
}

pub fn paymium_public_order_book_ws_policy() -> PaymiumPublicOrderBookWsPolicy {
    PaymiumPublicOrderBookWsPolicy {
        url: "https://paymium.com/public",
        path: PAYMIUM_PUBLIC_SOCKET_PATH,
        protocol: PAYMIUM_PUBLIC_SOCKET_PROTOCOL,
        stream_event: PAYMIUM_PUBLIC_STREAM_EVENT,
        book_stream: PAYMIUM_PUBLIC_BOOK_STREAM,
        interval_ms: None,
        depth: None,
        sequence: None,
        checksum: None,
        update_semantics: "public socket.io stream pushes changed bids/asks price levels; amount/quantity 0 deletes the price level when present",
        rest_snapshot_endpoint: "GET /api/v1/data/eur/depth",
        resync: "initialize local book from REST /data/eur/depth before consuming the public stream; after disconnect, stale socket, parse error, or suspected message loss, reconnect and fully rebuild from REST because Paymium documents no sequence or checksum",
    }
}

pub fn paymium_public_socket_subscription_spec(
    subscription: &PublicStreamSubscription,
    ws_url: &str,
) -> ExchangeApiResult<PaymiumPublicSocketSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    normalize_paymium_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    Ok(PaymiumPublicSocketSubscriptionSpec {
        url: ws_url.to_string(),
        path: PAYMIUM_PUBLIC_SOCKET_PATH,
        protocol: PAYMIUM_PUBLIC_SOCKET_PROTOCOL,
        connect_packet: PAYMIUM_PUBLIC_SOCKET_CONNECT_PACKET,
        event: PAYMIUM_PUBLIC_STREAM_EVENT,
        stream: paymium_public_stream_name(&subscription.kind)?,
        subscribe_payload: None,
        unsubscribe_payload: None,
    })
}

pub fn paymium_public_order_book_rebuild_strategy() -> &'static str {
    "Paymium public book runtime must build the initial BTC/EUR book with REST GET /api/v1/data/eur/depth, then consume socket.io v1.3 stream event payloads containing bids/asks changes; no fixed interval, depth, sequence, or checksum is documented, so every disconnect or suspected gap requires a full REST rebuild before resuming."
}

pub fn paymium_public_socket_descriptor() -> Value {
    json!({
        "url": "https://paymium.com/public",
        "path": PAYMIUM_PUBLIC_SOCKET_PATH,
        "protocol": PAYMIUM_PUBLIC_SOCKET_PROTOCOL,
        "connect_packet": PAYMIUM_PUBLIC_SOCKET_CONNECT_PACKET,
        "event": PAYMIUM_PUBLIC_STREAM_EVENT,
        "streams": [PAYMIUM_PUBLIC_BOOK_STREAM, "trades", "ticker"],
        "subscription": "connect_only_public_stream"
    })
}

pub fn paymium_user_socket_descriptor(user_channel_id: &str) -> Value {
    json!({
        "url": "https://paymium.com/user",
        "path": PAYMIUM_PUBLIC_SOCKET_PATH,
        "protocol": PAYMIUM_PUBLIC_SOCKET_PROTOCOL,
        "emit": ["channel", user_channel_id],
        "event": PAYMIUM_PUBLIC_STREAM_EVENT
    })
}

fn paymium_public_stream_name(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    Ok(match kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "bids_asks",
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.public_socketio_candles_unverified",
            })
        }
    })
}
