#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::coinmate_pair;
use super::signing::{coinmate_hmac_signature, coinmate_signature_payload};
use super::CoinmateGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const COINMATE_WS_PING_INTERVAL_MS: i64 = 30_000;
const COINMATE_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const COINMATE_WS_STALE_MESSAGE_MS: i64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinmatePublicOrderBookWsPolicy {
    pub url: &'static str,
    pub channel_template: &'static str,
    pub interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub resync: &'static str,
}

pub fn coinmate_public_order_book_ws_policy() -> CoinmatePublicOrderBookWsPolicy {
    CoinmatePublicOrderBookWsPolicy {
        url: "wss://coinmate.io/api/websocket",
        channel_template: "order_book-{PAIR}",
        interval_ms: None,
        depth: None,
        sequence: None,
        checksum: None,
        resync: "reconnect/resubscribe and rebuild from REST /orderBook because no official sequence or checksum is documented",
    }
}

impl CoinmateGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmate.public_streams_disabled",
            });
        }
        Ok(format!(
            "coinmate:{}:{}",
            self.config.ws_url,
            coinmate_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "coinmate.private_streams_offline_payload_only",
        })
    }
}

pub fn coinmate_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn coinmate_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "event": "subscribe",
        "data": {
            "channel": coinmate_public_channel(subscription)
        }
    })
}

pub fn coinmate_unsubscribe_payload(channel: &str) -> Value {
    json!({
        "event": "unsubscribe",
        "data": {
            "channel": channel
        }
    })
}

pub fn coinmate_private_subscribe_payload(
    kind: PrivateStreamKind,
    client_id: &str,
    public_key: &str,
    private_key: &str,
    nonce: &str,
    symbol: Option<&str>,
) -> ExchangeApiResult<Value> {
    let channel = coinmate_private_channel(kind, client_id, symbol);
    let payload = coinmate_signature_payload(nonce, client_id, public_key);
    let signature = coinmate_hmac_signature(private_key, &payload)?;
    Ok(json!({
        "event": "subscribe",
        "data": {
            "channel": channel,
            "clientId": client_id,
            "publicKey": public_key,
            "nonce": nonce,
            "signature": signature
        }
    }))
}

pub fn coinmate_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = coinmate_pair(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::Trades => format!("trades-{symbol}"),
        PublicStreamKind::Ticker => format!("statistics-{symbol}"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("order_book-{symbol}")
        }
        PublicStreamKind::Candles { .. } => format!("trades-{symbol}"),
    }
}

pub fn coinmate_private_channel(
    kind: PrivateStreamKind,
    client_id: &str,
    symbol: Option<&str>,
) -> String {
    match (kind, symbol) {
        (PrivateStreamKind::Orders, Some(symbol)) => {
            format!("private-open_orders-{client_id}-{}", coinmate_pair(symbol))
        }
        (PrivateStreamKind::Orders, None) => format!("private-open_orders-{client_id}"),
        (PrivateStreamKind::Balances, _) | (PrivateStreamKind::Account, _) => {
            format!("private-user_balances-{client_id}")
        }
        (PrivateStreamKind::Fills, Some(symbol)) => {
            format!("private-user-trades-{client_id}-{}", coinmate_pair(symbol))
        }
        (PrivateStreamKind::Fills, None) => format!("private-user-trades-{client_id}"),
        (PrivateStreamKind::Positions, _) => format!("private-open_orders-{client_id}"),
    }
}

pub fn coinmate_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        COINMATE_WS_PING_INTERVAL_MS,
        COINMATE_WS_PONG_TIMEOUT_MS,
        COINMATE_WS_STALE_MESSAGE_MS,
    )
}

pub fn parse_ws_event_type(value: &Value) -> Option<String> {
    value
        .get("event")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub fn parse_ws_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let payload = value
        .get("payload")
        .or_else(|| value.get("data").and_then(|data| data.get("payload")))
        .unwrap_or(value);
    let bids = payload
        .get("bids")
        .or_else(|| payload.get("Bids"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate ws order book fixture missing bids".to_string(),
        })?;
    let asks = payload
        .get("asks")
        .or_else(|| payload.get("Asks"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate ws order book fixture missing asks".to_string(),
        })?;
    Ok((bids.len(), asks.len()))
}
