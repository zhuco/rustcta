#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::parse_orderbook_snapshot;
use super::ModetradeGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const MODETRADE_WS_PING_INTERVAL_MS: i64 = 30_000;
const MODETRADE_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const MODETRADE_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl ModetradeGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "modetrade.public_stream_runtime_spec_only",
            });
        }
        Ok(format!(
            "modetrade:{}:{}",
            self.config.public_ws_url,
            modetrade_public_subscribe_payload(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "modetrade.private_streams_require_orderly_account_auth",
        })
    }
}

pub fn modetrade_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn modetrade_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let topic = modetrade_public_topic(subscription);
    json!({
        "id": format!("modetrade-{topic}"),
        "event": "subscribe",
        "topic": topic,
    })
}

pub fn modetrade_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let topic = modetrade_public_topic(subscription);
    json!({
        "id": format!("modetrade-{topic}"),
        "event": "unsubscribe",
        "topic": topic,
    })
}

pub fn modetrade_private_auth_payload(
    orderly_key: &str,
    timestamp_ms: i64,
    signature: &str,
) -> Value {
    json!({
        "id": "modetrade-auth",
        "event": "auth",
        "params": {
            "orderly_key": orderly_key,
            "timestamp": timestamp_ms,
            "sign": signature,
        }
    })
}

pub fn modetrade_ping_payload() -> Value {
    json!({ "event": "ping" })
}

pub fn parse_modetrade_public_orderbook_update(
    exchange_id: &ExchangeId,
    subscription: &PublicStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let wrapped = json!({
        "success": true,
        "timestamp": value.get("ts").or_else(|| value.get("timestamp")).cloned(),
        "data": {
            "symbol": data
                .get("symbol")
                .or_else(|| data.get("s"))
                .cloned()
                .unwrap_or_else(|| Value::String(subscription.symbol.exchange_symbol.symbol.clone())),
            "seq": data.get("seq").or_else(|| data.get("sequence")).cloned(),
            "bids": data.get("bids").or_else(|| data.get("b")).cloned().unwrap_or_else(|| json!([])),
            "asks": data.get("asks").or_else(|| data.get("a")).cloned().unwrap_or_else(|| json!([])),
        }
    });
    parse_orderbook_snapshot(exchange_id, subscription.symbol.clone(), &wrapped)
}

pub fn modetrade_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        MODETRADE_WS_PING_INTERVAL_MS,
        MODETRADE_WS_PONG_TIMEOUT_MS,
        MODETRADE_WS_STALE_MESSAGE_MS,
    )
}

pub fn modetrade_public_topic(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_ascii_uppercase();
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("{symbol}@orderbookupdate")
        }
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    }
}
