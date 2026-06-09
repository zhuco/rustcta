#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::AarkGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const AARK_WS_PING_INTERVAL_MS: i64 = 30_000;
const AARK_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const AARK_WS_STALE_MESSAGE_MS: i64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AarkOrderBookWsPolicy {
    pub url: &'static str,
    pub path_template: &'static str,
    pub topic_template: &'static str,
    pub interval_ms: u64,
    pub sequence_fields: &'static [&'static str],
    pub checksum: Option<&'static str>,
    pub resync: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AarkPrevTsContinuity {
    First,
    Continuous,
    Gap { previous_ts: u64, prev_ts: u64 },
}

impl AarkPrevTsContinuity {
    pub fn requires_resync(self) -> bool {
        matches!(self, AarkPrevTsContinuity::Gap { .. })
    }
}

pub fn aark_orderbook_ws_policy() -> AarkOrderBookWsPolicy {
    AarkOrderBookWsPolicy {
        url: "wss://ws-evm.orderly.org/ws/stream",
        path_template: "/{account_id}",
        topic_template: "{symbol}@orderbookupdate",
        interval_ms: 200,
        sequence_fields: &["ts", "prevTs"],
        checksum: None,
        resync: "request a fresh orderbook snapshot and resubscribe after prevTs gap, reconnect, stale stream, parse error, or suspected loss",
    }
}

pub fn aark_check_prev_ts_continuity(
    previous_ts: Option<u64>,
    prev_ts: Option<u64>,
) -> AarkPrevTsContinuity {
    let (Some(previous_ts), Some(prev_ts)) = (previous_ts, prev_ts) else {
        return AarkPrevTsContinuity::First;
    };
    if previous_ts == prev_ts {
        AarkPrevTsContinuity::Continuous
    } else {
        AarkPrevTsContinuity::Gap {
            previous_ts,
            prev_ts,
        }
    }
}

pub fn aark_orderbook_update_timestamps(value: &Value) -> (Option<u64>, Option<u64>) {
    let data = value.get("data").unwrap_or(value);
    (
        data.get("ts").and_then(value_as_u64),
        data.get("prevTs").and_then(value_as_u64),
    )
}

impl AarkGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "aark.public_stream_runtime_spec_only",
            });
        }
        Ok(format!(
            "aark:{}:{}",
            self.config.public_ws_url,
            aark_public_subscribe_payload(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "aark.private_streams_require_orderly_account_auth",
        })
    }
}

pub fn aark_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn aark_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let topic = aark_public_topic(subscription);
    json!({
        "id": format!("aark-{topic}"),
        "event": "subscribe",
        "topic": topic,
    })
}

pub fn aark_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let topic = aark_public_topic(subscription);
    json!({
        "id": format!("aark-{topic}"),
        "event": "unsubscribe",
        "topic": topic,
    })
}

pub fn aark_private_auth_payload(orderly_key: &str, timestamp_ms: i64, signature: &str) -> Value {
    json!({
        "id": "aark-auth",
        "event": "auth",
        "params": {
            "orderly_key": orderly_key,
            "timestamp": timestamp_ms,
            "sign": signature,
        }
    })
}

pub fn aark_ping_payload() -> Value {
    json!({ "event": "ping" })
}

pub fn aark_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        AARK_WS_PING_INTERVAL_MS,
        AARK_WS_PONG_TIMEOUT_MS,
        AARK_WS_STALE_MESSAGE_MS,
    )
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub fn aark_public_topic(subscription: &PublicStreamSubscription) -> String {
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
