#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    CapabilitySupport, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    HeartbeatCapability, OrderBookResponse, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, ReconnectCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::parse_orderbook_snapshot;
use super::BullishGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const BULLISH_WS_PING_INTERVAL_MS: i64 = 240_000;
const BULLISH_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const BULLISH_WS_STALE_MESSAGE_MS: i64 = 300_000;

impl BullishGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bullish.public_streams_disabled",
            });
        }
        let topic = bullish_public_topic(&subscription);
        let symbol = normalize_symbol(&subscription.symbol.exchange_symbol.symbol);
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.public_ws_url,
            "payload": bullish_subscribe_payload(&topic, Some(&symbol), "1"),
            "unsubscribe_payload": bullish_unsubscribe_payload(&topic, Some(&symbol), "2"),
            "keepalive": bullish_keepalive_payload("keepalive-1"),
            "order_book": bullish_order_book_stream_spec(),
            "reconnect": bullish_reconnect_spec(),
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams || self.config.jwt_token.is_none() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bullish.private_streams_require_jwt_cookie_auth",
            });
        }
        Ok(format!("bullish:{}", self.config.private_ws_url))
    }
}

pub fn bullish_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

#[derive(Debug, Clone, PartialEq)]
pub enum BullishPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    SubscriptionAck { message: Option<String> },
    KeepaliveAck,
    Error(Value),
    Ignored(Value),
}

pub fn bullish_subscribe_payload(topic: &str, symbol: Option<&str>, id: &str) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("topic".to_string(), json!(topic));
    if let Some(symbol) = symbol {
        params.insert("symbol".to_string(), json!(normalize_symbol(symbol)));
    }
    json!({
        "jsonrpc": "2.0",
        "type": "command",
        "method": "subscribe",
        "params": params,
        "id": id,
    })
}

pub fn bullish_unsubscribe_payload(topic: &str, symbol: Option<&str>, id: &str) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("topic".to_string(), json!(topic));
    if let Some(symbol) = symbol {
        params.insert("symbol".to_string(), json!(normalize_symbol(symbol)));
    }
    json!({
        "jsonrpc": "2.0",
        "type": "command",
        "method": "unsubscribe",
        "params": params,
        "id": id,
    })
}

pub fn bullish_keepalive_payload(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "type": "command",
        "method": "keepalivePing",
        "params": {},
        "id": id,
    })
}

pub fn bullish_order_book_stream_spec() -> Value {
    json!({
        "url_path": "/trading-api/v1/market-data/orderbook",
        "topics": ["l1Orderbook", "l2Orderbook"],
        "subscribe": "json_rpc_2_command",
        "push_interval_ms": null,
        "depth": null,
        "sequence_fields": ["sequenceNumber", "sequenceNumberRange"],
        "sequence_policy": "reconnect_on_out_of_order_or_gap",
        "checksum": null,
        "resync": "rest_hybrid_orderbook_snapshot",
    })
}

pub fn bullish_stream_runtime_capability(
    public_streams: bool,
    private_streams: bool,
) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: if public_streams {
            CapabilitySupport::ws_only(
                "Bullish public JSON-RPC L1/L2 order-book specs and parser are implemented; REST hybrid snapshot is required for resync",
            )
        } else {
            CapabilitySupport::unsupported("bullish.public_streams_disabled")
        },
        private: if private_streams {
            CapabilitySupport::unsupported("bullish.private_streams_require_jwt_cookie_auth")
        } else {
            CapabilitySupport::unsupported("bullish.private_streams_disabled")
        },
        supports_subscribe: public_streams,
        supports_unsubscribe: public_streams,
        supports_public_subscribe: public_streams,
        supports_public_unsubscribe: public_streams,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(BULLISH_WS_PING_INTERVAL_MS as u64),
            timeout_ms: Some(BULLISH_WS_PONG_TIMEOUT_MS as u64),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: false,
            positions: false,
            orders: false,
        },
        reconnect_requires_login: false,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

pub fn bullish_reconnect_spec() -> Value {
    json!({
        "keepalive_interval_ms": BULLISH_WS_PING_INTERVAL_MS,
        "pong_timeout_ms": BULLISH_WS_PONG_TIMEOUT_MS,
        "stale_message_ms": BULLISH_WS_STALE_MESSAGE_MS,
        "requires_resubscribe": true,
        "orderbook_requires_rest_snapshot": true,
    })
}

pub fn bullish_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        BULLISH_WS_PING_INTERVAL_MS,
        BULLISH_WS_PONG_TIMEOUT_MS,
        BULLISH_WS_STALE_MESSAGE_MS,
    )
}

fn bullish_public_topic(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "tick".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "l2Orderbook".to_string()
        }
        PublicStreamKind::Candles { interval } => format!("candles:{interval}"),
    }
}

pub fn parse_bullish_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BullishPublicStreamMessage> {
    if let Some(result) = value.get("result") {
        let message = result
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string);
        if message
            .as_deref()
            .is_some_and(|message| message.contains("keepalivePong"))
        {
            return Ok(BullishPublicStreamMessage::KeepaliveAck);
        }
        return Ok(BullishPublicStreamMessage::SubscriptionAck { message });
    }
    if value.get("error").is_some()
        || value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("error"))
    {
        return Ok(BullishPublicStreamMessage::Error(value.clone()));
    }
    if !is_order_book_message(value) {
        return Ok(BullishPublicStreamMessage::Ignored(value.clone()));
    }
    let data = bullish_order_book_payload(exchange_id, value)?;
    let normalized = normalize_order_book_payload(&data);
    let book = parse_orderbook_snapshot(exchange_id, symbol, &normalized)?;
    Ok(BullishPublicStreamMessage::OrderBook(book))
}

pub fn parse_bullish_public_stream_events(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match parse_bullish_public_stream_message(exchange_id, symbol, value)? {
        BullishPublicStreamMessage::OrderBook(order_book) => {
            Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(
                OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    order_book,
                },
            )])
        }
        BullishPublicStreamMessage::SubscriptionAck { .. }
        | BullishPublicStreamMessage::KeepaliveAck
        | BullishPublicStreamMessage::Ignored(_) => Ok(Vec::new()),
        BullishPublicStreamMessage::Error(raw) => {
            Err(ExchangeApiError::Exchange(rustcta_types::ExchangeError {
                schema_version: rustcta_types::SchemaVersion::current(),
                exchange_id: exchange_id.clone(),
                class: rustcta_types::ExchangeErrorClass::ExchangeUnavailable,
                code: Some("BULLISH_STREAM_ERROR".to_string()),
                message: "Bullish public stream error".to_string(),
                retry_after_ms: None,
                order_id: None,
                client_order_id: None,
                raw: Some(raw),
                occurred_at: chrono::Utc::now(),
            }))
        }
    }
}

fn is_order_book_message(value: &Value) -> bool {
    value
        .get("dataType")
        .and_then(Value::as_str)
        .is_some_and(|data_type| {
            matches!(data_type, "V1TABookLevel1" | "V1TALevel2" | "V1TAOrderBook")
        })
        || value.get("data").is_some_and(|data| {
            data.get("sequenceNumber").is_some() || data.get("sequenceNumberRange").is_some()
        })
}

fn bullish_order_book_payload(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<Value> {
    let data = value.get("data").unwrap_or(value);
    if let Some(rows) = data.as_array() {
        return rows.first().cloned().ok_or_else(|| {
            ExchangeApiError::Exchange(rustcta_types::ExchangeError {
                schema_version: rustcta_types::SchemaVersion::current(),
                exchange_id: exchange_id.clone(),
                class: rustcta_types::ExchangeErrorClass::Decode,
                code: Some("BULLISH_STREAM_PARSE".to_string()),
                message: "Bullish order-book stream snapshot data is empty".to_string(),
                retry_after_ms: None,
                order_id: None,
                client_order_id: None,
                raw: Some(value.clone()),
                occurred_at: chrono::Utc::now(),
            })
        });
    }
    Ok(data.clone())
}

fn normalize_order_book_payload(data: &Value) -> Value {
    let mut object = data.as_object().cloned().unwrap_or_default();
    if !object.contains_key("bids") {
        if let Some(bid) = object.get("bid").cloned() {
            object.insert("bids".to_string(), bid);
        }
    }
    if !object.contains_key("asks") {
        if let Some(ask) = object.get("ask").cloned() {
            object.insert("asks".to_string(), ask);
        }
    }
    Value::Object(object)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '/', '_'], "")
        .to_ascii_uppercase()
}
