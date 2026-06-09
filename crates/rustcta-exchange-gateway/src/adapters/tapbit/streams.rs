#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    CapabilitySupport, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    HeartbeatCapability, OrderBookResponse, PublicStreamKind, PublicStreamSubscription,
    ReconnectCapability, StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{
    normalize_depth, normalize_spot_symbol, normalize_swap_symbol, parse_spot_orderbook_snapshot,
    parse_swap_orderbook_snapshot,
};
use super::TapbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::StreamReconnectPolicy;

pub const TAPBIT_WS_PONG_PAYLOAD: &str = "pong";
pub const TAPBIT_WS_PING_INTERVAL_SECONDS: u64 = 5;
pub const TAPBIT_WS_MAX_MISSED_PINGS: u64 = 2;
pub const TAPBIT_WS_PING_INTERVAL_MS: i64 = 5_000;
pub const TAPBIT_WS_PONG_TIMEOUT_MS: i64 = 10_000;
pub const TAPBIT_WS_MAX_BOOK_DEPTH: u32 = 200;

impl TapbitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        let payload = tapbit_public_subscribe_payload(&subscription)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.public_ws_url,
            "payload": payload,
            "heartbeat": tapbit_heartbeat_spec(),
            "order_book": tapbit_order_book_stream_spec(),
        })
        .to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TapbitPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    Ticker(Value),
    SubscriptionAck { event: Option<String> },
    Pong,
    Ping,
}

pub fn tapbit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [tapbit_public_topic(subscription)?],
    }))
}

pub fn tapbit_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "unsubscribe",
        "args": [tapbit_public_topic(subscription)?],
    }))
}

pub fn tapbit_heartbeat_spec() -> Value {
    json!({
        "server_ping_interval_seconds": TAPBIT_WS_PING_INTERVAL_SECONDS,
        "max_missed_server_pings": TAPBIT_WS_MAX_MISSED_PINGS,
        "client_pong_payload": TAPBIT_WS_PONG_PAYLOAD,
    })
}

pub fn tapbit_order_book_stream_spec() -> Value {
    json!({
        "channels": {
            "spot": "spot/orderBook.{instrument_id}.[depth]",
            "perpetual": "usdt/orderBook.{instrument_id}.[depth]",
        },
        "supported_depths": [5, 10, 50, 100, 200],
        "selected_depth": TAPBIT_WS_MAX_BOOK_DEPTH,
        "push_interval_ms": null,
        "sequence_field": "version",
        "sequence_policy": "strictly_increasing",
        "checksum": null,
        "resync": "rest_order_book_snapshot_after_reconnect_or_version_gap",
    })
}

pub fn tapbit_ws_pong_payload() -> &'static str {
    TAPBIT_WS_PONG_PAYLOAD
}

pub fn tapbit_stream_runtime_capability() -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::ws_only(
            "Tapbit public WS orderBook/ticker specs and parsers are implemented; REST snapshot is required for order-book resync",
        ),
        private: CapabilitySupport::unsupported(
            "tapbit.subscribe_private_stream.no_official_private_ws_topics",
        ),
        supports_subscribe: true,
        supports_unsubscribe: true,
        supports_public_subscribe: true,
        supports_public_unsubscribe: true,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ServerPing,
            interval_ms: Some(TAPBIT_WS_PING_INTERVAL_MS as u64),
            timeout_ms: Some(TAPBIT_WS_PONG_TIMEOUT_MS as u64),
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

pub fn tapbit_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: TAPBIT_WS_PING_INTERVAL_MS,
        pong_timeout_ms: TAPBIT_WS_PONG_TIMEOUT_MS,
        stale_message_ms: TAPBIT_WS_PONG_TIMEOUT_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_tapbit_public_stream_message(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<TapbitPublicStreamMessage> {
    if is_ping(value) {
        return Ok(TapbitPublicStreamMessage::Ping);
    }
    if is_pong(value) {
        return Ok(TapbitPublicStreamMessage::Pong);
    }
    if value.get("event").is_some() || value.get("op").is_some() {
        return Ok(TapbitPublicStreamMessage::SubscriptionAck {
            event: value
                .get("event")
                .or_else(|| value.get("op"))
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    let topic = value.get("topic").and_then(Value::as_str).ok_or_else(|| {
        tapbit_stream_parse_error(
            exchange_id.clone(),
            "tapbit stream message missing topic",
            value,
        )
    })?;
    if topic.contains("/orderBook.") {
        let book_value = order_book_payload(value);
        let book = match market_type {
            MarketType::Spot => parse_spot_orderbook_snapshot(exchange_id, symbol, &book_value)?,
            MarketType::Perpetual => {
                parse_swap_orderbook_snapshot(exchange_id, symbol, &book_value)?
            }
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "tapbit.public_stream_market_type",
                });
            }
        };
        return Ok(TapbitPublicStreamMessage::OrderBook(book));
    }
    if topic.contains("/ticker.") {
        return Ok(TapbitPublicStreamMessage::Ticker(value.clone()));
    }
    Err(ExchangeApiError::Unsupported {
        operation: "tapbit.public_stream_topic",
    })
}

pub fn parse_tapbit_public_stream_events(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match parse_tapbit_public_stream_message(exchange_id, market_type, symbol, value)? {
        TapbitPublicStreamMessage::OrderBook(order_book) => {
            Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(
                OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    order_book,
                },
            )])
        }
        TapbitPublicStreamMessage::Ping | TapbitPublicStreamMessage::Pong => {
            Ok(vec![ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: chrono::Utc::now(),
            }])
        }
        TapbitPublicStreamMessage::SubscriptionAck { .. }
        | TapbitPublicStreamMessage::Ticker(_) => Ok(Vec::new()),
    }
}

fn order_book_payload(value: &Value) -> Value {
    value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .unwrap_or_else(|| value.clone())
}

fn tapbit_public_topic(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    match subscription.symbol.market_type {
        MarketType::Spot => tapbit_spot_public_topic(subscription),
        MarketType::Perpetual => tapbit_perpetual_public_topic(subscription),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.public_stream_market_type",
        }),
    }
}

fn is_ping(value: &Value) -> bool {
    match value {
        Value::String(text) => text.eq_ignore_ascii_case("ping"),
        Value::Object(map) => {
            map.get("ping").is_some_and(|value| !value.is_null())
                || map
                    .get("event")
                    .and_then(Value::as_str)
                    .is_some_and(|event| event.eq_ignore_ascii_case("ping"))
        }
        _ => false,
    }
}

fn is_pong(value: &Value) -> bool {
    match value {
        Value::String(text) => text.eq_ignore_ascii_case("pong"),
        Value::Object(map) => {
            map.get("pong").is_some_and(|value| !value.is_null())
                || map
                    .get("event")
                    .and_then(Value::as_str)
                    .is_some_and(|event| event.eq_ignore_ascii_case("pong"))
        }
        _ => false,
    }
}

fn tapbit_stream_parse_error(
    exchange_id: ExchangeId,
    message: &str,
    raw: &Value,
) -> ExchangeApiError {
    ExchangeApiError::Exchange(rustcta_types::ExchangeError {
        schema_version: rustcta_types::SchemaVersion::current(),
        exchange_id,
        class: rustcta_types::ExchangeErrorClass::Decode,
        code: Some("TAPBIT_STREAM_PARSE".to_string()),
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(raw.clone()),
        occurred_at: chrono::Utc::now(),
    })
}

fn tapbit_spot_public_topic(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let instrument_id =
        normalize_spot_symbol(&subscription.symbol.exchange_symbol.symbol)?.replace('/', "");
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok(format!(
            "spot/orderBook.{instrument_id}.{}",
            normalize_stream_depth(subscription)?
        )),
        PublicStreamKind::Ticker => Ok(format!("spot/ticker.{instrument_id}")),
        PublicStreamKind::Trades => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.spot_public_trades_stream",
        }),
        PublicStreamKind::Candles { .. } => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.spot_public_candles_stream",
        }),
    }
}

fn tapbit_perpetual_public_topic(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let instrument_id = normalize_swap_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok(format!(
            "usdt/orderBook.{instrument_id}.{}",
            normalize_stream_depth(subscription)?
        )),
        PublicStreamKind::Ticker => Ok(format!("usdt/ticker.{instrument_id}")),
        PublicStreamKind::Trades => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.usdt_public_trades_stream",
        }),
        PublicStreamKind::Candles { .. } => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.usdt_public_candles_stream",
        }),
    }
}

fn normalize_stream_depth(subscription: &PublicStreamSubscription) -> ExchangeApiResult<u32> {
    let depth = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            normalize_depth(TAPBIT_WS_MAX_BOOK_DEPTH)
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "tapbit.public_stream_depth_kind",
            });
        }
    };
    Ok(depth)
}
