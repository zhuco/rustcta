#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AuthRenewalKind, AuthRenewalPolicy, ExchangeApiError, ExchangeApiResult,
    ExchangeStreamEvent, OrderBookResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{
    normalize_spot_symbol, parse_futures_orderbook_snapshot, parse_spot_orderbook_snapshot,
};
use super::private_parser::{parse_fills, parse_order_state};
use super::signing::sign_coinstore_payload;
use super::CoinstoreGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::StreamReconnectPolicy;

pub const COINSTORE_SPOT_WS_SERVER_PING_INTERVAL_SECONDS: u64 = 180;
pub const COINSTORE_SPOT_WS_PONG_TIMEOUT_SECONDS: u64 = 600;
pub const COINSTORE_FUTURES_SOCKET_IO_PING_PAYLOAD: &str = "2";
pub const COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD: &str = "3";

impl CoinstoreGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let payload = coinstore_public_subscribe_payload(&subscription)?;
        let url = match subscription.symbol.market_type {
            MarketType::Spot => &self.config.spot_public_ws_url,
            MarketType::Perpetual => &self.config.futures_public_ws_url,
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": url,
            "payload": payload,
            "heartbeat": coinstore_public_heartbeat_spec(subscription.symbol.market_type),
            "reconnect": coinstore_stream_reconnect_policy(),
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinstore.private_stream")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        if market_type != MarketType::Perpetual {
            return self.unsupported("coinstore.spot_private_stream");
        }
        let payloads = coinstore_futures_private_initial_payloads(
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            &subscription,
        )?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.futures_private_ws_url,
            "payloads": payloads,
            "heartbeat": {
                "socket_io_ping": COINSTORE_FUTURES_SOCKET_IO_PING_PAYLOAD,
                "socket_io_pong": COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD
            },
            "reconnect": coinstore_stream_reconnect_policy(),
        })
        .to_string())
    }
}

pub fn coinstore_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
        supports_positions: false,
        supports_account: false,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::Ack,
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn coinstore_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    match subscription.symbol.market_type {
        MarketType::Spot => Ok(json!({
            "op": "SUB",
            "channel": [coinstore_spot_public_channel(subscription)?],
            "id": 1,
        })),
        MarketType::Perpetual => Ok(socket_io_event(
            "subscribe",
            json!({
                "header": { "type": 1003 },
                "body": {
                    "topics": [coinstore_futures_public_topic(subscription)?]
                }
            }),
        )),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.public_stream_market_type",
        }),
    }
}

pub fn coinstore_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    match subscription.symbol.market_type {
        MarketType::Spot => Ok(json!({
            "op": "UNSUB",
            "channel": [coinstore_spot_public_channel(subscription)?],
            "id": 1,
        })),
        MarketType::Perpetual => Ok(socket_io_event(
            "unsubscribe",
            json!({
                "header": { "type": 1004 },
                "body": {
                    "topics": [coinstore_futures_public_topic(subscription)?]
                }
            }),
        )),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.public_stream_market_type",
        }),
    }
}

pub fn coinstore_futures_private_initial_payloads(
    api_key: &str,
    api_secret: &str,
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Vec<Value>> {
    let expires = Utc::now().timestamp_millis();
    let signature = sign_coinstore_payload(api_secret, expires, "")?;
    Ok(vec![
        socket_io_event(
            "auth",
            json!({
                "header": { "type": 1001 },
                "body": {
                    "apiKey": api_key,
                    "expires": expires.to_string(),
                    "signature": signature,
                }
            }),
        ),
        socket_io_event(
            "subscribe",
            json!({
                "header": { "type": 1003 },
                "body": {
                    "topics": [coinstore_private_topic(&subscription.kind)?]
                }
            }),
        ),
    ])
}

pub fn coinstore_spot_pong_payload(epoch_millis: i64) -> Value {
    json!({
        "op": "pong",
        "epochMillis": epoch_millis,
    })
}

pub fn coinstore_futures_socket_io_pong_payload() -> &'static str {
    COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD
}

pub fn coinstore_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: 30_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 180_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn coinstore_auth_renewal_policy() -> AuthRenewalPolicy {
    AuthRenewalPolicy {
        kind: AuthRenewalKind::ReLogin,
        renew_before_expiry_ms: 30_000,
        renewal_interval_ms: Some(25_000),
        reconnect_on_renewal_failure: true,
        resubscribe_after_renewal: true,
    }
}

pub fn parse_coinstore_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinstoreWsMessage> {
    if is_spot_pong(value) || value == COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD {
        return Ok(CoinstoreWsMessage::Pong);
    }
    if value == COINSTORE_FUTURES_SOCKET_IO_PING_PAYLOAD {
        return Ok(CoinstoreWsMessage::Ping);
    }
    if value.get("op").is_some() || value.get("event").is_some() {
        return Ok(CoinstoreWsMessage::Ack(value.clone()));
    }
    let channel = value
        .get("channel")
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if channel.contains("depth")
        || (symbol.market_type == MarketType::Perpetual && channel == "future_snapshot_depth")
    {
        return Ok(CoinstoreWsMessage::OrderBook(
            parse_coinstore_stream_order_book(exchange_id, symbol, value)?,
        ));
    }
    if channel.contains("ticker") || channel == "indicator" {
        return Ok(CoinstoreWsMessage::Ticker(value.clone()));
    }
    if channel.contains("trade") || channel == "future_tick" {
        return Ok(CoinstoreWsMessage::Trade(value.clone()));
    }
    Err(ExchangeApiError::Exchange(rustcta_types::ExchangeError {
        schema_version: rustcta_types::SchemaVersion::current(),
        exchange_id: exchange_id.clone(),
        class: rustcta_types::ExchangeErrorClass::Decode,
        code: Some("COINSTORE_STREAM_PARSE".to_string()),
        message: "coinstore stream message missing recognized channel".to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    }))
}

pub fn parse_coinstore_public_stream_events(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match parse_coinstore_public_stream_message(exchange_id, symbol, value)? {
        CoinstoreWsMessage::Ping | CoinstoreWsMessage::Pong => {
            Ok(vec![ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            }])
        }
        CoinstoreWsMessage::OrderBook(book) => {
            Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(book)])
        }
        CoinstoreWsMessage::Ack(_)
        | CoinstoreWsMessage::Ticker(_)
        | CoinstoreWsMessage::Trade(_)
        | CoinstoreWsMessage::Private(_) => Ok(Vec::new()),
    }
}

pub fn parse_coinstore_private_stream_message(value: &Value) -> CoinstoreWsMessage {
    if value == COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD {
        return CoinstoreWsMessage::Pong;
    }
    if value == COINSTORE_FUTURES_SOCKET_IO_PING_PAYLOAD {
        return CoinstoreWsMessage::Ping;
    }
    let topic = value
        .get("topic")
        .or_else(|| value.get("channel"))
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if topic == "match" {
        CoinstoreWsMessage::Private(value.clone())
    } else {
        CoinstoreWsMessage::Ack(value.clone())
    }
}

pub fn parse_coinstore_private_stream_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match parse_coinstore_private_stream_message(value) {
        CoinstoreWsMessage::Ping | CoinstoreWsMessage::Pong => {
            Ok(vec![ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            }])
        }
        CoinstoreWsMessage::Ack(_) => Ok(Vec::new()),
        CoinstoreWsMessage::Private(_) => {
            let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
            match subscription.kind {
                PrivateStreamKind::Orders => Ok(vec![ExchangeStreamEvent::OrderUpdate(
                    parse_order_state(exchange_id, symbol_hint, market_type, value)?,
                )]),
                PrivateStreamKind::Fills => {
                    let rows_payload = private_match_rows_payload(value);
                    let fills = parse_fills(
                        exchange_id,
                        tenant_id,
                        account_id,
                        symbol_hint,
                        market_type,
                        &rows_payload,
                    )?;
                    Ok(fills.into_iter().map(ExchangeStreamEvent::Fill).collect())
                }
                PrivateStreamKind::Balances
                | PrivateStreamKind::Positions
                | PrivateStreamKind::Account => Ok(Vec::new()),
            }
        }
        CoinstoreWsMessage::OrderBook(_)
        | CoinstoreWsMessage::Ticker(_)
        | CoinstoreWsMessage::Trade(_) => Ok(Vec::new()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinstoreWsMessage {
    Ack(Value),
    Ping,
    Pong,
    OrderBook(OrderBookResponse),
    Ticker(Value),
    Trade(Value),
    Private(Value),
}

fn coinstore_spot_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let symbol = normalize_spot_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    match &subscription.kind {
        PublicStreamKind::Trades => Ok(format!("{symbol}@trade")),
        PublicStreamKind::Ticker => Ok(format!("{symbol}@ticker")),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            Ok(format!("{symbol}@depth@{}", 20))
        }
        PublicStreamKind::Candles { interval } => Ok(format!(
            "{symbol}@kline@{}",
            coinstore_spot_kline_interval(interval)?
        )),
    }
}

fn coinstore_futures_public_topic(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    match &subscription.kind {
        PublicStreamKind::Trades => Ok("future_tick".to_string()),
        PublicStreamKind::Ticker => Ok("indicator".to_string()),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            Ok("future_snapshot_depth".to_string())
        }
        PublicStreamKind::Candles { .. } => Ok("future_kline".to_string()),
    }
}

fn coinstore_private_topic(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("match"),
        PrivateStreamKind::Balances => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.private_balances_stream",
        }),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.private_positions_stream",
        }),
        PrivateStreamKind::Account => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.private_account_stream",
        }),
    }
}

fn coinstore_spot_kline_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval {
        "1m" | "min_1" => Ok("min_1"),
        "5m" | "min_5" => Ok("min_5"),
        "15m" | "min_15" => Ok("min_15"),
        "30m" | "min_30" => Ok("min_30"),
        "1h" | "hour_1" => Ok("hour_1"),
        "4h" | "hour_4" => Ok("hour_4"),
        "12h" | "hour_12" => Ok("hour_12"),
        "1d" | "day_1" => Ok("day_1"),
        "1w" | "week_1" => Ok("week_1"),
        "1M" | "mon_1" => Ok("mon_1"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.spot_kline_interval",
        }),
    }
}

fn coinstore_public_heartbeat_spec(market_type: MarketType) -> Value {
    match market_type {
        MarketType::Spot => json!({
            "server_ping_interval_seconds": COINSTORE_SPOT_WS_SERVER_PING_INTERVAL_SECONDS,
            "pong_timeout_seconds": COINSTORE_SPOT_WS_PONG_TIMEOUT_SECONDS,
            "client_pong_payload": coinstore_spot_pong_payload(Utc::now().timestamp_millis()),
        }),
        MarketType::Perpetual => json!({
            "socket_io_ping": COINSTORE_FUTURES_SOCKET_IO_PING_PAYLOAD,
            "socket_io_pong": COINSTORE_FUTURES_SOCKET_IO_PONG_PAYLOAD,
        }),
        _ => Value::Null,
    }
}

fn socket_io_event(event: &str, body: Value) -> Value {
    Value::String(format!("42{}", json!([event, body])))
}

fn is_spot_pong(value: &Value) -> bool {
    value
        .get("op")
        .and_then(Value::as_str)
        .is_some_and(|op| op.eq_ignore_ascii_case("pong"))
}

fn parse_coinstore_stream_order_book(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let order_book = match symbol.market_type {
        MarketType::Spot => parse_spot_orderbook_snapshot(exchange_id, symbol.clone(), value)?,
        MarketType::Perpetual => {
            parse_futures_orderbook_snapshot(exchange_id, symbol.clone(), value)?
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinstore.public_stream_order_book_market_type",
            })
        }
    };
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(symbol.exchange, None),
        order_book,
    })
}

fn private_match_rows_payload(value: &Value) -> Value {
    let payload = value.get("data").unwrap_or(value);
    if payload.is_array() {
        json!({ "data": payload })
    } else {
        json!({ "data": [payload] })
    }
}
