#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, SchemaVersion};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, parse_orderbook_snapshot};
use super::private_parser::parse_order_state;
use super::signing::{prehash, sign_prehash};
use super::AscendexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const ASCENDEX_WS_PING_INTERVAL_MS: i64 = 15_000;
const ASCENDEX_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const ASCENDEX_WS_STALE_MESSAGE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq)]
pub enum AscendexPublicStreamMessage {
    SubscriptionAck {
        id: Option<String>,
        channel: Option<String>,
    },
    Pong,
    Heartbeat,
    OrderBook(OrderBookResponse),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AscendexPrivateStreamMessage {
    AuthAck {
        id: Option<String>,
    },
    SubscriptionAck {
        id: Option<String>,
        channel: Option<String>,
    },
    Pong,
    Heartbeat,
    Events(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AscendexWsSessionEvent {
    Public(AscendexPublicStreamMessage),
    Private(AscendexPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone)]
pub struct AscendexPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct AscendexPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    auth_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

pub fn ascendex_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
        supports_positions: false,
        supports_account: false,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

impl AscendexGatewayAdapter {
    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<AscendexPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let endpoint =
            public_ws_endpoint(&self.config.rest_base_url, subscription.symbol.market_type);
        AscendexPublicWsSession::new(self.exchange_id.clone(), endpoint, subscription)
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<AscendexPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let (account_group, api_key, api_secret) =
            self.private_credentials("ascendex.private_ws_session")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let endpoint = private_ws_endpoint(&self.config.rest_base_url, account_group, market_type);
        AscendexPrivateWsSession::new(
            self.exchange_id.clone(),
            endpoint,
            subscription,
            market_type,
            symbol_hint,
            api_key,
            api_secret,
        )
    }

    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.public_ws_session(subscription)?;
        let channel = session
            .subscribe_payload
            .get("ch")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let endpoint = session.url;
        Ok(format!("ascendex:{endpoint}:{channel}"))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.private_ws_session(subscription, None)?;
        let channel = session
            .subscribe_payload
            .get("ch")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let endpoint = session.url;
        Ok(format!("ascendex:{endpoint}:{channel}"))
    }
}

impl AscendexPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload =
            public_subscribe_payload(&subscription, subscription.context.request_id.as_deref())?;
        let mut state =
            StreamRuntimeState::new(exchange_id.clone(), subscription.symbol.market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            symbol: subscription.symbol,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>, id: Option<&str>) -> Value {
        self.state.on_ping_sent(now);
        ping_payload(id)
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) {
        self.state.on_connected(now);
    }

    pub fn on_disconnected(&mut self, now: DateTime<Utc>) {
        self.state.on_disconnected(now);
    }

    pub fn supervisor_action(
        &self,
        now: DateTime<Utc>,
        policy: &StreamReconnectPolicy,
    ) -> StreamSupervisorAction {
        self.state.decide(now, policy)
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<AscendexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_public_stream_message(
            &self.exchange_id,
            self.symbol.market_type,
            self.symbol.clone(),
            &value,
        )?;
        if matches!(
            message,
            AscendexPublicStreamMessage::Heartbeat | AscendexPublicStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![AscendexWsSessionEvent::Public(message.clone())];
        match message {
            AscendexPublicStreamMessage::Heartbeat => {
                events.push(AscendexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
            }
            AscendexPublicStreamMessage::OrderBook(book) => {
                events.push(AscendexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(book),
                ]));
            }
            AscendexPublicStreamMessage::SubscriptionAck { .. }
            | AscendexPublicStreamMessage::Pong => {}
        }
        Ok(events)
    }
}

impl AscendexPrivateWsSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        market_type: MarketType,
        symbol_hint: Option<SymbolScope>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let auth_payload = auth_payload(
            api_key,
            api_secret,
            market_type,
            subscription.context.request_id.as_deref(),
        )?;
        let subscribe_payload = private_subscribe_payload(
            &subscription,
            market_type,
            subscription.context.request_id.as_deref(),
        )?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            market_type,
            symbol_hint,
            auth_payload,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.auth_payload.clone(), self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>, id: Option<&str>) -> Value {
        self.state.on_ping_sent(now);
        ping_payload(id)
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) {
        self.state.on_connected(now);
    }

    pub fn on_disconnected(&mut self, now: DateTime<Utc>) {
        self.state.on_disconnected(now);
    }

    pub fn supervisor_action(
        &self,
        now: DateTime<Utc>,
        policy: &StreamReconnectPolicy,
    ) -> StreamSupervisorAction {
        self.state.decide(now, policy)
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<AscendexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_private_stream_message(
            &self.exchange_id,
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(
            message,
            AscendexPrivateStreamMessage::Heartbeat | AscendexPrivateStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![AscendexWsSessionEvent::Private(message.clone())];
        match message {
            AscendexPrivateStreamMessage::Heartbeat => {
                events.push(AscendexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
            }
            AscendexPrivateStreamMessage::Events(stream_events) => {
                events.push(AscendexWsSessionEvent::Stream(stream_events));
            }
            AscendexPrivateStreamMessage::AuthAck { .. }
            | AscendexPrivateStreamMessage::SubscriptionAck { .. }
            | AscendexPrivateStreamMessage::Pong => {}
        }
        Ok(events)
    }
}

pub fn ascendex_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: ASCENDEX_WS_PING_INTERVAL_MS,
        pong_timeout_ms: ASCENDEX_WS_PONG_TIMEOUT_MS,
        stale_message_ms: ASCENDEX_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: Option<&str>,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "sub",
        "id": id.unwrap_or("subscribe"),
        "ch": public_channel(subscription)?,
    }))
}

pub fn private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    id: Option<&str>,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "sub",
        "id": id.unwrap_or("private-subscribe"),
        "ch": private_channel(subscription, market_type)?,
    }))
}

pub fn auth_payload(
    api_key: &str,
    api_secret: &str,
    market_type: MarketType,
    id: Option<&str>,
) -> ExchangeApiResult<Value> {
    let timestamp = Utc::now().timestamp_millis().to_string();
    let sign_path = if market_type == MarketType::Perpetual {
        "v2/stream"
    } else {
        "stream"
    };
    let signature = sign_prehash(api_secret, &prehash(&timestamp, sign_path))?;
    Ok(json!({
        "op": "auth",
        "id": id.unwrap_or("auth"),
        "t": timestamp,
        "key": api_key,
        "sig": signature,
    }))
}

pub fn ping_payload(id: Option<&str>) -> Value {
    match id {
        Some(id) => json!({ "op": "ping", "id": id }),
        None => json!({ "op": "ping" }),
    }
}

pub fn pong_payload() -> Value {
    json!({ "op": "pong" })
}

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<AscendexPublicStreamMessage> {
    if is_server_ping(value) {
        return Ok(AscendexPublicStreamMessage::Heartbeat);
    }
    if value.get("m").and_then(Value::as_str) == Some("pong") {
        return Ok(AscendexPublicStreamMessage::Pong);
    }
    if matches!(
        value.get("m").and_then(Value::as_str),
        Some("sub" | "unsub")
    ) {
        return Ok(AscendexPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_str).map(str::to_string),
            channel: value.get("ch").and_then(Value::as_str).map(str::to_string),
        });
    }
    if matches!(
        value.get("m").and_then(Value::as_str),
        Some("depth" | "depth-snapshot")
    ) {
        let order_book = parse_orderbook_snapshot(exchange_id, market_type, symbol, value)?;
        return Ok(AscendexPublicStreamMessage::OrderBook(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book,
        }));
    }
    Err(stream_parse_error(
        exchange_id.clone(),
        "unsupported ascendex public stream message",
        value,
    ))
}

pub fn parse_private_stream_message(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol_hint: Option<rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<AscendexPrivateStreamMessage> {
    if is_server_ping(value) {
        return Ok(AscendexPrivateStreamMessage::Heartbeat);
    }
    match value.get("m").and_then(Value::as_str) {
        Some("pong") => return Ok(AscendexPrivateStreamMessage::Pong),
        Some("auth") => {
            return Ok(AscendexPrivateStreamMessage::AuthAck {
                id: value.get("id").and_then(Value::as_str).map(str::to_string),
            });
        }
        Some("sub" | "unsub") => {
            return Ok(AscendexPrivateStreamMessage::SubscriptionAck {
                id: value.get("id").and_then(Value::as_str).map(str::to_string),
                channel: value.get("ch").and_then(Value::as_str).map(str::to_string),
            });
        }
        Some("order" | "futures-order") => {
            let row = value
                .get("data")
                .or_else(|| value.get("info"))
                .unwrap_or(value);
            let order = parse_order_state(exchange_id, symbol_hint.as_ref(), market_type, row)?;
            return Ok(AscendexPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::OrderUpdate(order),
            ]));
        }
        _ => {}
    }
    Err(stream_parse_error(
        exchange_id.clone(),
        "unsupported ascendex private stream message",
        value,
    ))
}

pub fn public_ws_endpoint(rest_base_url: &str, market_type: MarketType) -> String {
    format!(
        "{}/api/pro/{}/stream",
        ws_base(rest_base_url),
        if market_type == MarketType::Perpetual {
            "v2"
        } else {
            "v1"
        }
    )
}

pub fn private_ws_endpoint(
    rest_base_url: &str,
    account_group: &str,
    market_type: MarketType,
) -> String {
    format!(
        "{}/{}/api/pro/{}/stream",
        ws_base(rest_base_url),
        account_group.trim_matches('/'),
        if market_type == MarketType::Perpetual {
            "v2"
        } else {
            "v1"
        }
    )
}

fn public_channel(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let symbol = normalize_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    Ok(match &subscription.kind {
        PublicStreamKind::Trades => format!("trades:{symbol}"),
        PublicStreamKind::Ticker => format!("bbo:{symbol}"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("depth:{symbol}")
        }
        PublicStreamKind::Candles { interval } => format!("bar:{interval}:{symbol}"),
    })
}

fn private_channel(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    if market_type == MarketType::Perpetual {
        return match subscription.kind {
            PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("futures-order".to_string()),
            PrivateStreamKind::Balances
            | PrivateStreamKind::Positions
            | PrivateStreamKind::Account => Ok("futures-account-update".to_string()),
        };
    }
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account => {
            Ok("order:cash".to_string())
        }
        PrivateStreamKind::Balances => Ok("order:cash".to_string()),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "ascendex.spot_private_positions_stream",
        }),
    }
}

fn ws_base(rest_base_url: &str) -> String {
    rest_base_url
        .trim_end_matches('/')
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
}

fn is_server_ping(value: &Value) -> bool {
    value.get("m").and_then(Value::as_str) == Some("ping")
        || value.get("op").and_then(Value::as_str) == Some("ping")
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid AscendEX websocket JSON: {error}"),
            &Value::String(text.to_string()),
        )
    })
}

fn stream_parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
