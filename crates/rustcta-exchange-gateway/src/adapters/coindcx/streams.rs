#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{coindcx_futures_symbol, coindcx_market_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_fills, parse_order, parse_positions};
use super::signing::coindcx_signature_for_payload;
use super::CoinDcxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const COINDCX_WS_PING_INTERVAL_MS: i64 = 25_000;
const COINDCX_WS_PONG_TIMEOUT_MS: i64 = 35_000;
const COINDCX_WS_STALE_MESSAGE_MS: i64 = 45_000;

#[derive(Debug, Clone, PartialEq)]
pub enum CoinDcxPublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Heartbeat,
    OrderBook(OrderBookResponse),
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinDcxPrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Heartbeat,
    Events(Vec<ExchangeStreamEvent>),
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinDcxWsSessionEvent {
    Public(CoinDcxPublicStreamMessage),
    Private(CoinDcxPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct CoinDcxPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct CoinDcxPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl CoinDcxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        Ok(format!(
            "coindcx-socketio:{}:{}",
            session.url,
            stream_label(&session.subscribe_payload).unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coindcx.subscribe_private_stream")?;
        let session = self.private_ws_session(subscription)?;
        Ok(format!(
            "coindcx-socketio:{}:{}:{}",
            session.url,
            stream_label(&session.subscribe_payload).unwrap_or("coindcx"),
            session.account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<CoinDcxPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        CoinDcxPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.spot_ws_url.clone(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<CoinDcxPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coindcx.private_ws_session")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&subscription.context, "coindcx.private_ws_session")?;
        CoinDcxPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.spot_ws_url.clone(),
            subscription,
            tenant_id,
            account_id,
            market_type,
            self.config.api_key.clone().unwrap_or_default(),
            self.config.api_secret.clone().unwrap_or_default(),
        )
    }
}

impl CoinDcxPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = coindcx_public_subscribe_payload(&subscription)?;
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

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        coindcx_ping_payload()
    }

    pub fn reconnect_policy() -> StreamReconnectPolicy {
        StreamReconnectPolicy {
            ping_interval_ms: COINDCX_WS_PING_INTERVAL_MS,
            pong_timeout_ms: COINDCX_WS_PONG_TIMEOUT_MS,
            stale_message_ms: COINDCX_WS_STALE_MESSAGE_MS,
            reconnect_backoff_ms: 1_000,
            max_reconnect_attempts: None,
        }
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
    ) -> ExchangeApiResult<Vec<CoinDcxWsSessionEvent>> {
        let value = parse_socketio_text(text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message =
            parse_coindcx_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(
            message,
            CoinDcxPublicStreamMessage::Heartbeat | CoinDcxPublicStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![CoinDcxWsSessionEvent::Public(message.clone())];
        match message {
            CoinDcxPublicStreamMessage::Heartbeat => {
                events.push(CoinDcxWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
                events.push(CoinDcxWsSessionEvent::Outbound(coindcx_ping_payload()));
            }
            CoinDcxPublicStreamMessage::OrderBook(book) => {
                events.push(CoinDcxWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(book),
                ]));
            }
            CoinDcxPublicStreamMessage::SubscriptionAck { .. }
            | CoinDcxPublicStreamMessage::Pong
            | CoinDcxPublicStreamMessage::Ignored => {}
        }
        Ok(events)
    }
}

impl CoinDcxPrivateWsSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        tenant_id: TenantId,
        account_id: AccountId,
        market_type: MarketType,
        api_key: String,
        api_secret: String,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload =
            coindcx_private_subscribe_payload(&api_key, &api_secret, subscription.kind.clone())?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        coindcx_ping_payload()
    }

    pub fn reconnect_policy() -> StreamReconnectPolicy {
        CoinDcxPublicWsSession::reconnect_policy()
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
    ) -> ExchangeApiResult<Vec<CoinDcxWsSessionEvent>> {
        let value = parse_socketio_text(text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_coindcx_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            &value,
        )?;
        if matches!(
            message,
            CoinDcxPrivateStreamMessage::Heartbeat | CoinDcxPrivateStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![CoinDcxWsSessionEvent::Private(message.clone())];
        match message {
            CoinDcxPrivateStreamMessage::Heartbeat => {
                events.push(CoinDcxWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
                events.push(CoinDcxWsSessionEvent::Outbound(coindcx_ping_payload()));
            }
            CoinDcxPrivateStreamMessage::Events(stream_events) if !stream_events.is_empty() => {
                events.push(CoinDcxWsSessionEvent::Stream(stream_events));
            }
            CoinDcxPrivateStreamMessage::Events(_)
            | CoinDcxPrivateStreamMessage::SubscriptionAck { .. }
            | CoinDcxPrivateStreamMessage::Pong
            | CoinDcxPrivateStreamMessage::Ignored => {}
        }
        Ok(events)
    }
}

pub fn coindcx_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

fn coindcx_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = if subscription.symbol.market_type == MarketType::Perpetual {
        coindcx_futures_symbol(&subscription.symbol.exchange_symbol.symbol)
    } else {
        coindcx_market_symbol(&subscription.symbol.exchange_symbol.symbol)
    };
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("{symbol}@orderbook@50-futures")
            } else {
                format!("{symbol}@orderbook@50")
            }
        }
        PublicStreamKind::Trades => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("{symbol}@trades-futures")
            } else {
                format!("{symbol}@trades")
            }
        }
        PublicStreamKind::Ticker => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("{symbol}@prices-futures")
            } else {
                format!("{symbol}@prices")
            }
        }
        PublicStreamKind::Candles { interval } => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("{symbol}_{interval}-futures")
            } else {
                format!("{symbol}_{interval}")
            }
        }
    };
    Ok(json!({
        "event": "join",
        "channelName": channel,
        "transport": "socket.io",
    }))
}

fn coindcx_private_subscribe_payload(
    api_key: &str,
    api_secret: &str,
    kind: PrivateStreamKind,
) -> ExchangeApiResult<Value> {
    let channel = "coindcx";
    let signature = coindcx_signature_for_payload(api_secret, r#"{"channel":"coindcx"}"#)?;
    Ok(json!({
        "event": "join",
        "channelName": channel,
        "apiKey": api_key,
        "authSignature": signature,
        "kind": format!("{kind:?}").to_ascii_lowercase(),
        "transport": "socket.io",
    }))
}

fn coindcx_ping_payload() -> Value {
    json!({ "event": "ping", "data": "Ping message" })
}

fn parse_socketio_text(text: &str) -> ExchangeApiResult<Value> {
    if text == "2" || text.eq_ignore_ascii_case("ping") {
        return Ok(json!({ "event": "ping" }));
    }
    if text == "3" || text.eq_ignore_ascii_case("pong") {
        return Ok(json!({ "event": "pong" }));
    }
    let payload = text
        .strip_prefix("42")
        .or_else(|| text.strip_prefix("40"))
        .unwrap_or(text);
    serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
        message: format!("invalid CoinDCX Socket.IO text frame: {error}"),
    })
}

fn parse_coindcx_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinDcxPublicStreamMessage> {
    if is_ping(value) {
        return Ok(CoinDcxPublicStreamMessage::Heartbeat);
    }
    if is_pong(value) {
        return Ok(CoinDcxPublicStreamMessage::Pong);
    }
    if let Some(channel) = ack_channel(value) {
        return Ok(CoinDcxPublicStreamMessage::SubscriptionAck { channel });
    }
    let event = socket_event_name(value).unwrap_or_default();
    let payload = socket_event_payload(value).unwrap_or(value);
    if event.contains("orderbook") || payload.get("bids").is_some() || payload.get("asks").is_some()
    {
        let book = OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book: parse_orderbook_snapshot(exchange_id, symbol, payload)?,
        };
        return Ok(CoinDcxPublicStreamMessage::OrderBook(book));
    }
    Ok(CoinDcxPublicStreamMessage::Ignored)
}

fn parse_coindcx_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<CoinDcxPrivateStreamMessage> {
    if is_ping(value) {
        return Ok(CoinDcxPrivateStreamMessage::Heartbeat);
    }
    if is_pong(value) {
        return Ok(CoinDcxPrivateStreamMessage::Pong);
    }
    if let Some(channel) = ack_channel(value) {
        return Ok(CoinDcxPrivateStreamMessage::SubscriptionAck { channel });
    }
    let event = socket_event_name(value).unwrap_or_default();
    let payload = socket_event_payload(value).unwrap_or(value);
    let mut events = Vec::new();
    if event.contains("order") {
        if let Some(order) = parse_order(exchange_id, None, market_type, payload)? {
            events.push(ExchangeStreamEvent::OrderUpdate(order));
        }
    } else if event.contains("trade") {
        if let Some(order) = parse_order(exchange_id, None, market_type, payload)? {
            events.push(ExchangeStreamEvent::OrderUpdate(order));
        }
        events.extend(
            parse_fills(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                None,
                market_type,
                &json!({ "data": [payload.clone()] }),
            )?
            .into_iter()
            .map(ExchangeStreamEvent::Fill),
        );
    } else if event.contains("position") || event.contains("df-position") {
        let positions = parse_positions(exchange_id, tenant_id, account_id, &[], payload)?;
        events.push(ExchangeStreamEvent::PositionSnapshot(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions,
            },
        ));
    } else if event.contains("balance") {
        let balances = parse_balances(
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            &[],
            &json!({ "data": [payload.clone()] }),
        )?;
        events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances,
        }));
    }
    if events.is_empty() {
        Ok(CoinDcxPrivateStreamMessage::Ignored)
    } else {
        Ok(CoinDcxPrivateStreamMessage::Events(events))
    }
}

fn is_ping(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("ping") || value.as_str() == Some("ping")
}

fn is_pong(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("pong") || value.as_str() == Some("pong")
}

fn ack_channel(value: &Value) -> Option<Option<String>> {
    let event = value.get("event").and_then(Value::as_str)?;
    if event == "joined" || event == "join" || event == "subscribed" {
        Some(
            value
                .get("channelName")
                .or_else(|| value.get("channel"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
        )
    } else {
        None
    }
}

fn socket_event_name(value: &Value) -> Option<String> {
    if let Some(array) = value.as_array() {
        return array.first().and_then(Value::as_str).map(str::to_string);
    }
    value
        .get("event")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn socket_event_payload(value: &Value) -> Option<&Value> {
    value
        .as_array()
        .and_then(|array| array.get(1))
        .or_else(|| value.get("data"))
        .or_else(|| value.get("payload"))
}

fn stream_label(value: &Value) -> Option<&str> {
    value
        .get("channelName")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
}
