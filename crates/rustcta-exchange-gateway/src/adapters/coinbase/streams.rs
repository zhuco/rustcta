#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, OrderBookLevel,
    OrderBookSnapshot, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{normalize_coinbase_symbol, number_from_value, validation_error};
use super::private_parser::parse_order_state;
use super::CoinbaseGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

impl CoinbaseGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        let request = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "coinbase:{}:{}:{}",
            session.url,
            request
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            request
                .get("product_ids")
                .and_then(Value::as_array)
                .and_then(|products| products.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("coinbase.subscribe_private_stream")?;
        let account_id = subscription.account_id.clone();
        let session = self.private_ws_session(subscription)?;
        let request = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "coinbase:{}:{}:{}",
            session.url,
            request
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<CoinbasePublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        CoinbasePublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
            self.optional_bearer_token(),
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<CoinbasePrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("coinbase.private_ws_session")?;
        CoinbasePrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            self.config.bearer_token.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum CoinbasePublicStreamMessage {
    OrderBook(OrderBookResponse),
    Heartbeat,
    SubscriptionAck,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinbasePublicOrderBookWsPolicy {
    pub public_channel: &'static str,
    pub heartbeat_channel: &'static str,
    pub public_interval_ms: Option<u64>,
    pub depth: &'static str,
    pub sequence_field: &'static str,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub rest_snapshot_endpoint: &'static str,
    pub resync: &'static str,
}

pub fn coinbase_public_order_book_ws_policy() -> CoinbasePublicOrderBookWsPolicy {
    CoinbasePublicOrderBookWsPolicy {
        public_channel: "level2",
        heartbeat_channel: "heartbeats",
        public_interval_ms: None,
        depth: "full level2 order book deltas; no fixed official depth parameter",
        sequence_field: "sequence_num",
        checksum: None,
        update_semantics: "snapshot plus update events with absolute new_quantity; quantity 0 removes the price level",
        rest_snapshot_endpoint: "GET /api/v3/brokerage/product_book",
        resync: "subscribe level2 and heartbeats, rebuild from REST product_book after reconnect, heartbeat loss, sequence_num gap, or sequence regression",
    }
}

pub fn coinbase_sequence_is_contiguous(previous: Option<u64>, next: u64) -> bool {
    match previous {
        Some(previous) => next == previous.saturating_add(1),
        None => true,
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum CoinbaseWsSessionEvent {
    Public(CoinbasePublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CoinbasePublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    market_type: MarketType,
    canonical_symbol: CanonicalSymbol,
    subscribe_payload: Value,
    heartbeat_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CoinbasePrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    subscribe_payload: Value,
    heartbeat_payload: Value,
    state: StreamRuntimeState,
}

impl CoinbasePublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
        jwt: Option<String>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "coinbase public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let market_type = subscription.symbol.market_type;
        let canonical_symbol = subscription
            .symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase public WS session requires canonical_symbol".to_string(),
            })?;
        let subscribe_payload = coinbase_public_subscribe_payload(&subscription, jwt.as_deref())?;
        let heartbeat_payload = coinbase_heartbeat_subscribe_payload(jwt.as_deref());
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 2;
        Ok(Self {
            url,
            exchange_id,
            market_type,
            canonical_symbol,
            subscribe_payload,
            heartbeat_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![
            self.subscribe_payload.clone(),
            self.heartbeat_payload.clone(),
        ]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        self.heartbeat_payload.clone()
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
    ) -> ExchangeApiResult<Vec<CoinbaseWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        if is_coinbase_heartbeat(&value) {
            self.state.on_pong(now);
            let heartbeat = ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: self.exchange_id.clone(),
                received_at: now,
            };
            return Ok(vec![
                CoinbaseWsSessionEvent::Public(CoinbasePublicStreamMessage::Heartbeat),
                CoinbaseWsSessionEvent::Stream(vec![heartbeat]),
            ]);
        }
        if is_subscription_ack(&value) {
            return Ok(vec![CoinbaseWsSessionEvent::Public(
                CoinbasePublicStreamMessage::SubscriptionAck,
            )]);
        }
        let channel = value
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if channel == "level2" {
            let response = parse_level2_snapshot_event(
                &self.exchange_id,
                self.market_type,
                self.canonical_symbol.clone(),
                &value,
            )?;
            return Ok(vec![
                CoinbaseWsSessionEvent::Public(CoinbasePublicStreamMessage::OrderBook(
                    response.clone(),
                )),
                CoinbaseWsSessionEvent::Stream(vec![ExchangeStreamEvent::OrderBookSnapshot(
                    response,
                )]),
            ]);
        }
        Err(ExchangeApiError::Unsupported {
            operation: "coinbase.public_ws_channel",
        })
    }
}

impl CoinbasePrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        jwt: String,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "coinbase private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "coinbase private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let subscribe_payload = coinbase_private_subscribe_payload(&subscription, Some(&jwt))?;
        let heartbeat_payload = coinbase_heartbeat_subscribe_payload(Some(&jwt));
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 2;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            market_type,
            subscribe_payload,
            heartbeat_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![
            self.subscribe_payload.clone(),
            self.heartbeat_payload.clone(),
        ]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        self.heartbeat_payload.clone()
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
    ) -> ExchangeApiResult<Vec<CoinbaseWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let stream_events = parse_coinbase_private_stream_events(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            &value,
        )?;
        if stream_events
            .iter()
            .any(|event| matches!(event, ExchangeStreamEvent::Heartbeat { .. }))
        {
            self.state.on_pong(now);
        }
        Ok(vec![
            CoinbaseWsSessionEvent::Private(stream_events.clone()),
            CoinbaseWsSessionEvent::Stream(stream_events),
        ])
    }
}

pub fn coinbase_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    jwt: Option<&str>,
) -> ExchangeApiResult<Value> {
    let mut payload = json!({
        "type": "subscribe",
        "product_ids": [normalize_coinbase_symbol(&subscription.symbol.exchange_symbol.symbol)?],
        "channel": coinbase_channel(&subscription.kind)?,
    });
    if let Some(jwt) = jwt.filter(|jwt| !jwt.trim().is_empty()) {
        payload["jwt"] = Value::String(jwt.to_string());
    }
    Ok(payload)
}

pub fn coinbase_heartbeat_subscribe_payload(jwt: Option<&str>) -> Value {
    let mut payload = json!({
        "type": "subscribe",
        "channel": "heartbeats",
    });
    if let Some(jwt) = jwt.filter(|jwt| !jwt.trim().is_empty()) {
        payload["jwt"] = Value::String(jwt.to_string());
    }
    payload
}

pub fn coinbase_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    jwt: Option<&str>,
) -> ExchangeApiResult<Value> {
    let jwt = jwt
        .filter(|jwt| !jwt.trim().is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "coinbase.private_stream_missing_jwt",
        })?;
    Ok(json!({
        "type": "subscribe",
        "channel": private_channel(&subscription.kind)?,
        "product_ids": [],
        "jwt": jwt,
    }))
}

pub fn parse_coinbase_private_stream_events(
    exchange_id: &ExchangeId,
    _tenant_id: TenantId,
    _account_id: AccountId,
    _market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_coinbase_heartbeat(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_subscription_ack(value) {
        return Ok(Vec::new());
    }
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match channel {
        "user" => parse_user_channel_events(exchange_id, value),
        "futures_balance_summary" => Ok(Vec::new()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinbase.private_ws_channel",
        }),
    }
}

pub fn parse_level2_snapshot_event(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    canonical_symbol: CanonicalSymbol,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let event = first_event(value)?;
    let product_id = event
        .get("product_id")
        .or_else(|| event.get("productId"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            stream_parse_error(
                exchange_id.clone(),
                "level2 event missing product_id",
                value,
            )
        })?;
    let updates = event
        .get("updates")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            stream_parse_error(exchange_id.clone(), "level2 event missing updates", value)
        })?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for update in updates {
        let side = update.get("side").and_then(Value::as_str).ok_or_else(|| {
            stream_parse_error(exchange_id.clone(), "level2 update missing side", update)
        })?;
        let price = update
            .get("price_level")
            .or_else(|| update.get("price"))
            .and_then(number_from_value)
            .ok_or_else(|| {
                stream_parse_error(exchange_id.clone(), "level2 update missing price", update)
            })?;
        let quantity = update
            .get("new_quantity")
            .or_else(|| update.get("quantity"))
            .or_else(|| update.get("size"))
            .and_then(number_from_value)
            .ok_or_else(|| {
                stream_parse_error(
                    exchange_id.clone(),
                    "level2 update missing quantity",
                    update,
                )
            })?;
        if quantity <= 0.0 {
            continue;
        }
        let level = OrderBookLevel::new(price, quantity).map_err(validation_error)?;
        if side.eq_ignore_ascii_case("bid") || side.eq_ignore_ascii_case("buy") {
            bids.push(level);
        } else if side.eq_ignore_ascii_case("ask") || side.eq_ignore_ascii_case("sell") {
            asks.push(level);
        }
    }
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(
        rustcta_types::ExchangeSymbol::new(exchange_id.clone(), market_type, product_id)
            .map_err(validation_error)?,
    );
    snapshot.sequence = value
        .get("sequence_num")
        .or_else(|| value.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.with_timezone(&Utc));
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), None),
        order_book: snapshot,
    })
}

fn coinbase_channel(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PublicStreamKind::Trades => Ok("market_trades"),
        PublicStreamKind::Ticker => Ok("ticker"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok("level2"),
        PublicStreamKind::Candles { .. } => Ok("candles"),
    }
}

fn private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account => {
            Ok("user")
        }
        PrivateStreamKind::Positions => Ok("user"),
        PrivateStreamKind::Balances => Err(ExchangeApiError::Unsupported {
            operation: "coinbase.private_stream_balances",
        }),
    }
}

fn parse_user_channel_events(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let events = value
        .get("events")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            stream_parse_error(exchange_id.clone(), "user message missing events", value)
        })?;
    let mut output = Vec::new();
    for event in events {
        if let Some(orders) = event.get("orders").and_then(Value::as_array) {
            for order in orders {
                output.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
                    exchange_id,
                    None,
                    order,
                )?));
            }
        }
    }
    Ok(output)
}

fn is_coinbase_heartbeat(value: &Value) -> bool {
    value.get("channel").and_then(Value::as_str) == Some("heartbeats")
        || value.get("type").and_then(Value::as_str) == Some("heartbeats")
}

fn is_subscription_ack(value: &Value) -> bool {
    value.get("channel").and_then(Value::as_str) == Some("subscriptions")
        || first_event(value)
            .ok()
            .and_then(|event| event.get("type").and_then(Value::as_str))
            .is_some_and(|kind| {
                kind.eq_ignore_ascii_case("subscriptions")
                    || kind.eq_ignore_ascii_case("subscribe")
                    || kind.eq_ignore_ascii_case("subscribed")
            })
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str::<Value>(text).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid Coinbase WS JSON: {error}"),
            &Value::String(text.to_string()),
        )
    })
}

fn first_event<'a>(value: &'a Value) -> ExchangeApiResult<&'a Value> {
    value
        .get("events")
        .and_then(Value::as_array)
        .and_then(|events| events.first())
        .ok_or_else(|| {
            stream_parse_error(
                ExchangeId::unchecked("coinbase"),
                "message missing events",
                value,
            )
        })
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
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
