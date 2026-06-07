#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamCapabilities, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, Fill, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_toobit_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::ToobitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

impl ToobitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let payload = toobit_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "toobit:{}:{}",
            self.config.public_ws_url,
            payload
                .get("topic")
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
        let listen_key = self
            .create_listen_key("toobit.subscribe_private_stream")
            .await?;
        Ok(format!(
            "toobit:{}",
            toobit_user_stream_url(&self.config.public_ws_url, &listen_key)
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ToobitPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    SubscriptionAck,
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ToobitPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Fill(Fill),
    Balances(BalancesResponse),
    Positions(rustcta_exchange_api::PositionsResponse),
    SubscriptionAck,
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ToobitWsSessionEvent {
    Public(ToobitPublicStreamMessage),
    Private(ToobitPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ToobitPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
    stale_book_ms: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ToobitPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    state: StreamRuntimeState,
}

impl ToobitPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
        stale_book_ms: u64,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "toobit public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = toobit_public_subscribe_payload(&subscription)?;
        let mut state =
            StreamRuntimeState::new(exchange_id.clone(), subscription.symbol.market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            symbol: subscription.symbol,
            subscribe_payload,
            state,
            stale_book_ms,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        json!({"ping": now.timestamp_millis()})
    }

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        if value.get("ping").is_some() {
            self.state.on_pong(now);
            Some(
                json!({"pong": value.get("ping").cloned().unwrap_or_else(|| json!(now.timestamp_millis()))}),
            )
        } else if value.get("pong").is_some() {
            self.state.on_pong(now);
            None
        } else {
            None
        }
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
    ) -> ExchangeApiResult<Vec<ToobitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(pong) = self.heartbeat_response(&value, Utc::now()) {
            return Ok(vec![ToobitWsSessionEvent::Outbound(pong)]);
        }
        let message = parse_toobit_public_stream_message(
            &self.exchange_id,
            self.symbol.clone(),
            self.stale_book_ms,
            &value,
        )?;
        let mut events = vec![ToobitWsSessionEvent::Public(message.clone())];
        if let ToobitPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(ToobitWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(self.exchange_id.clone(), None),
                    order_book: snapshot,
                }),
            ]));
        }
        Ok(events)
    }
}

impl ToobitPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "toobit private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "toobit private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let account_id = subscription.account_id.clone();
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            symbol_hint: None,
            state,
        })
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        json!({"ping": now.timestamp_millis()})
    }

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        if value.get("ping").is_some() {
            self.state.on_pong(now);
            Some(
                json!({"pong": value.get("ping").cloned().unwrap_or_else(|| json!(now.timestamp_millis()))}),
            )
        } else if value.get("pong").is_some() {
            self.state.on_pong(now);
            None
        } else {
            None
        }
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<ToobitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(pong) = self.heartbeat_response(&value, Utc::now()) {
            return Ok(vec![ToobitWsSessionEvent::Outbound(pong)]);
        }
        let messages = parse_toobit_private_stream_messages(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.as_ref(),
            &value,
        )?;
        let mut events = Vec::new();
        for message in messages {
            let stream_events = match &message {
                ToobitPrivateStreamMessage::Order(order) => {
                    vec![ExchangeStreamEvent::OrderUpdate(order.clone())]
                }
                ToobitPrivateStreamMessage::Fill(fill) => {
                    vec![ExchangeStreamEvent::Fill(fill.clone())]
                }
                ToobitPrivateStreamMessage::Balances(response) => {
                    vec![ExchangeStreamEvent::BalanceSnapshot(response.clone())]
                }
                ToobitPrivateStreamMessage::Positions(response) => {
                    vec![ExchangeStreamEvent::PositionSnapshot(response.clone())]
                }
                _ => Vec::new(),
            };
            events.push(ToobitWsSessionEvent::Private(message));
            if !stream_events.is_empty() {
                events.push(ToobitWsSessionEvent::Stream(stream_events));
            }
        }
        Ok(events)
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn supervisor_action(
        &self,
        now: DateTime<Utc>,
        policy: &StreamReconnectPolicy,
    ) -> StreamSupervisorAction {
        self.state.decide(now, policy)
    }
}

pub(super) fn toobit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_toobit_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let topic = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "depth",
        PublicStreamKind::Ticker => "markPrice",
        PublicStreamKind::Trades => "trade",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "toobit.public_ws_candles",
            })
        }
    };
    Ok(json!({
        "symbol": symbol,
        "topic": topic,
        "event": "sub",
        "params": {"binary": false}
    }))
}

pub(super) fn toobit_user_stream_url(public_ws_url: &str, listen_key: &str) -> String {
    let base = public_ws_url
        .trim_end_matches('/')
        .trim_end_matches("/quote/ws/v1")
        .trim_end_matches('/');
    format!("{base}/api/v1/ws/{listen_key}")
}

pub fn toobit_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            rustcta_exchange_api::PrivateOrderStreamEventKind::New,
            rustcta_exchange_api::PrivateOrderStreamEventKind::PartialFill,
            rustcta_exchange_api::PrivateOrderStreamEventKind::Fill,
            rustcta_exchange_api::PrivateOrderStreamEventKind::Cancel,
            rustcta_exchange_api::PrivateOrderStreamEventKind::Reject,
            rustcta_exchange_api::PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn parse_toobit_public_stream_message(
    exchange_id: &ExchangeId,
    fallback_symbol: SymbolScope,
    stale_book_ms: u64,
    value: &Value,
) -> ExchangeApiResult<ToobitPublicStreamMessage> {
    if value.get("pong").is_some() || value.get("ping").is_some() {
        return Ok(ToobitPublicStreamMessage::Pong);
    }
    if value.get("event").is_some() {
        return Ok(ToobitPublicStreamMessage::SubscriptionAck);
    }
    if value.get("topic").and_then(Value::as_str) != Some("depth") {
        return Ok(ToobitPublicStreamMessage::Raw(value.clone()));
    }
    let item = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .unwrap_or(value);
    let symbol = item
        .get("s")
        .or_else(|| item.get("symbol"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .map(|symbol| {
            rustcta_types::ExchangeSymbol::new(
                exchange_id.clone(),
                fallback_symbol.market_type,
                symbol.to_ascii_uppercase(),
            )
        })
        .transpose()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: error.to_string(),
        })?;
    let mut scope = fallback_symbol;
    if let Some(symbol) = symbol {
        scope.exchange_symbol = symbol;
    }
    Ok(ToobitPublicStreamMessage::OrderBook(
        parse_orderbook_snapshot(exchange_id, scope, item, stale_book_ms)?,
    ))
}

pub fn parse_toobit_private_stream_messages(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ToobitPrivateStreamMessage>> {
    let items = value
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![value]);
    let mut messages = Vec::new();
    for item in items {
        match item.get("e").and_then(Value::as_str) {
            Some("outboundAccountInfo") => {
                let response = BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances: parse_balances(
                        exchange_id,
                        tenant_id.clone(),
                        account_id.clone(),
                        market_type,
                        &[],
                        &json!({"data": {"balances": item.get("B").cloned().unwrap_or_else(|| json!([]))}}),
                    )?,
                };
                messages.push(ToobitPrivateStreamMessage::Balances(response));
            }
            Some("executionReport") => {
                let order_value = json!({
                    "symbol": item.get("s").cloned().unwrap_or(Value::Null),
                    "clientOrderId": item.get("c").cloned().unwrap_or(Value::Null),
                    "side": item.get("S").cloned().unwrap_or(Value::Null),
                    "type": item.get("o").cloned().unwrap_or(Value::Null),
                    "timeInForce": item.get("f").cloned().unwrap_or(Value::Null),
                    "origQty": item.get("q").cloned().unwrap_or(Value::Null),
                    "price": item.get("p").cloned().unwrap_or(Value::Null),
                    "status": item.get("X").cloned().unwrap_or(Value::Null),
                    "orderId": item.get("i").cloned().unwrap_or(Value::Null),
                    "executedQty": item.get("z").cloned().unwrap_or(Value::Null),
                    "time": item.get("O").cloned().unwrap_or(Value::Null),
                    "updateTime": item.get("E").cloned().unwrap_or(Value::Null),
                });
                messages.push(ToobitPrivateStreamMessage::Order(parse_order_state(
                    exchange_id,
                    fallback_symbol,
                    market_type,
                    &order_value,
                )?));
            }
            Some("ticketInfo") => {
                messages.extend(
                    parse_fills(
                        exchange_id,
                        tenant_id.clone(),
                        account_id.clone(),
                        fallback_symbol,
                        market_type,
                        &json!({"data": [item]}),
                    )?
                    .into_iter()
                    .map(ToobitPrivateStreamMessage::Fill),
                );
            }
            Some("ACCOUNT_UPDATE") => {
                let response = rustcta_exchange_api::PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions: parse_positions(
                        exchange_id,
                        tenant_id.clone(),
                        account_id.clone(),
                        item,
                    )?,
                };
                messages.push(ToobitPrivateStreamMessage::Positions(response));
            }
            _ => messages.push(ToobitPrivateStreamMessage::Raw(item.clone())),
        }
    }
    Ok(messages)
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        super::parser::parse_error(
            exchange_id.clone(),
            &error.to_string(),
            &Value::String(text.to_string()),
        )
    })
}
