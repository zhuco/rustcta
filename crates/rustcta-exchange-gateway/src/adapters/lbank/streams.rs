#![allow(dead_code)]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_spot_depth, normalize_spot_symbol, parse_spot_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_order_state};
use super::LBankGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

impl LBankGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let normalized_symbol = normalize_spot_symbol(&subscription.symbol.exchange_symbol.symbol)?;
        let session = self.public_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "lbank:{}:{}:{}",
            session.url,
            payload
                .get("subscribe")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            normalized_symbol
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("lbank.subscribe_private_stream")?;
        let account_id = subscription.account_id.clone();
        let session = self.private_ws_session(subscription).await?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "lbank:{}:{}:{}",
            session.url,
            payload
                .get("subscribe")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            account_id
        ))
    }

    pub(crate) fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<LBankPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        LBankPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.spot_public_ws_url.clone(),
            subscription,
        )
    }

    pub(crate) async fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<LBankPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("lbank.private_ws_session")?;
        let subscribe_key = self.create_spot_subscribe_key().await?;
        LBankPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.spot_private_ws_url.clone(),
            subscription,
            subscribe_key,
        )
    }

    pub async fn create_spot_subscribe_key(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_spot_signed_post(
                "lbank.subscribe_key.create",
                "/v2/subscribe/get_key.do",
                &HashMap::new(),
            )
            .await?;
        extract_subscribe_key(&value)
    }

    pub async fn refresh_spot_subscribe_key(&self, subscribe_key: &str) -> ExchangeApiResult<()> {
        let mut params = HashMap::new();
        params.insert("subscribeKey".to_string(), subscribe_key.to_string());
        let value = self
            .send_spot_signed_post(
                "lbank.subscribe_key.refresh",
                "/v2/subscribe/refresh_key.do",
                &params,
            )
            .await?;
        ensure_lbank_ws_key_ack("refresh", &value)
    }

    pub async fn destroy_spot_subscribe_key(&self, subscribe_key: &str) -> ExchangeApiResult<()> {
        let mut params = HashMap::new();
        params.insert("subscribeKey".to_string(), subscribe_key.to_string());
        let value = self
            .send_spot_signed_post(
                "lbank.subscribe_key.destroy",
                "/v2/subscribe/destroy_key.do",
                &params,
            )
            .await?;
        ensure_lbank_ws_key_ack("destroy", &value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LBankWsSessionEvent {
    Public(LBankWsControlMessage),
    Private(LBankWsControlMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LBankWsControlMessage {
    SubscriptionAck { channel: Option<String> },
    HeartbeatPing { nonce: String },
    HeartbeatPong { nonce: Option<String> },
    Raw(Value),
}

#[derive(Debug, Clone)]
pub struct LBankPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct LBankPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    subscribe_key: String,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl LBankPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "lbank public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        if subscription.symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "lbank.public_ws_session.market_type",
            });
        }
        let subscribe_payload = lbank_public_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), MarketType::Spot);
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
        lbank_ping_payload(&format!("rustcta-{}", now.timestamp_millis()))
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
    ) -> ExchangeApiResult<Vec<LBankWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let control = parse_lbank_ws_control_message(&value);
        let mut events = vec![LBankWsSessionEvent::Public(control.clone())];
        push_lbank_heartbeat_events(&mut self.state, &self.exchange_id, control, &mut events);
        if let Some(stream_events) =
            parse_lbank_public_stream_events(&self.exchange_id, self.symbol.clone(), &value)?
        {
            events.push(LBankWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

impl LBankPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        subscribe_key: String,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "lbank private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "lbank.private_ws_session.market_type",
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "LBank private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let subscribe_payload = lbank_private_subscribe_payload(&subscription, &subscribe_key)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), MarketType::Spot);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            subscribe_key,
            subscribe_payload,
            state,
        })
    }

    pub fn subscribe_key(&self) -> &str {
        &self.subscribe_key
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        lbank_ping_payload(&format!("rustcta-{}", now.timestamp_millis()))
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
    ) -> ExchangeApiResult<Vec<LBankWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let control = parse_lbank_ws_control_message(&value);
        let mut events = vec![LBankWsSessionEvent::Private(control.clone())];
        push_lbank_heartbeat_events(&mut self.state, &self.exchange_id, control, &mut events);
        if let Some(stream_events) = parse_lbank_private_stream_events(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            &value,
        )? {
            events.push(LBankWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn lbank_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn lbank_ping_payload(nonce: &str) -> Value {
    json!({
        "action": "ping",
        "ping": nonce,
    })
}

pub fn lbank_pong_payload(nonce: &str) -> Value {
    json!({
        "action": "pong",
        "pong": nonce,
    })
}

pub fn lbank_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let pair = normalize_spot_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok(json!({
            "action": "subscribe",
            "subscribe": "depth",
            "depth": normalize_spot_depth(100).to_string(),
            "pair": pair,
        })),
        PublicStreamKind::Trades => Ok(json!({
            "action": "subscribe",
            "subscribe": "trade",
            "pair": pair,
        })),
        PublicStreamKind::Ticker => Ok(json!({
            "action": "subscribe",
            "subscribe": "tick",
            "pair": pair,
        })),
        PublicStreamKind::Candles { interval } => Ok(json!({
            "action": "subscribe",
            "subscribe": "kbar",
            "kbar": normalize_kbar_interval(interval)?,
            "pair": pair,
        })),
    }
}

pub fn lbank_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    subscribe_key: &str,
) -> ExchangeApiResult<Value> {
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account => {
            Ok(json!({
                "action": "subscribe",
                "subscribe": "orderUpdate",
                "subscribeKey": subscribe_key,
                "pair": "all",
            }))
        }
        PrivateStreamKind::Balances => Ok(json!({
            "action": "subscribe",
            "subscribe": "assetUpdate",
            "subscribeKey": subscribe_key,
        })),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "lbank.spot_private_positions_stream",
        }),
    }
}

pub fn parse_lbank_ws_control_message(value: &Value) -> LBankWsControlMessage {
    if value
        .get("action")
        .and_then(Value::as_str)
        .is_some_and(|action| action.eq_ignore_ascii_case("ping"))
    {
        return LBankWsControlMessage::HeartbeatPing {
            nonce: value
                .get("ping")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        };
    }
    if value
        .get("action")
        .and_then(Value::as_str)
        .is_some_and(|action| action.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
    {
        return LBankWsControlMessage::HeartbeatPong {
            nonce: value
                .get("pong")
                .and_then(Value::as_str)
                .map(str::to_string),
        };
    }
    if value
        .get("action")
        .and_then(Value::as_str)
        .is_some_and(|action| action.eq_ignore_ascii_case("subscribe"))
        || value
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("success"))
    {
        return LBankWsControlMessage::SubscriptionAck {
            channel: value
                .get("subscribe")
                .or_else(|| value.get("type"))
                .and_then(Value::as_str)
                .map(str::to_string),
        };
    }
    LBankWsControlMessage::Raw(value.clone())
}

pub fn parse_lbank_public_stream_events(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<Vec<ExchangeStreamEvent>>> {
    if lbank_ws_channel(value).is_some_and(|channel| channel.eq_ignore_ascii_case("depth"))
        || value.get("depth").is_some()
    {
        let payload = value
            .get("data")
            .or_else(|| value.get("depth"))
            .unwrap_or(value);
        let order_book = parse_spot_orderbook_snapshot(exchange_id, symbol, payload)?;
        return Ok(Some(vec![ExchangeStreamEvent::OrderBookSnapshot(
            OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                order_book,
            },
        )]));
    }
    Ok(None)
}

pub fn parse_lbank_private_stream_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Option<Vec<ExchangeStreamEvent>>> {
    let channel = lbank_ws_channel(value).unwrap_or_default();
    if channel.eq_ignore_ascii_case("orderUpdate")
        || channel.eq_ignore_ascii_case("order")
        || value.get("order").is_some()
    {
        let payload = value
            .get("data")
            .or_else(|| value.get("order"))
            .unwrap_or(value);
        return Ok(Some(vec![ExchangeStreamEvent::OrderUpdate(
            parse_order_state(exchange_id, None, payload)?,
        )]));
    }
    if channel.eq_ignore_ascii_case("assetUpdate")
        || channel.eq_ignore_ascii_case("asset")
        || value.get("balances").is_some()
    {
        let payload = value
            .get("data")
            .or_else(|| value.get("asset"))
            .unwrap_or(value);
        let balances = parse_balances(exchange_id, tenant_id, account_id, &[], payload)?;
        return Ok(Some(vec![ExchangeStreamEvent::BalanceSnapshot(
            BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances,
            },
        )]));
    }
    Ok(None)
}

fn lbank_ws_channel(value: &Value) -> Option<&str> {
    value
        .get("type")
        .or_else(|| value.get("subscribe"))
        .or_else(|| value.get("channel"))
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
}

fn normalize_kbar_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim() {
        "1m" | "1min" => Ok("1min"),
        "5m" | "5min" => Ok("5min"),
        "15m" | "15min" => Ok("15min"),
        "30m" | "30min" => Ok("30min"),
        "1h" | "1hr" => Ok("1hr"),
        "4h" | "4hr" => Ok("4hr"),
        "1d" | "day" => Ok("day"),
        "1w" | "week" => Ok("week"),
        "1M" | "month" => Ok("month"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "lbank.kbar_interval",
        }),
    }
}

fn extract_subscribe_key(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("data")
        .and_then(|data| {
            data.as_str()
                .or_else(|| data.get("subscribeKey").and_then(Value::as_str))
                .or_else(|| data.get("key").and_then(Value::as_str))
        })
        .or_else(|| value.get("subscribeKey").and_then(Value::as_str))
        .or_else(|| value.get("key").and_then(Value::as_str))
        .filter(|key| !key.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("lbank subscribe key response missing subscribeKey: {value}"),
        })
}

fn ensure_lbank_ws_key_ack(operation: &str, value: &Value) -> ExchangeApiResult<()> {
    if value.get("result").and_then(Value::as_bool) == Some(true)
        || value
            .get("result")
            .and_then(Value::as_str)
            .is_some_and(|result| result.eq_ignore_ascii_case("true"))
    {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("lbank subscribe key {operation} failed: {value}"),
    })
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("{exchange_id} lbank WS message is not JSON: {error}: {text}"),
    })
}

fn push_lbank_heartbeat_events(
    state: &mut StreamRuntimeState,
    exchange_id: &ExchangeId,
    control: LBankWsControlMessage,
    events: &mut Vec<LBankWsSessionEvent>,
) {
    match control {
        LBankWsControlMessage::HeartbeatPing { nonce } => {
            state.on_pong(Utc::now());
            events.push(LBankWsSessionEvent::Outbound(lbank_pong_payload(&nonce)));
            events.push(LBankWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: exchange_id.clone(),
                    received_at: Utc::now(),
                },
            ]));
        }
        LBankWsControlMessage::HeartbeatPong { .. } => {
            state.on_pong(Utc::now());
            events.push(LBankWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: exchange_id.clone(),
                    received_at: Utc::now(),
                },
            ]));
        }
        LBankWsControlMessage::SubscriptionAck { .. } | LBankWsControlMessage::Raw(_) => {}
    }
}
