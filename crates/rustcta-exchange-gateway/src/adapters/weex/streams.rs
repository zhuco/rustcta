#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, Fill, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_weex_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::signing::sign_prehash;
use super::WeexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const WEEX_DEPTH_LEVELS: [u32; 2] = [15, 200];
const WEEX_DEFAULT_DEPTH_LEVEL: u32 = 15;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeexPublicOrderBookWsPolicy {
    pub supported_depth_levels: &'static [u32],
    pub default_depth_level: u32,
    pub fixed_update_interval_ms: Option<u64>,
    pub first_update_id_field: &'static str,
    pub last_update_id_field: &'static str,
    pub snapshot_marker: &'static str,
    pub update_marker: &'static str,
    pub checksum: Option<&'static str>,
    pub gap_recovery: &'static str,
}

pub fn weex_public_order_book_ws_policy() -> WeexPublicOrderBookWsPolicy {
    WeexPublicOrderBookWsPolicy {
        supported_depth_levels: &WEEX_DEPTH_LEVELS,
        default_depth_level: WEEX_DEFAULT_DEPTH_LEVEL,
        fixed_update_interval_ms: None,
        first_update_id_field: "U",
        last_update_id_field: "u",
        snapshot_marker: "SNAPSHOT",
        update_marker: "CHANGED",
        checksum: None,
        gap_recovery:
            "resubscribe to receive a fresh automatic snapshot when U/u continuity is broken",
    }
}

impl WeexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let payload = weex_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "weex:{}:{}",
            self.public_ws_url(subscription.symbol.market_type),
            first_subscription_arg(&payload)
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
        let (api_key, api_secret, passphrase) =
            self.private_credentials("weex.subscribe_private_stream")?;
        let _headers = weex_private_auth_headers(
            api_key,
            api_secret,
            passphrase,
            Utc::now().timestamp_millis(),
        )?;
        let payload = weex_private_subscribe_payload(&subscription)?;
        Ok(format!(
            "weex:{}:{}",
            self.private_ws_url(subscription.market_type.unwrap_or(MarketType::Spot)),
            first_subscription_arg(&payload)
        ))
    }

    fn public_ws_url(&self, market_type: MarketType) -> &str {
        match market_type {
            MarketType::Perpetual => &self.config.contract_public_ws_url,
            _ => &self.config.spot_public_ws_url,
        }
    }

    fn private_ws_url(&self, market_type: MarketType) -> &str {
        match market_type {
            MarketType::Perpetual => &self.config.contract_private_ws_url,
            _ => &self.config.spot_private_ws_url,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum WeexPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    SubscriptionAck,
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum WeexPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Fill(Fill),
    Balances(rustcta_exchange_api::BalancesResponse),
    Positions(rustcta_exchange_api::PositionsResponse),
    SubscriptionAck,
    LoginAck,
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum WeexWsSessionEvent {
    Public(WeexPublicStreamMessage),
    Private(WeexPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WeexPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    last_depth_update_id: Option<i64>,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WeexPrivateWsSession {
    pub url: String,
    pub auth_headers: HashMap<String, String>,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl WeexPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "weex public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = weex_public_subscribe_payload(&subscription)?;
        let mut state =
            StreamRuntimeState::new(exchange_id.clone(), subscription.symbol.market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            symbol: subscription.symbol,
            subscribe_payload,
            last_depth_update_id: None,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        if is_weex_heartbeat(value) {
            self.state.on_pong(now);
            Some(weex_pong_payload(value.get("id").and_then(Value::as_i64)))
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
    ) -> ExchangeApiResult<Vec<WeexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(pong) = self.heartbeat_response(&value, Utc::now()) {
            return Ok(vec![WeexWsSessionEvent::Outbound(pong)]);
        }
        if let Some((first, last)) = depth_sequence_range(&value) {
            if let Some(previous) = self.last_depth_update_id {
                if first > previous + 1 {
                    self.state.on_sequence_gap();
                }
            }
            self.last_depth_update_id = Some(last);
        }
        let message =
            parse_weex_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        let mut events = vec![WeexWsSessionEvent::Public(message.clone())];
        if let WeexPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(WeexWsSessionEvent::Stream(vec![
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

impl WeexPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        api_key: String,
        api_secret: String,
        passphrase: String,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "weex private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "weex private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let auth_headers = weex_private_auth_headers(
            &api_key,
            &api_secret,
            &passphrase,
            Utc::now().timestamp_millis(),
        )?;
        let subscribe_payload = weex_private_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            auth_headers,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            market_type,
            symbol_hint: None,
            subscribe_payload,
            state,
        })
    }

    pub fn with_symbol_hint(mut self, symbol_hint: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol_hint);
        self
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        if is_weex_heartbeat(value) {
            self.state.on_pong(now);
            Some(weex_pong_payload(value.get("id").and_then(Value::as_i64)))
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
    ) -> ExchangeApiResult<Vec<WeexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(pong) = self.heartbeat_response(&value, Utc::now()) {
            return Ok(vec![WeexWsSessionEvent::Outbound(pong)]);
        }
        let message = parse_weex_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            self.market_type,
            &value,
        )?;
        let mut events = vec![WeexWsSessionEvent::Private(message.clone())];
        let stream_events = private_message_stream_events(&self.exchange_id, message);
        if !stream_events.is_empty() {
            events.push(WeexWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn weex_private_stream_capabilities() -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::Expired,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn weex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_weex_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?
    .to_ascii_uppercase();
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            return Ok(json!({
                "method": "SUBSCRIBE",
                "params": [weex_public_depth_channel(
                    &subscription.symbol.exchange_symbol.symbol,
                    subscription.symbol.market_type,
                    WEEX_DEFAULT_DEPTH_LEVEL,
                )?],
                "id": 1
            }));
        }
        PublicStreamKind::Trades => "trade",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { interval } => {
            return candle_subscribe_payload(symbol, interval)
        }
    };
    Ok(json!({
        "method": "SUBSCRIBE",
        "params": [format!("{symbol}@{channel}")],
        "id": 1
    }))
}

pub fn weex_public_depth_channel(
    symbol: &str,
    market_type: MarketType,
    requested_depth: u32,
) -> ExchangeApiResult<String> {
    let symbol = normalize_weex_symbol(symbol, market_type)?.to_ascii_uppercase();
    let depth = normalize_weex_depth_level(requested_depth);
    Ok(format!("{symbol}@depth{depth}"))
}

fn normalize_weex_depth_level(requested_depth: u32) -> u32 {
    if requested_depth <= 15 {
        15
    } else {
        200
    }
}

pub fn weex_private_auth_headers(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    timestamp_millis: i64,
) -> ExchangeApiResult<HashMap<String, String>> {
    let timestamp = timestamp_millis.to_string();
    let signature = sign_prehash(api_secret, &format!("{timestamp}/v3/ws/private"))?;
    Ok(HashMap::from([
        ("User-Agent".to_string(), "RustCTA-Gateway/0.3".to_string()),
        ("ACCESS-KEY".to_string(), api_key.to_string()),
        ("ACCESS-PASSPHRASE".to_string(), passphrase.to_string()),
        ("ACCESS-TIMESTAMP".to_string(), timestamp),
        ("ACCESS-SIGN".to_string(), signature),
    ]))
}

pub fn weex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    match subscription.market_type.unwrap_or(MarketType::Spot) {
        MarketType::Spot | MarketType::Perpetual => {}
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "weex.private_stream_market_type",
            });
        }
    };
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "fills",
        PrivateStreamKind::Balances => "account",
        PrivateStreamKind::Positions => "positions",
        PrivateStreamKind::Account => "account",
    };
    Ok(json!({
        "method": "SUBSCRIBE",
        "params": [channel],
        "id": 2
    }))
}

pub fn weex_pong_payload(id: Option<i64>) -> Value {
    json!({ "method": "PONG", "id": id.unwrap_or(1) })
}

pub fn is_weex_heartbeat(value: &Value) -> bool {
    let method = value
        .get("method")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("op"))
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    matches!(method.as_deref(), Some("PING" | "PONG"))
        || value.get("ping").is_some()
        || value.get("pong").is_some()
}

#[allow(dead_code)]
pub fn parse_weex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WeexPublicStreamMessage> {
    if is_subscription_ack(value) {
        return Ok(WeexPublicStreamMessage::SubscriptionAck);
    }
    if is_weex_heartbeat(value) {
        return Ok(WeexPublicStreamMessage::Pong);
    }
    if stream_channel(value)
        .is_some_and(|channel| channel.contains("depth") || channel.contains("book"))
        || value.get("bids").is_some()
        || value.get("asks").is_some()
    {
        return Ok(WeexPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, stream_payload(value))?,
        ));
    }
    Ok(WeexPublicStreamMessage::Raw(value.clone()))
}

#[allow(dead_code)]
pub fn parse_weex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: Option<SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<WeexPrivateStreamMessage> {
    if is_subscription_ack(value) {
        return Ok(WeexPrivateStreamMessage::SubscriptionAck);
    }
    if is_weex_heartbeat(value) {
        return Ok(WeexPrivateStreamMessage::Pong);
    }
    let payload = stream_payload(value);
    let channel = stream_channel(value).unwrap_or_default();
    if channel.contains("order") {
        return Ok(WeexPrivateStreamMessage::Order(parse_order_state(
            exchange_id,
            symbol.as_ref(),
            market_type,
            payload,
        )?));
    }
    if channel.contains("fill") || channel.contains("trade") {
        let symbol = symbol.ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "weex fill stream parser requires symbol hint".to_string(),
        })?;
        let fills = parse_fills(
            exchange_id,
            tenant_id,
            account_id,
            Some(&symbol),
            market_type,
            payload,
        )?;
        return fills
            .into_iter()
            .next()
            .map(WeexPrivateStreamMessage::Fill)
            .ok_or_else(|| {
                stream_parse_error(exchange_id.clone(), "WEEX fill stream empty", value)
            });
    }
    if channel.contains("position") {
        return Ok(WeexPrivateStreamMessage::Positions(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                positions: parse_positions(exchange_id, tenant_id, account_id, payload)?,
            },
        ));
    }
    if channel.contains("account") || channel.contains("balance") {
        return Ok(WeexPrivateStreamMessage::Balances(
            rustcta_exchange_api::BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    &[],
                    payload,
                )?,
            },
        ));
    }
    Ok(WeexPrivateStreamMessage::Raw(value.clone()))
}

fn candle_subscribe_payload(symbol: String, interval: &str) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "SUBSCRIBE",
        "params": [format!("{symbol}@kline_{}", normalize_interval(interval)?)],
        "id": 1
    }))
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "1min" => Ok("1m"),
        "5m" | "5min" => Ok("5m"),
        "15m" | "15min" => Ok("15m"),
        "30m" | "30min" => Ok("30m"),
        "1h" | "60m" => Ok("1h"),
        "4h" | "240m" => Ok("4h"),
        "1d" | "1day" => Ok("1d"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "weex.candle_interval",
        }),
    }
}

fn first_subscription_arg(value: &Value) -> String {
    value
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.first())
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

fn stream_payload(value: &Value) -> &Value {
    value
        .get("data")
        .and_then(|data| {
            data.get("d")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .or(Some(data))
        })
        .or_else(|| {
            value
                .get("d")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
        })
        .or_else(|| value.get("payload"))
        .unwrap_or(value)
}

fn stream_channel(value: &Value) -> Option<String> {
    value
        .get("channel")
        .or_else(|| value.get("topic"))
        .or_else(|| value.get("stream"))
        .or_else(|| value.get("e"))
        .or_else(|| value.get("arg").and_then(|arg| arg.get("channel")))
        .and_then(Value::as_str)
        .map(|channel| channel.to_ascii_lowercase())
}

fn is_subscription_ack(value: &Value) -> bool {
    value
        .get("result")
        .and_then(Value::as_str)
        .is_some_and(|result| result.eq_ignore_ascii_case("success"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("subscribe"))
        || value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.eq_ignore_ascii_case("SUBSCRIBE"))
            && value.get("code").and_then(Value::as_i64) == Some(0)
}

fn stream_parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = rustcta_types::ExchangeError::new(
        exchange_id,
        rustcta_types::ExchangeErrorClass::Decode,
        message,
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        let mut exchange_error = rustcta_types::ExchangeError::new(
            exchange_id.clone(),
            rustcta_types::ExchangeErrorClass::Decode,
            format!("failed to decode WEEX websocket message: {error}"),
            Utc::now(),
        );
        exchange_error.raw = Some(Value::String(text.to_string()));
        ExchangeApiError::Exchange(exchange_error)
    })
}

fn depth_sequence_range(value: &Value) -> Option<(i64, i64)> {
    let payload = stream_payload(value);
    let first = payload
        .get("U")
        .or_else(|| payload.get("firstUpdateId"))
        .and_then(value_as_i64)?;
    let last = payload
        .get("u")
        .or_else(|| payload.get("lastUpdateId"))
        .and_then(value_as_i64)
        .unwrap_or(first);
    Some((first, last))
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn private_message_stream_events(
    exchange_id: &ExchangeId,
    message: WeexPrivateStreamMessage,
) -> Vec<ExchangeStreamEvent> {
    match message {
        WeexPrivateStreamMessage::Order(order) => vec![ExchangeStreamEvent::OrderUpdate(order)],
        WeexPrivateStreamMessage::Fill(fill) => vec![ExchangeStreamEvent::Fill(fill)],
        WeexPrivateStreamMessage::Balances(balances) => {
            vec![ExchangeStreamEvent::BalanceSnapshot(balances)]
        }
        WeexPrivateStreamMessage::Positions(positions) => {
            vec![ExchangeStreamEvent::PositionSnapshot(positions)]
        }
        WeexPrivateStreamMessage::Pong => vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }],
        WeexPrivateStreamMessage::SubscriptionAck
        | WeexPrivateStreamMessage::LoginAck
        | WeexPrivateStreamMessage::Raw(_) => Vec::new(),
    }
}
