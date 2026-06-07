#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PositionsResponse, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookSnapshot, OrderSide,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_deepcoin_symbol, parse_error, parse_orderbook_snapshot, split_deepcoin_symbol,
    validation_error, value_as_i64, value_as_string,
};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::DeepcoinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const PRIVATE_LISTEN_KEY_ACQUIRE_ENDPOINT: &str = "/deepcoin/listenkey/acquire";
const PRIVATE_LISTEN_KEY_EXTEND_ENDPOINT: &str = "/deepcoin/listenkey/extend";
const DEEPCOIN_WS_HEARTBEAT_PAYLOAD: &str = "ping";

#[derive(Debug, Clone, PartialEq)]
pub enum DeepcoinPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    Trades(Vec<DeepcoinPublicTrade>),
    SubscriptionAck { local_no: Option<u64> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeepcoinPrivateStreamMessage {
    Events(Vec<ExchangeStreamEvent>),
    SubscriptionAck,
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepcoinPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepcoinListenKey {
    pub listen_key: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub private_ws_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeepcoinWsSessionEvent {
    Public(DeepcoinPublicStreamMessage),
    Private(DeepcoinPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    OutboundText(String),
}

#[derive(Debug, Clone)]
pub struct DeepcoinPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct DeepcoinPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    listen_key: DeepcoinListenKey,
    state: StreamRuntimeState,
}

impl DeepcoinGatewayAdapter {
    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<DeepcoinPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let ws_url = match subscription.symbol.market_type {
            MarketType::Spot => self.config.public_spot_ws_url.clone(),
            MarketType::Perpetual => self.config.public_swap_ws_url.clone(),
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "deepcoin.public_stream_market_type",
                });
            }
        };
        DeepcoinPublicWsSession::new(self.exchange_id.clone(), ws_url, subscription)
    }

    pub async fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<DeepcoinPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let _kind = deepcoin_private_stream_kind(&subscription.kind, market_type)?;
        let listen_key = self.acquire_private_stream_listen_key().await?;
        DeepcoinPrivateWsSession::new(
            self.exchange_id.clone(),
            subscription,
            listen_key,
            symbol_hint,
        )
    }

    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.public_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "deepcoin:{}:{}:{}",
            session.url,
            payload
                .get("Topic")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            payload
                .get("Symbol")
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
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let _kind = deepcoin_private_stream_kind(&subscription.kind, market_type)?;
        Ok(self
            .acquire_private_stream_listen_key()
            .await?
            .private_ws_url)
    }

    pub async fn acquire_private_stream_listen_key(&self) -> ExchangeApiResult<DeepcoinListenKey> {
        let value = self
            .rest
            .send_signed_get(
                PRIVATE_LISTEN_KEY_ACQUIRE_ENDPOINT,
                &std::collections::HashMap::new(),
            )
            .await?;
        parse_deepcoin_listen_key(&self.exchange_id, &self.config.private_ws_url, &value)
    }

    pub async fn extend_private_stream_listen_key(
        &self,
        listen_key: &str,
    ) -> ExchangeApiResult<DeepcoinListenKey> {
        let listen_key = listen_key.trim();
        if listen_key.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin listenkey extend requires non-empty listen_key".to_string(),
            });
        }
        let body_text = deepcoin_listen_key_extend_body(listen_key);
        let value = self
            .rest
            .send_signed_get_body(
                PRIVATE_LISTEN_KEY_EXTEND_ENDPOINT,
                &body_text,
                "application/x-www-form-urlencoded",
            )
            .await?;
        parse_deepcoin_listen_key(&self.exchange_id, &self.config.private_ws_url, &value)
    }
}

impl DeepcoinPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = deepcoin_public_subscribe_payload(&subscription, 1)?;
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

    pub fn heartbeat_request(&self) -> String {
        deepcoin_ws_heartbeat_payload().to_string()
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
    ) -> ExchangeApiResult<Vec<DeepcoinWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_deepcoin_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, DeepcoinPublicStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![DeepcoinWsSessionEvent::Public(message.clone())];
        if let DeepcoinPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(DeepcoinWsSessionEvent::Stream(vec![
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

impl DeepcoinPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        subscription: PrivateStreamSubscription,
        listen_key: DeepcoinListenKey,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "deepcoin private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "deepcoin private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url: listen_key.private_ws_url.clone(),
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            market_type,
            symbol_hint,
            listen_key,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        Vec::new()
    }

    pub fn heartbeat_request(&self) -> String {
        deepcoin_ws_heartbeat_payload().to_string()
    }

    pub fn listen_key(&self) -> &DeepcoinListenKey {
        &self.listen_key
    }

    pub fn listen_key_renewal_due(&self, now: DateTime<Utc>) -> bool {
        self.listen_key
            .expires_at
            .is_some_and(|expires_at| expires_at.signed_duration_since(now).num_seconds() <= 300)
    }

    pub fn update_listen_key(&mut self, listen_key: DeepcoinListenKey) {
        self.url = listen_key.private_ws_url.clone();
        self.listen_key = listen_key;
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
    ) -> ExchangeApiResult<Vec<DeepcoinWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message = parse_deepcoin_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, DeepcoinPrivateStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![DeepcoinWsSessionEvent::Private(message.clone())];
        if let DeepcoinPrivateStreamMessage::Events(stream_events) = message {
            events.push(DeepcoinWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn deepcoin_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    local_no: u64,
) -> ExchangeApiResult<Value> {
    deepcoin_public_topic_payload(subscription, local_no, "1")
}

pub fn deepcoin_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    local_no: u64,
) -> ExchangeApiResult<Value> {
    deepcoin_public_topic_payload(subscription, local_no, "2")
}

pub fn deepcoin_public_unsubscribe_all_payload(local_no: u64) -> Value {
    json!({
        "Action": "0",
        "LocalNo": local_no,
        "ResumeNo": -1,
    })
}

fn deepcoin_public_topic_payload(
    subscription: &PublicStreamSubscription,
    local_no: u64,
    action: &'static str,
) -> ExchangeApiResult<Value> {
    let mut symbol = deepcoin_ws_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let topic = match &subscription.kind {
        PublicStreamKind::Trades => "trade",
        PublicStreamKind::Ticker => "market",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            symbol.push_str("_0.1");
            "book25"
        }
        PublicStreamKind::Candles { interval } => {
            return Ok(json!({
                "Action": action,
                "Symbol": symbol,
                "PeriodID": normalize_ws_interval(interval)?,
                "LocalNo": local_no,
                "ResumeNo": -1,
                "Count": 100,
                "Topic": "kline",
            }));
        }
    };
    Ok(json!({
        "Action": action,
        "Symbol": symbol,
        "LocalNo": local_no,
        "ResumeNo": -1,
        "Topic": topic,
    }))
}

pub fn deepcoin_private_ws_url(base_url: &str, listen_key: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };
    format!("{base_url}{separator}listenKey={listen_key}")
}

pub fn deepcoin_ws_heartbeat_payload() -> &'static str {
    DEEPCOIN_WS_HEARTBEAT_PAYLOAD
}

pub fn deepcoin_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: 15_000,
        pong_timeout_ms: 25_000,
        stale_message_ms: 20_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn deepcoin_listen_key_extend_body(listen_key: &str) -> String {
    format!("listenkey={listen_key}")
}

pub fn parse_deepcoin_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<DeepcoinPublicStreamMessage> {
    if value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
    {
        return Ok(DeepcoinPublicStreamMessage::Pong);
    }
    if value.get("LocalNo").is_some() || value.get("SendTopicAction").is_some() {
        return Ok(DeepcoinPublicStreamMessage::SubscriptionAck {
            local_no: value
                .get("LocalNo")
                .or_else(|| value.pointer("/SendTopicAction/LocalNo"))
                .and_then(value_as_u64),
        });
    }
    if is_deepcoin_book_message(value) {
        return Ok(DeepcoinPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, &normalize_public_book_payload(value))?,
        ));
    }
    if is_deepcoin_trade_message(value) {
        return Ok(DeepcoinPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol,
            value,
        )?));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported deepcoin public stream message",
        value,
    ))
}

fn parse_deepcoin_listen_key(
    exchange_id: &ExchangeId,
    private_ws_url: &str,
    value: &Value,
) -> ExchangeApiResult<DeepcoinListenKey> {
    let data = value.get("data").unwrap_or(value);
    let listen_key = data
        .get("listenkey")
        .or_else(|| data.get("listenKey"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deepcoin listenkey response missing listenkey",
                value,
            )
        })?
        .to_string();
    Ok(DeepcoinListenKey {
        private_ws_url: deepcoin_private_ws_url(private_ws_url, &listen_key),
        listen_key,
        expires_at: data
            .get("expire_time")
            .or_else(|| data.get("expireTime"))
            .and_then(value_as_i64)
            .and_then(timestamp_from_seconds_or_millis),
    })
}

pub fn parse_deepcoin_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<DeepcoinPrivateStreamMessage> {
    if value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
    {
        return Ok(DeepcoinPrivateStreamMessage::Pong);
    }
    if value
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("subscribe"))
    {
        return Ok(DeepcoinPrivateStreamMessage::SubscriptionAck);
    }

    let mut events = Vec::new();
    for row in private_rows(value, "Order") {
        events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            symbol_hint.as_ref(),
            market_type,
            &normalize_private_order(row, market_type),
        )?));
    }
    for row in private_rows(value, "Trade") {
        let normalized = json!({ "data": [normalize_private_trade(row, market_type)] });
        for fill in parse_fills(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            symbol_hint.as_ref(),
            market_type,
            &normalized,
        )? {
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    }
    for row in private_rows(value, "Account") {
        let normalized = json!({ "data": { "details": [normalize_private_balance(row)] } });
        let balances = parse_balances(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            market_type,
            &[],
            &normalized,
        )?;
        events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances,
        }));
    }
    for row in private_rows(value, "Position") {
        let normalized = json!({ "data": [normalize_private_position(row)] });
        let positions = parse_positions(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            &normalized,
        )?;
        if !positions.is_empty() {
            events.push(ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions,
            }));
        }
    }

    if value.get("data").is_some() && events.is_empty() {
        parse_rest_style_private_events(
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            symbol_hint.as_ref(),
            value,
            &mut events,
        )?;
    }

    if events.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            "unsupported deepcoin private stream message",
            value,
        ));
    }
    Ok(DeepcoinPrivateStreamMessage::Events(events))
}

fn parse_rest_style_private_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
    events: &mut Vec<ExchangeStreamEvent>,
) -> ExchangeApiResult<()> {
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if channel.contains("order") {
        events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            symbol_hint,
            market_type,
            value,
        )?));
    } else if channel.contains("fill") || channel.contains("trade") {
        for fill in parse_fills(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint,
            market_type,
            value,
        )? {
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    } else if channel.contains("balance") || channel.contains("account") {
        let balances = parse_balances(exchange_id, tenant_id, account_id, market_type, &[], value)?;
        events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances,
        }));
    } else if channel.contains("position") {
        let positions = parse_positions(exchange_id, tenant_id, account_id, value)?;
        events.push(ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            positions,
        }));
    }
    Ok(())
}

fn deepcoin_private_stream_kind(
    kind: &PrivateStreamKind,
    market_type: MarketType,
) -> ExchangeApiResult<&'static str> {
    match (kind, market_type) {
        (PrivateStreamKind::Orders, _) => Ok("Order"),
        (PrivateStreamKind::Fills, _) => Ok("Trade"),
        (PrivateStreamKind::Balances | PrivateStreamKind::Account, _) => Ok("Account"),
        (PrivateStreamKind::Positions, MarketType::Perpetual) => Ok("Position"),
        (PrivateStreamKind::Positions, MarketType::Spot) => Err(ExchangeApiError::Unsupported {
            operation: "deepcoin.spot_private_positions_stream",
        }),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "deepcoin.private_stream_market_type",
        }),
    }
}

fn deepcoin_ws_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let normalized = normalize_deepcoin_symbol(symbol, market_type)?;
    let (base, quote) = split_deepcoin_symbol(&normalized, market_type).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Deepcoin WS symbol from {symbol}"),
        }
    })?;
    Ok(match market_type {
        MarketType::Spot => format!("{base}/{quote}"),
        MarketType::Perpetual => format!("{base}{quote}"),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "deepcoin.public_stream_market_type",
            });
        }
    })
}

fn normalize_ws_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "12h" | "1d" | "1w" | "1o" | "1y" => {
            Ok(interval.trim().to_ascii_lowercase())
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "deepcoin.ws_kline_interval",
        }),
    }
}

fn is_deepcoin_book_message(value: &Value) -> bool {
    value
        .get("d")
        .and_then(Value::as_array)
        .is_some_and(|rows| {
            rows.iter()
                .any(|row| row.get("a").is_some() || row.get("b").is_some())
        })
        || value.get("data").is_some_and(|data| {
            data.get("asks").is_some()
                || data.get("bids").is_some()
                || data.as_array().is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row.get("asks").is_some() || row.get("bids").is_some())
                })
        })
}

fn normalize_public_book_payload(value: &Value) -> Value {
    let data = value
        .get("data")
        .and_then(|data| data.as_array().and_then(|rows| rows.first()).or(Some(data)))
        .or_else(|| value.get("d").and_then(Value::as_array)?.first())
        .unwrap_or(value);
    json!({
        "data": {
            "asks": data.get("asks").or_else(|| data.get("a")).cloned().unwrap_or(Value::Array(Vec::new())),
            "bids": data.get("bids").or_else(|| data.get("b")).cloned().unwrap_or(Value::Array(Vec::new())),
            "ts": value.get("tt").or_else(|| value.get("mt")).or_else(|| data.get("ts")).cloned().unwrap_or(Value::Null),
        }
    })
}

fn is_deepcoin_trade_message(value: &Value) -> bool {
    value
        .get("d")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.iter().any(|row| row.get("P").is_some()))
        || value
            .get("r")
            .and_then(Value::as_array)
            .is_some_and(|rows| rows.iter().any(|row| row.pointer("/d/P").is_some()))
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<DeepcoinPublicTrade>> {
    let rows = value
        .get("d")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let fallback_rows = value
        .get("r")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    rows.iter()
        .chain(fallback_rows.iter().filter_map(|row| row.get("d")))
        .map(|row| {
            let price = value_as_string(row.get("P")).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "deepcoin public trade missing price",
                    row,
                )
            })?;
            let quantity = value_as_string(row.get("V")).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "deepcoin public trade missing quantity",
                    row,
                )
            })?;
            Ok(DeepcoinPublicTrade {
                symbol: symbol.clone(),
                trade_id: value_as_string(row.get("TradeID").or_else(|| row.get("TI"))),
                side: parse_deepcoin_numeric_side(row.get("D")),
                price,
                quantity,
                traded_at: row
                    .get("T")
                    .or_else(|| value.get("tt"))
                    .and_then(value_as_i64)
                    .and_then(timestamp_from_seconds_or_millis)
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn private_rows<'a>(value: &'a Value, table: &str) -> Vec<&'a Value> {
    value
        .get("result")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| {
            row.get("table")
                .and_then(Value::as_str)
                .is_some_and(|actual| actual.eq_ignore_ascii_case(table))
        })
        .filter_map(|row| row.get("data"))
        .collect()
}

fn normalize_private_order(row: &Value, market_type: MarketType) -> Value {
    let symbol = normalize_short_symbol(row.get("I"), market_type);
    json!({
        "instType": inst_type(market_type),
        "instId": symbol,
        "ordId": value_as_string(row.get("OS")),
        "clOrdId": value_as_string(row.get("L")),
        "side": numeric_side_text(row.get("D")),
        "posSide": numeric_position_side_text(row.get("p")),
        "ordType": numeric_order_type_text(row.get("OT").or_else(|| row.get("O"))),
        "state": numeric_order_status_text(row.get("Or")),
        "px": row.get("P").cloned().unwrap_or(Value::Null),
        "sz": row.get("V").cloned().unwrap_or(Value::Null),
        "accFillSz": row.get("v").cloned().unwrap_or(Value::Null),
        "avgPx": row.get("t").cloned().unwrap_or(Value::Null),
        "cTime": millis_from_seconds_value(row.get("IT")),
        "uTime": millis_from_seconds_value(row.get("U")),
    })
}

fn normalize_private_trade(row: &Value, market_type: MarketType) -> Value {
    json!({
        "instType": inst_type(market_type),
        "instId": normalize_short_symbol(row.get("I"), market_type),
        "tradeId": value_as_string(row.get("TI")),
        "ordId": value_as_string(row.get("OS")),
        "fillPx": row.get("P").cloned().unwrap_or(Value::Null),
        "fillSz": row.get("V").cloned().unwrap_or(Value::Null),
        "side": numeric_side_text(row.get("D")),
        "posSide": numeric_position_side_text(row.get("o")),
        "execType": if row.get("m").and_then(Value::as_str) == Some("1") { "M" } else { "T" },
        "feeCcy": value_as_string(row.get("f").or_else(|| row.get("CC"))),
        "fee": row.get("F").cloned().unwrap_or(Value::Null),
        "pnl": row.get("CP").cloned().unwrap_or(Value::Null),
        "ts": millis_from_seconds_value(row.get("IT").or_else(|| row.get("TT"))),
    })
}

fn normalize_private_balance(row: &Value) -> Value {
    json!({
        "ccy": value_as_string(row.get("C").or_else(|| row.get("c")).or_else(|| row.get("CC"))),
        "bal": row.get("B").or_else(|| row.get("b")).cloned().unwrap_or(Value::Null),
        "availBal": row.get("A").or_else(|| row.get("a")).cloned().unwrap_or(Value::Null),
        "frozenBal": row.get("F").or_else(|| row.get("f")).cloned().unwrap_or(Value::Null),
    })
}

fn normalize_private_position(row: &Value) -> Value {
    json!({
        "instId": normalize_short_symbol(row.get("I"), MarketType::Perpetual),
        "posSide": numeric_position_side_text(row.get("p")),
        "pos": row.get("Po").cloned().unwrap_or(Value::Null),
        "avgPx": row.get("OP").cloned().unwrap_or(Value::Null),
        "lever": row.get("l").cloned().unwrap_or(Value::Null),
        "unrealizedProfit": row.get("CP").cloned().unwrap_or(Value::Null),
    })
}

fn normalize_short_symbol(value: Option<&Value>, market_type: MarketType) -> Option<String> {
    let raw = value_as_string(value)?;
    normalize_deepcoin_symbol(&raw, market_type).ok()
}

fn inst_type(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual => "SWAP",
        _ => "UNKNOWN",
    }
}

fn numeric_side_text(value: Option<&Value>) -> &'static str {
    match value.and_then(value_as_string_ref).as_deref() {
        Some("1") | Some("sell") => "sell",
        _ => "buy",
    }
}

fn parse_deepcoin_numeric_side(value: Option<&Value>) -> OrderSide {
    match value.and_then(value_as_string_ref).as_deref() {
        Some("1") | Some("sell") => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn numeric_position_side_text(value: Option<&Value>) -> &'static str {
    match value.and_then(value_as_string_ref).as_deref() {
        Some("1") | Some("long") => "long",
        Some("2") | Some("short") => "short",
        _ => "net",
    }
}

fn numeric_order_type_text(value: Option<&Value>) -> &'static str {
    match value.and_then(value_as_string_ref).as_deref() {
        Some("9") | Some("market") => "market",
        Some("3") | Some("post_only") => "post_only",
        Some("4") | Some("ioc") => "ioc",
        _ => "limit",
    }
}

fn numeric_order_status_text(value: Option<&Value>) -> &'static str {
    match value.and_then(value_as_string_ref).as_deref() {
        Some("1") | Some("new") => "live",
        Some("2") | Some("partially_filled") => "partially_filled",
        Some("3") | Some("filled") => "filled",
        Some("4") | Some("canceled") => "canceled",
        Some("5") | Some("rejected") => "rejected",
        _ => "unknown",
    }
}

fn value_as_string_ref(value: &Value) -> Option<String> {
    value_as_string(Some(value)).map(|text| text.to_ascii_lowercase())
}

fn millis_from_seconds_value(value: Option<&Value>) -> Value {
    value
        .and_then(value_as_i64)
        .map(|timestamp| {
            if timestamp < 10_000_000_000 {
                json!(timestamp * 1_000)
            } else {
                json!(timestamp)
            }
        })
        .unwrap_or(Value::Null)
}

fn timestamp_from_seconds_or_millis(timestamp: i64) -> Option<DateTime<Utc>> {
    if timestamp < 10_000_000_000 {
        DateTime::<Utc>::from_timestamp(timestamp, 0)
    } else {
        DateTime::<Utc>::from_timestamp_millis(timestamp)
    }
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            "empty deepcoin websocket message",
            &Value::Null,
        ));
    }
    if trimmed.eq_ignore_ascii_case("ping") || trimmed.eq_ignore_ascii_case("pong") {
        return Ok(Value::String(trimmed.to_ascii_lowercase()));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        parse_error(
            exchange_id.clone(),
            &format!("invalid deepcoin websocket JSON: {error}"),
            &json!({ "message": trimmed }),
        )
    })
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

#[allow(dead_code)]
fn canonical_symbol_for_compact(
    exchange_id: &ExchangeId,
    raw_symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = normalize_deepcoin_symbol(raw_symbol, market_type)?;
    let (base, quote) = split_deepcoin_symbol(&symbol, market_type).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "deepcoin stream symbol cannot be split",
            &json!({ "symbol": raw_symbol }),
        )
    })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}
