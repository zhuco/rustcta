#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, SchemaVersion};
use serde_json::{json, Value};

use super::parser::{
    normalize_coinw_perp_base, normalize_coinw_perp_symbol, normalize_coinw_spot_symbol,
    parse_orderbook_snapshot,
};
use super::private_parser::{parse_balances, parse_order, parse_positions};
use super::CoinwGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const COINW_WS_PING_INTERVAL_MS: i64 = 15_000;
const COINW_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const COINW_WS_STALE_MESSAGE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq)]
pub enum CoinwPublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Heartbeat,
    OrderBook(OrderBookResponse),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinwPrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Heartbeat,
    Events(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinwWsSessionEvent {
    Public(CoinwPublicStreamMessage),
    Private(CoinwPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct CoinwPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct CoinwPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl CoinwGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        Ok(format!(
            "coinw:{}:{}",
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
        self.ensure_private_rest("coinw.subscribe_private_stream")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let session = self.private_ws_session(subscription, None)?;
        Ok(format!(
            "coinw:{}:{}:{}",
            session.url,
            stream_label(&session.subscribe_payload).unwrap_or("unknown"),
            session.account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<CoinwPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let url = match subscription.symbol.market_type {
            MarketType::Spot => self.config.spot_public_ws_url.clone(),
            MarketType::Perpetual => self.config.futures_public_ws_url.clone(),
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        CoinwPublicWsSession::new(self.exchange_id.clone(), url, subscription)
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<CoinwPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinw.private_ws_session")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&subscription.context, "coinw.private_ws_session")?;
        let url = match market_type {
            MarketType::Spot => self.config.spot_private_ws_url.clone(),
            MarketType::Perpetual => self.config.futures_private_ws_url.clone(),
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        CoinwPrivateWsSession::new(
            self.exchange_id.clone(),
            url,
            subscription,
            tenant_id,
            account_id,
            market_type,
            symbol_hint,
        )
    }
}

impl CoinwPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = coinw_public_subscribe_payload(&subscription)?;
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
        coinw_ping_payload()
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
    ) -> ExchangeApiResult<Vec<CoinwWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message =
            parse_coinw_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(
            message,
            CoinwPublicStreamMessage::Heartbeat | CoinwPublicStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![CoinwWsSessionEvent::Public(message.clone())];
        match message {
            CoinwPublicStreamMessage::Heartbeat => {
                events.push(CoinwWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
                events.push(CoinwWsSessionEvent::Outbound(coinw_pong_payload()));
            }
            CoinwPublicStreamMessage::OrderBook(book) => {
                events.push(CoinwWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(book),
                ]));
            }
            CoinwPublicStreamMessage::SubscriptionAck { .. } | CoinwPublicStreamMessage::Pong => {}
        }
        Ok(events)
    }
}

impl CoinwPrivateWsSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        tenant_id: TenantId,
        account_id: AccountId,
        market_type: MarketType,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = coinw_private_subscribe_payload(&subscription, market_type)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            symbol_hint,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        coinw_ping_payload()
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
    ) -> ExchangeApiResult<Vec<CoinwWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_coinw_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(
            message,
            CoinwPrivateStreamMessage::Heartbeat | CoinwPrivateStreamMessage::Pong
        ) {
            self.state.on_pong(now);
        }
        let mut events = vec![CoinwWsSessionEvent::Private(message.clone())];
        match message {
            CoinwPrivateStreamMessage::Heartbeat => {
                events.push(CoinwWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
                events.push(CoinwWsSessionEvent::Outbound(coinw_pong_payload()));
            }
            CoinwPrivateStreamMessage::Events(stream_events) => {
                events.push(CoinwWsSessionEvent::Stream(stream_events));
            }
            CoinwPrivateStreamMessage::SubscriptionAck { .. } | CoinwPrivateStreamMessage::Pong => {
            }
        }
        Ok(events)
    }
}

pub fn coinw_private_stream_capabilities() -> PrivateStreamCapabilities {
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
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn coinw_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    match subscription.symbol.market_type {
        MarketType::Spot => coinw_spot_public_subscribe_payload(subscription),
        MarketType::Perpetual => coinw_futures_public_subscribe_payload(subscription),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinw.public_stream_market_type",
        }),
    }
}

pub fn coinw_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    match market_type {
        MarketType::Spot => {
            let stream_type = match subscription.kind {
                PrivateStreamKind::Orders | PrivateStreamKind::Fills => "order",
                PrivateStreamKind::Balances | PrivateStreamKind::Account => "assets",
                PrivateStreamKind::Positions => {
                    return Err(ExchangeApiError::Unsupported {
                        operation: "coinw.spot_private_positions_stream",
                    });
                }
            };
            Ok(json!({
                "event": "sub",
                "params": {
                    "biz": "exchange",
                    "type": stream_type,
                },
            }))
        }
        MarketType::Perpetual => {
            let stream_type = match subscription.kind {
                PrivateStreamKind::Orders | PrivateStreamKind::Fills => "order",
                PrivateStreamKind::Balances | PrivateStreamKind::Account => "assets",
                PrivateStreamKind::Positions => "position",
            };
            Ok(json!({
                "event": "sub",
                "params": {
                    "biz": "futures",
                    "type": stream_type,
                },
            }))
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinw.private_stream_market_type",
        }),
    }
}

fn coinw_spot_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol =
        normalize_coinw_spot_symbol(&subscription.symbol.exchange_symbol.symbol)?.replace('_', "-");
    let args = match &subscription.kind {
        PublicStreamKind::OrderBookDelta => format!("spot/level2:{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("spot/level2_20:{symbol}"),
        PublicStreamKind::Trades => format!("spot/match:{symbol}"),
        PublicStreamKind::Ticker => format!("spot/market-api-ticker:{symbol}"),
        PublicStreamKind::Candles { interval } => {
            format!("spot/candle-{}:{symbol}", normalize_interval(interval)?)
        }
    };
    Ok(json!({
        "event": "subscribe",
        "args": args,
    }))
}

fn coinw_futures_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let pair_code = normalize_coinw_perp_base(&subscription.symbol.exchange_symbol.symbol)?;
    let mut params = json!({
        "biz": "futures",
        "pairCode": pair_code,
    });
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            params["type"] = json!("depth");
        }
        PublicStreamKind::Trades => {
            params["type"] = json!("fills");
        }
        PublicStreamKind::Ticker => {
            params["type"] = json!("ticker");
        }
        PublicStreamKind::Candles { interval } => {
            params["type"] = json!("candles");
            params["interval"] = json!(normalize_interval(interval)?);
        }
    }
    Ok(json!({
        "event": "sub",
        "params": params,
    }))
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<&str> {
    match interval.trim() {
        "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d" | "1w"
        | "1M" => Ok(interval.trim()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinw.public_stream_candle_interval",
        }),
    }
}

fn stream_label(payload: &Value) -> Option<&str> {
    payload
        .get("args")
        .and_then(Value::as_str)
        .or_else(|| payload.get("params")?.get("type")?.as_str())
}

pub fn coinw_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: COINW_WS_PING_INTERVAL_MS,
        pong_timeout_ms: COINW_WS_PONG_TIMEOUT_MS,
        stale_message_ms: COINW_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn coinw_ping_payload() -> Value {
    json!({ "event": "ping" })
}

pub fn coinw_pong_payload() -> Value {
    json!({ "event": "pong" })
}

pub fn parse_coinw_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinwPublicStreamMessage> {
    if is_ping(value) {
        return Ok(CoinwPublicStreamMessage::Heartbeat);
    }
    if is_pong(value) {
        return Ok(CoinwPublicStreamMessage::Pong);
    }
    if is_subscription_ack(value) {
        return Ok(CoinwPublicStreamMessage::SubscriptionAck {
            channel: stream_channel(value),
        });
    }
    if stream_kind(value).is_some_and(|kind| {
        kind.contains("depth") || kind.contains("level2") || kind.contains("book")
    }) || stream_data(value).get("bids").is_some()
        || stream_data(value).get("asks").is_some()
    {
        let book = parse_orderbook_snapshot(
            exchange_id,
            symbol.clone(),
            &normalize_public_order_book_message(symbol.market_type, &symbol, value)?,
        )?;
        return Ok(CoinwPublicStreamMessage::OrderBook(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book: book,
        }));
    }
    Err(stream_parse_error(
        exchange_id.clone(),
        "unsupported CoinW public stream message",
        value,
    ))
}

pub fn parse_coinw_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<CoinwPrivateStreamMessage> {
    if is_ping(value) {
        return Ok(CoinwPrivateStreamMessage::Heartbeat);
    }
    if is_pong(value) {
        return Ok(CoinwPrivateStreamMessage::Pong);
    }
    if is_subscription_ack(value) {
        return Ok(CoinwPrivateStreamMessage::SubscriptionAck {
            channel: stream_channel(value),
        });
    }
    let kind = stream_kind(value).unwrap_or_default();
    let data = stream_data(value);
    if kind.contains("asset") || kind.contains("balance") || kind.contains("account") {
        return Ok(CoinwPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    &[],
                    &json!({ "data": data }),
                )?,
            }),
        ]));
    }
    if kind.contains("position") {
        return Ok(CoinwPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &[],
                    &json!({ "data": [data.clone()] }),
                )?,
            }),
        ]));
    }
    if kind.contains("order") || data.get("orderNumber").is_some() || data.get("orderId").is_some()
    {
        let normalized = normalize_private_order_message(market_type, symbol_hint.as_ref(), data)?;
        if let Some(order) =
            parse_order(exchange_id, symbol_hint.as_ref(), market_type, &normalized)?
        {
            return Ok(CoinwPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::OrderUpdate(order),
            ]));
        }
    }
    Err(stream_parse_error(
        exchange_id.clone(),
        "unsupported CoinW private stream message",
        value,
    ))
}

fn normalize_public_order_book_message(
    market_type: MarketType,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Value> {
    let data = stream_data(value);
    Ok(match market_type {
        MarketType::Spot => {
            let pair = normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)?;
            json!({
                "data": [{
                    "pair": pair,
                    "bids": data.get("bids").cloned().unwrap_or(Value::Null),
                    "asks": data.get("asks").cloned().unwrap_or(Value::Null),
                }]
            })
        }
        MarketType::Perpetual => json!({
            "data": {
                "bids": normalize_futures_levels(data.get("bids").or_else(|| data.get("b"))),
                "asks": normalize_futures_levels(data.get("asks").or_else(|| data.get("a"))),
                "ts": data.get("ts").or_else(|| data.get("time")).cloned().unwrap_or(Value::Null),
            }
        }),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.stream_order_book_market_type",
            });
        }
    })
}

fn normalize_futures_levels(value: Option<&Value>) -> Value {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Value::Array(Vec::new());
    };
    Value::Array(
        levels
            .iter()
            .map(|level| {
                if level.get("p").is_some() && level.get("m").is_some() {
                    return level.clone();
                }
                let price = level
                    .as_array()
                    .and_then(|items| items.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                let quantity = level
                    .as_array()
                    .and_then(|items| items.get(1))
                    .cloned()
                    .unwrap_or(Value::Null);
                json!({ "p": price, "m": quantity })
            })
            .collect(),
    )
}

fn normalize_private_order_message(
    market_type: MarketType,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Value> {
    let mut normalized = value.clone();
    if let Value::Object(map) = &mut normalized {
        if !map.contains_key("id") {
            if let Some(order_id) = map.get("orderId").cloned() {
                map.insert("id".to_string(), order_id);
            }
        }
        if !map.contains_key("totalPiece") {
            if let Some(order_volume) = map.get("orderVolume").cloned() {
                map.insert("totalPiece".to_string(), order_volume);
            }
        }
        if market_type == MarketType::Spot {
            if !map.contains_key("currencyPair") {
                if let Some(symbol) = map
                    .get("symbol")
                    .or_else(|| map.get("pair"))
                    .cloned()
                    .or_else(|| {
                        symbol_hint.map(|symbol| json!(symbol.exchange_symbol.symbol.clone()))
                    })
                {
                    map.insert("currencyPair".to_string(), symbol);
                }
            }
            if !map.contains_key("type") {
                if let Some(side) = map.get("side").cloned() {
                    map.insert("type".to_string(), side);
                }
            }
        } else {
            if !map.contains_key("instrument") {
                if let Some(symbol) = map
                    .get("symbol")
                    .or_else(|| map.get("base"))
                    .cloned()
                    .or_else(|| {
                        symbol_hint.map(|symbol| json!(symbol.exchange_symbol.symbol.clone()))
                    })
                {
                    let symbol = symbol
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| symbol.to_string());
                    map.insert(
                        "instrument".to_string(),
                        json!(normalize_coinw_perp_symbol(&symbol)?),
                    );
                }
            }
        }
    }
    Ok(json!({ "data": normalized }))
}

fn stream_data(value: &Value) -> &Value {
    value
        .get("data")
        .or_else(|| value.get("params").and_then(|params| params.get("data")))
        .or_else(|| value.get("tick"))
        .unwrap_or(value)
}

fn stream_kind(value: &Value) -> Option<String> {
    value
        .get("type")
        .or_else(|| value.get("channel"))
        .or_else(|| value.get("topic"))
        .or_else(|| value.get("args"))
        .and_then(Value::as_str)
        .or_else(|| value.get("params")?.get("type")?.as_str())
        .map(|value| value.to_ascii_lowercase())
}

fn stream_channel(value: &Value) -> Option<String> {
    value
        .get("channel")
        .or_else(|| value.get("topic"))
        .or_else(|| value.get("args"))
        .and_then(Value::as_str)
        .or_else(|| value.get("params")?.get("type")?.as_str())
        .map(str::to_string)
}

fn is_subscription_ack(value: &Value) -> bool {
    let event = value
        .get("event")
        .or_else(|| value.get("op"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    event.eq_ignore_ascii_case("sub")
        || event.eq_ignore_ascii_case("subscribe")
        || event.eq_ignore_ascii_case("subscribed")
        || value.get("result").is_some()
}

fn is_ping(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("ping"))
        || value
            .get("event")
            .or_else(|| value.get("op"))
            .and_then(Value::as_str)
            .is_some_and(|text| text.eq_ignore_ascii_case("ping"))
}

fn is_pong(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
        || value
            .get("event")
            .or_else(|| value.get("op"))
            .and_then(Value::as_str)
            .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("ping") || trimmed.eq_ignore_ascii_case("pong") {
        return Ok(json!({ "event": trimmed.to_ascii_lowercase() }));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid CoinW websocket JSON: {error}"),
            &Value::String(trimmed.to_string()),
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
