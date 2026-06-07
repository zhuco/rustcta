#![allow(dead_code)]

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, OrderSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, parse_order_book};
use super::private_parser::{
    parse_balances, parse_fills, parse_order_state, parse_positions, stream_order_payload,
};
use super::signing::sign_payload;
use super::CointrGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const COINTR_WS_PING_INTERVAL_MS: i64 = 15_000;
const COINTR_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const COINTR_WS_STALE_MESSAGE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq)]
pub enum CointrPublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    OrderBook(OrderBookResponse),
    Trades(Vec<CointrPublicTrade>),
    Ticker(CointrTicker),
    Candle(CointrCandle),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CointrPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CointrTicker {
    pub symbol: SymbolScope,
    pub last_price: Option<String>,
    pub open_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub volume: Option<String>,
    pub quote_volume: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CointrCandle {
    pub symbol: SymbolScope,
    pub interval: Option<String>,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: Option<String>,
    pub usdt_volume: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CointrPrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Events(Vec<ExchangeStreamEvent>),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CointrWsSessionEvent {
    Public(CointrPublicStreamMessage),
    Private(CointrPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct CointrPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct CointrPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol_hint: Option<SymbolScope>,
    login_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl CointrPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = public_subscribe_payload(&subscription)?;
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
        ping_payload()
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
    ) -> ExchangeApiResult<Vec<CointrWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        if is_ping(&value) {
            self.state.on_ping_sent(now);
            return Ok(vec![CointrWsSessionEvent::Outbound(pong_payload())]);
        }
        let message = parse_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, CointrPublicStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![CointrWsSessionEvent::Public(message.clone())];
        if let CointrPublicStreamMessage::OrderBook(book) = message {
            events.push(CointrWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::OrderBookSnapshot(book),
            ]));
        }
        Ok(events)
    }
}

impl CointrPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
        login_payload: Value,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let subscribe_payload = private_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            symbol_hint,
            login_payload,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.login_payload.clone(), self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        ping_payload()
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<CointrWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        if is_ping(&value) {
            self.state.on_ping_sent(now);
            return Ok(vec![CointrWsSessionEvent::Outbound(pong_payload())]);
        }
        let message =
            parse_private_stream_message(&self.exchange_id, self.symbol_hint.clone(), &value)?;
        if matches!(message, CointrPrivateStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![CointrWsSessionEvent::Private(message.clone())];
        if let CointrPrivateStreamMessage::Events(stream_events) = message {
            events.push(CointrWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

impl CointrGatewayAdapter {
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
            "cointr:{}:{}",
            self.config.public_ws_url,
            payload["args"][0]["channel"].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.private_ws_session(subscription, None)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .last()
            .unwrap_or(Value::Null);
        Ok(format!(
            "cointr:{}:{}",
            self.config.private_ws_url,
            payload["args"][0]["channel"].as_str().unwrap_or("unknown")
        ))
    }

    pub(crate) fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<CointrPublicWsSession> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        CointrPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub(crate) fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<CointrPrivateWsSession> {
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let (api_key, api_secret, passphrase, _) =
            self.private_credentials("cointr.subscribe_private_stream")?;
        let timestamp_seconds = Utc::now().timestamp();
        let login_payload =
            private_login_payload(api_key, api_secret, passphrase, timestamp_seconds)?;
        CointrPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            symbol_hint,
            login_payload,
        )
    }
}

pub fn public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [public_channel_arg(subscription)?],
    }))
}

pub fn private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "channel": private_channel(&subscription.kind)?,
        }],
    }))
}

pub fn private_login_payload(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    timestamp_seconds: i64,
) -> ExchangeApiResult<Value> {
    let signature = sign_payload(api_secret, timestamp_seconds, "GET", "/user/verify", "")?;
    Ok(json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp_seconds.to_string(),
            "sign": signature,
        }],
    }))
}

pub fn ping_payload() -> Value {
    json!({ "ping": Utc::now().timestamp_millis() })
}

pub fn pong_payload() -> Value {
    json!({ "pong": Utc::now().timestamp_millis() })
}

pub fn cointr_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: COINTR_WS_PING_INTERVAL_MS,
        pong_timeout_ms: COINTR_WS_PONG_TIMEOUT_MS,
        stale_message_ms: COINTR_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CointrPublicStreamMessage> {
    if is_pong(value) {
        return Ok(CointrPublicStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(CointrPublicStreamMessage::SubscriptionAck {
            channel: event_channel(value),
        });
    }
    let channel = event_channel(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let data = public_data(value);
    if channel.contains("book") || channel.contains("depth") || data.get("bids").is_some() {
        let order_book = parse_order_book(exchange_id, symbol, data)?;
        return Ok(CointrPublicStreamMessage::OrderBook(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book,
        }));
    }
    if channel.contains("trade") || data.get("tradeId").is_some() {
        return Ok(CointrPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol,
            value,
        )?));
    }
    if channel.contains("ticker") || data.get("lastPr").is_some() || data.get("last").is_some() {
        return Ok(CointrPublicStreamMessage::Ticker(parse_public_ticker(
            symbol, data,
        )?));
    }
    if channel.contains("candle") || channel.contains("kline") || data.as_array().is_some() {
        return Ok(CointrPublicStreamMessage::Candle(parse_public_candle(
            exchange_id,
            symbol,
            data,
            Some(&channel),
        )?));
    }
    Ok(CointrPublicStreamMessage::Raw(value.clone()))
}

pub fn parse_private_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<CointrPrivateStreamMessage> {
    if is_pong(value) {
        return Ok(CointrPrivateStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(CointrPrivateStreamMessage::SubscriptionAck {
            channel: event_channel(value),
        });
    }
    let channel = event_channel(value).unwrap_or_default();
    if channel.contains("order")
        || value.get("e").and_then(Value::as_str) == Some("executionReport")
    {
        let payload = stream_order_payload(value);
        let order = parse_order_state(exchange_id, symbol_hint.as_ref(), &payload)?;
        return Ok(CointrPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::OrderUpdate(order),
        ]));
    }
    if channel.contains("fill") || channel.contains("trade") {
        let Some(symbol) = symbol_hint.as_ref() else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cointr private fill stream parser requires symbol hint".to_string(),
            });
        };
        let fills = parse_fills(
            exchange_id,
            rustcta_types::TenantId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            rustcta_types::AccountId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            symbol,
            value.get("data").unwrap_or(value),
        )?;
        return Ok(CointrPrivateStreamMessage::Events(
            fills.into_iter().map(ExchangeStreamEvent::Fill).collect(),
        ));
    }
    if channel.contains("account") || channel.contains("balance") {
        let balances = parse_balances(
            exchange_id,
            rustcta_types::TenantId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            rustcta_types::AccountId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            &[],
            symbol_hint
                .as_ref()
                .map(|symbol| symbol.market_type)
                .unwrap_or(MarketType::Spot),
            value.get("data").unwrap_or(value),
        )?;
        return Ok(CointrPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(rustcta_exchange_api::BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances,
            }),
        ]));
    }
    if channel.contains("position") {
        let positions = parse_positions(
            exchange_id,
            rustcta_types::TenantId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            rustcta_types::AccountId::new("stream").map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            value.get("data").unwrap_or(value),
        )?;
        return Ok(CointrPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::PositionSnapshot(rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions,
            }),
        ]));
    }
    Ok(CointrPrivateStreamMessage::Raw(value.clone()))
}

fn public_channel_arg(subscription: &PublicStreamSubscription) -> ExchangeApiResult<Value> {
    let symbol = normalize_symbol(&subscription.symbol)?;
    let inst_type = match subscription.symbol.market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual => "USDT-FUTURES",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cointr.public_stream_market_type",
            })
        }
    };
    Ok(json!({
        "instType": inst_type,
        "channel": public_channel_name(&subscription.kind),
        "instId": symbol,
    }))
}

fn public_channel_name(kind: &PublicStreamKind) -> String {
    match kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "books5".to_string()
        }
        PublicStreamKind::Trades => "trade".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::Candles { interval } => format!("candle{interval}"),
    }
}

fn private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders => Ok("user.orders"),
        PrivateStreamKind::Fills => Ok("user.fills"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("user.account"),
        PrivateStreamKind::Positions => Ok("user.positions"),
    }
}

fn is_ack(value: &Value) -> bool {
    matches!(
        value
            .get("event")
            .or_else(|| value.get("op"))
            .and_then(Value::as_str),
        Some("sub") | Some("subscribe") | Some("subscribed")
    ) || value.get("success").and_then(Value::as_bool) == Some(true)
}

fn is_ping(value: &Value) -> bool {
    value.get("ping").is_some()
        || value.get("event").and_then(Value::as_str) == Some("ping")
        || value.get("op").and_then(Value::as_str) == Some("ping")
}

fn is_pong(value: &Value) -> bool {
    value.get("pong").is_some()
        || value.get("event").and_then(Value::as_str) == Some("pong")
        || value.get("op").and_then(Value::as_str) == Some("pong")
}

fn public_data(value: &Value) -> &Value {
    if let Some(array) = value.get("data").and_then(Value::as_array) {
        return array.first().unwrap_or(value);
    }
    value.get("data").unwrap_or(value)
}

fn public_rows(value: &Value) -> Vec<&Value> {
    match value.get("data").unwrap_or(value) {
        Value::Array(rows) => rows.iter().collect(),
        data => vec![data],
    }
}

fn event_channel(value: &Value) -> Option<String> {
    value
        .get("channel")
        .or_else(|| value.get("stream"))
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("arg")
                .and_then(|arg| arg.get("channel"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<CointrPublicTrade>> {
    public_rows(value)
        .into_iter()
        .map(|row| {
            Ok(CointrPublicTrade {
                symbol: symbol.clone(),
                trade_id: text(
                    row.get("tradeId")
                        .or_else(|| row.get("id"))
                        .or_else(|| row.get("t")),
                ),
                side: parse_side(row.get("side").or_else(|| row.get("S"))),
                price: text(row.get("price").or_else(|| row.get("p"))).ok_or_else(|| {
                    super::parser::parse_error(
                        exchange_id.clone(),
                        "Cointr trade missing price",
                        row,
                    )
                })?,
                quantity: text(
                    row.get("size")
                        .or_else(|| row.get("qty"))
                        .or_else(|| row.get("quantity"))
                        .or_else(|| row.get("q")),
                )
                .ok_or_else(|| {
                    super::parser::parse_error(
                        exchange_id.clone(),
                        "Cointr trade missing quantity",
                        row,
                    )
                })?,
                traded_at: timestamp_from_fields(row, &["ts", "time", "T", "E"])
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn parse_public_ticker(symbol: SymbolScope, value: &Value) -> ExchangeApiResult<CointrTicker> {
    Ok(CointrTicker {
        symbol,
        last_price: text(
            value
                .get("lastPr")
                .or_else(|| value.get("last"))
                .or_else(|| value.get("lastPrice"))
                .or_else(|| value.get("close"))
                .or_else(|| value.get("c")),
        ),
        open_price: text(
            value
                .get("open24h")
                .or_else(|| value.get("open"))
                .or_else(|| value.get("o")),
        ),
        high_price: text(
            value
                .get("high24h")
                .or_else(|| value.get("high"))
                .or_else(|| value.get("h")),
        ),
        low_price: text(
            value
                .get("low24h")
                .or_else(|| value.get("low"))
                .or_else(|| value.get("l")),
        ),
        volume: text(
            value
                .get("baseVolume")
                .or_else(|| value.get("vol24h"))
                .or_else(|| value.get("volume"))
                .or_else(|| value.get("v")),
        ),
        quote_volume: text(
            value
                .get("quoteVolume")
                .or_else(|| value.get("quoteVol"))
                .or_else(|| value.get("qv")),
        ),
        updated_at: timestamp_from_fields(value, &["ts", "time", "E"]).unwrap_or_else(Utc::now),
    })
}

fn parse_public_candle(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
    channel: Option<&str>,
) -> ExchangeApiResult<CointrCandle> {
    if let Some(row) = value.as_array() {
        return parse_public_candle_array(exchange_id, symbol, row, channel);
    }
    Ok(CointrCandle {
        symbol,
        interval: text(value.get("interval").or_else(|| value.get("i")))
            .or_else(|| channel.and_then(candle_interval_from_channel)),
        opened_at: timestamp_from_fields(value, &["ts", "time", "start", "startTime", "t"])
            .ok_or_else(|| {
                super::parser::parse_error(
                    exchange_id.clone(),
                    "Cointr candle missing start",
                    value,
                )
            })?,
        open: text(value.get("open").or_else(|| value.get("o"))).ok_or_else(|| {
            super::parser::parse_error(exchange_id.clone(), "Cointr candle missing open", value)
        })?,
        high: text(value.get("high").or_else(|| value.get("h"))).ok_or_else(|| {
            super::parser::parse_error(exchange_id.clone(), "Cointr candle missing high", value)
        })?,
        low: text(value.get("low").or_else(|| value.get("l"))).ok_or_else(|| {
            super::parser::parse_error(exchange_id.clone(), "Cointr candle missing low", value)
        })?,
        close: text(value.get("close").or_else(|| value.get("c"))).ok_or_else(|| {
            super::parser::parse_error(exchange_id.clone(), "Cointr candle missing close", value)
        })?,
        volume: text(value.get("volume").or_else(|| value.get("v"))).ok_or_else(|| {
            super::parser::parse_error(exchange_id.clone(), "Cointr candle missing volume", value)
        })?,
        quote_volume: text(value.get("quoteVolume").or_else(|| value.get("qv"))),
        usdt_volume: text(value.get("usdtVolume")),
        status: text(value.get("status")),
    })
}

fn parse_public_candle_array(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    row: &[Value],
    channel: Option<&str>,
) -> ExchangeApiResult<CointrCandle> {
    Ok(CointrCandle {
        symbol,
        interval: channel.and_then(candle_interval_from_channel),
        opened_at: row
            .first()
            .and_then(value_as_i64)
            .and_then(timestamp_from_number)
            .ok_or_else(|| {
                super::parser::parse_error(
                    exchange_id.clone(),
                    "Cointr candle missing array start",
                    &Value::Array(row.to_vec()),
                )
            })?,
        open: text(row.get(1)).ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Cointr candle missing array open",
                &Value::Array(row.to_vec()),
            )
        })?,
        high: text(row.get(2)).ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Cointr candle missing array high",
                &Value::Array(row.to_vec()),
            )
        })?,
        low: text(row.get(3)).ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Cointr candle missing array low",
                &Value::Array(row.to_vec()),
            )
        })?,
        close: text(row.get(4)).ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Cointr candle missing array close",
                &Value::Array(row.to_vec()),
            )
        })?,
        volume: text(row.get(5)).ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Cointr candle missing array volume",
                &Value::Array(row.to_vec()),
            )
        })?,
        quote_volume: text(row.get(6)),
        usdt_volume: text(row.get(7)),
        status: text(row.get(8)),
    })
}

fn parse_side(value: Option<&Value>) -> OrderSide {
    if value
        .and_then(Value::as_str)
        .is_some_and(|side| side.eq_ignore_ascii_case("sell") || side.eq_ignore_ascii_case("ask"))
    {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(timestamp_from_number)
}

fn timestamp_from_number(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000_000 {
        Utc.timestamp_micros(value).single()
    } else {
        Utc.timestamp_millis_opt(value).single()
    }
}

fn candle_interval_from_channel(channel: &str) -> Option<String> {
    let normalized = channel.to_ascii_lowercase();
    normalized
        .split(['/', ':', '@', '.'])
        .find_map(|part| {
            part.strip_prefix("candle")
                .or_else(|| part.strip_prefix("kline_"))
        })
        .filter(|interval| !interval.is_empty())
        .map(str::to_string)
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        ExchangeApiError::Exchange(ExchangeError {
            schema_version: SchemaVersion::current(),
            exchange_id: exchange_id.clone(),
            class: ExchangeErrorClass::Decode,
            code: None,
            message: format!("invalid Cointr websocket JSON: {error}"),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: Some(Value::String(text.to_string())),
            occurred_at: Utc::now(),
        })
    })
}
