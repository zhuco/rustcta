#![allow(dead_code)]

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, OrderSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, parse_order_book};
use super::private_parser::{parse_balances, parse_order_state, stream_order_payload};
use super::BiconomyGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const BICONOMY_WS_PING_INTERVAL_MS: i64 = 15_000;
const BICONOMY_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const BICONOMY_WS_STALE_MESSAGE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq)]
pub enum BiconomyPublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    OrderBook(OrderBookResponse),
    Trades(Vec<BiconomyPublicTrade>),
    Ticker(BiconomyTicker),
    Candle(BiconomyCandle),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BiconomyPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BiconomyTicker {
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
pub struct BiconomyCandle {
    pub symbol: SymbolScope,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub close: String,
    pub high: String,
    pub low: String,
    pub volume: String,
    pub quote_volume: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BiconomyPrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Events(Vec<ExchangeStreamEvent>),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BiconomyWsSessionEvent {
    Public(BiconomyPublicStreamMessage),
    Private(BiconomyPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct BiconomyPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct BiconomyPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl BiconomyPublicWsSession {
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
    ) -> ExchangeApiResult<Vec<BiconomyWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        if is_ping(&value) {
            self.state.on_ping_sent(now);
            return Ok(vec![BiconomyWsSessionEvent::Outbound(pong_payload())]);
        }
        let message = parse_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, BiconomyPublicStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![BiconomyWsSessionEvent::Public(message.clone())];
        if let BiconomyPublicStreamMessage::OrderBook(book) = message {
            events.push(BiconomyWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::OrderBookSnapshot(book),
            ]));
        }
        Ok(events)
    }
}

impl BiconomyPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "Biconomy private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let subscribe_payload = private_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
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
        ping_payload()
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<BiconomyWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        if is_ping(&value) {
            self.state.on_ping_sent(now);
            return Ok(vec![BiconomyWsSessionEvent::Outbound(pong_payload())]);
        }
        let message = parse_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, BiconomyPrivateStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![BiconomyWsSessionEvent::Private(message.clone())];
        if let BiconomyPrivateStreamMessage::Events(stream_events) = message {
            events.push(BiconomyWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

impl BiconomyGatewayAdapter {
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
            "biconomy:{}:{}",
            self.config.public_ws_url,
            payload["method"].as_str().unwrap_or("unknown")
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
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "biconomy:{}:{}",
            self.config.private_ws_url,
            payload["channel"].as_str().unwrap_or("unknown")
        ))
    }

    pub(crate) fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<BiconomyPublicWsSession> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        BiconomyPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub(crate) fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<BiconomyPrivateWsSession> {
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        self.private_credentials("biconomy.subscribe_private_stream")?;
        BiconomyPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            symbol_hint,
        )
    }
}

pub fn public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = ws_symbol(&subscription.symbol)?;
    let payload = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => json!({
            "method": "depth.subscribe",
            "params": [symbol, 50, "0.01"],
            "id": 1,
        }),
        PublicStreamKind::Trades => json!({
            "method": "deals.subscribe",
            "params": [symbol],
            "id": 1,
        }),
        PublicStreamKind::Ticker => json!({
            "method": "state.subscribe",
            "params": [symbol],
            "id": 1,
        }),
        PublicStreamKind::Candles { interval } => json!({
            "method": "kline.subscribe",
            "params": [symbol, normalize_candle_interval(interval)?],
            "id": 1,
        }),
    };
    Ok(payload)
}

pub fn private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "sub",
        "channel": private_channel(&subscription.kind)?,
    }))
}

pub fn ping_payload() -> Value {
    json!({ "method": "server.ping", "params": [], "id": Utc::now().timestamp_millis() })
}

pub fn pong_payload() -> Value {
    json!({ "pong": Utc::now().timestamp_millis() })
}

pub fn biconomy_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: BICONOMY_WS_PING_INTERVAL_MS,
        pong_timeout_ms: BICONOMY_WS_PONG_TIMEOUT_MS,
        stale_message_ms: BICONOMY_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BiconomyPublicStreamMessage> {
    if is_pong(value) {
        return Ok(BiconomyPublicStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(BiconomyPublicStreamMessage::SubscriptionAck {
            channel: public_message_name(value).map(str::to_string),
        });
    }
    let method = public_message_name(value).unwrap_or_default();
    if method.contains("depth") || value.get("bids").is_some() || depth_payload(value).is_some() {
        let book_value = depth_payload(value).unwrap_or_else(|| value.clone());
        let order_book = parse_order_book(exchange_id, symbol, &book_value)?;
        return Ok(BiconomyPublicStreamMessage::OrderBook(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book,
        }));
    }
    if method.contains("deals") || method.contains("trade") {
        return Ok(BiconomyPublicStreamMessage::Trades(parse_trades(
            exchange_id,
            symbol,
            value,
        )?));
    }
    if method.contains("state") || method.contains("ticker") || method.contains("price") {
        return Ok(BiconomyPublicStreamMessage::Ticker(parse_ticker(
            symbol, value,
        )?));
    }
    if method.contains("kline") || method.contains("candle") {
        return Ok(BiconomyPublicStreamMessage::Candle(parse_candle(
            exchange_id,
            symbol,
            value,
        )?));
    }
    Ok(BiconomyPublicStreamMessage::Raw(value.clone()))
}

pub fn parse_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BiconomyPrivateStreamMessage> {
    if is_pong(value) {
        return Ok(BiconomyPrivateStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(BiconomyPrivateStreamMessage::SubscriptionAck {
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if channel.contains("order")
        || value.get("e").and_then(Value::as_str) == Some("executionReport")
    {
        let payload = stream_order_payload(value);
        let order = parse_order_state(exchange_id, symbol_hint.as_ref(), &payload)?;
        return Ok(BiconomyPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::OrderUpdate(order),
        ]));
    }
    if channel.contains("balance") || value.get("balances").is_some() {
        let payload = value.get("data").unwrap_or(value);
        let balances = parse_balances(exchange_id, tenant_id, account_id, &[], payload)?;
        return Ok(BiconomyPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances,
            }),
        ]));
    }
    Ok(BiconomyPrivateStreamMessage::Raw(value.clone()))
}

fn private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account => {
            Ok("spot/user.order")
        }
        PrivateStreamKind::Balances => Ok("spot/user.balance"),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "biconomy.private_positions_stream",
        }),
    }
}

fn is_ack(value: &Value) -> bool {
    matches!(
        value.get("event").and_then(Value::as_str),
        Some("sub") | Some("subscribe") | Some("subscribed")
    ) || value.get("success").and_then(Value::as_bool) == Some(true)
        || value
            .get("result")
            .and_then(|result| result.get("status"))
            .and_then(Value::as_str)
            == Some("success")
}

fn is_ping(value: &Value) -> bool {
    value.get("ping").is_some()
        || value.get("event").and_then(Value::as_str) == Some("ping")
        || value.get("op").and_then(Value::as_str) == Some("ping")
        || value.get("method").and_then(Value::as_str) == Some("server.ping")
}

fn is_pong(value: &Value) -> bool {
    value.get("pong").is_some()
        || value.get("event").and_then(Value::as_str) == Some("pong")
        || value.get("op").and_then(Value::as_str) == Some("pong")
        || value.get("result").and_then(Value::as_str) == Some("pong")
        || value.get("method").and_then(Value::as_str) == Some("pong")
}

fn normalize_candle_interval(interval: &str) -> ExchangeApiResult<u64> {
    let seconds = match interval.trim() {
        "1m" => 60,
        "3m" => 180,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1_800,
        "1h" => 3_600,
        "2h" => 7_200,
        "4h" => 14_400,
        "6h" => 21_600,
        "12h" => 43_200,
        "1d" => 86_400,
        "1w" => 604_800,
        other => {
            return Err(ExchangeApiError::Unsupported {
                operation: match other {
                    "" => "biconomy.candles_empty_interval",
                    _ => "biconomy.candles_interval",
                },
            });
        }
    };
    Ok(seconds)
}

fn ws_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    if symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "biconomy.non_spot_market",
        });
    }
    let raw = symbol.exchange_symbol.symbol.trim();
    if raw.contains('_') {
        return Ok(raw.to_ascii_uppercase());
    }
    if let Some(canonical) = &symbol.canonical_symbol {
        return Ok(format!(
            "{}_{}",
            canonical.base_asset().to_ascii_uppercase(),
            canonical.quote_asset().to_ascii_uppercase()
        ));
    }
    let normalized = normalize_symbol(symbol)?;
    if let Some(stripped) = normalized.strip_suffix("USDT") {
        if !stripped.is_empty() {
            return Ok(format!("{}_USDT", stripped.to_ascii_uppercase()));
        }
    }
    Ok(normalized)
}

fn public_message_name(value: &Value) -> Option<&str> {
    value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
}

fn depth_payload(value: &Value) -> Option<Value> {
    let params = value.get("params")?.as_array()?;
    params
        .iter()
        .find(|item| item.get("bids").is_some() || item.get("asks").is_some())
        .cloned()
}

fn parse_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<BiconomyPublicTrade>> {
    let rows = trade_rows(value).ok_or_else(|| {
        super::parser::parse_error(
            exchange_id.clone(),
            "Biconomy trade message missing rows",
            value,
        )
    })?;
    let trades = rows
        .iter()
        .map(|row| {
            let side = match string_field(row, &["type", "side"])
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str()
            {
                "sell" | "ask" => OrderSide::Sell,
                _ => OrderSide::Buy,
            };
            BiconomyPublicTrade {
                symbol: symbol.clone(),
                trade_id: string_field(row, &["id", "trade_id", "tradeId"]),
                side,
                price: string_field(row, &["price", "p"]).unwrap_or_default(),
                quantity: string_field(row, &["amount", "quantity", "qty", "q"])
                    .unwrap_or_default(),
                traded_at: timestamp_from_fields(row, &["time", "timestamp", "ts"])
                    .unwrap_or_else(Utc::now),
            }
        })
        .collect();
    Ok(trades)
}

fn parse_ticker(symbol: SymbolScope, value: &Value) -> ExchangeApiResult<BiconomyTicker> {
    let data = params_object(value).unwrap_or_else(|| value.get("data").unwrap_or(value));
    Ok(BiconomyTicker {
        symbol,
        last_price: string_field(data, &["last", "close", "price"]),
        open_price: string_field(data, &["open"]),
        high_price: string_field(data, &["high"]),
        low_price: string_field(data, &["low"]),
        volume: string_field(data, &["volume", "vol"]),
        quote_volume: string_field(data, &["deal", "quote_volume", "quoteVolume"]),
        updated_at: timestamp_from_fields(data, &["time", "timestamp", "ts"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_candle(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BiconomyCandle> {
    let row = candle_row(value).ok_or_else(|| {
        super::parser::parse_error(
            exchange_id.clone(),
            "Biconomy candle message missing row",
            value,
        )
    })?;
    if let Some(values) = row.as_array() {
        return Ok(BiconomyCandle {
            symbol,
            opened_at: values
                .first()
                .and_then(timestamp_from_value)
                .unwrap_or_else(Utc::now),
            open: values.get(1).and_then(value_string).unwrap_or_default(),
            close: values.get(2).and_then(value_string).unwrap_or_default(),
            high: values.get(3).and_then(value_string).unwrap_or_default(),
            low: values.get(4).and_then(value_string).unwrap_or_default(),
            volume: values.get(5).and_then(value_string).unwrap_or_default(),
            quote_volume: values.get(6).and_then(value_string),
        });
    }
    Ok(BiconomyCandle {
        symbol,
        opened_at: timestamp_from_fields(row, &["time", "timestamp", "ts"])
            .unwrap_or_else(Utc::now),
        open: string_field(row, &["open", "o"]).unwrap_or_default(),
        close: string_field(row, &["close", "c"]).unwrap_or_default(),
        high: string_field(row, &["high", "h"]).unwrap_or_default(),
        low: string_field(row, &["low", "l"]).unwrap_or_default(),
        volume: string_field(row, &["volume", "vol", "v"]).unwrap_or_default(),
        quote_volume: string_field(row, &["deal", "quote_volume", "quoteVolume"]),
    })
}

fn trade_rows(value: &Value) -> Option<&[Value]> {
    if let Some(params) = value.get("params").and_then(Value::as_array) {
        if let Some(rows) = params.iter().find_map(Value::as_array) {
            return Some(rows.as_slice());
        }
    }
    value
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn params_object(value: &Value) -> Option<&Value> {
    value
        .get("params")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| item.is_object())
}

fn candle_row(value: &Value) -> Option<&Value> {
    if let Some(params) = value.get("params").and_then(Value::as_array) {
        if let Some(first) = params.first() {
            if first
                .as_array()
                .and_then(|values| values.first())
                .is_some_and(|value| value.is_array())
            {
                return first.as_array().and_then(|rows| rows.first());
            }
            if first.is_array() {
                return Some(first);
            }
        }
        if params.len() >= 6 {
            return Some(value.get("params")?);
        }
    }
    value
        .get("data")
        .unwrap_or(value)
        .as_object()
        .map(|_| value.get("data").unwrap_or(value))
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_string))
}

fn value_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(timestamp_from_value))
}

fn timestamp_from_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64() {
        return timestamp_from_number(number);
    }
    if let Some(number) = value.as_f64() {
        return timestamp_from_number((number * 1_000.0).round() as i64);
    }
    let text = value.as_str()?;
    if let Ok(number) = text.parse::<i64>() {
        return timestamp_from_number(number);
    }
    if let Ok(number) = text.parse::<f64>() {
        return timestamp_from_number((number * 1_000.0).round() as i64);
    }
    None
}

fn timestamp_from_number(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        ExchangeApiError::Exchange(ExchangeError {
            schema_version: SchemaVersion::current(),
            exchange_id: exchange_id.clone(),
            class: ExchangeErrorClass::Decode,
            code: None,
            message: format!("invalid Biconomy websocket JSON: {error}"),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: Some(Value::String(text.to_string())),
            occurred_at: Utc::now(),
        })
    })
}
