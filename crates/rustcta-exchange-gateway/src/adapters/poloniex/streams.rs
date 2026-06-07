#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderSide, PositionSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{
    decimal_as_f64, first_timestamp_millis, normalize_depth, normalize_poloniex_symbol,
    parse_error, parse_orderbook_snapshot, parse_side, split_poloniex_symbol, value_as_i64,
    value_as_string,
};
use super::private_parser::{parse_balances, parse_order_state, parse_positions};
use super::signing::{sign_payload, signature_payload};
use super::PoloniexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const POLONIEX_WS_HEARTBEAT_PAYLOAD: &str = r#"{"event":"ping"}"#;

#[derive(Debug, Clone, PartialEq)]
pub enum PoloniexPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trades(Vec<PoloniexPublicTrade>),
    Ticker(PoloniexTicker24h),
    Candle(PoloniexCandle),
    SubscriptionAck { channel: String },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoloniexPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub quote_quantity: Option<String>,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoloniexTicker24h {
    pub symbol: SymbolScope,
    pub last_price: Option<String>,
    pub bid_price: Option<String>,
    pub bid_quantity: Option<String>,
    pub ask_price: Option<String>,
    pub ask_quantity: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub price_change: Option<String>,
    pub volume: Option<String>,
    pub quote_volume: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoloniexCandle {
    pub symbol: SymbolScope,
    pub interval: String,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PoloniexWsSessionEvent {
    Public(PoloniexPublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone)]
pub struct PoloniexPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct PoloniexPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    auth_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl PoloniexGatewayAdapter {
    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<PoloniexPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        PoloniexPublicWsSession::new(
            self.exchange_id.clone(),
            public_ws_url(&self.config, subscription.symbol.market_type).to_string(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<PoloniexPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (api_key, api_secret, _) = self.private_credentials("poloniex.private_ws_session")?;
        PoloniexPrivateWsSession::new(
            self.exchange_id.clone(),
            private_ws_url(&self.config, market_type).to_string(),
            subscription,
            api_key.to_string(),
            api_secret.to_string(),
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
            "poloniex:{}:{}:{}",
            session.url,
            payload
                .get("channel")
                .and_then(Value::as_array)
                .and_then(|channels| channels.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            payload
                .get("symbols")
                .and_then(Value::as_array)
                .and_then(|symbols| symbols.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown")
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
            "poloniex:{}:{}:{}",
            session.url,
            payload
                .get("channel")
                .and_then(Value::as_array)
                .and_then(|channels| channels.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            session.account_id
        ))
    }
}

impl PoloniexPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "poloniex public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        if !matches!(
            subscription.symbol.market_type,
            MarketType::Spot | MarketType::Perpetual
        ) {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.public_ws_session.market_type",
            });
        }
        let subscribe_payload = poloniex_public_subscribe_payload(&subscription)?;
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
        poloniex_ws_heartbeat_payload().to_string()
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
    ) -> ExchangeApiResult<Vec<PoloniexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_poloniex_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, PoloniexPublicStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![PoloniexWsSessionEvent::Public(message.clone())];
        match message {
            PoloniexPublicStreamMessage::OrderBook(order_book) => {
                events.push(PoloniexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(self.exchange_id.clone(), None),
                        order_book,
                    }),
                ]));
            }
            PoloniexPublicStreamMessage::Pong => {
                events.push(PoloniexWsSessionEvent::Stream(vec![
                    poloniex_heartbeat_event(self.exchange_id.clone()),
                ]));
            }
            PoloniexPublicStreamMessage::Trades(_)
            | PoloniexPublicStreamMessage::Ticker(_)
            | PoloniexPublicStreamMessage::Candle(_)
            | PoloniexPublicStreamMessage::SubscriptionAck { .. } => {}
        }
        Ok(events)
    }
}

impl PoloniexPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        api_key: String,
        api_secret: String,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "poloniex private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "poloniex private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.private_ws_session.market_type",
            });
        }
        let auth_payload =
            poloniex_ws_auth_payload(&api_key, &api_secret, Utc::now().timestamp_millis())?;
        let subscribe_payload = poloniex_private_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
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

    pub fn heartbeat_request(&self) -> String {
        poloniex_ws_heartbeat_payload().to_string()
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn with_symbol_hint(mut self, symbol_hint: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol_hint);
        self
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
    ) -> ExchangeApiResult<Vec<PoloniexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if is_poloniex_ws_pong(&value) {
            self.state.on_pong(Utc::now());
            let heartbeat = poloniex_heartbeat_event(self.exchange_id.clone());
            return Ok(vec![
                PoloniexWsSessionEvent::Private(vec![heartbeat.clone()]),
                PoloniexWsSessionEvent::Stream(vec![heartbeat]),
            ]);
        }
        let stream_events = parse_poloniex_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            Some(self.market_type),
            &value,
        )?;
        let mut events = vec![PoloniexWsSessionEvent::Private(stream_events.clone())];
        if !stream_events.is_empty() {
            events.push(PoloniexWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn poloniex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_poloniex_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let channel = poloniex_public_channel(&subscription.kind, subscription.symbol.market_type)?;
    let mut payload = json!({
        "event": "subscribe",
        "channel": [channel],
        "symbols": [symbol],
    });
    if matches!(
        subscription.kind,
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta
    ) {
        payload["depth"] = json!(normalize_depth(20, subscription.symbol.market_type).min(20));
    }
    Ok(payload)
}

pub fn poloniex_ws_auth_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_millis: i64,
) -> ExchangeApiResult<Value> {
    let timestamp = timestamp_millis.to_string();
    let params = HashMap::new();
    let payload = signature_payload("GET", "/ws", &params, &timestamp, None);
    Ok(json!({
        "event": "subscribe",
        "channel": ["auth"],
        "params": {
            "key": api_key,
            "signTimestamp": timestamp,
            "signatureMethod": "HmacSHA256",
            "signatureVersion": "2",
            "signature": sign_payload(api_secret, &payload)?,
        },
    }))
}

pub fn poloniex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
    let channel = match (market_type, &subscription.kind) {
        (_, PrivateStreamKind::Orders) | (_, PrivateStreamKind::Fills) => "orders",
        (MarketType::Spot, PrivateStreamKind::Balances)
        | (MarketType::Spot, PrivateStreamKind::Account) => "balances",
        (MarketType::Perpetual, PrivateStreamKind::Balances)
        | (MarketType::Perpetual, PrivateStreamKind::Account) => "account",
        (MarketType::Perpetual, PrivateStreamKind::Positions) => "positions",
        (MarketType::Spot, PrivateStreamKind::Positions) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.spot_positions_stream",
            });
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.private_stream_market_type",
            });
        }
    };
    Ok(json!({
        "event": "subscribe",
        "channel": [channel],
        "symbols": ["all"],
    }))
}

pub fn poloniex_ws_heartbeat_payload() -> &'static str {
    POLONIEX_WS_HEARTBEAT_PAYLOAD
}

pub fn poloniex_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: 20_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 30_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn is_poloniex_ws_pong(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("pong")
        || value.get("type").and_then(Value::as_str) == Some("pong")
        || value.get("op").and_then(Value::as_str) == Some("pong")
}

pub fn parse_poloniex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PoloniexPublicStreamMessage> {
    if is_poloniex_ws_pong(value) {
        return Ok(PoloniexPublicStreamMessage::Pong);
    }
    if value.get("event").and_then(Value::as_str) == Some("subscribe") {
        return Ok(PoloniexPublicStreamMessage::SubscriptionAck {
            channel: value_as_string(value.get("channel")).unwrap_or_default(),
        });
    }
    let channel = value.get("channel").and_then(Value::as_str).unwrap_or("");
    match channel_family(channel) {
        "book" | "book_lv2" => Ok(PoloniexPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(
                exchange_id,
                symbol_hint,
                &normalize_book_message_for_rest_parser(value),
            )?,
        )),
        "trades" => Ok(PoloniexPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        "ticker" | "tickers" => Ok(PoloniexPublicStreamMessage::Ticker(parse_ticker(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        "candles" => Ok(PoloniexPublicStreamMessage::Candle(parse_candle(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported poloniex public stream message",
            value,
        )),
    }
}

pub fn parse_poloniex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    market_type_hint: Option<MarketType>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if value.get("channel").and_then(Value::as_str) == Some("auth")
        || value.get("event").and_then(Value::as_str) == Some("subscribe")
    {
        return Ok(Vec::new());
    }
    let channel = value.get("channel").and_then(Value::as_str).unwrap_or("");
    let data = stream_data(value)?;
    match channel {
        "orders" => parse_private_order_events(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint,
            market_type_hint,
            data,
        ),
        "balances" | "account" => parse_private_balance_event(
            exchange_id,
            tenant_id,
            account_id,
            market_type_hint.unwrap_or(MarketType::Spot),
            data,
        ),
        "positions" => {
            let positions = parse_positions(exchange_id, tenant_id, account_id, &json!(data))?;
            Ok(vec![ExchangeStreamEvent::PositionSnapshot(
                rustcta_exchange_api::PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions,
                },
            )])
        }
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported poloniex private stream message",
            value,
        )),
    }
}

fn public_ws_url(config: &super::PoloniexGatewayConfig, market_type: MarketType) -> &str {
    if market_type == MarketType::Perpetual {
        &config.futures_public_ws_url
    } else {
        &config.spot_public_ws_url
    }
}

fn private_ws_url(config: &super::PoloniexGatewayConfig, market_type: MarketType) -> &str {
    if market_type == MarketType::Perpetual {
        &config.futures_private_ws_url
    } else {
        &config.spot_private_ws_url
    }
}

fn poloniex_public_channel(
    kind: &PublicStreamKind,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    match kind {
        PublicStreamKind::Trades => Ok("trades".to_string()),
        PublicStreamKind::Ticker => {
            if market_type == MarketType::Perpetual {
                Ok("tickers".to_string())
            } else {
                Ok("ticker".to_string())
            }
        }
        PublicStreamKind::OrderBookSnapshot => Ok("book".to_string()),
        PublicStreamKind::OrderBookDelta => Ok("book_lv2".to_string()),
        PublicStreamKind::Candles { interval } => poloniex_candle_channel(interval),
    }
}

fn poloniex_candle_channel(interval: &str) -> ExchangeApiResult<String> {
    let channel = match interval.trim() {
        "1m" | "1min" => "candles_minute_1",
        "5m" | "5min" => "candles_minute_5",
        "10m" | "10min" => "candles_minute_10",
        "15m" | "15min" => "candles_minute_15",
        "30m" | "30min" => "candles_minute_30",
        "1h" | "1hr" => "candles_hour_1",
        "2h" | "2hr" => "candles_hour_2",
        "4h" | "4hr" => "candles_hour_4",
        "6h" | "6hr" => "candles_hour_6",
        "12h" | "12hr" => "candles_hour_12",
        "1d" | "day" => "candles_day_1",
        "3d" => "candles_day_3",
        "1w" | "week" => "candles_week_1",
        "1M" | "month" => "candles_month_1",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.candle_interval",
            });
        }
    };
    Ok(channel.to_string())
}

fn channel_family(channel: &str) -> &str {
    if channel.starts_with("candles_") {
        "candles"
    } else {
        channel
    }
}

fn normalize_book_message_for_rest_parser(value: &Value) -> Value {
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|mut item| {
            if let Some(object) = item.as_object_mut() {
                if object.get("symbol").is_none() {
                    if let Some(symbol) = object.get("s").cloned() {
                        object.insert("symbol".to_string(), symbol);
                    }
                }
                if object.get("ts").is_none() {
                    if let Some(ts) = object.get("cT").cloned() {
                        object.insert("ts".to_string(), ts);
                    }
                }
            }
            item
        })
        .collect::<Vec<_>>();
    json!({ "data": items })
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    fallback_symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<PoloniexPublicTrade>> {
    stream_data(value)?
        .iter()
        .map(|trade| {
            Ok(PoloniexPublicTrade {
                symbol: symbol_from_stream_item(exchange_id, fallback_symbol.market_type, trade)
                    .unwrap_or_else(|_| fallback_symbol.clone()),
                trade_id: value_as_string(trade.get("id")),
                side: parse_side(
                    exchange_id,
                    trade
                        .get("side")
                        .or_else(|| trade.get("takerSide"))
                        .and_then(Value::as_str)
                        .unwrap_or("BUY"),
                )?,
                price: required_text(exchange_id, trade, &["price", "px"])?,
                quantity: required_text(exchange_id, trade, &["quantity", "qty"])?,
                quote_quantity: value_as_string(trade.get("amount").or_else(|| trade.get("amt"))),
                traded_at: first_timestamp_millis(trade, &["createTime", "cT", "ts"])
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn parse_ticker(
    exchange_id: &ExchangeId,
    fallback_symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PoloniexTicker24h> {
    let ticker = first_stream_item(value)?;
    Ok(PoloniexTicker24h {
        symbol: symbol_from_stream_item(exchange_id, fallback_symbol.market_type, ticker)
            .unwrap_or(fallback_symbol),
        last_price: value_as_string(ticker.get("close").or_else(|| ticker.get("c"))),
        bid_price: value_as_string(ticker.get("bPx")),
        bid_quantity: value_as_string(ticker.get("bSz")),
        ask_price: value_as_string(ticker.get("aPx")),
        ask_quantity: value_as_string(ticker.get("aSz")),
        high_price: value_as_string(ticker.get("high").or_else(|| ticker.get("h"))),
        low_price: value_as_string(ticker.get("low").or_else(|| ticker.get("l"))),
        price_change: value_as_string(ticker.get("dailyChange").or_else(|| ticker.get("dC"))),
        volume: value_as_string(ticker.get("quantity").or_else(|| ticker.get("qty"))),
        quote_volume: value_as_string(ticker.get("amount").or_else(|| ticker.get("amt"))),
        updated_at: first_timestamp_millis(ticker, &["ts", "closeTime", "cT"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_candle(
    exchange_id: &ExchangeId,
    fallback_symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PoloniexCandle> {
    let item = first_stream_item(value)?;
    let channel = value.get("channel").and_then(Value::as_str).unwrap_or("");
    if let Some(row) = item.as_array() {
        let symbol = row
            .first()
            .and_then(Value::as_str)
            .map(|symbol| symbol_from_text(exchange_id, fallback_symbol.market_type, symbol))
            .transpose()?
            .unwrap_or(fallback_symbol);
        return Ok(PoloniexCandle {
            symbol,
            interval: interval_from_candle_channel(channel).to_string(),
            low: row_text(exchange_id, row, 1)?,
            high: row_text(exchange_id, row, 2)?,
            open: row_text(exchange_id, row, 3)?,
            close: row_text(exchange_id, row, 4)?,
            quote_volume: Some(row_text(exchange_id, row, 5)?),
            volume: row_text(exchange_id, row, 6)?,
            opened_at: row
                .get(8)
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
            closed_at: row
                .get(9)
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis),
        });
    }
    Ok(PoloniexCandle {
        symbol: symbol_from_stream_item(exchange_id, fallback_symbol.market_type, item)
            .unwrap_or(fallback_symbol),
        interval: interval_from_candle_channel(channel).to_string(),
        opened_at: first_timestamp_millis(item, &["startTime", "sT"]).unwrap_or_else(Utc::now),
        closed_at: first_timestamp_millis(item, &["closeTime", "cT"]),
        open: required_text(exchange_id, item, &["open", "o"])?,
        high: required_text(exchange_id, item, &["high", "h"])?,
        low: required_text(exchange_id, item, &["low", "l"])?,
        close: required_text(exchange_id, item, &["close", "c"])?,
        volume: required_text(exchange_id, item, &["quantity", "qty"])?,
        quote_volume: value_as_string(item.get("amount").or_else(|| item.get("amt"))),
    })
}

fn parse_private_order_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    market_type_hint: Option<MarketType>,
    data: &[Value],
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let mut events = Vec::new();
    for item in data {
        let market_type = market_type_hint.unwrap_or_else(|| infer_market_type(item));
        let symbol = symbol_hint
            .clone()
            .or_else(|| symbol_from_stream_item(exchange_id, market_type, item).ok());
        events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            symbol.as_ref(),
            market_type,
            item,
        )?));
        if is_fill_event(item) {
            let fill = fill_from_order_event(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                item,
                symbol,
                market_type,
            )?;
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    }
    Ok(events)
}

fn parse_private_balance_event(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    data: &[Value],
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let value = if market_type == MarketType::Spot {
        json!({ "balances": data })
    } else {
        json!(data)
    };
    let balances = parse_balances(exchange_id, tenant_id, account_id, market_type, &[], &value)?;
    Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
        BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances,
        },
    )])
}

fn fill_from_order_event(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    item: &Value,
    symbol_hint: Option<SymbolScope>,
    market_type: MarketType,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_hint
        .map(Ok)
        .unwrap_or_else(|| symbol_from_stream_item(exchange_id, market_type, item))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "poloniex stream fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(
        item.get("tradePrice")
            .or_else(|| item.get("px"))
            .or_else(|| item.get("avgPx")),
    )
    .unwrap_or(0.0);
    let quantity = decimal_as_f64(
        item.get("tradeQty")
            .or_else(|| item.get("fillSz"))
            .or_else(|| item.get("execQty"))
            .or_else(|| item.get("sz")),
    )
    .unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(item.get("orderId").or_else(|| item.get("ordId"))),
        client_order_id: value_as_string(item.get("clientOrderId").or_else(|| item.get("clOrdId"))),
        fill_id: value_as_string(item.get("tradeId").or_else(|| item.get("trdId"))),
        side: parse_side(
            exchange_id,
            item.get("side").and_then(Value::as_str).unwrap_or("BUY"),
        )?,
        position_side: item
            .get("posSide")
            .and_then(Value::as_str)
            .map(|value| match value.to_ascii_uppercase().as_str() {
                "LONG" => PositionSide::Long,
                "SHORT" => PositionSide::Short,
                _ => PositionSide::Net,
            })
            .unwrap_or(PositionSide::Net),
        status: FillStatus::Confirmed,
        liquidity_role: item
            .get("matchRole")
            .and_then(Value::as_str)
            .map(|role| match role.to_ascii_uppercase().as_str() {
                "MAKER" => LiquidityRole::Maker,
                "TAKER" => LiquidityRole::Taker,
                _ => LiquidityRole::Unknown,
            })
            .unwrap_or(LiquidityRole::Unknown),
        price,
        quantity,
        quote_quantity: decimal_as_f64(
            item.get("tradeAmount")
                .or_else(|| item.get("execAmt"))
                .or_else(|| item.get("amt")),
        )
        .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(item.get("feeCurrency").or_else(|| item.get("feeCcy"))),
        fee_amount: decimal_as_f64(item.get("tradeFee").or_else(|| item.get("feeAmt"))),
        fee_rate: None,
        realized_pnl: None,
        filled_at: first_timestamp_millis(item, &["tradeTime", "uTime", "ts"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn stream_data(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            stream_parse_error(
                value
                    .get("exchange")
                    .and_then(Value::as_str)
                    .and_then(|value| ExchangeId::new(value).ok())
                    .unwrap_or_else(|| ExchangeId::unchecked("poloniex")),
                "poloniex stream message missing data",
                value,
            )
        })
}

fn first_stream_item(value: &Value) -> ExchangeApiResult<&Value> {
    stream_data(value)?.first().ok_or_else(|| {
        stream_parse_error(
            ExchangeId::unchecked("poloniex"),
            "poloniex stream message has empty data",
            value,
        )
    })
}

fn symbol_from_stream_item(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = value
        .get("symbol")
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "stream item missing symbol", value))?;
    symbol_from_text(exchange_id, market_type, symbol)
}

fn symbol_from_text(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = symbol.to_ascii_uppercase();
    let (base, quote) = split_poloniex_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn infer_market_type(value: &Value) -> MarketType {
    if value
        .get("symbol")
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .is_some_and(|symbol| symbol.to_ascii_uppercase().ends_with("_PERP"))
    {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    }
}

fn is_fill_event(value: &Value) -> bool {
    value
        .get("eventType")
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("trade"))
        || value.get("fillSz").is_some()
        || value.get("tradeQty").is_some()
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<String> {
    for field in fields {
        if let Some(text) = value_as_string(value.get(*field)) {
            return Ok(text);
        }
    }
    Err(parse_error(
        exchange_id.clone(),
        "stream field missing required text",
        value,
    ))
}

fn row_text(exchange_id: &ExchangeId, row: &[Value], index: usize) -> ExchangeApiResult<String> {
    value_as_string(row.get(index)).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "stream row missing required value",
            &Value::Array(row.to_vec()),
        )
    })
}

fn interval_from_candle_channel(channel: &str) -> &str {
    match channel {
        "candles_minute_1" => "1m",
        "candles_minute_5" => "5m",
        "candles_minute_10" => "10m",
        "candles_minute_15" => "15m",
        "candles_minute_30" => "30m",
        "candles_hour_1" => "1h",
        "candles_hour_2" => "2h",
        "candles_hour_4" => "4h",
        "candles_hour_6" => "6h",
        "candles_hour_12" => "12h",
        "candles_day_1" => "1d",
        "candles_day_3" => "3d",
        "candles_week_1" => "1w",
        "candles_month_1" => "1M",
        _ => "unknown",
    }
}

fn stream_parse_error(exchange: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange,
        ExchangeErrorClass::Decode,
        format!("{message}: {value}"),
        Utc::now(),
    );
    error.code = Some("poloniex_stream_parse".to_string());
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            "empty poloniex websocket message",
            &Value::Null,
        ));
    }
    if trimmed.eq_ignore_ascii_case("pong") {
        return Ok(json!({ "event": "pong" }));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        parse_error(
            exchange_id.clone(),
            &format!("invalid poloniex websocket JSON: {error}"),
            &json!({ "raw": trimmed }),
        )
    })
}

fn poloniex_heartbeat_event(exchange: ExchangeId) -> ExchangeStreamEvent {
    ExchangeStreamEvent::Heartbeat {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange,
        received_at: Utc::now(),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
