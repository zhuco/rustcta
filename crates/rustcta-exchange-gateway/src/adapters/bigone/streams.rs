#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PositionsResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderSide};
use serde_json::{json, Value};

use super::parser::{normalize_bigone_symbol, parse_error, parse_order_book};
use super::private_parser::{parse_balances, parse_order_state, parse_positions};
use super::signing::bigone_jwt;
use super::BigOneGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const BIGONE_WS_PING_INTERVAL_MS: i64 = 15_000;
const BIGONE_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const BIGONE_WS_STALE_MESSAGE_MS: i64 = 45_000;

#[derive(Debug, Clone, PartialEq)]
pub enum BigOnePublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    OrderBook(OrderBookResponse),
    Trades(Vec<BigOnePublicTrade>),
    Ticker(BigOneTicker),
    Candle(BigOneCandle),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigOnePublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigOneTicker {
    pub symbol: SymbolScope,
    pub open_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub last_price: Option<String>,
    pub volume: Option<String>,
    pub bid_price: Option<String>,
    pub bid_quantity: Option<String>,
    pub ask_price: Option<String>,
    pub ask_quantity: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigOneCandle {
    pub symbol: SymbolScope,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BigOnePrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong,
    Events(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BigOneWsSessionEvent {
    Public(BigOnePublicStreamMessage),
    Private(BigOnePrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct BigOnePublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct BigOnePrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    auth_payload: Option<Value>,
    state: StreamRuntimeState,
}

impl BigOneGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.public_ws_session(subscription)?;
        Ok(format!(
            "bigone:{}:{}",
            session.url,
            channel_label(&session.subscribe_payload).unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let session = self.private_ws_session(subscription, None)?;
        Ok(format!(
            "bigone:{}:{}:{}",
            session.url,
            channel_label(&session.subscribe_payload).unwrap_or("unknown"),
            session.account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<BigOnePublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let url = if subscription.symbol.market_type == MarketType::Perpetual {
            self.config.contract_public_ws_url.clone()
        } else {
            self.config.spot_public_ws_url.clone()
        };
        BigOnePublicWsSession::new(self.exchange_id.clone(), url, subscription)
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<BigOnePrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&subscription.context, "bigone.private_ws_session")?;
        let (api_key, api_secret) = self.private_credentials("bigone.private_ws_session")?;
        let url = if market_type == MarketType::Perpetual {
            self.config.contract_private_ws_url.clone()
        } else {
            self.config.spot_private_ws_url.clone()
        };
        BigOnePrivateWsSession::new(
            self.exchange_id.clone(),
            url,
            subscription,
            tenant_id,
            account_id,
            market_type,
            symbol_hint,
            api_key,
            api_secret,
        )
    }
}

impl BigOnePublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        let subscribe_payload = bigone_public_subscribe_payload(&subscription)?;
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
        bigone_ping_payload()
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
    ) -> ExchangeApiResult<Vec<BigOneWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message =
            parse_bigone_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, BigOnePublicStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![BigOneWsSessionEvent::Public(message.clone())];
        if let BigOnePublicStreamMessage::OrderBook(book) = message {
            events.push(BigOneWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::OrderBookSnapshot(book),
            ]));
        }
        Ok(events)
    }
}

impl BigOnePrivateWsSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        tenant_id: TenantId,
        account_id: AccountId,
        market_type: MarketType,
        symbol_hint: Option<SymbolScope>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Self> {
        let subscribe_payload = bigone_private_subscribe_payload(&subscription, market_type)?;
        let auth_payload = Some(json!({
            "request": "auth",
            "params": {
                "token": bigone_jwt(api_key, api_secret, Utc::now().timestamp_millis())?,
            }
        }));
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
            auth_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        self.auth_payload
            .clone()
            .into_iter()
            .chain(std::iter::once(self.subscribe_payload.clone()))
            .collect()
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        bigone_ping_payload()
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<BigOneWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_bigone_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, BigOnePrivateStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![BigOneWsSessionEvent::Private(message.clone())];
        if let BigOnePrivateStreamMessage::Events(stream_events) = message {
            events.push(BigOneWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn bigone_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_bigone_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    );
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "depth",
        PublicStreamKind::Trades => "trade",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => "kline",
    };
    if subscription.symbol.market_type == MarketType::Perpetual {
        Ok(json!({
            "request": "subscribe",
            "channel": channel,
            "instrument_id": symbol,
        }))
    } else {
        let request_id = "1";
        match &subscription.kind {
            PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok(json!({
                "requestId": request_id,
                "subscribeMarketDepthRequest": {
                    "market": symbol,
                    "limit": 50,
                }
            })),
            PublicStreamKind::Trades => Ok(json!({
                "requestId": request_id,
                "subscribeMarketTradesRequest": {
                    "market": symbol,
                    "limit": 20,
                }
            })),
            PublicStreamKind::Ticker => Ok(json!({
                "requestId": request_id,
                "subscribeMarketsTickerRequest": {
                    "markets": [symbol],
                }
            })),
            PublicStreamKind::Candles { interval } => Ok(json!({
                "requestId": request_id,
                "subscribeMarketCandlesRequest": {
                    "market": symbol,
                    "period": normalize_candle_interval(interval),
                }
            })),
        }
    }
}

pub fn bigone_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "orders",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "accounts",
        PrivateStreamKind::Positions => "positions",
    };
    Ok(json!({
        "request": "subscribe",
        "channel": channel,
        "market_type": if market_type == MarketType::Perpetual { "contract" } else { "spot" },
    }))
}

pub fn bigone_ping_payload() -> Value {
    json!({ "request": "ping" })
}

pub fn bigone_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: BIGONE_WS_PING_INTERVAL_MS,
        pong_timeout_ms: BIGONE_WS_PONG_TIMEOUT_MS,
        stale_message_ms: BIGONE_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_bigone_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BigOnePublicStreamMessage> {
    if is_pong(value) {
        return Ok(BigOnePublicStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(BigOnePublicStreamMessage::SubscriptionAck {
            channel: channel_label(value).map(str::to_string),
        });
    }
    let channel = channel_label(value).unwrap_or_default();
    let channel_lower = channel.to_ascii_lowercase();
    if channel_lower.contains("trade") || trade_rows(value).is_some() {
        return Ok(BigOnePublicStreamMessage::Trades(parse_public_trades(
            symbol, value,
        )));
    }
    if channel_lower.contains("candle") || channel_lower.contains("kline") {
        return Ok(BigOnePublicStreamMessage::Candle(parse_public_candle(
            symbol, value,
        )));
    }
    if channel_lower.contains("ticker") {
        return Ok(BigOnePublicStreamMessage::Ticker(parse_public_ticker(
            symbol, value,
        )));
    }
    if candle_payload(value).is_some() {
        return Ok(BigOnePublicStreamMessage::Candle(parse_public_candle(
            symbol, value,
        )));
    }
    if ticker_payload(value).is_some() {
        return Ok(BigOnePublicStreamMessage::Ticker(parse_public_ticker(
            symbol, value,
        )));
    }
    if channel_lower.contains("depth")
        || channel_lower.contains("book")
        || value.get("bids").is_some()
        || value.get("asks").is_some()
        || value
            .get("data")
            .is_some_and(|data| data.get("bids").is_some())
    {
        let book = parse_order_book(exchange_id, symbol, value)?;
        return Ok(BigOnePublicStreamMessage::OrderBook(book));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported BigONE public websocket message",
        value,
    ))
}

pub fn parse_bigone_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BigOnePrivateStreamMessage> {
    if is_pong(value) {
        return Ok(BigOnePrivateStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(BigOnePrivateStreamMessage::SubscriptionAck {
            channel: channel_label(value).map(str::to_string),
        });
    }
    let channel = channel_label(value).unwrap_or_default();
    if channel.contains("account") || channel.contains("balance") {
        return Ok(BigOnePrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    &[],
                    value,
                )?,
            }),
        ]));
    }
    if channel.contains("position") {
        return Ok(BigOnePrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(exchange_id, tenant_id, account_id, value)?,
            }),
        ]));
    }
    if channel.contains("order") || value.get("order").is_some() || value.get("id").is_some() {
        return Ok(BigOnePrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                symbol_hint.as_ref(),
                market_type,
                value.get("order").unwrap_or(value),
            )?),
        ]));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported BigONE private websocket message",
        value,
    ))
}

fn channel_label(value: &Value) -> Option<&str> {
    value
        .get("channel")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .or_else(|| value.get("data")?.get("channel")?.as_str())
        .or_else(|| {
            request_channel(value)
                .or_else(|| value.get("response")?.get("type")?.as_str())
                .or_else(|| value.get("request")?.get("type")?.as_str())
        })
}

fn is_ack(value: &Value) -> bool {
    value
        .get("request")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .is_some_and(|event| event.contains("subscribe") || event.contains("subscribed"))
        || value
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || value.get("requestId").is_some()
            && value.get("error").is_none()
            && value.get("data").is_none()
            && value.get("result").is_none()
}

fn is_pong(value: &Value) -> bool {
    value
        .get("response")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("request"))
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("pong") {
        return Ok(json!({ "response": "pong" }));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        parse_error(
            exchange_id.clone(),
            &format!("invalid BigONE websocket json: {error}"),
            &json!({ "raw": trimmed }),
        )
    })
}

fn normalize_candle_interval(interval: &str) -> String {
    match interval.trim() {
        "1m" => "MIN1",
        "5m" => "MIN5",
        "15m" => "MIN15",
        "30m" => "MIN30",
        "1h" => "HOUR1",
        "4h" => "HOUR4",
        "1d" => "DAY1",
        other => other,
    }
    .to_string()
}

fn request_channel(value: &Value) -> Option<&'static str> {
    for (field, channel) in [
        ("subscribeMarketDepthRequest", "depth"),
        ("unsubscribeMarketDepthRequest", "depth"),
        ("subscribeMarketTradesRequest", "trade"),
        ("unsubscribeMarketTradesRequest", "trade"),
        ("subscribeMarketsTickerRequest", "ticker"),
        ("unsubscribeMarketsTickerRequest", "ticker"),
        ("subscribeMarketCandlesRequest", "kline"),
        ("unsubscribeMarketCandlesRequest", "kline"),
    ] {
        if value.get(field).is_some() {
            return Some(channel);
        }
    }
    None
}

fn parse_public_trades(symbol: SymbolScope, value: &Value) -> Vec<BigOnePublicTrade> {
    let Some(rows) = trade_rows(value) else {
        return Vec::new();
    };
    rows.iter()
        .map(|row| BigOnePublicTrade {
            symbol: symbol.clone(),
            trade_id: string_field(row, &["id", "trade_id", "tradeId"]),
            side: parse_trade_side(string_field(row, &["takerSide", "side"]).as_deref()),
            price: string_field(row, &["price", "p"]).unwrap_or_default(),
            quantity: string_field(row, &["amount", "size", "quantity", "q"]).unwrap_or_default(),
            traded_at: timestamp_field(row, &["createdAt", "created_at", "time", "timestamp"])
                .unwrap_or_else(Utc::now),
        })
        .collect()
}

fn parse_public_ticker(symbol: SymbolScope, value: &Value) -> BigOneTicker {
    let data = ticker_payload(value).unwrap_or_else(|| data_object(value));
    let bid = data.get("bid");
    let ask = data.get("ask");
    BigOneTicker {
        symbol,
        open_price: string_field(data, &["open", "open_price"]),
        high_price: string_field(data, &["high", "high_price"]),
        low_price: string_field(data, &["low", "low_price"]),
        last_price: string_field(data, &["close", "last", "last_price", "price"]),
        volume: string_field(data, &["volume", "amount", "base_volume"]),
        bid_price: price_level_text(bid, 0).or_else(|| string_field(data, &["bidPrice"])),
        bid_quantity: price_level_text(bid, 1).or_else(|| string_field(data, &["bidAmount"])),
        ask_price: price_level_text(ask, 0).or_else(|| string_field(data, &["askPrice"])),
        ask_quantity: price_level_text(ask, 1).or_else(|| string_field(data, &["askAmount"])),
        updated_at: timestamp_field(data, &["updatedAt", "updated_at", "time", "timestamp"])
            .unwrap_or_else(Utc::now),
    }
}

fn parse_public_candle(symbol: SymbolScope, value: &Value) -> BigOneCandle {
    let data = candle_payload(value).unwrap_or_else(|| data_object(value));
    if let Some(row) = data.as_array() {
        return BigOneCandle {
            symbol,
            opened_at: row
                .first()
                .and_then(timestamp_value)
                .unwrap_or_else(Utc::now),
            open: row.get(1).and_then(value_string).unwrap_or_default(),
            high: row.get(2).and_then(value_string).unwrap_or_default(),
            low: row.get(3).and_then(value_string).unwrap_or_default(),
            close: row.get(4).and_then(value_string).unwrap_or_default(),
            volume: row.get(5).and_then(value_string).unwrap_or_default(),
        };
    }
    BigOneCandle {
        symbol,
        opened_at: timestamp_field(data, &["time", "createdAt", "created_at", "timestamp"])
            .unwrap_or_else(Utc::now),
        open: string_field(data, &["open", "o"]).unwrap_or_default(),
        high: string_field(data, &["high", "h"]).unwrap_or_default(),
        low: string_field(data, &["low", "l"]).unwrap_or_default(),
        close: string_field(data, &["close", "c"]).unwrap_or_default(),
        volume: string_field(data, &["volume", "amount", "v"]).unwrap_or_default(),
    }
}

fn trade_rows(value: &Value) -> Option<&[Value]> {
    let data = data_object(value);
    if let Some(rows) = data
        .get("trades")
        .or_else(|| value.get("trades"))
        .and_then(Value::as_array)
    {
        return Some(rows.as_slice());
    }
    data.as_array().map(Vec::as_slice).filter(|rows| {
        rows.first()
            .is_some_and(|row| row.get("price").is_some() && row.get("id").is_some())
    })
}

fn ticker_payload(value: &Value) -> Option<&Value> {
    let data = data_object(value);
    if data.get("close").is_some() || data.get("last").is_some() || data.get("ticker").is_some() {
        return data.get("ticker").or(Some(data));
    }
    data.get("tickers")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
}

fn candle_payload(value: &Value) -> Option<&Value> {
    let data = data_object(value);
    data.get("candle")
        .or_else(|| data.get("kline"))
        .or_else(|| data.get("candlestick"))
        .or_else(|| {
            if data.get("open").is_some() && data.get("close").is_some() {
                Some(data)
            } else {
                None
            }
        })
}

fn data_object(value: &Value) -> &Value {
    value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("payload"))
        .unwrap_or(value)
}

fn parse_trade_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "ASK" | "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_string))
}

fn price_level_text(value: Option<&Value>, index: usize) -> Option<String> {
    let value = value?;
    if let Some(row) = value.as_array() {
        return row.get(index).and_then(value_string);
    }
    let field = if index == 0 {
        ["price", "p"]
    } else {
        ["amount", "quantity"]
    };
    string_field(value, &field)
}

fn value_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn timestamp_field(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(timestamp_value))
}

fn timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        return DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .or_else(|| text.parse::<i64>().ok().and_then(timestamp_i64));
    }
    value.as_i64().and_then(timestamp_i64)
}

fn timestamp_i64(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        DateTime::<Utc>::from_timestamp(value, 0)
    }
}
