#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    PositionsResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, SchemaVersion};
use serde_json::{json, Value};

use super::parser::{
    normalize_phemex_symbol, parse_orderbook_snapshot, parse_public_trades, PhemexCandle,
    PhemexPublicTrade, PhemexTicker24h,
};
use super::private_parser::{
    parse_account_balances, parse_order_state, parse_positions, parse_recent_fills,
    symbol_scope_from_phemex_row,
};
use super::signing::sign_phemex_ws_auth;
use super::PhemexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

const PHEMEX_WS_PING_INTERVAL_MS: i64 = 15_000;
const PHEMEX_WS_PONG_TIMEOUT_MS: i64 = 30_000;
const PHEMEX_WS_STALE_MESSAGE_MS: i64 = 45_000;

#[derive(Debug, Clone, PartialEq)]
pub enum PhemexPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trades(Vec<PhemexPublicTrade>),
    Candle(PhemexCandle),
    Ticker(PhemexTicker24h),
    SubscriptionAck { id: Option<u64> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhemexPrivateStreamMessage {
    Events(Vec<ExchangeStreamEvent>),
    RiskAccount(PhemexRiskAccountStream),
    SubscriptionAck { id: Option<u64> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexRiskAccountStream {
    pub risk_units: Vec<Value>,
    pub risk_wallets: Vec<Value>,
    pub raw: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhemexWsSessionEvent {
    Public(PhemexPublicStreamMessage),
    Private(PhemexPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone)]
pub struct PhemexPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct PhemexPrivateWsSession {
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

impl PhemexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "phemex:{}:{}:{}",
            session.url,
            payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            normalize_phemex_symbol(
                &session.symbol.exchange_symbol.symbol,
                session.symbol.market_type
            )?
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (api_key, api_secret) = self.private_credentials("phemex.subscribe_private_stream")?;
        let session = self.private_ws_session(
            subscription,
            api_key,
            api_secret,
            Utc::now().timestamp() + 60,
        )?;
        let payload = session
            .initial_requests()
            .into_iter()
            .nth(1)
            .unwrap_or(Value::Null);
        Ok(format!(
            "phemex:{}:{}:{}",
            session.url,
            payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            session.account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<PhemexPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        PhemexPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        api_key: &str,
        api_secret: &str,
        expiry: i64,
    ) -> ExchangeApiResult<PhemexPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        PhemexPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            market_type,
            api_key,
            api_secret,
            expiry,
        )
    }
}

impl PhemexPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "phemex public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = phemex_public_subscribe_payload(&subscription, 1)?;
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
        phemex_ws_ping_payload(now.timestamp_millis() as u64)
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
    ) -> ExchangeApiResult<Vec<PhemexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_phemex_public_stream_message(
            &self.exchange_id,
            self.symbol.market_type,
            self.symbol.clone(),
            &value,
        )?;
        if matches!(message, PhemexPublicStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![PhemexWsSessionEvent::Public(message.clone())];
        match message {
            PhemexPublicStreamMessage::OrderBook(book) => {
                events.push(PhemexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(
                        rustcta_exchange_api::OrderBookResponse {
                            schema_version: EXCHANGE_API_SCHEMA_VERSION,
                            metadata: response_metadata(self.exchange_id.clone(), None),
                            order_book: book,
                        },
                    ),
                ]));
            }
            PhemexPublicStreamMessage::Pong => {
                events.push(PhemexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
            }
            PhemexPublicStreamMessage::Trades(_)
            | PhemexPublicStreamMessage::Candle(_)
            | PhemexPublicStreamMessage::Ticker(_)
            | PhemexPublicStreamMessage::SubscriptionAck { .. } => {}
        }
        Ok(events)
    }
}

impl PhemexPrivateWsSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        market_type: MarketType,
        api_key: &str,
        api_secret: &str,
        expiry: i64,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "phemex private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "phemex private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let auth_payload = phemex_ws_auth_payload(api_key, api_secret, expiry, 1);
        let subscribe_payload = phemex_private_subscribe_payload(&subscription, market_type, 2)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            market_type,
            symbol_hint: None,
            auth_payload,
            subscribe_payload,
            state,
        })
    }

    pub fn with_symbol_hint(mut self, symbol_hint: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol_hint);
        self
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.auth_payload.clone(), self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        phemex_ws_ping_payload(now.timestamp_millis() as u64)
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
    ) -> ExchangeApiResult<Vec<PhemexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        self.state.on_message(now);
        let message = parse_phemex_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, PhemexPrivateStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![PhemexWsSessionEvent::Private(message.clone())];
        match message {
            PhemexPrivateStreamMessage::Events(stream_events) => {
                events.push(PhemexWsSessionEvent::Stream(stream_events));
            }
            PhemexPrivateStreamMessage::Pong => {
                events.push(PhemexWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: now,
                    },
                ]));
            }
            PhemexPrivateStreamMessage::RiskAccount(_)
            | PhemexPrivateStreamMessage::SubscriptionAck { .. } => {}
        }
        Ok(events)
    }
}

pub fn phemex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_phemex_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let method = match (&subscription.kind, subscription.symbol.market_type) {
        (
            PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot,
            MarketType::Spot,
        ) => "orderbook.subscribe",
        (
            PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot,
            MarketType::Perpetual,
        ) => "orderbook_p.subscribe",
        (PublicStreamKind::Trades, MarketType::Spot) => "trade.subscribe",
        (PublicStreamKind::Trades, MarketType::Perpetual) => "trade_p.subscribe",
        (PublicStreamKind::Candles { .. }, MarketType::Spot) => "kline.subscribe",
        (PublicStreamKind::Candles { .. }, MarketType::Perpetual) => "kline_p.subscribe",
        (PublicStreamKind::Ticker, MarketType::Spot) => "spot_market24h.subscribe",
        (PublicStreamKind::Ticker, MarketType::Perpetual) => "perp_market24h_pack_p.subscribe",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.public_stream_market_type",
            });
        }
    };
    let params = match &subscription.kind {
        PublicStreamKind::Ticker => json!([]),
        PublicStreamKind::Candles { interval } => {
            json!([symbol, normalize_ws_kline_interval(interval)?])
        }
        PublicStreamKind::OrderBookSnapshot => json!([symbol, true]),
        _ => json!([symbol]),
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params,
    }))
}

pub fn phemex_ws_auth_payload(api_key: &str, api_secret: &str, expiry: i64, id: u64) -> Value {
    json!({
        "id": id,
        "method": "user.auth",
        "params": [
            "API",
            api_key,
            sign_phemex_ws_auth(api_secret, api_key, expiry),
            expiry
        ],
    })
}

pub fn phemex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    id: u64,
) -> ExchangeApiResult<Value> {
    let method = match (market_type, &subscription.kind) {
        (MarketType::Spot, PrivateStreamKind::Orders)
        | (MarketType::Spot, PrivateStreamKind::Fills)
        | (MarketType::Spot, PrivateStreamKind::Balances)
        | (MarketType::Spot, PrivateStreamKind::Account) => "wo.subscribe",
        (MarketType::Perpetual, PrivateStreamKind::Orders)
        | (MarketType::Perpetual, PrivateStreamKind::Fills)
        | (MarketType::Perpetual, PrivateStreamKind::Balances)
        | (MarketType::Perpetual, PrivateStreamKind::Positions)
        | (MarketType::Perpetual, PrivateStreamKind::Account) => "aop_p.subscribe",
        (MarketType::Spot, PrivateStreamKind::Positions) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.spot_private_positions_stream",
            });
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.private_stream_market_type",
            });
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": [],
    }))
}

pub fn phemex_risk_account_subscribe_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "ras_p.subscribe",
        "params": {},
    })
}

pub fn phemex_ws_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "server.ping",
        "params": [],
    })
}

pub fn phemex_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: PHEMEX_WS_PING_INTERVAL_MS,
        pong_timeout_ms: PHEMEX_WS_PONG_TIMEOUT_MS,
        stale_message_ms: PHEMEX_WS_STALE_MESSAGE_MS,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_phemex_public_stream_message(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PhemexPublicStreamMessage> {
    if value.get("pong").is_some() || is_phemex_pong(value) {
        return Ok(PhemexPublicStreamMessage::Pong);
    }
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(PhemexPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(value_as_u64),
        });
    }
    if value.get("book").is_some() || value.get("orderbook_p").is_some() {
        return Ok(PhemexPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, value)?,
        ));
    }
    if value.get("trades").is_some() || value.get("trades_p").is_some() {
        return Ok(PhemexPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol,
            value,
        )?));
    }
    if value.get("kline").is_some() || value.get("kline_p").is_some() {
        return Ok(PhemexPublicStreamMessage::Candle(parse_ws_candle(
            exchange_id,
            market_type,
            symbol,
            value,
        )?));
    }
    if value.get("spot_market24h").is_some()
        || value.get("market24h").is_some()
        || value.get("perp_market24h_pack_p").is_some()
        || value.get("market24h_p").is_some()
    {
        return Ok(PhemexPublicStreamMessage::Ticker(parse_ws_ticker(
            exchange_id,
            symbol,
            value,
        )?));
    }
    Err(stream_parse_error(
        exchange_id.clone(),
        "unsupported phemex stream message",
        value,
    ))
}

pub fn parse_phemex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<PhemexPrivateStreamMessage> {
    if value.get("pong").is_some() || is_phemex_pong(value) {
        return Ok(PhemexPrivateStreamMessage::Pong);
    }
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(PhemexPrivateStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(value_as_u64),
        });
    }
    if value.get("risk_units").is_some() || value.get("risk_wallets").is_some() {
        return Ok(PhemexPrivateStreamMessage::RiskAccount(parse_risk_account(
            value,
        )));
    }

    let mut events = Vec::new();
    if value.get("wallets").is_some()
        || value.get("wallets_mao").is_some()
        || value.get("accounts_p").is_some()
    {
        let balance_value = normalize_private_balance_payload(value);
        let balances = parse_account_balances(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            market_type,
            &[],
            &balance_value,
        )?;
        events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances,
        }));
    }

    if value.get("positions_p").is_some() {
        let positions = parse_positions(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            &[],
            value,
        )?;
        events.push(ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            positions,
        }));
    }

    for order in private_order_rows(value) {
        let hint = symbol_hint
            .as_ref()
            .filter(|symbol| {
                order
                    .get("symbol")
                    .and_then(Value::as_str)
                    .is_none_or(|raw| raw == symbol.exchange_symbol.symbol)
            })
            .or(symbol_hint.as_ref());
        events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            hint,
            order,
        )?));
    }

    for fill in private_fill_rows(value) {
        let symbol = if let Some(symbol) = symbol_hint.as_ref().filter(|symbol| {
            fill.get("symbol")
                .and_then(Value::as_str)
                .is_none_or(|raw| raw == symbol.exchange_symbol.symbol)
        }) {
            symbol.clone()
        } else {
            symbol_scope_from_phemex_row(exchange_id, market_type, fill)?
        };
        let fill_value = json!({ "data": { "rows": [fill.clone()] } });
        for fill in parse_recent_fills(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            &symbol,
            &fill_value,
        )? {
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    }

    if events.is_empty() {
        return Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported phemex private stream message",
            value,
        ));
    }
    Ok(PhemexPrivateStreamMessage::Events(events))
}

fn parse_ws_ticker(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PhemexTicker24h> {
    let ticker = value
        .get("spot_market24h")
        .or_else(|| value.get("market24h"))
        .or_else(|| value.get("perp_market24h_pack_p"))
        .or_else(|| value.get("market24h_p"))
        .ok_or_else(|| stream_parse_error(exchange_id.clone(), "ticker missing payload", value))?;
    if let Some(ticker) = packed_ticker_row(ticker, &symbol) {
        return super::parser::parse_ticker_24h(exchange_id, symbol, &ticker);
    }
    super::parser::parse_ticker_24h(exchange_id, symbol, ticker)
}

fn parse_ws_candle(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PhemexCandle> {
    let row = value
        .get("kline")
        .or_else(|| value.get("kline_p"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| stream_parse_error(exchange_id.clone(), "kline missing rows", value))?;
    let interval = value
        .get("resolution")
        .and_then(Value::as_i64)
        .map(ws_resolution_to_interval)
        .unwrap_or_else(|| "unknown".to_string());
    if market_type == MarketType::Perpetual {
        return super::parser::parse_perp_candles(exchange_id, symbol, &interval, &json!([row]))
            .and_then(|mut candles| {
                candles.pop().ok_or_else(|| {
                    stream_parse_error(exchange_id.clone(), "kline parser returned empty", value)
                })
            });
    }
    parse_spot_ws_candle(exchange_id, symbol, &interval, row)
}

fn parse_spot_ws_candle(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    interval: &str,
    value: &Value,
) -> ExchangeApiResult<PhemexCandle> {
    let row = value.as_array().ok_or_else(|| {
        stream_parse_error(exchange_id.clone(), "spot kline row is not array", value)
    })?;
    Ok(PhemexCandle {
        symbol,
        interval: interval.to_string(),
        opened_at: row
            .first()
            .and_then(timestamp_from_ns_or_ms)
            .ok_or_else(|| {
                stream_parse_error(exchange_id.clone(), "kline missing timestamp", value)
            })?,
        open: scaled_text(row.get(3).or_else(|| row.get(1)), 8)
            .ok_or_else(|| stream_parse_error(exchange_id.clone(), "kline missing open", value))?,
        high: scaled_text(row.get(4).or_else(|| row.get(2)), 8)
            .ok_or_else(|| stream_parse_error(exchange_id.clone(), "kline missing high", value))?,
        low: scaled_text(row.get(5).or_else(|| row.get(3)), 8)
            .ok_or_else(|| stream_parse_error(exchange_id.clone(), "kline missing low", value))?,
        close: scaled_text(row.get(6).or_else(|| row.get(4)), 8)
            .ok_or_else(|| stream_parse_error(exchange_id.clone(), "kline missing close", value))?,
        volume: scaled_text(row.get(7).or_else(|| row.get(5)), 8),
        turnover: scaled_text(row.get(8).or_else(|| row.get(6)), 8),
    })
}

fn normalize_ws_kline_interval(interval: &str) -> ExchangeApiResult<u64> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "60" => Ok(60),
        "5m" | "300" => Ok(300),
        "15m" | "900" => Ok(900),
        "30m" | "1800" => Ok(1_800),
        "1h" | "3600" => Ok(3_600),
        "4h" | "14400" => Ok(14_400),
        "1d" | "86400" => Ok(86_400),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "phemex.ws_kline_interval",
        }),
    }
}

fn ws_resolution_to_interval(resolution: i64) -> String {
    match resolution {
        60 => "1m",
        300 => "5m",
        900 => "15m",
        1800 => "30m",
        3600 => "1h",
        14400 => "4h",
        86400 => "1d",
        _ => return resolution.to_string(),
    }
    .to_string()
}

fn scaled_text(value: Option<&Value>, scale: u32) -> Option<String> {
    let raw = value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))?;
    if scale == 0 {
        return Some(raw.to_string());
    }
    let sign = if raw < 0 { "-" } else { "" };
    let digits = raw.abs().to_string();
    if digits.len() <= scale as usize {
        let padded = format!(
            "{}{}",
            "0".repeat(scale as usize + 1 - digits.len()),
            digits
        );
        let split = padded.len() - scale as usize;
        return Some(
            format!("{sign}{}.{}", &padded[..split], &padded[split..])
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string(),
        );
    }
    let split = digits.len() - scale as usize;
    Some(
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
    )
}

fn timestamp_from_ns_or_ms(value: &Value) -> Option<DateTime<Utc>> {
    let timestamp = value_as_u64(value)? as i64;
    if timestamp > 10_000_000_000_000 {
        DateTime::<Utc>::from_timestamp(
            timestamp / 1_000_000_000,
            (timestamp % 1_000_000_000) as u32,
        )
    } else if timestamp > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(timestamp)
    } else {
        DateTime::<Utc>::from_timestamp(timestamp, 0)
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn packed_ticker_row(value: &Value, symbol: &rustcta_exchange_api::SymbolScope) -> Option<Value> {
    let fields = value.get("fields")?.as_array()?;
    let rows = value.get("data")?.as_array()?;
    let preferred_symbol =
        normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type).ok();
    let row = rows.iter().filter_map(Value::as_array).find(|row| {
        let symbol_index = fields
            .iter()
            .position(|field| field.as_str().is_some_and(|field| field == "symbol"));
        match (symbol_index, preferred_symbol.as_deref()) {
            (Some(index), Some(expected)) => row
                .get(index)
                .and_then(Value::as_str)
                .is_some_and(|actual| actual.eq_ignore_ascii_case(expected)),
            _ => true,
        }
    })?;
    let mut object = serde_json::Map::new();
    for (field, value) in fields.iter().zip(row.iter()) {
        if let Some(field) = field.as_str() {
            object.insert(field.to_string(), value.clone());
        }
    }
    Some(Value::Object(object))
}

fn normalize_private_balance_payload(value: &Value) -> Value {
    if value.get("accounts_p").is_some() {
        return json!({ "accounts_p": value.get("accounts_p").cloned().unwrap_or_default() });
    }
    if value.get("wallets_mao").is_some() {
        return json!({ "wallets": value.get("wallets_mao").cloned().unwrap_or_default() });
    }
    value.clone()
}

fn private_order_rows(value: &Value) -> Vec<&Value> {
    let mut rows = Vec::new();
    if let Some(orders) = value.get("orders_p").and_then(Value::as_array) {
        rows.extend(orders.iter());
    }
    if let Some(orders) = value.get("orders").and_then(Value::as_array) {
        rows.extend(orders.iter());
    }
    for key in ["orders", "orders_mao"] {
        if let Some(orders) = value.get(key).and_then(Value::as_object) {
            for bucket in ["open", "closed"] {
                if let Some(items) = orders.get(bucket).and_then(Value::as_array) {
                    rows.extend(items.iter());
                }
            }
        }
    }
    rows
}

fn private_fill_rows(value: &Value) -> Vec<&Value> {
    let mut rows = Vec::new();
    for key in ["fills", "fills_p"] {
        if let Some(fills) = value.get(key).and_then(Value::as_array) {
            rows.extend(fills.iter());
        }
    }
    for key in ["orders", "orders_mao"] {
        if let Some(fills) = value
            .get(key)
            .and_then(Value::as_object)
            .and_then(|orders| orders.get("fills"))
            .and_then(Value::as_array)
        {
            rows.extend(fills.iter());
        }
    }
    rows
}

fn parse_risk_account(value: &Value) -> PhemexRiskAccountStream {
    PhemexRiskAccountStream {
        risk_units: value
            .get("risk_units")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        risk_wallets: value
            .get("risk_wallets")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        raw: value.clone(),
    }
}

fn is_phemex_pong(value: &Value) -> bool {
    value
        .get("result")
        .and_then(Value::as_str)
        .is_some_and(|result| result.eq_ignore_ascii_case("pong"))
        || value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.eq_ignore_ascii_case("server.pong"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("pong"))
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(stream_parse_error(
            exchange_id.clone(),
            "empty phemex websocket message",
            &Value::Null,
        ));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid phemex websocket JSON: {error}"),
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
