#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, Fill, MarketType, OrderBookSnapshot, OrderSide};
use serde_json::{json, Value};

use super::parser::{normalize_bitmex_symbol, parse_orderbook_snapshot};
use super::private_parser::{
    parse_margin_balances, parse_order_state, parse_positions, parse_recent_fills,
};
use super::signing::bitmex_ws_auth_signature;
use super::BitmexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

impl BitmexGatewayAdapter {
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
            "bitmex:{}:{}",
            session.url,
            payload
                .get("args")
                .and_then(Value::as_array)
                .and_then(|args| args.first())
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
        self.ensure_optional_supported_market_type(subscription.market_type)?;
        let session = self.private_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .last()
            .unwrap_or(Value::Null);
        Ok(format!(
            "bitmex:{}:{}",
            session.url,
            payload
                .get("args")
                .and_then(Value::as_array)
                .and_then(|args| args.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<BitmexPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        BitmexPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<BitmexPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_optional_supported_market_type(subscription.market_type)?;
        self.ensure_private_rest("bitmex.private_ws_session")?;
        let credentials = self
            .config
            .api_key
            .as_deref()
            .zip(self.config.api_secret.as_deref())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitmex.private_ws_session",
            })?;
        BitmexPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            credentials.0.to_string(),
            credentials.1.to_string(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum BitmexPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    Trades(Vec<BitmexPublicTrade>),
    SubscriptionAck,
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum BitmexPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Fills(Vec<Fill>),
    Balance(rustcta_exchange_api::BalancesResponse),
    Position(rustcta_exchange_api::PositionsResponse),
    SubscriptionAck,
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct BitmexPublicTrade {
    pub symbol: SymbolScope,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub traded_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum BitmexWsSessionEvent {
    Public(BitmexPublicStreamMessage),
    Private(BitmexPrivateStreamMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BitmexPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BitmexPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    auth_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl BitmexPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "bitmex public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = bitmex_public_subscribe_payload(&subscription)?;
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
        bitmex_ping_payload()
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
    ) -> ExchangeApiResult<Vec<BitmexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_bitmex_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, BitmexPublicStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![BitmexWsSessionEvent::Public(message.clone())];
        if let BitmexPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(BitmexWsSessionEvent::Stream(vec![
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

impl BitmexPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        api_key: String,
        api_secret: String,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "bitmex private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitmex private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        let expires = Utc::now().timestamp() + 60;
        let auth_payload = bitmex_private_auth_payload(&api_key, &api_secret, expires);
        let subscribe_payload = bitmex_private_subscribe_payload(&subscription)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
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
        bitmex_ping_payload()
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
    ) -> ExchangeApiResult<Vec<BitmexWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message = parse_bitmex_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, BitmexPrivateStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![BitmexWsSessionEvent::Private(message.clone())];
        let stream_events = private_message_stream_events(&self.exchange_id, message);
        if !stream_events.is_empty() {
            events.push(BitmexWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn bitmex_private_stream_capabilities() -> PrivateStreamCapabilities {
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

#[allow(dead_code)]
pub fn parse_bitmex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BitmexPublicStreamMessage> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(BitmexPublicStreamMessage::SubscriptionAck);
    }
    if is_bitmex_heartbeat(value) {
        return Ok(BitmexPublicStreamMessage::Pong);
    }
    match value.get("table").and_then(Value::as_str) {
        Some("orderBookL2") => {
            let data = value
                .get("data")
                .cloned()
                .unwrap_or(Value::Array(Vec::new()));
            Ok(BitmexPublicStreamMessage::OrderBook(
                parse_orderbook_snapshot(exchange_id, symbol, &data)?,
            ))
        }
        Some("trade") => Ok(BitmexPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol,
            value,
        )?)),
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported BitMEX public stream message",
            value,
        )),
    }
}

#[allow(dead_code)]
pub fn parse_bitmex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BitmexPrivateStreamMessage> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(BitmexPrivateStreamMessage::SubscriptionAck);
    }
    if is_bitmex_heartbeat(value) {
        return Ok(BitmexPrivateStreamMessage::Pong);
    }
    let data = value
        .get("data")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    match value.get("table").and_then(Value::as_str) {
        Some("order") => Ok(BitmexPrivateStreamMessage::Order(parse_order_state(
            exchange_id,
            symbol.as_ref(),
            &data,
        )?)),
        Some("execution") => {
            let symbol = symbol.ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitmex execution stream parser requires symbol hint".to_string(),
            })?;
            Ok(BitmexPrivateStreamMessage::Fills(parse_recent_fills(
                exchange_id,
                tenant_id,
                account_id,
                &symbol,
                &data,
            )?))
        }
        Some("margin") | Some("wallet") => {
            let balances = parse_margin_balances(exchange_id, tenant_id, account_id, &[], &data)?;
            Ok(BitmexPrivateStreamMessage::Balance(
                rustcta_exchange_api::BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: rustcta_exchange_api::ResponseMetadata::new(
                        exchange_id.clone(),
                        Utc::now(),
                    ),
                    balances,
                },
            ))
        }
        Some("position") => {
            let positions = parse_positions(exchange_id, tenant_id, account_id, &data)?;
            Ok(BitmexPrivateStreamMessage::Position(
                rustcta_exchange_api::PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: rustcta_exchange_api::ResponseMetadata::new(
                        exchange_id.clone(),
                        Utc::now(),
                    ),
                    positions,
                },
            ))
        }
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported BitMEX private stream message",
            value,
        )),
    }
}

pub fn bitmex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_bitmex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("orderBookL2:{symbol}")
        }
        PublicStreamKind::Trades => format!("trade:{symbol}"),
        PublicStreamKind::Ticker => format!("quote:{symbol}"),
        PublicStreamKind::Candles { interval } => {
            format!("tradeBin{}:{symbol}", normalize_interval(interval)?)
        }
    };
    Ok(json!({
        "op": "subscribe",
        "args": [channel],
    }))
}

pub fn bitmex_private_auth_payload(api_key: &str, api_secret: &str, expires: i64) -> Value {
    json!({
        "op": "authKeyExpires",
        "args": [
            api_key,
            expires,
            bitmex_ws_auth_signature(api_secret, expires)
        ],
    })
}

pub fn bitmex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "order",
        PrivateStreamKind::Fills => "execution",
        PrivateStreamKind::Balances => "margin",
        PrivateStreamKind::Positions => "position",
        PrivateStreamKind::Account => "wallet",
    };
    if subscription.market_type == Some(MarketType::Spot)
        && matches!(subscription.kind, PrivateStreamKind::Positions)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitmex.spot_private_positions_stream",
        });
    }
    Ok(json!({
        "op": "subscribe",
        "args": [channel],
    }))
}

pub fn bitmex_ping_payload() -> Value {
    json!({ "op": "ping" })
}

pub fn bitmex_stream_reconnect_policy() -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: 30_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 30_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn is_bitmex_heartbeat(value: &Value) -> bool {
    value.get("pong").is_some()
        || value
            .get("op")
            .and_then(Value::as_str)
            .is_some_and(|op| op.eq_ignore_ascii_case("pong"))
        || value
            .get("info")
            .and_then(Value::as_str)
            .is_some_and(|info| info.to_ascii_lowercase().contains("welcome"))
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "1min" => Ok("1m".to_string()),
        "5m" | "5min" => Ok("5m".to_string()),
        "1h" | "60m" => Ok("1h".to_string()),
        "1d" | "1day" => Ok("1d".to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitmex.candle_interval",
        }),
    }
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<BitmexPublicTrade>> {
    let trades = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        stream_parse_error(
            exchange_id.clone(),
            "BitMEX trade stream missing data",
            value,
        )
    })?;
    trades
        .iter()
        .map(|trade| {
            let side = match trade
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("Buy")
                .to_ascii_lowercase()
                .as_str()
            {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                other => {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: format!("unsupported BitMEX trade side {other}"),
                    });
                }
            };
            Ok(BitmexPublicTrade {
                symbol: symbol.clone(),
                side,
                price: number_from_value(trade.get("price")).ok_or_else(|| {
                    stream_parse_error(exchange_id.clone(), "BitMEX trade missing price", trade)
                })?,
                quantity: number_from_value(trade.get("size")).ok_or_else(|| {
                    stream_parse_error(exchange_id.clone(), "BitMEX trade missing size", trade)
                })?,
                traded_at: trade
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                    .map(|timestamp| timestamp.with_timezone(&Utc))
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn private_message_stream_events(
    exchange_id: &ExchangeId,
    message: BitmexPrivateStreamMessage,
) -> Vec<ExchangeStreamEvent> {
    match message {
        BitmexPrivateStreamMessage::Order(order) => vec![ExchangeStreamEvent::OrderUpdate(order)],
        BitmexPrivateStreamMessage::Fills(fills) => fills
            .into_iter()
            .map(ExchangeStreamEvent::Fill)
            .collect::<Vec<_>>(),
        BitmexPrivateStreamMessage::Balance(response) => {
            vec![ExchangeStreamEvent::BalanceSnapshot(response)]
        }
        BitmexPrivateStreamMessage::Position(response) => {
            vec![ExchangeStreamEvent::PositionSnapshot(response)]
        }
        BitmexPrivateStreamMessage::Pong => vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }],
        BitmexPrivateStreamMessage::SubscriptionAck => Vec::new(),
    }
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid BitMEX websocket JSON: {error}"),
            &Value::String(text.to_string()),
        )
    })
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
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
