#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId, MarketType,
    SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_deribit_symbol, parse_error, parse_orderbook_snapshot, symbol_scope_from_instrument,
    value_as_u64,
};
use super::private_parser::{parse_order_state, parse_positions};
use super::DeribitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

#[derive(Debug, Clone, PartialEq)]
pub enum DeribitWsSessionEvent {
    Public(DeribitPublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeribitPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    SubscriptionAck { channels: Vec<String> },
    Heartbeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeribitBookStreamOptions {
    pub group: &'static str,
    pub depth: u32,
    pub interval: &'static str,
}

impl Default for DeribitBookStreamOptions {
    fn default() -> Self {
        Self {
            group: "none",
            depth: 20,
            interval: "100ms",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeribitBookChangeIds {
    pub change_id: u64,
    pub prev_change_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DeribitPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    last_change_id: Option<u64>,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct DeribitPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    auth_payload: Value,
    subscribe_payload: Value,
    symbol_hint: Option<SymbolScope>,
    state: StreamRuntimeState,
}

impl DeribitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_stream {
            return Err(ExchangeApiError::Unsupported {
                operation: "deribit.subscribe_public_stream",
            });
        }
        let session = self.public_ws_session(subscription)?;
        Ok(format!("deribit:{}:public", session.url))
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
        if !self.config.private_stream_available() {
            return self.unsupported_private("deribit.subscribe_private_stream");
        }
        let session = self.private_ws_session(subscription)?;
        Ok(format!(
            "deribit:{}:private:{}",
            session.url, session.account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<DeribitPublicWsSession> {
        DeribitPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<DeribitPrivateWsSession> {
        let client_id = self
            .config
            .client_id
            .clone()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "deribit.private_ws_session",
            })?;
        let client_secret =
            self.config
                .client_secret
                .clone()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "deribit.private_ws_session",
                })?;
        DeribitPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            client_id,
            client_secret,
            subscription,
        )
    }
}

impl DeribitPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = deribit_public_subscribe_payload(&subscription, 1)?;
        let mut state =
            StreamRuntimeState::new(exchange_id.clone(), subscription.symbol.market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            symbol: subscription.symbol,
            subscribe_payload,
            last_change_id: None,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        deribit_heartbeat_payload(now.timestamp_millis() as u64)
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) {
        self.last_change_id = None;
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
    ) -> ExchangeApiResult<Vec<DeribitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_deribit_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, DeribitPublicStreamMessage::Heartbeat) {
            self.state.on_pong(Utc::now());
        }
        if matches!(message, DeribitPublicStreamMessage::OrderBook(_)) {
            self.apply_order_book_change_ids(&value);
        }
        let mut events = vec![DeribitWsSessionEvent::Public(message.clone())];
        match message {
            DeribitPublicStreamMessage::OrderBook(snapshot) => {
                events.push(DeribitWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(self.exchange_id.clone(), None),
                        order_book: snapshot,
                    }),
                ]));
            }
            DeribitPublicStreamMessage::Heartbeat => {
                events.push(DeribitWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: Utc::now(),
                    },
                ]));
            }
            DeribitPublicStreamMessage::SubscriptionAck { .. } => {}
        }
        Ok(events)
    }

    fn apply_order_book_change_ids(&mut self, value: &Value) {
        if let Some(change_ids) = deribit_book_change_ids(value) {
            if !deribit_change_id_is_contiguous(self.last_change_id, change_ids) {
                self.state.on_sequence_gap();
            }
            self.last_change_id = Some(change_ids.change_id);
        }
    }
}

impl DeribitPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        client_id: String,
        client_secret: String,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "deribit private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "deribit private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Futures);
        let auth_payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        });
        let subscribe_payload = deribit_private_subscribe_payload(&subscription, 2)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            auth_payload,
            subscribe_payload,
            symbol_hint: None,
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
        deribit_heartbeat_payload(now.timestamp_millis() as u64)
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
    ) -> ExchangeApiResult<Vec<DeribitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let stream_events = parse_deribit_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        if is_heartbeat(&value) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![DeribitWsSessionEvent::Private(stream_events.clone())];
        if !stream_events.is_empty() {
            events.push(DeribitWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn deribit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/subscribe",
        "params": { "channels": [deribit_public_channel(subscription)?] }
    }))
}

pub fn deribit_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "private/subscribe",
        "params": { "channels": [deribit_private_channel(&subscription.kind, subscription.market_type)] }
    }))
}

pub fn deribit_heartbeat_payload(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/test",
        "params": {}
    })
}

pub fn parse_deribit_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<DeribitPublicStreamMessage> {
    if is_heartbeat(value) {
        return Ok(DeribitPublicStreamMessage::Heartbeat);
    }
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(DeribitPublicStreamMessage::SubscriptionAck {
            channels: Vec::new(),
        });
    }
    let params = value.get("params").unwrap_or(value);
    let channel = params
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if channel.starts_with("book.") {
        let data = params.get("data").unwrap_or(params);
        return Ok(DeribitPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol_hint, data)?,
        ));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported deribit public stream message",
        value,
    ))
}

pub fn parse_deribit_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_heartbeat(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(Vec::new());
    }
    let params = value.get("params").unwrap_or(value);
    let channel = params
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = params.get("data").unwrap_or(params);
    if channel.starts_with("user.orders.") {
        let rows = data
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(std::slice::from_ref(data));
        return rows
            .iter()
            .map(|row| {
                parse_order_state(exchange_id, symbol_hint.as_ref(), row)
                    .map(ExchangeStreamEvent::OrderUpdate)
            })
            .collect();
    }
    if channel.starts_with("user.trades.") {
        return parse_trade_events(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint.as_ref(),
            data,
        );
    }
    if channel.starts_with("user.portfolio.") {
        return parse_portfolio_event(exchange_id, tenant_id, account_id, data);
    }
    if channel.starts_with("user.changes.") {
        return parse_user_changes(exchange_id, tenant_id, account_id, symbol_hint, data);
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported deribit private stream message",
        value,
    ))
}

fn deribit_public_channel(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let symbol = normalize_deribit_symbol(&subscription.symbol)?;
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            deribit_public_order_book_channel_for_symbol(
                &symbol,
                DeribitBookStreamOptions::default(),
            )
        }
        PublicStreamKind::Trades => Ok(format!("trades.{symbol}.100ms")),
        PublicStreamKind::Ticker => Ok(format!("ticker.{symbol}.100ms")),
        PublicStreamKind::Candles { interval } => Ok(format!(
            "chart.trades.{symbol}.{}",
            normalize_interval(interval)
        )),
    }
}

pub fn deribit_public_order_book_channel(
    subscription: &PublicStreamSubscription,
    options: DeribitBookStreamOptions,
) -> ExchangeApiResult<String> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let symbol = normalize_deribit_symbol(&subscription.symbol)?;
    deribit_public_order_book_channel_for_symbol(&symbol, options)
}

fn deribit_public_order_book_channel_for_symbol(
    symbol: &str,
    options: DeribitBookStreamOptions,
) -> ExchangeApiResult<String> {
    validate_deribit_book_stream_options(options)?;
    Ok(format!(
        "book.{symbol}.{}.{}.{}",
        options.group, options.depth, options.interval
    ))
}

fn validate_deribit_book_stream_options(
    options: DeribitBookStreamOptions,
) -> ExchangeApiResult<()> {
    if !matches!(
        options.group,
        "none" | "1" | "2" | "5" | "10" | "25" | "100" | "250"
    ) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported deribit book group `{}`", options.group),
        });
    }
    if !matches!(options.depth, 1 | 10 | 20) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported deribit book depth `{}`", options.depth),
        });
    }
    if !matches!(options.interval, "raw" | "100ms" | "agg2") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported deribit book interval `{}`", options.interval),
        });
    }
    Ok(())
}

pub fn deribit_book_change_ids(value: &Value) -> Option<DeribitBookChangeIds> {
    let params = value.get("params").unwrap_or(value);
    let channel = params.get("channel").and_then(Value::as_str)?;
    if !channel.starts_with("book.") {
        return None;
    }
    let data = params.get("data").unwrap_or(params);
    Some(DeribitBookChangeIds {
        change_id: value_as_u64(data.get("change_id")?)?,
        prev_change_id: data.get("prev_change_id").and_then(value_as_u64),
    })
}

pub fn deribit_change_id_is_contiguous(
    previous_change_id: Option<u64>,
    current: DeribitBookChangeIds,
) -> bool {
    match (previous_change_id, current.prev_change_id) {
        (Some(previous), Some(prev_change_id)) => previous == prev_change_id,
        _ => true,
    }
}

fn deribit_private_channel(kind: &PrivateStreamKind, market_type: Option<MarketType>) -> String {
    let instrument_scope = match market_type {
        Some(MarketType::Option) => "option",
        _ => "future",
    };
    match kind {
        PrivateStreamKind::Orders => format!("user.orders.{instrument_scope}.raw"),
        PrivateStreamKind::Fills => format!("user.trades.{instrument_scope}.raw"),
        PrivateStreamKind::Positions => format!("user.changes.{instrument_scope}.raw"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => {
            "user.portfolio.any".to_string()
        }
    }
}

fn parse_trade_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let rows = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(std::slice::from_ref(data));
    rows.iter()
        .map(|row| {
            super::private_parser::parse_fills(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                symbol_hint,
                &json!({ "result": { "trades": [row.clone()] } }),
            )
            .and_then(|mut fills| {
                fills.pop().map(ExchangeStreamEvent::Fill).ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "deribit trade event did not parse fill",
                        row,
                    )
                })
            })
        })
        .collect()
}

fn parse_user_changes(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let mut events = Vec::new();
    if let Some(orders) = data.get("orders").and_then(Value::as_array) {
        for order in orders {
            events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                symbol_hint.as_ref(),
                order,
            )?));
        }
    }
    if let Some(trades) = data.get("trades").and_then(Value::as_array) {
        events.extend(parse_trade_events(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            symbol_hint.as_ref(),
            &Value::Array(trades.clone()),
        )?);
    }
    if let Some(positions) = data.get("positions") {
        let positions = parse_positions(
            exchange_id,
            tenant_id,
            account_id,
            &json!({ "result": positions.clone() }),
        )?;
        events.push(ExchangeStreamEvent::PositionSnapshot(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions,
            },
        ));
    }
    Ok(events)
}

fn parse_portfolio_event(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let asset = data
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or("USD")
        .to_ascii_uppercase();
    let total = number_value(data.get("equity").or_else(|| data.get("balance"))).unwrap_or(0.0);
    let available = number_value(data.get("available_funds")).unwrap_or(total);
    let balance = ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Futures,
        balances: vec![
            AssetBalance::new(asset, total, available, (total - available).max(0.0))
                .map_err(super::parser::validation_error)?,
        ],
        observed_at: Utc::now(),
    };
    Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
        BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances: vec![balance],
        },
    )])
}

fn normalize_interval(interval: &str) -> String {
    match interval {
        "1m" => "1".to_string(),
        "3m" => "3".to_string(),
        "5m" => "5".to_string(),
        "15m" => "15".to_string(),
        "30m" => "30".to_string(),
        "1h" => "60".to_string(),
        "1d" => "1D".to_string(),
        other => other.to_string(),
    }
}

fn number_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn is_heartbeat(value: &Value) -> bool {
    value.get("method").and_then(Value::as_str) == Some("heartbeat")
        || value.get("testnet").is_some()
        || value.get("result").and_then(Value::as_str) == Some("ok")
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let value = serde_json::from_str::<Value>(text).map_err(|error| {
        ExchangeApiError::Exchange(ExchangeError {
            schema_version: SchemaVersion::current(),
            exchange_id: exchange_id.clone(),
            class: ExchangeErrorClass::Decode,
            code: None,
            message: error.to_string(),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: Some(Value::String(text.to_string())),
            occurred_at: Utc::now(),
        })
    })?;
    if let Some(error) = value.get("error") {
        return Err(parse_error(
            exchange_id.clone(),
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("deribit websocket error"),
            &value,
        ));
    }
    Ok(value)
}

pub fn symbol_from_stream_instrument(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let instrument = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit stream missing instrument_name",
                value,
            )
        })?;
    symbol_scope_from_instrument(exchange_id, instrument)
}
