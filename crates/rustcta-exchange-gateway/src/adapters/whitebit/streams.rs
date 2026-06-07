#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, MarketType,
    OrderSide, PositionSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_whitebit_symbol, parse_error, parse_orderbook_snapshot, validation_error,
    value_as_string,
};
use super::private_parser::{
    parse_balances, parse_order_state, parse_positions, parse_recent_fills,
};
use super::WhiteBitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

impl WhiteBitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        Ok(format!(
            "whitebit:{}:{}:{}",
            session.url,
            payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            payload
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
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
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let account_id = subscription.account_id.clone();
        let session = self.private_ws_session(subscription).await?;
        let payload = session
            .initial_requests()
            .into_iter()
            .last()
            .unwrap_or(Value::Null);
        Ok(format!(
            "whitebit:{}:{}:{}",
            session.url,
            payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            account_id
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<WhiteBitPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        WhiteBitPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub async fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<WhiteBitPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("whitebit.private_ws_session")?;
        let token = self.websocket_token().await?;
        WhiteBitPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            token,
            subscription,
        )
    }

    async fn websocket_token(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(
                "whitebit.websocket_token",
                "/api/v4/profile/websocket_token",
                &json!({}),
            )
            .await?;
        value
            .get("websocket_token")
            .or_else(|| value.get("token"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("whitebit websocket token response missing token: {value}"),
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhiteBitPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trades(Vec<WhiteBitPublicTrade>),
    Ticker(WhiteBitTicker),
    Candle(WhiteBitCandle),
    SubscriptionAck { id: Option<u64> },
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhiteBitPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhiteBitTicker {
    pub symbol: SymbolScope,
    pub bid_price: Option<String>,
    pub bid_quantity: Option<String>,
    pub ask_price: Option<String>,
    pub ask_quantity: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhiteBitCandle {
    pub symbol: SymbolScope,
    pub interval_seconds: Option<u64>,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhiteBitWsSessionEvent {
    Public(WhiteBitPublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct WhiteBitPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct WhiteBitPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    authorize_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl WhiteBitPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "whitebit public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = whitebit_public_subscribe_payload(&subscription, 1)?;
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

    pub fn heartbeat_request(&mut self, id: u64, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        whitebit_ws_ping_payload(id)
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
    ) -> ExchangeApiResult<Vec<WhiteBitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_whitebit_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, WhiteBitPublicStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![WhiteBitWsSessionEvent::Public(message.clone())];
        match message {
            WhiteBitPublicStreamMessage::OrderBook(snapshot) => {
                events.push(WhiteBitWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(self.exchange_id.clone(), None),
                        order_book: snapshot,
                    }),
                ]));
            }
            WhiteBitPublicStreamMessage::Pong => {
                events.push(WhiteBitWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::Heartbeat {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: self.exchange_id.clone(),
                        received_at: Utc::now(),
                    },
                ]));
            }
            _ => {}
        }
        Ok(events)
    }
}

impl WhiteBitPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        token: String,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "whitebit private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "whitebit private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let authorize_payload = whitebit_private_authorize_payload(&token, 1)?;
        let subscribe_payload = whitebit_private_subscribe_payload(&subscription, 2)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            symbol_hint: None,
            authorize_payload,
            subscribe_payload,
            state,
        })
    }

    pub fn with_symbol_hint(mut self, symbol_hint: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol_hint);
        self
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![
            self.authorize_payload.clone(),
            self.subscribe_payload.clone(),
        ]
    }

    pub fn heartbeat_request(&mut self, id: u64, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        whitebit_ws_ping_payload(id)
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
    ) -> ExchangeApiResult<Vec<WhiteBitWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let stream_events = parse_whitebit_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        if whitebit_ws_is_pong(&value) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![WhiteBitWsSessionEvent::Private(stream_events.clone())];
        if !stream_events.is_empty() {
            events.push(WhiteBitWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn whitebit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    let market = normalize_whitebit_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok(json!({
            "id": id,
            "method": "depth_subscribe",
            "params": [market, 100, "0", true],
        })),
        PublicStreamKind::Trades => Ok(json!({
            "id": id,
            "method": "trades_subscribe",
            "params": [market],
        })),
        PublicStreamKind::Ticker => Ok(json!({
            "id": id,
            "method": "bookTicker_subscribe",
            "params": [market],
        })),
        PublicStreamKind::Candles { interval } => Ok(json!({
            "id": id,
            "method": "candles_subscribe",
            "params": [market, normalize_candle_interval(interval)?],
        })),
    }
}

fn normalize_candle_interval(interval: &str) -> ExchangeApiResult<u64> {
    let seconds = match interval.trim() {
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1_800,
        "1h" => 3_600,
        "4h" => 14_400,
        "1d" => 86_400,
        other => {
            return Err(ExchangeApiError::Unsupported {
                operation: match other {
                    "" => "whitebit.candles_empty_interval",
                    _ => "whitebit.candles_interval",
                },
            });
        }
    };
    Ok(seconds)
}

pub fn whitebit_ws_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "ping",
        "params": [],
    })
}

pub fn whitebit_ws_server_time_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "server.time",
        "params": [],
    })
}

pub fn whitebit_private_authorize_payload(token: &str, id: u64) -> ExchangeApiResult<Value> {
    let token = token.trim();
    if token.is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.private_ws_missing_token",
        });
    }
    Ok(json!({
        "id": id,
        "method": "authorize",
        "params": [token, "public"],
    }))
}

pub fn whitebit_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "ordersPending_subscribe",
        PrivateStreamKind::Fills => "deals_subscribe",
        PrivateStreamKind::Balances => match subscription.market_type {
            Some(rustcta_types::MarketType::Perpetual) => "balanceMargin_subscribe",
            _ => "balanceSpot_subscribe",
        },
        PrivateStreamKind::Positions => "positionsMargin_subscribe",
        PrivateStreamKind::Account => "balanceSpot_subscribe",
    };
    let params = match subscription.kind {
        PrivateStreamKind::Fills => json!([[]]),
        _ => json!([]),
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params,
    }))
}

pub fn whitebit_ws_is_pong(value: &Value) -> bool {
    value.get("result").and_then(Value::as_str) == Some("pong")
        || value.get("method").and_then(Value::as_str) == Some("pong")
}

pub fn parse_whitebit_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WhiteBitPublicStreamMessage> {
    if whitebit_ws_is_pong(value) {
        return Ok(WhiteBitPublicStreamMessage::Pong);
    }
    if is_subscription_ack(value) {
        return Ok(WhiteBitPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_u64),
        });
    }
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match method {
        "depth_update" | "depth" => Ok(WhiteBitPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol_hint, &normalize_depth_message(value))?,
        )),
        "trades_update" | "trades" => Ok(WhiteBitPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        "bookTicker_update" | "bookTicker" => Ok(WhiteBitPublicStreamMessage::Ticker(
            parse_public_ticker(exchange_id, symbol_hint, value)?,
        )),
        "candles_update" | "candles" => Ok(WhiteBitPublicStreamMessage::Candle(
            parse_public_candle(exchange_id, symbol_hint, value)?,
        )),
        _ => Ok(WhiteBitPublicStreamMessage::Raw(value.clone())),
    }
}

pub fn parse_whitebit_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if whitebit_ws_is_pong(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_subscription_ack(value) {
        return Ok(Vec::new());
    }
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match method {
        "ordersPending_update" | "ordersExecuted_update" | "orders_update" => {
            parse_private_order_events(exchange_id, tenant_id, account_id, symbol_hint, value)
        }
        "deals_update" | "deals" => {
            Ok(
                parse_private_fills(exchange_id, tenant_id, account_id, symbol_hint, value)?
                    .into_iter()
                    .map(ExchangeStreamEvent::Fill)
                    .collect(),
            )
        }
        "balanceSpot_update" | "balanceMargin_update" | "balances_update" => {
            let balances =
                parse_private_balances(exchange_id, tenant_id, account_id, method, value)?;
            Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
                BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances,
                },
            )])
        }
        "positionsMargin_update" | "positions_update" => {
            let positions = parse_positions(
                exchange_id,
                tenant_id,
                account_id,
                symbol_hint.as_ref().map(|symbol| &symbol.exchange_symbol),
                &json!({ "data": private_items(value) }),
            )?;
            Ok(vec![ExchangeStreamEvent::PositionSnapshot(
                rustcta_exchange_api::PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions,
                },
            )])
        }
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported whitebit private stream message",
            value,
        )),
    }
}

fn parse_private_order_events(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let mut events = Vec::new();
    for item in private_items(value) {
        let order = parse_order_state(exchange_id, symbol_hint.as_ref(), &item)?;
        let filled_quantity = order.filled_quantity.parse::<f64>().unwrap_or(0.0);
        events.push(ExchangeStreamEvent::OrderUpdate(order));
        if filled_quantity > 0.0 {
            if let Ok(fills) = parse_recent_fills(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                symbol_hint
                    .as_ref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "whitebit private order fill requires symbol_hint".to_string(),
                    })?,
                &json!({ "data": [item] }),
            ) {
                events.extend(fills.into_iter().map(ExchangeStreamEvent::Fill));
            }
        }
    }
    Ok(events)
}

fn parse_private_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let symbol = symbol_hint.ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "whitebit private fills stream requires symbol_hint".to_string(),
    })?;
    parse_recent_fills(
        exchange_id,
        tenant_id,
        account_id,
        &symbol,
        &json!({ "data": private_items(value) }),
    )
}

fn parse_private_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    method: &str,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = private_items(value);
    let market_type = if method.contains("Margin") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    };
    let normalized = json!({ "data": items });
    match parse_balances(
        exchange_id,
        tenant_id.clone(),
        account_id.clone(),
        market_type,
        &[],
        &normalized,
    ) {
        Ok(balances) => Ok(balances),
        Err(_) => {
            let mut assets = Vec::new();
            for item in private_items(value) {
                let asset =
                    text_from_fields(&item, &["ticker", "ccy", "asset", "coin", "currency"])
                        .unwrap_or_else(|| "UNKNOWN".to_string())
                        .to_ascii_uppercase();
                let available =
                    number_from_fields(&item, &["available", "available_balance", "balance"])
                        .unwrap_or(0.0);
                let locked =
                    number_from_fields(&item, &["freeze", "frozen", "locked"]).unwrap_or(0.0);
                let total = number_from_fields(&item, &["total"]).unwrap_or(available + locked);
                assets.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
            Ok(vec![ExchangeBalance {
                schema_version: SchemaVersion::current(),
                tenant_id,
                account_id,
                exchange_id: exchange_id.clone(),
                market_type,
                balances: assets,
                observed_at: Utc::now(),
            }])
        }
    }
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<WhiteBitPublicTrade>> {
    private_items(value)
        .into_iter()
        .map(|item| {
            Ok(WhiteBitPublicTrade {
                symbol: symbol_from_item(exchange_id, &item).unwrap_or_else(|| symbol_hint.clone()),
                trade_id: value_as_string(item.get("id").or_else(|| item.get("tradeId"))),
                side: parse_side(item.get("side").and_then(Value::as_str).unwrap_or("buy"))?,
                price: text_from_fields(&item, &["price"]).unwrap_or_else(|| "0".to_string()),
                quantity: text_from_fields(&item, &["amount", "quantity", "volume"])
                    .unwrap_or_else(|| "0".to_string()),
                traded_at: timestamp_from_fields(&item, &["time", "timestamp", "created_at"])
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn parse_public_ticker(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WhiteBitTicker> {
    let data = value
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.first())
        .unwrap_or(value);
    Ok(WhiteBitTicker {
        symbol: symbol_from_item(exchange_id, data).unwrap_or(symbol_hint),
        bid_price: text_from_fields(data, &["bid", "bidPrice", "best_bid"]),
        bid_quantity: text_from_fields(data, &["bid_amount", "bidQty", "bidQuantity"]),
        ask_price: text_from_fields(data, &["ask", "askPrice", "best_ask"]),
        ask_quantity: text_from_fields(data, &["ask_amount", "askQty", "askQuantity"]),
        updated_at: timestamp_from_fields(data, &["timestamp", "time"]).unwrap_or_else(Utc::now),
    })
}

fn parse_public_candle(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WhiteBitCandle> {
    let data = value
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.first())
        .unwrap_or(value);
    let array = data.as_array();
    let opened_at = array
        .and_then(|items| items.first())
        .and_then(value_as_f64)
        .and_then(timestamp_to_utc)
        .or_else(|| timestamp_from_fields(data, &["time", "timestamp"]))
        .unwrap_or_else(Utc::now);
    Ok(WhiteBitCandle {
        symbol: symbol_from_item(exchange_id, data).unwrap_or(symbol_hint),
        interval_seconds: value
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.get(2))
            .and_then(Value::as_u64),
        opened_at,
        open: array_text(array, 1)
            .or_else(|| text_from_fields(data, &["open"]))
            .unwrap_or_else(|| "0".to_string()),
        high: array_text(array, 2)
            .or_else(|| text_from_fields(data, &["high"]))
            .unwrap_or_else(|| "0".to_string()),
        low: array_text(array, 3)
            .or_else(|| text_from_fields(data, &["low"]))
            .unwrap_or_else(|| "0".to_string()),
        close: array_text(array, 4)
            .or_else(|| text_from_fields(data, &["close"]))
            .unwrap_or_else(|| "0".to_string()),
        volume: array_text(array, 5)
            .or_else(|| text_from_fields(data, &["volume"]))
            .unwrap_or_else(|| "0".to_string()),
    })
}

fn normalize_depth_message(value: &Value) -> Value {
    let params = value.get("params").and_then(Value::as_array);
    let book = params.and_then(|items| items.first()).unwrap_or(value);
    json!({
        "data": {
            "depth": {
                "bids": book.get("bids").cloned().unwrap_or_else(|| json!([])),
                "asks": book.get("asks").cloned().unwrap_or_else(|| json!([])),
            },
            "last": book.get("last").or_else(|| book.get("sequence")).cloned().unwrap_or(Value::Null),
            "timestamp": book.get("timestamp").or_else(|| book.get("time")).cloned().unwrap_or(Value::Null),
        }
    })
}

fn private_items(value: &Value) -> Vec<Value> {
    let data = value
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.first())
        .or_else(|| value.get("data"))
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    match data {
        Value::Array(items) => items.clone(),
        Value::Object(_) => vec![data.clone()],
        _ => Vec::new(),
    }
}

fn symbol_from_item(exchange_id: &ExchangeId, value: &Value) -> Option<SymbolScope> {
    let market = text_from_fields(value, &["market", "symbol"])?;
    let market = normalize_whitebit_symbol(&market).ok()?;
    let market_type = if market.ends_with("_PERP") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    };
    let (base, quote) = if market_type == MarketType::Perpetual {
        (
            market.trim_end_matches("_PERP").to_string(),
            "USDT".to_string(),
        )
    } else {
        market
            .split_once('_')
            .map(|(base, quote)| (base.to_string(), quote.to_string()))
            .unwrap_or_else(|| (market.clone(), "USDT".to_string()))
    };
    Some(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: CanonicalSymbol::new(base, quote).ok(),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, market).ok()?,
    })
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported whitebit stream side {other}"),
        }),
    }
}

fn parse_position_side(value: Option<&str>, market_type: MarketType) -> PositionSide {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" | "buy" => PositionSide::Long,
        "short" | "sell" => PositionSide::Short,
        "both" | "net" if market_type == MarketType::Perpetual => PositionSide::Net,
        _ if market_type == MarketType::Spot => PositionSide::None,
        _ => PositionSide::Net,
    }
}

fn is_subscription_ack(value: &Value) -> bool {
    value.get("error").is_some_and(Value::is_null)
        || value.get("result").and_then(Value::as_str) == Some("success")
        || value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.ends_with("_subscribe"))
            && value.get("result").and_then(Value::as_bool) == Some(true)
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        parse_error(
            exchange_id.clone(),
            &format!("invalid whitebit websocket json: {error}"),
            &Value::String(text.to_string()),
        )
    })
}

fn text_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value_as_string(value.get(*field)))
}

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_f64))
}

fn array_text(array: Option<&Vec<Value>>, index: usize) -> Option<String> {
    array
        .and_then(|items| items.get(index))
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_f64)
            .and_then(timestamp_to_utc)
    })
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_str()?.parse().ok())
}

fn timestamp_to_utc(timestamp: f64) -> Option<DateTime<Utc>> {
    if timestamp > 10_000_000_000.0 {
        DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
    } else {
        let seconds = timestamp.trunc() as i64;
        let nanos = ((timestamp.fract()) * 1_000_000_000.0) as u32;
        DateTime::<Utc>::from_timestamp(seconds, nanos)
    }
}
