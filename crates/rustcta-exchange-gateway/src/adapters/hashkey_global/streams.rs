#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PositionsResponse, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus,
    LiquidityRole, MarketType, OrderBookSnapshot, OrderSide, PositionSide, SchemaVersion,
};
use serde_json::{json, Map, Value};

use super::parser::{
    normalize_hashkey_global_symbol, parse_error, parse_orderbook_snapshot,
    split_hashkey_global_symbol, validation_error,
};
use super::private_parser::{parse_order_state, parse_positions};
use super::HashKeyGlobalGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

#[derive(Debug, Clone, PartialEq)]
pub enum HashKeyGlobalPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    Trades(Vec<HashKeyGlobalPublicTrade>),
    SubscriptionAck { id: Option<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HashKeyGlobalPrivateStreamMessage {
    Events(Vec<ExchangeStreamEvent>),
    SubscriptionAck { channel: Option<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HashKeyGlobalWsSessionEvent {
    Public(HashKeyGlobalPublicStreamMessage),
    Private(HashKeyGlobalPrivateStreamMessage),
    Stream(ExchangeStreamEvent),
    HeartbeatResponse(Value),
}

#[derive(Debug, Clone)]
pub struct HashKeyGlobalPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct HashKeyGlobalPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HashKeyGlobalPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

impl HashKeyGlobalGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let payload = hashkey_global_public_subscribe_payload(&subscription, "1")?;
        Ok(format!(
            "hashkey_global:{}:{}",
            self.config.spot_public_ws_url,
            payload
                .get("params")
                .and_then(|params| params.get("channel"))
                .and_then(Value::as_str)
                .or_else(|| {
                    payload
                        .get("params")
                        .and_then(Value::as_array)
                        .and_then(|items| items.first())
                        .and_then(Value::as_str)
                })
                .unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let channel = hashkey_global_private_channel(&subscription.kind, market_type)?;
        let url = match market_type {
            MarketType::Spot => self.config.spot_private_ws_url.clone(),
            MarketType::Perpetual => self.config.futures_private_ws_url.clone(),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        Ok(format!(
            "hashkey_global:{url}:{channel}:{}",
            subscription.account_id
        ))
    }
}

pub fn hashkey_global_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: &str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let symbol = hashkey_global_ws_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    if subscription.symbol.market_type == MarketType::Spot {
        let channel = match &subscription.kind {
            PublicStreamKind::Trades => format!("market_{symbol}_trade_ticker"),
            PublicStreamKind::Ticker => format!("market_{symbol}_ticker"),
            PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
                format!("market_{symbol}_simple_depth_step0")
            }
            PublicStreamKind::Candles { interval } => format!("market_{symbol}_kline_{interval}"),
        };
        return Ok(json!({
            "event": "sub",
            "params": {
                "channel": channel,
                "cb_id": id,
            }
        }));
    }
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("{symbol}@depth")
        }
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    };
    Ok(json!({
        "method": "sub",
        "params": [channel],
        "id": id,
    }))
}

pub fn hashkey_global_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let channel = hashkey_global_private_channel(&subscription.kind, market_type)?;
    Ok(json!({
        "event": "sub",
        "params": {
            "channel": channel,
        }
    }))
}

pub fn hashkey_global_private_listen_key_url(
    base_url: &str,
    listen_key: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let listen_key = listen_key.trim();
    if listen_key.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "hashkey_global private stream requires listenKey".to_string(),
        });
    }
    let suffix = match market_type {
        MarketType::Spot => format!("?listenKey={listen_key}"),
        MarketType::Perpetual => format!("?streams={listen_key}"),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "hashkey_global.private_stream_market_type",
            });
        }
    };
    Ok(format!("{}{}", base_url.trim_end_matches('/'), suffix))
}

pub fn hashkey_global_ws_pong_payload(ts: Option<&str>) -> Value {
    match ts {
        Some(ts) => json!({"event": "pong", "ts": ts, "pong": ts}),
        None => json!("pong"),
    }
}

pub fn parse_hashkey_global_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<HashKeyGlobalPublicStreamMessage> {
    if value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong") || text.eq_ignore_ascii_case("ping"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("pong"))
    {
        return Ok(HashKeyGlobalPublicStreamMessage::Pong);
    }
    if value.get("ping").is_some() || value.get("pong").is_some() {
        return Ok(HashKeyGlobalPublicStreamMessage::Pong);
    }
    if value.get("result").is_some() || value.get("id").is_some() && value.get("data").is_none() {
        return Ok(HashKeyGlobalPublicStreamMessage::SubscriptionAck {
            id: value_to_string(value.get("id")),
        });
    }
    if value
        .get("event_rep")
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("subed"))
        || value
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("ok"))
            && value.get("channel").is_some()
    {
        return Ok(HashKeyGlobalPublicStreamMessage::SubscriptionAck {
            id: value_to_string(value.get("channel").or_else(|| value.get("id"))),
        });
    }
    let normalized_public;
    let data = if let Some(tick) = value.get("tick") {
        normalized_public = normalize_public_tick(tick, value);
        &normalized_public
    } else {
        stream_data(value)
    };
    let channel = value
        .get("stream")
        .or_else(|| value.get("channel"))
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if data.get("bids").is_some()
        || data.get("asks").is_some()
        || channel.contains("depth")
        || channel.contains("book")
    {
        return Ok(HashKeyGlobalPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol_hint, data)?,
        ));
    }
    if data.get("price").is_some()
        || data.get("p").is_some()
        || channel.contains("trade")
        || channel.contains("aggtrade")
    {
        return Ok(HashKeyGlobalPublicStreamMessage::Trades(vec![
            parse_public_trade(exchange_id, symbol_hint, data)?,
        ]));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported hashkey_global public stream message",
        value,
    ))
}

pub fn parse_hashkey_global_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<HashKeyGlobalPrivateStreamMessage> {
    if value
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("ping"))
        || value.get("ping").is_some()
    {
        return Ok(HashKeyGlobalPrivateStreamMessage::Pong);
    }
    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("ok"))
    {
        return Ok(HashKeyGlobalPrivateStreamMessage::SubscriptionAck {
            channel: value_to_string(value.get("channel")),
        });
    }
    let event = value
        .get("e")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let data = stream_data(value);
    if event.contains("order_trade_update") {
        let order_data = value.get("o").or_else(|| data.get("o")).unwrap_or(data);
        let normalized = normalize_order_event(order_data);
        let mut events = vec![ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            symbol_hint.as_ref(),
            market_type,
            &normalized,
        )?)];
        if decimal_as_f64(order_data.get("l")).unwrap_or(0.0) > 0.0 {
            events.push(ExchangeStreamEvent::Fill(parse_private_fill(
                exchange_id,
                tenant_id,
                account_id,
                market_type,
                symbol_hint,
                order_data,
            )?));
        }
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(events));
    }
    if event.contains("account_update") {
        if looks_like_position_payload(data) {
            return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions: parse_positions(
                        exchange_id,
                        tenant_id,
                        account_id,
                        &json!({"data": {"account": [{"positionVos": [{"positions": [normalize_position_event(data)]}]}]}}),
                    )?,
                }),
            ]));
        }
        let mut events = vec![ExchangeStreamEvent::BalanceSnapshot(parse_balance_event(
            exchange_id,
            tenant_id.clone(),
            account_id.clone(),
            market_type,
            data.get("a").unwrap_or(data),
        )?)];
        if let Some(positions) = data
            .get("a")
            .and_then(|account| account.get("P"))
            .and_then(Value::as_array)
        {
            for position in positions {
                let snapshot = PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions: parse_positions(
                        exchange_id,
                        tenant_id.clone(),
                        account_id.clone(),
                        &json!({"data": {"account": [{"positionVos": [{"positions": [normalize_position_event(position)]}]}]}}),
                    )?,
                };
                if !snapshot.positions.is_empty() {
                    events.push(ExchangeStreamEvent::PositionSnapshot(snapshot));
                }
            }
        }
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(events));
    }
    if event.contains("account") || event.contains("balance") {
        if looks_like_position_payload(data) {
            return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions: parse_positions(
                        exchange_id,
                        tenant_id,
                        account_id,
                        &json!({"data": {"account": [{"positionVos": [{"positions": [normalize_position_event(data)]}]}]}}),
                    )?,
                }),
            ]));
        }
        if let Some(account_update) = data.get("a") {
            return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::BalanceSnapshot(parse_balance_event(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    account_update,
                )?),
            ]));
        }
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(parse_balance_event(
                exchange_id,
                tenant_id,
                account_id,
                market_type,
                data,
            )?),
        ]));
    }
    if event.contains("position") {
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &json!({"data": {"account": [{"positionVos": [{"positions": [data.clone()]}]}]}}),
                )?,
            }),
        ]));
    }
    if event.contains("trade") || event.contains("execution") {
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::Fill(parse_private_fill(
                exchange_id,
                tenant_id,
                account_id,
                market_type,
                symbol_hint,
                data,
            )?),
        ]));
    }
    if event.contains("order") || data.get("orderId").is_some() || data.get("i").is_some() {
        let normalized = normalize_order_event(data);
        return Ok(HashKeyGlobalPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                symbol_hint.as_ref(),
                market_type,
                &normalized,
            )?),
        ]));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported hashkey_global private stream message",
        value,
    ))
}

fn hashkey_global_private_channel(
    kind: &PrivateStreamKind,
    market_type: MarketType,
) -> ExchangeApiResult<&'static str> {
    match (kind, market_type) {
        (PrivateStreamKind::Orders, _) | (PrivateStreamKind::Fills, _) => Ok("user_order_update"),
        (PrivateStreamKind::Balances, MarketType::Spot)
        | (PrivateStreamKind::Account, MarketType::Spot) => Ok("user_balance_update"),
        (PrivateStreamKind::Balances, MarketType::Perpetual)
        | (PrivateStreamKind::Account, MarketType::Perpetual) => Ok("user_account_update"),
        (PrivateStreamKind::Positions, MarketType::Perpetual) => Ok("user_account_update"),
        (PrivateStreamKind::Positions, MarketType::Spot) => Err(ExchangeApiError::Unsupported {
            operation: "hashkey_global.spot_positions_stream",
        }),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "hashkey_global.private_stream_kind",
        }),
    }
}

impl HashKeyGlobalPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = hashkey_global_public_subscribe_payload(&subscription, "1")?;
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

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        let ts = value
            .get("ping")
            .or_else(|| value.get("ts"))
            .and_then(Value::as_str);
        let is_ping = value
            .as_str()
            .is_some_and(|text| text.eq_ignore_ascii_case("ping"))
            || value.get("ping").is_some()
            || value
                .get("event")
                .and_then(Value::as_str)
                .is_some_and(|event| event.eq_ignore_ascii_case("ping"));
        if is_ping {
            self.state.on_message(now);
            self.state.on_pong(now);
            Some(hashkey_global_ws_pong_payload(ts))
        } else {
            None
        }
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<HashKeyGlobalWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        if let Some(response) = self.heartbeat_response(&value, now) {
            return Ok(vec![HashKeyGlobalWsSessionEvent::HeartbeatResponse(
                response,
            )]);
        }
        self.state.on_message(now);
        let message = parse_hashkey_global_public_stream_message(
            &self.exchange_id,
            self.symbol.clone(),
            &value,
        )?;
        if matches!(message, HashKeyGlobalPublicStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![HashKeyGlobalWsSessionEvent::Public(message.clone())];
        if let HashKeyGlobalPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(HashKeyGlobalWsSessionEvent::Stream(
                ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(self.exchange_id.clone(), None),
                    order_book: snapshot,
                }),
            ));
        }
        Ok(events)
    }
}

impl HashKeyGlobalPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        market_type: MarketType,
        listen_key: &str,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload =
            hashkey_global_private_subscribe_payload(&subscription, market_type)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url: hashkey_global_private_listen_key_url(&url, listen_key, market_type)?,
            exchange_id,
            tenant_id: subscription.context.tenant_id.clone().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "hashkey_global private ws session requires tenant_id".to_string(),
                }
            })?,
            account_id: subscription.account_id,
            market_type,
            symbol_hint: None,
            subscribe_payload,
            state,
        })
    }

    pub fn with_symbol_hint(mut self, symbol: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol);
        self
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
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

    pub fn heartbeat_response(&mut self, value: &Value, now: DateTime<Utc>) -> Option<Value> {
        let ts = value
            .get("ts")
            .or_else(|| value.get("ping"))
            .and_then(Value::as_str);
        let is_ping = value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("ping"))
            || value.get("ping").is_some();
        if is_ping {
            self.state.on_message(now);
            self.state.on_pong(now);
            Some(hashkey_global_ws_pong_payload(ts))
        } else {
            None
        }
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<HashKeyGlobalWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        let now = Utc::now();
        if let Some(response) = self.heartbeat_response(&value, now) {
            return Ok(vec![HashKeyGlobalWsSessionEvent::HeartbeatResponse(
                response,
            )]);
        }
        self.state.on_message(now);
        let message = parse_hashkey_global_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.market_type,
            self.symbol_hint.clone(),
            &value,
        )?;
        if matches!(message, HashKeyGlobalPrivateStreamMessage::Pong) {
            self.state.on_pong(now);
        }
        let mut events = vec![HashKeyGlobalWsSessionEvent::Private(message.clone())];
        if let HashKeyGlobalPrivateStreamMessage::Events(stream_events) = message {
            events.extend(
                stream_events
                    .into_iter()
                    .map(HashKeyGlobalWsSessionEvent::Stream),
            );
        }
        Ok(events)
    }
}

fn hashkey_global_ws_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let symbol = normalize_hashkey_global_symbol(symbol, market_type)?;
    Ok(if market_type == MarketType::Spot {
        symbol.to_ascii_lowercase()
    } else {
        let (base, quote) = split_hashkey_global_symbol(&symbol).ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: format!("cannot normalize HashKeyGlobal WS symbol {symbol}"),
            }
        })?;
        format!(
            "{}_{}",
            base.to_ascii_lowercase(),
            quote.to_ascii_lowercase()
        )
    })
}

fn stream_data(value: &Value) -> &Value {
    value
        .get("data")
        .or_else(|| value.get("d"))
        .or_else(|| value.get("o"))
        .unwrap_or(value)
}

fn order_event_payload<'a>(root: &'a Value, data: &'a Value) -> &'a Value {
    root.get("o").or_else(|| data.get("o")).unwrap_or(data)
}

fn looks_like_position_payload(value: &Value) -> bool {
    (value.get("symbol").is_some()
        || value.get("s").is_some()
        || value.get("contractName").is_some())
        && (value.get("volume").is_some()
            || value.get("pa").is_some()
            || value.get("positionAmt").is_some())
}

fn normalize_public_tick(tick: &Value, root: &Value) -> Value {
    let bids = tick
        .get("buys")
        .or_else(|| tick.get("bids"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let asks = tick
        .get("sells")
        .or_else(|| tick.get("asks"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    json!({
        "bids": bids,
        "asks": asks,
        "ts": tick.get("ts").or_else(|| root.get("ts")).cloned().unwrap_or(Value::Null)
    })
}

fn parse_public_trade(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<HashKeyGlobalPublicTrade> {
    let side = if let Some(side) = value
        .get("side")
        .or_else(|| value.get("S"))
        .and_then(Value::as_str)
    {
        parse_side(exchange_id, side)?
    } else if value.get("m").and_then(Value::as_bool).unwrap_or(false) {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    Ok(HashKeyGlobalPublicTrade {
        symbol: symbol_hint,
        trade_id: value_to_string(
            value
                .get("id")
                .or_else(|| value.get("t"))
                .or_else(|| value.get("tradeId")),
        ),
        side,
        price: value_to_string(value.get("price").or_else(|| value.get("p")))
            .unwrap_or_else(|| "0".to_string()),
        quantity: value_to_string(value.get("qty").or_else(|| value.get("q")))
            .unwrap_or_else(|| "0".to_string()),
        traded_at: first_timestamp_millis(value, &["time", "T", "E"]).unwrap_or_else(Utc::now),
    })
}

fn parse_balance_event(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<BalancesResponse> {
    let mut balances = Vec::new();
    let rows = value
        .get("B")
        .or_else(|| value.get("balances"))
        .or_else(|| value.get("balance"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(value));
    for row in rows {
        let asset = row
            .get("a")
            .or_else(|| row.get("asset"))
            .or_else(|| row.get("c"))
            .or_else(|| row.get("marginCoin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        let available = decimal_as_f64(
            row.get("f")
                .or_else(|| row.get("free"))
                .or_else(|| row.get("cw"))
                .or_else(|| row.get("accountNormal")),
        )
        .unwrap_or(0.0);
        let locked = decimal_as_f64(
            row.get("l")
                .or_else(|| row.get("locked"))
                .or_else(|| row.get("lb")),
        )
        .unwrap_or(0.0);
        let total = decimal_as_f64(row.get("wb").or_else(|| row.get("balance")))
            .unwrap_or(available + locked);
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
    }
    Ok(BalancesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), None),
        balances: vec![ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id,
            account_id,
            exchange_id: exchange_id.clone(),
            market_type,
            balances,
            observed_at: Utc::now(),
        }],
    })
}

fn parse_private_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_hint
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, market_type, value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hashkey_global stream fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(
        value
            .get("L")
            .or_else(|| value.get("price"))
            .or_else(|| value.get("p")),
    )
    .unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("l")
            .or_else(|| value.get("q"))
            .or_else(|| value.get("qty")),
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
        order_id: value_to_string(value.get("i").or_else(|| value.get("orderId"))),
        client_order_id: value_to_string(value.get("c").or_else(|| value.get("clientOrderId"))),
        fill_id: value_to_string(value.get("t").or_else(|| value.get("tradeId"))),
        side: parse_side(
            exchange_id,
            value
                .get("S")
                .or_else(|| value.get("side"))
                .and_then(value_as_side_text)
                .unwrap_or("BUY"),
        )?,
        position_side: parse_position_side(
            value
                .get("ps")
                .or_else(|| value.get("positionSide"))
                .and_then(Value::as_str),
        ),
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value.get("m")),
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: value_to_string(value.get("N").or_else(|| value.get("feeCoin"))),
        fee_amount: decimal_as_f64(value.get("n").or_else(|| value.get("fee"))),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("rp")),
        filled_at: first_timestamp_millis(value, &["T", "time", "E"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn normalize_order_event(value: &Value) -> Value {
    json!({
        "symbol": value_to_string(value.get("s").or_else(|| value.get("symbol"))).unwrap_or_default(),
        "orderId": value.get("i").or_else(|| value.get("orderId")).cloned().unwrap_or(Value::Null),
        "clientOrderId": value.get("c").or_else(|| value.get("clientOrderId")).cloned().unwrap_or(Value::Null),
        "side": value.get("S").or_else(|| value.get("side")).and_then(value_as_side_text).map(|text| Value::String(text.to_string())).unwrap_or_else(|| json!("BUY")),
        "type": value.get("o").or_else(|| value.get("type")).and_then(value_as_order_type_text).map(|text| Value::String(text.to_string())).unwrap_or_else(|| json!("LIMIT")),
        "timeInForce": value.get("f").or_else(|| value.get("timeInForce")).cloned().unwrap_or(Value::Null),
        "status": value.get("X").or_else(|| value.get("status")).and_then(value_as_status_text).map(|text| Value::String(text.to_string())).unwrap_or_else(|| json!("NEW")),
        "origQty": value.get("q").or_else(|| value.get("origQty")).cloned().unwrap_or_else(|| json!("0")),
        "executedQty": value.get("z").or_else(|| value.get("executedQty")).cloned().unwrap_or_else(|| json!("0")),
        "price": value.get("p").or_else(|| value.get("price")).cloned().unwrap_or(Value::Null),
        "avgPrice": value.get("ap").or_else(|| value.get("avgPrice")).cloned().unwrap_or(Value::Null),
        "updateTime": value.get("T").or_else(|| value.get("E")).cloned().unwrap_or(Value::Null),
        "positionSide": value.get("ps").or_else(|| value.get("positionSide")).cloned().unwrap_or(Value::Null),
        "reduceOnly": value.get("R").or_else(|| value.get("reduceOnly")).cloned().unwrap_or(Value::Null),
    })
}

fn normalize_position_event(value: &Value) -> Value {
    let mut object = value.as_object().cloned().unwrap_or_else(Map::new);
    if let Some(symbol) = value.get("s").or_else(|| value.get("symbol")).cloned() {
        object.entry("symbol".to_string()).or_insert(symbol);
    }
    if let Some(amount) = value
        .get("pa")
        .or_else(|| value.get("positionAmt"))
        .cloned()
    {
        object.entry("volume".to_string()).or_insert(amount);
    }
    if let Some(entry) = value.get("ep").or_else(|| value.get("entryPrice")).cloned() {
        object.entry("avgPrice".to_string()).or_insert(entry);
    }
    if let Some(side) = value
        .get("ps")
        .or_else(|| value.get("positionSide"))
        .cloned()
    {
        object.entry("positionSide".to_string()).or_insert(side);
    }
    Value::Object(object)
}

fn symbol_from_payload(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = value
        .get("s")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "stream payload missing symbol", value))?
        .to_ascii_uppercase()
        .replace(
            '_',
            if market_type == MarketType::Spot {
                ""
            } else {
                "-"
            },
        );
    let (base, quote) = split_hashkey_global_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn parse_side(exchange_id: &ExchangeId, side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" | "B" | "1" => Ok(OrderSide::Buy),
        "SELL" | "S" | "2" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "invalid side",
            &json!(side),
        )),
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" | "BUY" => PositionSide::Long,
        "SHORT" | "SELL" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_liquidity(value: Option<&Value>) -> LiquidityRole {
    match value {
        Some(Value::Bool(true)) => LiquidityRole::Maker,
        Some(Value::Bool(false)) => LiquidityRole::Taker,
        Some(Value::String(text)) if text.eq_ignore_ascii_case("maker") => LiquidityRole::Maker,
        Some(Value::String(text)) if text.eq_ignore_ascii_case("taker") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn value_to_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_side_text(value: &Value) -> Option<&'static str> {
    match value {
        Value::String(text) if text.eq_ignore_ascii_case("BUY") || text == "1" => Some("BUY"),
        Value::String(text) if text.eq_ignore_ascii_case("SELL") || text == "2" => Some("SELL"),
        Value::Number(number) if number.as_i64() == Some(1) => Some("BUY"),
        Value::Number(number) if number.as_i64() == Some(2) => Some("SELL"),
        _ => None,
    }
}

fn value_as_order_type_text(value: &Value) -> Option<&'static str> {
    match value {
        Value::String(text) if text.eq_ignore_ascii_case("MARKET") || text == "1" => Some("MARKET"),
        Value::String(text) if text.eq_ignore_ascii_case("LIMIT") || text == "2" => Some("LIMIT"),
        Value::Number(number) if number.as_i64() == Some(1) => Some("MARKET"),
        Value::Number(number) if number.as_i64() == Some(2) => Some("LIMIT"),
        _ => value.as_str().map(|_| "LIMIT"),
    }
}

fn value_as_status_text(value: &Value) -> Option<&'static str> {
    match value {
        Value::String(text) if text.eq_ignore_ascii_case("NEW") || text == "1" => Some("NEW"),
        Value::String(text) if text.eq_ignore_ascii_case("FILLED") || text == "2" => Some("FILLED"),
        Value::String(text) if text.eq_ignore_ascii_case("PARTIALLY_FILLED") || text == "3" => {
            Some("PARTIALLY_FILLED")
        }
        Value::String(text) if text.eq_ignore_ascii_case("CANCELLED") || text == "4" => {
            Some("CANCELLED")
        }
        Value::String(text) if text.eq_ignore_ascii_case("REJECTED") || text == "5" => {
            Some("REJECTED")
        }
        Value::Number(number) if number.as_i64() == Some(1) => Some("NEW"),
        Value::Number(number) if number.as_i64() == Some(2) => Some("FILLED"),
        Value::Number(number) if number.as_i64() == Some(3) => Some("PARTIALLY_FILLED"),
        Value::Number(number) if number.as_i64() == Some(4) => Some("CANCELLED"),
        Value::Number(number) if number.as_i64() == Some(5) => Some("REJECTED"),
        _ => None,
    }
}

fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str()?.parse().ok())
                .and_then(DateTime::<Utc>::from_timestamp_millis)
        })
    })
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            "empty hashkey_global websocket message",
            &Value::Null,
        ));
    }
    if trimmed.eq_ignore_ascii_case("ping") || trimmed.eq_ignore_ascii_case("pong") {
        return Ok(json!({ "event": trimmed.to_ascii_lowercase() }));
    }
    serde_json::from_str(trimmed).map_err(|error| {
        parse_error(
            exchange_id.clone(),
            &format!("invalid hashkey_global websocket JSON: {error}"),
            &json!({ "raw": trimmed }),
        )
    })
}
