#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, PositionSide,
    SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_woo_symbol, parse_error, parse_orderbook_snapshot, parse_woo_market_symbol,
    string_or_number, validation_error, value_as_i64,
};
use super::private_parser::{parse_order_state, parse_positions};
use super::WooGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

#[derive(Debug, Clone, PartialEq)]
pub enum WooPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trade(WooPublicTrade),
    Ticker(WooTicker24h),
    Candle(WooCandle),
    SubscriptionAck { topics: Vec<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooPublicTrade {
    pub symbol: SymbolScope,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub source: Option<i64>,
    pub rpi: bool,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooTicker24h {
    pub symbol: SymbolScope,
    pub open_price: Option<String>,
    pub last_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub volume: Option<String>,
    pub quote_volume: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooCandle {
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

impl WooGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_stream {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.subscribe_public_stream",
            });
        }
        let session = self.public_ws_session(subscription)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        let topic = payload
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        Ok(format!(
            "woo:{}:{}",
            self.config.public_ws_url.trim_end_matches('/'),
            topic
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
        if !self.config.private_stream_available() {
            return self.unsupported_private("woo.subscribe_private_stream");
        }
        let account_id = subscription.account_id.clone();
        let session = self.private_ws_session(subscription).await?;
        let payload = session
            .initial_requests()
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        let topic = payload
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        Ok(format!("woo:{}:{}:{}", session.url, topic, account_id))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<WooPublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_stream {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.public_ws_session",
            });
        }
        WooPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub async fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<WooPrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        if !self.config.private_stream_available() {
            return self.unsupported_private("woo.private_ws_session");
        }
        let listen_key = self.rest.get_listen_key().await?;
        WooPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            listen_key,
            subscription,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WooWsSessionEvent {
    Public(WooPublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct WooPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct WooPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl WooPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.symbol.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "woo public WS session cannot serve exchange {}",
                    subscription.symbol.exchange
                ),
            });
        }
        let subscribe_payload = woo_public_subscribe_payload(&subscription, 1)?;
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
        woo_ping_payload(now.timestamp_millis())
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

    pub fn handle_text_message(&mut self, text: &str) -> ExchangeApiResult<Vec<WooWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let message =
            parse_woo_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        if matches!(message, WooPublicStreamMessage::Pong) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![WooWsSessionEvent::Public(message.clone())];
        match message {
            WooPublicStreamMessage::OrderBook(snapshot) => {
                events.push(WooWsSessionEvent::Stream(vec![
                    ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(self.exchange_id.clone(), None),
                        order_book: snapshot,
                    }),
                ]));
            }
            WooPublicStreamMessage::Pong => {
                events.push(WooWsSessionEvent::Stream(vec![
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

impl WooPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        base_url: String,
        listen_key: String,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.exchange != exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "woo private WS session cannot serve exchange {}",
                    subscription.exchange
                ),
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "woo private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let subscribe_payload = woo_private_subscribe_payload(&subscription, 1)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url: format!("{}?key={listen_key}", base_url.trim_end_matches('/')),
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            symbol_hint: None,
            subscribe_payload,
            state,
        })
    }

    pub fn with_symbol_hint(mut self, symbol_hint: SymbolScope) -> Self {
        self.symbol_hint = Some(symbol_hint);
        self
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn heartbeat_request(&mut self, now: DateTime<Utc>) -> Value {
        self.state.on_ping_sent(now);
        woo_ping_payload(now.timestamp_millis())
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

    pub fn handle_text_message(&mut self, text: &str) -> ExchangeApiResult<Vec<WooWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let stream_events = parse_woo_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        if value.get("cmd").and_then(Value::as_str) == Some("PONG") {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![WooWsSessionEvent::Private(stream_events.clone())];
        if !stream_events.is_empty() {
            events.push(WooWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

pub fn woo_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id.to_string(),
        "cmd": "SUBSCRIBE",
        "params": [woo_public_topic(subscription)?],
    }))
}

pub fn woo_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id.to_string(),
        "cmd": "SUBSCRIBE",
        "params": [woo_private_topic(&subscription.kind)?],
    }))
}

pub fn woo_ping_payload(timestamp_millis: i64) -> Value {
    json!({
        "cmd": "PING",
        "ts": timestamp_millis,
    })
}

pub fn parse_woo_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WooPublicStreamMessage> {
    if value.get("cmd").and_then(Value::as_str) == Some("PONG") {
        return Ok(WooPublicStreamMessage::Pong);
    }
    if value.get("cmd").and_then(Value::as_str) == Some("SUBSCRIBE") {
        let topics = value
            .get("data")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        return Ok(WooPublicStreamMessage::SubscriptionAck { topics });
    }

    match topic_family(value.get("topic").and_then(Value::as_str)) {
        "orderbook" | "orderbook10" | "orderbookupdate" => Ok(WooPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol_hint, &normalize_book_message(value))?,
        )),
        "trade" => Ok(WooPublicStreamMessage::Trade(parse_public_trade(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        "ticker" => Ok(WooPublicStreamMessage::Ticker(parse_ticker(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        "kline" => Ok(WooPublicStreamMessage::Candle(parse_candle(
            exchange_id,
            symbol_hint,
            value,
        )?)),
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported woo public stream message",
            value,
        )),
    }
}

pub fn parse_woo_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if value.get("cmd").and_then(Value::as_str) == Some("PONG") {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if value.get("cmd").and_then(Value::as_str) == Some("SUBSCRIBE") {
        return Ok(Vec::new());
    }

    match topic_family(value.get("topic").and_then(Value::as_str)) {
        "executionreport" => {
            parse_execution_report(exchange_id, tenant_id, account_id, symbol_hint, value)
        }
        "balance" => {
            let balances = parse_stream_balances(exchange_id, tenant_id, account_id, value)?;
            Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
                BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances,
                },
            )])
        }
        "position" => {
            let normalized = normalize_position_message(value);
            let positions = parse_positions(exchange_id, tenant_id, account_id, &[], &normalized)?;
            Ok(vec![ExchangeStreamEvent::PositionSnapshot(
                rustcta_exchange_api::PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions,
                },
            )])
        }
        "account" => Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]),
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported woo private stream message",
            value,
        )),
    }
}

fn woo_public_topic(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let symbol = normalize_woo_symbol(&subscription.symbol)?;
    match &subscription.kind {
        PublicStreamKind::Trades => Ok(format!("trade@{symbol}")),
        PublicStreamKind::Ticker => Ok(format!("ticker@{symbol}")),
        PublicStreamKind::OrderBookDelta => Ok(format!("orderbookupdate@{symbol}@50")),
        PublicStreamKind::OrderBookSnapshot => Ok(format!("orderbook10@{symbol}")),
        PublicStreamKind::Candles { interval } => {
            Ok(format!("kline@{symbol}@{}", normalize_interval(interval)?))
        }
    }
}

fn woo_private_topic(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("executionreport"),
        PrivateStreamKind::Balances => Ok("balance"),
        PrivateStreamKind::Positions => Ok("position"),
        PrivateStreamKind::Account => Ok("account"),
    }
}

fn parse_public_trade(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WooPublicTrade> {
    let data = value.get("data").unwrap_or(value);
    Ok(WooPublicTrade {
        symbol: symbol_from_data(exchange_id, data).unwrap_or(symbol_hint),
        side: parse_side(
            exchange_id,
            data.get("sd").and_then(Value::as_str).unwrap_or("BUY"),
        )?,
        price: required_text(exchange_id, data, &["px", "price"])?,
        quantity: required_text(exchange_id, data, &["sx", "quantity"])?,
        source: data.get("src").and_then(value_as_i64),
        rpi: data.get("rpi").and_then(Value::as_bool).unwrap_or(false),
        traded_at: timestamp_from_fields(data, &["ts"]).unwrap_or_else(Utc::now),
    })
}

fn parse_ticker(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WooTicker24h> {
    let data = value.get("data").unwrap_or(value);
    Ok(WooTicker24h {
        symbol: symbol_from_data(exchange_id, data).unwrap_or(symbol_hint),
        open_price: string_or_number(data.get("o")),
        last_price: string_or_number(data.get("c")),
        high_price: string_or_number(data.get("h")),
        low_price: string_or_number(data.get("l")),
        volume: string_or_number(data.get("v")),
        quote_volume: string_or_number(data.get("a")),
        updated_at: timestamp_from_fields(data, &["ts", "tts"]).unwrap_or_else(Utc::now),
    })
}

fn parse_candle(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<WooCandle> {
    let data = value.get("data").unwrap_or(value);
    Ok(WooCandle {
        symbol: symbol_from_data(exchange_id, data).unwrap_or(symbol_hint),
        interval: string_or_number(data.get("t")).unwrap_or_else(|| "unknown".to_string()),
        opened_at: timestamp_from_fields(data, &["st"]).unwrap_or_else(Utc::now),
        closed_at: timestamp_from_fields(data, &["et"]),
        open: required_text(exchange_id, data, &["o"])?,
        high: required_text(exchange_id, data, &["h"])?,
        low: required_text(exchange_id, data, &["l"])?,
        close: required_text(exchange_id, data, &["c"])?,
        volume: required_text(exchange_id, data, &["v"])?,
        quote_volume: string_or_number(data.get("a")),
    })
}

fn parse_execution_report(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let data = value.get("data").unwrap_or(value);
    let mt = data.get("mt").and_then(value_as_i64).unwrap_or(0);
    let mut normalized = normalize_execution_report(data);
    if mt != 0 {
        normalized["status"] = json!("REJECTED");
        normalized["type"] = json!("LIMIT");
        normalized["side"] = json!("BUY");
        normalized["quantity"] = json!("0");
    }
    let order = parse_order_state(exchange_id, symbol_hint.as_ref(), &normalized)
        .map(ExchangeStreamEvent::OrderUpdate)?;
    let mut events = vec![order];
    if mt == 0
        && normalized
            .get("executedQuantity")
            .or_else(|| normalized.get("executed"))
            .and_then(|value| string_or_number(Some(value)))
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|quantity| quantity > 0.0)
    {
        events.push(ExchangeStreamEvent::Fill(parse_stream_fill(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint.as_ref(),
            data,
        )?));
    }
    Ok(events)
}

fn parse_stream_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let balances = value
        .get("data")
        .unwrap_or(value)
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "woo balance stream missing balances",
                value,
            )
        })?;
    let mut assets = Vec::new();
    for balance in balances {
        let asset = required_text(exchange_id, balance, &["t", "token"])?.to_ascii_uppercase();
        let total = number_text(balance, &["h", "holding"]).unwrap_or_else(|| "0".to_string());
        let locked = number_text(balance, &["f", "frozen"]).unwrap_or_else(|| "0".to_string());
        let total_value = total.parse::<f64>().unwrap_or(0.0);
        let locked_value = locked.parse::<f64>().unwrap_or(0.0);
        let available = (total_value - locked_value).max(0.0);
        assets.push(
            AssetBalance::new(asset, total_value, available, locked_value)
                .map_err(validation_error)?,
        );
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances: assets,
        observed_at: Utc::now(),
    }])
}

fn parse_stream_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_data(exchange_id, value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo stream fill requires canonical_symbol".to_string(),
            })?;
    let price = number_text(value, &["epx", "px"])
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let quantity = number_text(value, &["esx"])
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("oid")),
        client_order_id: string_or_number(value.get("cid")).filter(|value| value != "0"),
        fill_id: string_or_number(value.get("tid")),
        side: parse_side(
            exchange_id,
            value.get("sd").and_then(Value::as_str).unwrap_or("BUY"),
        )?,
        position_side: parse_position_side(value.get("ps").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("mk").and_then(Value::as_bool) {
            Some(true) => LiquidityRole::Maker,
            Some(false) => LiquidityRole::Taker,
            None => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: string_or_number(value.get("fa")),
        fee_amount: value
            .get("f")
            .and_then(|value| string_or_number(Some(value)))
            .and_then(|value| value.parse::<f64>().ok())
            .map(f64::abs),
        fee_rate: None,
        realized_pnl: None,
        filled_at: timestamp_from_fields(value, &["ts"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn normalize_book_message(value: &Value) -> Value {
    let data = value.get("data").unwrap_or(value);
    json!({
        "data": {
            "symbol": data.get("s").or_else(|| data.get("symbol")).cloned().unwrap_or(Value::Null),
            "bids": data.get("bids").cloned().unwrap_or_else(|| json!([])),
            "asks": data.get("asks").cloned().unwrap_or_else(|| json!([])),
            "timestamp": data.get("ts").or_else(|| value.get("ts")).cloned().unwrap_or(Value::Null),
        }
    })
}

fn normalize_execution_report(value: &Value) -> Value {
    json!({
        "symbol": value.get("s").cloned().unwrap_or(Value::Null),
        "clientOrderId": value.get("cid").cloned().unwrap_or(Value::Null),
        "orderId": value.get("oid").cloned().unwrap_or(Value::Null),
        "type": value.get("t").cloned().unwrap_or_else(|| json!("LIMIT")),
        "side": value.get("sd").cloned().unwrap_or_else(|| json!("BUY")),
        "positionSide": value.get("ps").cloned().unwrap_or_else(|| json!("BOTH")),
        "quantity": value.get("sx").cloned().unwrap_or_else(|| json!("0")),
        "price": value.get("px").cloned().unwrap_or(Value::Null),
        "executed": value.get("tesx").or_else(|| value.get("esx")).cloned().unwrap_or_else(|| json!("0")),
        "averageExecutedPrice": value.get("aepx").or_else(|| value.get("epx")).cloned().unwrap_or(Value::Null),
        "status": value.get("ss").cloned().unwrap_or_else(|| json!("NEW")),
        "reduceOnly": value.get("ro").cloned().unwrap_or_else(|| json!(false)),
        "createdTime": value.get("ts").cloned().unwrap_or(Value::Null),
        "updatedTime": value.get("ts").cloned().unwrap_or(Value::Null),
    })
}

fn normalize_position_message(value: &Value) -> Value {
    let positions = value
        .get("data")
        .unwrap_or(value)
        .get("positions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|position| {
            json!({
                "symbol": position.get("s").cloned().unwrap_or(Value::Null),
                "holding": position.get("h").cloned().unwrap_or_else(|| json!("0")),
                "positionSide": position.get("ps").cloned().unwrap_or_else(|| json!("BOTH")),
                "pendingLongQty": position.get("plq").cloned().unwrap_or(Value::Null),
                "pendingShortQty": position.get("psq").cloned().unwrap_or(Value::Null),
                "settlePrice": position.get("sp").cloned().unwrap_or(Value::Null),
                "averageOpenPrice": position.get("aop").cloned().unwrap_or(Value::Null),
                "pnl24H": position.get("pnl").cloned().unwrap_or(Value::Null),
                "fee24H": position.get("fee").cloned().unwrap_or(Value::Null),
                "markPrice": position.get("mp").cloned().unwrap_or(Value::Null),
                "adlQuantile": position.get("aq").cloned().unwrap_or(Value::Null),
                "leverage": position.get("lv").cloned().unwrap_or(Value::Null),
                "timestamp": position.get("ts").cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    json!({ "data": { "positions": positions } })
}

fn symbol_from_data(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolScope> {
    let symbol_text = value
        .get("s")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "woo stream message missing symbol",
                value,
            )
        })?
        .to_ascii_uppercase();
    let (market_type, base, quote) = parse_woo_market_symbol(&symbol_text)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, &symbol_text)
            .map_err(validation_error)?,
    })
}

fn topic_family(topic: Option<&str>) -> &str {
    let Some(topic) = topic else {
        return "";
    };
    topic.split('@').next().unwrap_or(topic)
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval {
        "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d" | "3d"
        | "1w" | "1M" => Ok(interval.to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "woo.unsupported_kline_interval",
        }),
    }
}

fn parse_side(exchange_id: &ExchangeId, side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported woo stream side",
            &Value::String(side.to_string()),
        )),
    }
}

fn parse_position_side(side: Option<&str>) -> PositionSide {
    match side.unwrap_or("BOTH").to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" | "NET" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<String> {
    fields
        .iter()
        .find_map(|field| {
            value
                .get(*field)
                .and_then(|item| string_or_number(Some(item)))
        })
        .ok_or_else(|| {
            stream_parse_error(
                exchange_id.clone(),
                &format!("woo stream missing one of fields {fields:?}"),
                value,
            )
        })
}

fn number_text(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(|item| string_or_number(Some(item)))
    })
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
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

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        stream_parse_error(
            exchange_id.clone(),
            &format!("invalid woo websocket json: {error}"),
            &Value::String(text.to_string()),
        )
    })
}
