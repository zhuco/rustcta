#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, ExchangeError, ExchangeErrorClass, ExchangeId, MarketType, OrderSide, SchemaVersion,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_cryptocom_symbol, parse_error, parse_orderbook_snapshot, value_as_string,
};
use super::private_parser::{parse_balances, parse_order_state, parse_recent_fills, parse_side};
use super::signing::sign_request;
use super::CryptoComGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

pub const CRYPTOCOM_ORDERBOOK_DEPTHS: [u16; 2] = [10, 50];
pub const CRYPTOCOM_ORDERBOOK_UPDATE_FREQUENCIES_MS: [u16; 2] = [10, 100];
pub const CRYPTOCOM_DEFAULT_ORDERBOOK_DEPTH: u16 = 50;
pub const CRYPTOCOM_DEFAULT_ORDERBOOK_UPDATE_FREQUENCY_MS: u16 = 10;
pub const CRYPTOCOM_ORDERBOOK_DELTA_SUBSCRIPTION_TYPE: &str = "SNAPSHOT_AND_UPDATE";

#[derive(Debug, Clone, PartialEq)]
pub enum CryptoComPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trades(Vec<CryptoComPublicTrade>),
    Ticker(CryptoComTicker24h),
    Candle(CryptoComCandle),
    SubscriptionAck { id: Option<u64> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComPublicTrade {
    pub symbol: SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComTicker24h {
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
    pub open_interest: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComCandle {
    pub symbol: SymbolScope,
    pub interval: String,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CryptoComOrderBookDeltaDetails {
    pub snapshot: rustcta_types::OrderBookSnapshot,
    pub update_id: Option<u64>,
    pub previous_update_id: Option<u64>,
    pub update_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoComOrderBookSequenceAction {
    AcceptSnapshotOrFirstUpdate,
    ApplyUpdate,
    RebuildFromSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CryptoComPublicOrderBookWsPolicy {
    pub channel_template: &'static str,
    pub subscription_type: &'static str,
    pub supported_depths: &'static [u16],
    pub default_depth: u16,
    pub supported_update_frequencies_ms: &'static [u16],
    pub default_update_frequency_ms: u16,
    pub sequence_fields: &'static [&'static str],
    pub checksum: Option<&'static str>,
    pub rest_snapshot_operation: &'static str,
    pub resync_strategy: &'static str,
}

impl CryptoComPublicOrderBookWsPolicy {
    pub fn as_json(&self) -> Value {
        json!({
            "channel_template": self.channel_template,
            "subscription_type": self.subscription_type,
            "depth": {
                "default": self.default_depth,
                "supported": self.supported_depths,
            },
            "book_update_frequency_ms": {
                "default": self.default_update_frequency_ms,
                "supported": self.supported_update_frequencies_ms,
            },
            "sequence": {
                "fields": self.sequence_fields,
                "continuity": "incoming pu must equal previous local u; rebuild on gap or regression",
            },
            "checksum": self.checksum,
            "resync": {
                "operation": self.rest_snapshot_operation,
                "strategy": self.resync_strategy,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CryptoComWsSessionEvent {
    Public(CryptoComPublicStreamMessage),
    Private(Vec<ExchangeStreamEvent>),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone)]
pub struct CryptoComPublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    symbol: SymbolScope,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct CryptoComPrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    auth_payload: Value,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

impl CryptoComPublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = cryptocom_public_subscribe_payload(&subscription, 1)?;
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

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<CryptoComWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(response) = heartbeat_response(&value) {
            self.state.on_pong(Utc::now());
            return Ok(vec![CryptoComWsSessionEvent::Outbound(response)]);
        }
        let message =
            parse_cryptocom_public_stream_message(&self.exchange_id, self.symbol.clone(), &value)?;
        let mut events = vec![CryptoComWsSessionEvent::Public(message.clone())];
        if let CryptoComPublicStreamMessage::OrderBook(snapshot) = message {
            events.push(CryptoComWsSessionEvent::Stream(vec![
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

impl CryptoComPrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        api_key: &str,
        api_secret: &str,
        nonce: i64,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "cryptocom private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        let auth_payload = cryptocom_ws_auth_payload(api_key, api_secret, nonce, 1);
        let subscribe_payload = cryptocom_private_subscribe_payload(&subscription, 2)?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id,
            symbol_hint,
            auth_payload,
            subscribe_payload,
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.auth_payload.clone(), self.subscribe_payload.clone()]
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
    ) -> ExchangeApiResult<Vec<CryptoComWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if let Some(response) = heartbeat_response(&value) {
            self.state.on_pong(Utc::now());
            return Ok(vec![CryptoComWsSessionEvent::Outbound(response)]);
        }
        let events = parse_cryptocom_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            self.symbol_hint.clone(),
            &value,
        )?;
        Ok(vec![CryptoComWsSessionEvent::Private(events)])
    }
}

impl CryptoComGatewayAdapter {
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
            "cryptocom:{}:{}",
            self.config.public_ws_url,
            payload["params"]["channels"][0]
                .as_str()
                .unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let account_id = subscription.account_id.clone();
        let session = self.private_ws_session(subscription, None)?;
        let payload = session
            .initial_requests()
            .into_iter()
            .nth(1)
            .unwrap_or(Value::Null);
        Ok(format!(
            "cryptocom:{}:{}:{}",
            self.config.private_ws_url,
            payload["params"]["channels"][0]
                .as_str()
                .unwrap_or("unknown"),
            account_id
        ))
    }

    pub(crate) fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<CryptoComPublicWsSession> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_public_market(subscription.symbol.market_type)?;
        CryptoComPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub(crate) fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        symbol_hint: Option<SymbolScope>,
    ) -> ExchangeApiResult<CryptoComPrivateWsSession> {
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_public_market(market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("cryptocom.subscribe_private_stream")?;
        CryptoComPrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            api_key,
            api_secret,
            Utc::now().timestamp_millis(),
            symbol_hint,
        )
    }
}

pub fn cryptocom_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    if matches!(subscription.kind, PublicStreamKind::OrderBookDelta) {
        return cryptocom_public_orderbook_delta_subscribe_payload(
            &subscription.symbol,
            id,
            CRYPTOCOM_DEFAULT_ORDERBOOK_DEPTH,
            CRYPTOCOM_DEFAULT_ORDERBOOK_UPDATE_FREQUENCY_MS,
        );
    }
    Ok(json!({
        "id": id,
        "method": "subscribe",
        "params": {
            "channels": [cryptocom_public_channel(subscription)?],
        },
    }))
}

pub fn cryptocom_public_orderbook_delta_subscribe_payload(
    symbol: &SymbolScope,
    id: u64,
    depth: u16,
    update_frequency_ms: u16,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id,
        "method": "subscribe",
        "params": {
            "channels": [cryptocom_public_orderbook_channel(symbol, depth)?],
            "book_subscription_type": CRYPTOCOM_ORDERBOOK_DELTA_SUBSCRIPTION_TYPE,
            "book_update_frequency": cryptocom_orderbook_update_frequency_ms(update_frequency_ms)?,
        },
    }))
}

pub fn cryptocom_ws_auth_payload(api_key: &str, api_secret: &str, nonce: i64, id: u64) -> Value {
    let params = json!({});
    json!({
        "id": id,
        "method": "public/auth",
        "api_key": api_key,
        "sig": sign_request(api_secret, "public/auth", id, api_key, &params, nonce),
        "nonce": nonce,
    })
}

pub fn cryptocom_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id,
        "method": "subscribe",
        "params": {
            "channels": [cryptocom_private_channel(subscription)?],
        },
    }))
}

pub fn parse_cryptocom_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CryptoComPublicStreamMessage> {
    if value
        .get("id")
        .and_then(value_as_i64)
        .is_some_and(|id| id >= 0)
        && value.get("method").and_then(Value::as_str) == Some("subscribe")
        && value.get("result").is_none_or(|result| result.is_null())
    {
        return Ok(CryptoComPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(value_as_u64),
        });
    }
    if value.get("method").and_then(Value::as_str) == Some("public/heartbeat") {
        return Ok(CryptoComPublicStreamMessage::Pong);
    }
    let result = value.get("result").unwrap_or(value);
    match channel_family(result.get("channel").and_then(Value::as_str)) {
        "book" => Ok(CryptoComPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, result)?,
        )),
        "book.update" => Ok(CryptoComPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, &book_update_as_snapshot(result))?,
        )),
        "trade" => Ok(CryptoComPublicStreamMessage::Trades(parse_public_trades(
            exchange_id,
            symbol,
            result,
        )?)),
        "ticker" => Ok(CryptoComPublicStreamMessage::Ticker(parse_ticker(
            symbol, result,
        )?)),
        "candlestick" => Ok(CryptoComPublicStreamMessage::Candle(parse_candle(
            exchange_id,
            symbol,
            result,
        )?)),
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported cryptocom public stream message",
            value,
        )),
    }
}

pub fn parse_cryptocom_orderbook_delta_details(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CryptoComOrderBookDeltaDetails> {
    let result = value.get("result").unwrap_or(value);
    let data_item = first_result_item(result)?;
    let parse_value = if data_item.get("update").is_some() {
        book_update_as_snapshot(result)
    } else {
        result.clone()
    };
    let snapshot = parse_orderbook_snapshot(exchange_id, symbol, &parse_value)?;
    let update = data_item.get("update").unwrap_or(data_item);
    let update_id = data_item
        .get("u")
        .or_else(|| update.get("u"))
        .and_then(value_as_u64)
        .or(snapshot.sequence);
    let previous_update_id = data_item
        .get("pu")
        .or_else(|| update.get("pu"))
        .and_then(value_as_u64);
    let update_type = data_item
        .get("tt")
        .or_else(|| data_item.get("type"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(CryptoComOrderBookDeltaDetails {
        snapshot,
        update_id,
        previous_update_id,
        update_type,
    })
}

pub fn cryptocom_orderbook_sequence_action(
    previous_local_update_id: Option<u64>,
    message_previous_update_id: Option<u64>,
    message_update_id: Option<u64>,
) -> CryptoComOrderBookSequenceAction {
    let Some(message_update_id) = message_update_id else {
        return CryptoComOrderBookSequenceAction::RebuildFromSnapshot;
    };
    let Some(previous_local_update_id) = previous_local_update_id else {
        return CryptoComOrderBookSequenceAction::AcceptSnapshotOrFirstUpdate;
    };
    if message_update_id <= previous_local_update_id {
        return CryptoComOrderBookSequenceAction::RebuildFromSnapshot;
    }
    match message_previous_update_id {
        Some(previous) if previous == previous_local_update_id => {
            CryptoComOrderBookSequenceAction::ApplyUpdate
        }
        _ => CryptoComOrderBookSequenceAction::RebuildFromSnapshot,
    }
}

pub fn cryptocom_public_orderbook_ws_policy() -> CryptoComPublicOrderBookWsPolicy {
    CryptoComPublicOrderBookWsPolicy {
        channel_template: "book.{instrument_name}.{depth}",
        subscription_type: CRYPTOCOM_ORDERBOOK_DELTA_SUBSCRIPTION_TYPE,
        supported_depths: &CRYPTOCOM_ORDERBOOK_DEPTHS,
        default_depth: CRYPTOCOM_DEFAULT_ORDERBOOK_DEPTH,
        supported_update_frequencies_ms: &CRYPTOCOM_ORDERBOOK_UPDATE_FREQUENCIES_MS,
        default_update_frequency_ms: CRYPTOCOM_DEFAULT_ORDERBOOK_UPDATE_FREQUENCY_MS,
        sequence_fields: &["u", "pu"],
        checksum: None,
        rest_snapshot_operation: "get_order_book",
        resync_strategy:
            "subscribe with SNAPSHOT_AND_UPDATE, seed from the first snapshot, apply updates only when pu equals the previous local u, and rebuild from REST public/get-book plus fresh subscription on reconnect, pu gap, or regression",
    }
}

pub fn parse_cryptocom_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if value.get("method").and_then(Value::as_str) == Some("public/heartbeat") {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if value
        .get("id")
        .and_then(value_as_i64)
        .is_some_and(|id| id >= 0)
        && matches!(
            value.get("method").and_then(Value::as_str),
            Some("subscribe") | Some("public/auth")
        )
    {
        return Ok(Vec::new());
    }
    let result = value.get("result").unwrap_or(value);
    match channel_family(result.get("channel").and_then(Value::as_str)) {
        "user.order" => result
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|order| {
                parse_order_state(exchange_id, symbol_hint.as_ref(), order)
                    .map(ExchangeStreamEvent::OrderUpdate)
            })
            .collect(),
        "user.trade" => {
            let symbol = symbol_hint.ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cryptocom user.trade stream parsing requires symbol_hint".to_string(),
            })?;
            let fills = parse_recent_fills(exchange_id, tenant_id, account_id, &symbol, result)?;
            Ok(fills.into_iter().map(ExchangeStreamEvent::Fill).collect())
        }
        "user.balance" => {
            let balances = parse_balances(exchange_id, tenant_id, account_id, &[], result)?;
            Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
                BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances,
                },
            )])
        }
        _ => Err(stream_parse_error(
            exchange_id.clone(),
            "unsupported cryptocom private stream message",
            value,
        )),
    }
}

fn channel_family(channel: Option<&str>) -> &str {
    let Some(channel) = channel else {
        return "";
    };
    if channel.starts_with("book.update") {
        "book.update"
    } else if channel.starts_with("book.") || channel == "book" {
        "book"
    } else if channel.starts_with("trade.") || channel == "trade" {
        "trade"
    } else if channel.starts_with("ticker.") || channel == "ticker" {
        "ticker"
    } else if channel.starts_with("candlestick.") || channel == "candlestick" {
        "candlestick"
    } else if channel.starts_with("user.order.") || channel == "user.order" {
        "user.order"
    } else if channel.starts_with("user.trade.") || channel == "user.trade" {
        "user.trade"
    } else if channel == "user.balance" {
        "user.balance"
    } else {
        channel
    }
}

fn cryptocom_public_channel(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let symbol = normalize_cryptocom_symbol(&subscription.symbol)?;
    match &subscription.kind {
        PublicStreamKind::Trades => Ok(format!("trade.{symbol}")),
        PublicStreamKind::Ticker => Ok(format!("ticker.{symbol}")),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            cryptocom_public_orderbook_channel(
                &subscription.symbol,
                CRYPTOCOM_DEFAULT_ORDERBOOK_DEPTH,
            )
        }
        PublicStreamKind::Candles { interval } => Ok(format!(
            "candlestick.{}.{symbol}",
            normalize_interval(interval)?
        )),
    }
}

pub fn cryptocom_public_orderbook_channel(
    symbol: &SymbolScope,
    depth: u16,
) -> ExchangeApiResult<String> {
    let symbol = normalize_cryptocom_symbol(symbol)?;
    Ok(format!(
        "book.{symbol}.{}",
        cryptocom_orderbook_depth(depth)?
    ))
}

pub fn cryptocom_orderbook_depth(depth: u16) -> ExchangeApiResult<u16> {
    if CRYPTOCOM_ORDERBOOK_DEPTHS.contains(&depth) {
        Ok(depth)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "cryptocom order book depth must be one of {:?}, got {depth}",
                CRYPTOCOM_ORDERBOOK_DEPTHS
            ),
        })
    }
}

pub fn cryptocom_orderbook_update_frequency_ms(frequency_ms: u16) -> ExchangeApiResult<u16> {
    if CRYPTOCOM_ORDERBOOK_UPDATE_FREQUENCIES_MS.contains(&frequency_ms) {
        Ok(frequency_ms)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "cryptocom order book update frequency must be one of {:?} ms, got {frequency_ms}",
                CRYPTOCOM_ORDERBOOK_UPDATE_FREQUENCIES_MS
            ),
        })
    }
}

fn cryptocom_private_channel(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<String> {
    match subscription.kind {
        PrivateStreamKind::Orders => Ok("user.order".to_string()),
        PrivateStreamKind::Fills => Ok("user.trade".to_string()),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("user.balance".to_string()),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "cryptocom.private_stream_positions",
        }),
    }
}

fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<CryptoComPublicTrade>> {
    result_data(value)?
        .iter()
        .map(|trade| {
            Ok(CryptoComPublicTrade {
                symbol: symbol.clone(),
                trade_id: value_as_string(trade.get("d").or_else(|| trade.get("trade_id"))),
                side: trade
                    .get("s")
                    .or_else(|| trade.get("side"))
                    .and_then(Value::as_str)
                    .map(parse_side)
                    .transpose()?
                    .unwrap_or(OrderSide::Buy),
                price: required_text(exchange_id, trade, &["p", "price"])?,
                quantity: required_text(exchange_id, trade, &["q", "quantity"])?,
                traded_at: first_timestamp_millis(trade, &["t", "traded_at"])
                    .unwrap_or_else(Utc::now),
            })
        })
        .collect()
}

fn parse_ticker(symbol: SymbolScope, value: &Value) -> ExchangeApiResult<CryptoComTicker24h> {
    let ticker = first_result_item(value)?;
    Ok(CryptoComTicker24h {
        symbol,
        last_price: value_as_string(ticker.get("a").or_else(|| ticker.get("last_price"))),
        bid_price: value_as_string(ticker.get("b").or_else(|| ticker.get("bid_price"))),
        bid_quantity: value_as_string(ticker.get("bs").or_else(|| ticker.get("bid_size"))),
        ask_price: value_as_string(ticker.get("k").or_else(|| ticker.get("ask_price"))),
        ask_quantity: value_as_string(ticker.get("ks").or_else(|| ticker.get("ask_size"))),
        high_price: value_as_string(ticker.get("h").or_else(|| ticker.get("high_price"))),
        low_price: value_as_string(ticker.get("l").or_else(|| ticker.get("low_price"))),
        price_change: value_as_string(ticker.get("c").or_else(|| ticker.get("price_change"))),
        volume: value_as_string(ticker.get("v").or_else(|| ticker.get("volume"))),
        quote_volume: value_as_string(ticker.get("vv").or_else(|| ticker.get("quote_volume"))),
        open_interest: value_as_string(ticker.get("oi").or_else(|| ticker.get("open_interest"))),
        updated_at: first_timestamp_millis(ticker, &["t", "updated_at"]).unwrap_or_else(Utc::now),
    })
}

fn parse_candle(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CryptoComCandle> {
    let candle = first_result_item(value)?;
    Ok(CryptoComCandle {
        symbol,
        interval: value
            .get("interval")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        opened_at: first_timestamp_millis(candle, &["t"])
            .ok_or_else(|| stream_parse_error(exchange_id.clone(), "candle missing time", value))?,
        open: required_text(exchange_id, candle, &["o", "open"])?,
        high: required_text(exchange_id, candle, &["h", "high"])?,
        low: required_text(exchange_id, candle, &["l", "low"])?,
        close: required_text(exchange_id, candle, &["c", "close"])?,
        volume: required_text(exchange_id, candle, &["v", "volume"])?,
    })
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim() {
        "1m" | "M1" => Ok("1m".to_string()),
        "5m" | "M5" => Ok("5m".to_string()),
        "15m" | "M15" => Ok("15m".to_string()),
        "30m" | "M30" => Ok("30m".to_string()),
        "1h" | "H1" => Ok("1h".to_string()),
        "2h" | "H2" => Ok("2h".to_string()),
        "4h" | "H4" => Ok("4h".to_string()),
        "12h" | "H12" => Ok("12h".to_string()),
        "1D" | "1d" | "D1" => Ok("1D".to_string()),
        "7D" | "7d" => Ok("7D".to_string()),
        "14D" | "14d" => Ok("14D".to_string()),
        "1M" => Ok("1M".to_string()),
        other => Err(ExchangeApiError::Unsupported {
            operation: match other {
                "" => "cryptocom.empty_candle_interval",
                _ => "cryptocom.unsupported_candle_interval",
            },
        }),
    }
}

fn result_data(value: &Value) -> ExchangeApiResult<&Vec<Value>> {
    value.get("data").and_then(Value::as_array).ok_or_else(|| {
        stream_parse_error(
            ExchangeId::unchecked("cryptocom"),
            "stream data missing",
            value,
        )
    })
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text)
        .map_err(|error| stream_parse_error(exchange_id.clone(), &error.to_string(), &json!(text)))
}

fn heartbeat_response(value: &Value) -> Option<Value> {
    (value.get("method").and_then(Value::as_str) == Some("public/heartbeat")).then(|| {
        let id = value.get("id").cloned().unwrap_or_else(|| json!(-1));
        json!({
            "id": id,
            "method": "public/respond-heartbeat",
        })
    })
}

fn first_result_item(value: &Value) -> ExchangeApiResult<&Value> {
    result_data(value)?.first().ok_or_else(|| {
        stream_parse_error(
            ExchangeId::unchecked("cryptocom"),
            "stream data empty",
            value,
        )
    })
}

fn book_update_as_snapshot(value: &Value) -> Value {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let update = item.get("update").unwrap_or(item);
                    json!({
                        "asks": update.get("asks").cloned().unwrap_or_else(|| json!([])),
                        "bids": update.get("bids").cloned().unwrap_or_else(|| json!([])),
                        "t": item.get("t").cloned().unwrap_or(Value::Null),
                        "u": item.get("u").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({ "data": data })
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<String> {
    fields
        .iter()
        .find_map(|field| value_as_string(value.get(*field)))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                &format!("missing field {}", fields[0]),
                value,
            )
        })
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
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
