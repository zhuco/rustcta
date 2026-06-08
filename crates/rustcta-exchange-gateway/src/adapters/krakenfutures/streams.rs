#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, ExchangeSymbol, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_futures_symbol, parse_futures_orderbook_snapshot};
use super::signing::sign_futures_ws_challenge;
use super::KrakenFuturesGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::{StreamReconnectPolicy, StreamRuntimeState, StreamSupervisorAction};

#[derive(Debug, Clone, PartialEq)]
pub enum KrakenWsSessionEvent {
    Public(KrakenWsControlMessage),
    Private(KrakenWsControlMessage),
    Stream(Vec<ExchangeStreamEvent>),
    Outbound(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum KrakenWsControlMessage {
    SubscriptionAck { channel: Option<String> },
    Heartbeat,
    Pong,
    Raw(Value),
}

#[derive(Debug, Clone)]
pub struct KrakenPublicWsSession {
    pub url: String,
    exchange_id: rustcta_types::ExchangeId,
    subscription: PublicStreamSubscription,
    subscribe_payload: Value,
    state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct KrakenPrivateWsSession {
    pub url: String,
    exchange_id: rustcta_types::ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    subscription: PrivateStreamSubscription,
    subscribe_payload: Option<Value>,
    futures_subscription: Option<PrivateStreamSubscription>,
    futures_api_key: Option<String>,
    futures_api_secret: Option<String>,
    state: StreamRuntimeState,
}

impl KrakenPublicWsSession {
    pub fn new(
        exchange_id: rustcta_types::ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = krakenfutures_public_subscribe_payload(&subscription)?;
        let mut state =
            StreamRuntimeState::new(exchange_id.clone(), subscription.symbol.market_type);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            subscription,
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

    pub fn on_connected(&mut self) {
        self.state.on_connected(Utc::now());
    }

    pub fn on_disconnected(&mut self) {
        self.state.on_disconnected(Utc::now());
    }

    pub fn supervisor_action(&self, policy: &StreamReconnectPolicy) -> StreamSupervisorAction {
        self.state.decide(Utc::now(), policy)
    }

    pub fn heartbeat_request(&mut self) -> Value {
        self.state.on_ping_sent(Utc::now());
        krakenfutures_ws_ping_payload()
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<KrakenWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        let control = parse_krakenfutures_ws_control_message(&value);
        if matches!(
            control,
            KrakenWsControlMessage::Heartbeat | KrakenWsControlMessage::Pong
        ) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![KrakenWsSessionEvent::Public(control.clone())];
        if matches!(control, KrakenWsControlMessage::Heartbeat) {
            events.push(KrakenWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: self.exchange_id.clone(),
                    received_at: Utc::now(),
                },
            ]));
        }
        if let Some(stream_events) =
            parse_krakenfutures_public_stream_events(&self.exchange_id, &self.subscription, &value)?
        {
            events.push(KrakenWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

impl KrakenPrivateWsSession {
    pub fn new_futures(
        exchange_id: rustcta_types::ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        if subscription.market_type != Some(MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "krakenfutures.futures_private_stream_market_type",
            });
        }
        let api_key = api_key.trim();
        let api_secret = api_secret.trim();
        if api_key.is_empty() || api_secret.is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "krakenfutures.futures_private_stream_credentials",
            });
        }
        let tenant_id = subscription.context.tenant_id.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "Kraken private WS session requires context.tenant_id".to_string(),
            }
        })?;
        let mut state = StreamRuntimeState::new(exchange_id.clone(), MarketType::Perpetual);
        state.subscription_count = 1;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id: subscription.account_id.clone(),
            market_type: MarketType::Perpetual,
            subscription: subscription.clone(),
            subscribe_payload: None,
            futures_subscription: Some(subscription),
            futures_api_key: Some(api_key.to_string()),
            futures_api_secret: Some(api_secret.to_string()),
            state,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        self.subscribe_payload
            .clone()
            .map(|payload| vec![payload])
            .unwrap_or_else(|| vec![krakenfutures_futures_challenge_payload()])
    }

    pub fn apply_futures_challenge(&mut self, value: &Value) -> ExchangeApiResult<Value> {
        if self.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "krakenfutures.spot_private_stream_challenge",
            });
        }
        let challenge = value
            .get("message")
            .or_else(|| value.get("challenge"))
            .or_else(|| value.get("original_challenge"))
            .and_then(Value::as_str)
            .filter(|challenge| !challenge.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("Kraken Futures challenge response missing message: {value}"),
            })?;
        let subscription =
            self.futures_subscription
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "Kraken Futures private WS session missing subscription".to_string(),
                })?;
        let api_key =
            self.futures_api_key
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "Kraken Futures private WS session missing api key".to_string(),
                })?;
        let api_secret =
            self.futures_api_secret
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "Kraken Futures private WS session missing api secret".to_string(),
                })?;
        let signed_challenge = sign_futures_ws_challenge(api_secret, challenge)?;
        let payload = krakenfutures_futures_private_subscribe_payload(
            subscription,
            api_key,
            challenge,
            &signed_challenge,
        )?;
        self.subscribe_payload = Some(payload.clone());
        Ok(payload)
    }

    pub fn state(&self) -> &StreamRuntimeState {
        &self.state
    }

    pub fn on_connected(&mut self) {
        self.state.on_connected(Utc::now());
    }

    pub fn on_disconnected(&mut self) {
        self.state.on_disconnected(Utc::now());
    }

    pub fn supervisor_action(&self, policy: &StreamReconnectPolicy) -> StreamSupervisorAction {
        self.state.decide(Utc::now(), policy)
    }

    pub fn heartbeat_request(&mut self) -> Value {
        self.state.on_ping_sent(Utc::now());
        krakenfutures_ws_ping_payload()
    }

    pub fn handle_text_message(
        &mut self,
        text: &str,
    ) -> ExchangeApiResult<Vec<KrakenWsSessionEvent>> {
        let value = parse_ws_text(&self.exchange_id, text)?;
        self.state.on_message(Utc::now());
        if self.market_type == MarketType::Perpetual && is_krakenfutures_futures_challenge(&value) {
            let payload = self.apply_futures_challenge(&value)?;
            return Ok(vec![KrakenWsSessionEvent::Outbound(payload)]);
        }
        let control = parse_krakenfutures_ws_control_message(&value);
        if matches!(
            control,
            KrakenWsControlMessage::Heartbeat | KrakenWsControlMessage::Pong
        ) {
            self.state.on_pong(Utc::now());
        }
        let mut events = vec![KrakenWsSessionEvent::Private(control.clone())];
        if matches!(control, KrakenWsControlMessage::Heartbeat) {
            events.push(KrakenWsSessionEvent::Stream(vec![
                ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: self.exchange_id.clone(),
                    received_at: Utc::now(),
                },
            ]));
        }
        if let Some(stream_events) = parse_krakenfutures_private_stream_events(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            &self.subscription,
            &value,
        )? {
            events.push(KrakenWsSessionEvent::Stream(stream_events));
        }
        Ok(events)
    }
}

impl KrakenFuturesGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        let _payload = krakenfutures_public_subscribe_payload(&subscription)?;
        let url = self.config.futures_ws_url.as_str();
        let channel = futures_public_feed(&subscription.kind)?;
        let symbol = normalize_futures_symbol(&subscription.symbol)?;
        Ok(format!("kraken:{url}:{channel}:{symbol}"))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let _ = self.futures_credentials("krakenfutures.futures_private_stream")?;
        Ok(format!(
            "kraken:{}:{}:{}",
            self.config.futures_ws_url,
            futures_private_feed(&subscription.kind)?,
            subscription.account_id
        ))
    }

    pub(crate) fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<KrakenPublicWsSession> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        KrakenPublicWsSession::new(
            self.exchange_id.clone(),
            self.config.futures_ws_url.clone(),
            subscription,
        )
    }

    pub(crate) fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
        _spot_token: Option<&str>,
    ) -> ExchangeApiResult<KrakenPrivateWsSession> {
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let (api_key, api_secret) =
            self.futures_credentials("krakenfutures.futures_private_ws_session")?;
        KrakenPrivateWsSession::new_futures(
            self.exchange_id.clone(),
            self.config.futures_ws_url.clone(),
            subscription,
            api_key,
            api_secret,
        )
    }
}

pub(super) fn krakenfutures_ws_ping_payload() -> Value {
    json!({
        "method": "ping",
    })
}

pub(super) fn krakenfutures_futures_challenge_payload() -> Value {
    json!({
        "event": "challenge",
    })
}

pub(super) fn parse_krakenfutures_ws_control_message(value: &Value) -> KrakenWsControlMessage {
    if is_krakenfutures_ws_heartbeat(value) {
        return KrakenWsControlMessage::Heartbeat;
    }
    if is_krakenfutures_ws_pong(value) {
        return KrakenWsControlMessage::Pong;
    }
    if is_krakenfutures_ws_subscription_ack(value) {
        return KrakenWsControlMessage::SubscriptionAck {
            channel: channel_name(value).map(str::to_string),
        };
    }
    KrakenWsControlMessage::Raw(value.clone())
}

fn parse_krakenfutures_public_stream_events(
    exchange_id: &rustcta_types::ExchangeId,
    subscription: &PublicStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<Option<Vec<ExchangeStreamEvent>>> {
    if !matches!(
        subscription.kind,
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot
    ) || !is_public_order_book_message(value)
    {
        return Ok(None);
    }

    if subscription.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.public_stream_market_type",
        });
    }
    let book = futures_ws_order_book_payload(value);
    let snapshot =
        parse_futures_orderbook_snapshot(exchange_id, subscription.symbol.clone(), &book)?;

    Ok(Some(vec![ExchangeStreamEvent::OrderBookSnapshot(
        OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book: snapshot,
        },
    )]))
}

fn is_public_order_book_message(value: &Value) -> bool {
    value
        .get("channel")
        .and_then(Value::as_str)
        .is_some_and(|channel| channel.eq_ignore_ascii_case("book"))
        || value
            .get("feed")
            .and_then(Value::as_str)
            .is_some_and(|feed| feed.eq_ignore_ascii_case("book"))
        || value.get("orderBook").is_some()
        || value.get("orderbook").is_some()
        || value.get("book").is_some()
}

fn futures_ws_order_book_payload(value: &Value) -> Value {
    value
        .get("orderBook")
        .or_else(|| value.get("orderbook"))
        .or_else(|| value.get("book"))
        .or_else(|| value.get("data"))
        .cloned()
        .unwrap_or_else(|| value.clone())
}

fn parse_krakenfutures_private_stream_events(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    subscription: &PrivateStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<Option<Vec<ExchangeStreamEvent>>> {
    let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
    let feed = private_feed_name(value).unwrap_or_default();
    let payload = private_stream_payload(value);
    let symbol_hint = private_symbol_hint(exchange_id, market_type, payload)
        .or_else(|| private_symbol_hint(exchange_id, market_type, value));

    match subscription.kind {
        PrivateStreamKind::Orders => {
            if !feed_matches(feed, &["executions", "open_orders", "orders"]) {
                return Ok(None);
            }
            let events = private_rows(payload)
                .into_iter()
                .map(|row| {
                    let fallback_symbol = private_symbol_hint(exchange_id, market_type, row)
                        .or_else(|| symbol_hint.clone());
                    super::private_parser::parse_order_state(
                        exchange_id,
                        fallback_symbol.as_ref(),
                        private_order_id(row).as_deref(),
                        row,
                    )
                    .map(ExchangeStreamEvent::OrderUpdate)
                })
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            Ok(non_empty_stream_events(events))
        }
        PrivateStreamKind::Fills => {
            if !feed_matches(feed, &["executions", "fills", "trade", "trades"]) {
                return Ok(None);
            }
            let mut fills = Vec::new();
            for row in private_rows(payload) {
                let fallback_symbol = private_symbol_hint(exchange_id, market_type, row)
                    .or_else(|| symbol_hint.clone());
                let value = normalize_private_rows_payload("fills", row);
                fills.extend(super::private_parser::parse_recent_fills(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    fallback_symbol.as_ref(),
                    &value,
                )?);
            }
            Ok(non_empty_stream_events(
                fills.into_iter().map(ExchangeStreamEvent::Fill).collect(),
            ))
        }
        PrivateStreamKind::Balances | PrivateStreamKind::Account => {
            if !feed_matches(feed, &["balances", "account"]) {
                return Ok(None);
            }
            let value = normalize_private_balance_payload(payload);
            let balances = super::private_parser::parse_balances(
                exchange_id,
                tenant_id,
                account_id,
                market_type,
                &[],
                &value,
            )?;
            Ok(non_empty_stream_events(vec![
                ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances,
                }),
            ]))
        }
        PrivateStreamKind::Positions => {
            if market_type != MarketType::Perpetual
                || !feed_matches(feed, &["open_positions", "positions"])
            {
                return Ok(None);
            }
            let value = normalize_private_rows_payload("openPositions", payload);
            let positions = super::private_parser::parse_positions(
                exchange_id,
                tenant_id,
                account_id,
                &[],
                &value,
            )?;
            Ok(non_empty_stream_events(vec![
                ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions,
                }),
            ]))
        }
    }
}

fn non_empty_stream_events(events: Vec<ExchangeStreamEvent>) -> Option<Vec<ExchangeStreamEvent>> {
    if events.is_empty() {
        None
    } else {
        Some(events)
    }
}

fn feed_matches(feed: &str, expected: &[&str]) -> bool {
    if feed.is_empty() {
        return true;
    }
    expected
        .iter()
        .any(|expected| feed.eq_ignore_ascii_case(expected))
}

fn private_feed_name(value: &Value) -> Option<&str> {
    value
        .get("channel")
        .and_then(Value::as_str)
        .or_else(|| value.get("feed").and_then(Value::as_str))
        .or_else(|| value.get("event").and_then(Value::as_str))
}

fn private_stream_payload(value: &Value) -> &Value {
    value
        .get("data")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("balances"))
        .or_else(|| value.get("openPositions"))
        .or_else(|| value.get("positions"))
        .unwrap_or(value)
}

fn private_rows(value: &Value) -> Vec<&Value> {
    if let Some(rows) = value.as_array() {
        return rows.iter().collect();
    }
    value
        .get("orders")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("openPositions"))
        .or_else(|| value.get("positions"))
        .and_then(Value::as_array)
        .map(|rows| rows.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn normalize_private_rows_payload(key: &str, value: &Value) -> Value {
    if value.get(key).is_some() {
        return value.clone();
    }
    let mut object = serde_json::Map::new();
    object.insert(
        key.to_string(),
        if value.as_array().is_some() {
            value.clone()
        } else {
            Value::Array(private_rows(value).into_iter().cloned().collect())
        },
    );
    Value::Object(object)
}

fn normalize_private_balance_payload(value: &Value) -> Value {
    if value.get("balances").is_some() || value.get("accounts").is_some() {
        return value.clone();
    }
    let mut balances = serde_json::Map::new();
    if let Some(rows) = value.as_array() {
        for row in rows {
            if let Some((asset, balance)) = private_balance_row(row) {
                balances.insert(asset, balance);
            }
        }
        return Value::Object(balances);
    }
    if let Some((asset, balance)) = private_balance_row(value) {
        balances.insert(asset, balance);
        return Value::Object(balances);
    }
    value.clone()
}

fn private_balance_row(value: &Value) -> Option<(String, Value)> {
    let asset = value
        .get("asset")
        .or_else(|| value.get("currency"))
        .and_then(Value::as_str)?
        .to_string();
    let balance = json!({
        "balance": value
            .get("balance")
            .or_else(|| value.get("total"))
            .or_else(|| value.get("amount"))
            .cloned()
            .unwrap_or_else(|| Value::from("0")),
        "hold_trade": value
            .get("hold_trade")
            .or_else(|| value.get("hold"))
            .or_else(|| value.get("locked"))
            .cloned()
            .unwrap_or_else(|| Value::from("0")),
    });
    Some((asset, balance))
}

fn private_order_id(value: &Value) -> Option<String> {
    super::parser::string_or_number(
        value
            .get("txid")
            .or_else(|| value.get("order_id"))
            .or_else(|| value.get("orderId")),
    )
}

fn private_symbol_hint(
    exchange_id: &rustcta_types::ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> Option<SymbolScope> {
    let symbol_text = value
        .get("pair")
        .or_else(|| value.get("symbol"))
        .or_else(|| value.get("product_id"))
        .or_else(|| value.get("instrument"))
        .and_then(Value::as_str)?;
    if market_type != MarketType::Perpetual {
        return None;
    }
    let canonical = super::parser::canonical_from_futures_symbol(symbol_text)?;
    let exchange_symbol_text = symbol_text.trim().to_ascii_uppercase();
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol_text).ok()?;
    Some(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

pub(super) fn krakenfutures_private_stream_capabilities(
    enabled: bool,
    futures_enabled: bool,
) -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: enabled,
        supports_fills: enabled,
        supports_balances: enabled,
        supports_positions: futures_enabled,
        supports_account: enabled,
        order_event_kinds: if enabled {
            vec![
                PrivateOrderStreamEventKind::New,
                PrivateOrderStreamEventKind::PartialFill,
                PrivateOrderStreamEventKind::Fill,
                PrivateOrderStreamEventKind::Cancel,
                PrivateOrderStreamEventKind::Reject,
                PrivateOrderStreamEventKind::Expired,
                PrivateOrderStreamEventKind::BalanceUpdate,
            ]
        } else {
            Vec::new()
        },
        supports_client_order_id: enabled,
        supports_exchange_order_id: enabled,
    }
}

pub(super) fn krakenfutures_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    if subscription.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.public_stream_market_type",
        });
    }
    krakenfutures_futures_public_subscribe_payload(subscription)
}

pub(super) fn krakenfutures_futures_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    api_key: &str,
    original_challenge: &str,
    signed_challenge: &str,
) -> ExchangeApiResult<Value> {
    let api_key = api_key.trim();
    let original_challenge = original_challenge.trim();
    let signed_challenge = signed_challenge.trim();
    if api_key.is_empty() || original_challenge.is_empty() || signed_challenge.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Kraken Futures private stream auth fields must not be empty".to_string(),
        });
    }
    Ok(json!({
        "event": "subscribe",
        "feed": futures_private_feed(&subscription.kind)?,
        "api_key": api_key,
        "original_challenge": original_challenge,
        "signed_challenge": signed_challenge,
    }))
}

fn krakenfutures_futures_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "subscribe",
        "feed": futures_public_feed(&subscription.kind)?,
        "product_ids": [normalize_futures_symbol(&subscription.symbol)?],
    }))
}

fn futures_public_feed(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PublicStreamKind::Trades => Ok("trade"),
        PublicStreamKind::Ticker => Ok("ticker"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok("book"),
        PublicStreamKind::Candles { .. } => Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.futures_public_stream_candles",
        }),
    }
}

fn futures_private_feed(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders => Ok("open_orders"),
        PrivateStreamKind::Fills => Ok("fills"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("balances"),
        PrivateStreamKind::Positions => Ok("open_positions"),
    }
}

fn is_krakenfutures_futures_challenge(value: &Value) -> bool {
    value
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| event.eq_ignore_ascii_case("challenge"))
        || value
            .get("challenge")
            .and_then(Value::as_str)
            .is_some_and(|challenge| !challenge.trim().is_empty())
        || value
            .get("original_challenge")
            .and_then(Value::as_str)
            .is_some_and(|challenge| !challenge.trim().is_empty())
}

fn parse_ws_text(exchange_id: &rustcta_types::ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        ExchangeApiError::Exchange(rustcta_types::ExchangeError {
            schema_version: rustcta_types::SchemaVersion::current(),
            exchange_id: exchange_id.clone(),
            class: rustcta_types::ExchangeErrorClass::Decode,
            code: Some("KRAKEN_WS_PARSE".to_string()),
            message: format!("failed to parse Kraken WebSocket message: {error}"),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: Some(Value::String(text.to_string())),
            occurred_at: Utc::now(),
        })
    })
}

fn is_krakenfutures_ws_heartbeat(value: &Value) -> bool {
    value
        .get("channel")
        .and_then(Value::as_str)
        .is_some_and(|channel| channel.eq_ignore_ascii_case("heartbeat"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("heartbeat"))
}

fn is_krakenfutures_ws_pong(value: &Value) -> bool {
    value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.eq_ignore_ascii_case("pong"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
}

fn is_krakenfutures_ws_subscription_ack(value: &Value) -> bool {
    value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.eq_ignore_ascii_case("subscribe"))
        || value
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|event| event.eq_ignore_ascii_case("subscribed"))
        || value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| success)
}

fn channel_name(value: &Value) -> Option<&str> {
    value
        .get("channel")
        .and_then(Value::as_str)
        .or_else(|| value.get("feed").and_then(Value::as_str))
        .or_else(|| {
            value
                .get("result")
                .and_then(|result| result.get("channel"))
                .and_then(Value::as_str)
        })
}
