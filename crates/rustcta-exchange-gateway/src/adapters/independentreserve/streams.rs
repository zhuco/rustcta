#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::parser::{
    decimal_as_f64, independentreserve_asset, parse_orderbook_snapshot, parse_timestamp_value,
    split_symbol_assets, string_or_number,
};
use super::private_parser::{parse_balances, parse_fills, parse_order};
use super::IndependentReserveGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub enum IndependentReservePublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Heartbeat,
    OrderBook(OrderBookResponse),
    OrderBookEvent(IndependentReserveOrderBookEvent),
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndependentReservePrivateStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Heartbeat,
    Events(Vec<ExchangeStreamEvent>),
    Ignored,
}

#[derive(Debug, Clone)]
pub struct IndependentReservePublicWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    subscription: PublicStreamSubscription,
    subscribe_payload: Value,
}

#[derive(Debug, Clone)]
pub struct IndependentReservePrivateWsSession {
    pub url: String,
    exchange_id: ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    subscription: PrivateStreamSubscription,
    subscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndependentReservePublicOrderBookWsPolicy {
    pub url: &'static str,
    pub channel_template: &'static str,
    pub query_template: &'static str,
    pub subscribe_event: &'static str,
    pub subscribe_data: &'static str,
    pub unsubscribe_event: &'static str,
    pub interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub heartbeat_ms: Option<u64>,
    pub sequence_field: &'static str,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub resync: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndependentReserveOrderBookEventKind {
    NewOrder,
    OrderChanged,
    OrderCanceled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndependentReserveOrderBookEvent {
    pub kind: IndependentReserveOrderBookEventKind,
    pub channel: String,
    pub nonce: u64,
    pub order_type: Option<String>,
    pub order_guid: Option<String>,
    pub client_id: Option<String>,
    pub prices: BTreeMap<String, f64>,
    pub volume: Option<f64>,
    pub event_time: Option<chrono::DateTime<Utc>>,
}

impl IndependentReserveOrderBookEvent {
    pub fn is_delete(&self) -> bool {
        self.kind == IndependentReserveOrderBookEventKind::OrderCanceled || self.volume == Some(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndependentReserveNonceContinuity {
    First,
    Continuous,
    Gap { expected: u64, actual: u64 },
    NotIncreasing { previous: u64, actual: u64 },
}

impl IndependentReserveNonceContinuity {
    pub fn requires_reconnect(self) -> bool {
        matches!(
            self,
            IndependentReserveNonceContinuity::Gap { .. }
                | IndependentReserveNonceContinuity::NotIncreasing { .. }
        )
    }
}

pub fn independentreserve_public_order_book_ws_policy() -> IndependentReservePublicOrderBookWsPolicy
{
    IndependentReservePublicOrderBookWsPolicy {
        url: "wss://websockets.independentreserve.com",
        channel_template: "orderbook-{primary}",
        query_template: "?subscribe=orderbook-{primary}",
        subscribe_event: "Subscribe",
        subscribe_data: "[\"orderbook-{primary}\"]",
        unsubscribe_event: "Unsubscribe",
        interval_ms: None,
        depth: None,
        heartbeat_ms: Some(60_000),
        sequence_field: "Nonce",
        checksum: None,
        update_semantics: "orderbook channels publish NewOrder, OrderChanged, and OrderCanceled events per primary currency; official docs do not publish fixed interval or depth",
        resync: "track strictly increasing Nonce per channel/currency; reconnect and rebuild from REST /Public/GetOrderBook after nonce gaps, nonce regression, reconnect, stale stream, parse error, or suspected message loss",
    }
}

pub fn independentreserve_check_nonce_continuity(
    previous: Option<u64>,
    actual: u64,
) -> IndependentReserveNonceContinuity {
    let Some(previous) = previous else {
        return IndependentReserveNonceContinuity::First;
    };
    match actual.cmp(&previous.saturating_add(1)) {
        std::cmp::Ordering::Equal => IndependentReserveNonceContinuity::Continuous,
        std::cmp::Ordering::Greater => IndependentReserveNonceContinuity::Gap {
            expected: previous.saturating_add(1),
            actual,
        },
        std::cmp::Ordering::Less => {
            IndependentReserveNonceContinuity::NotIncreasing { previous, actual }
        }
    }
}

impl IndependentReserveGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        self.ensure_supported_quote(&subscription.symbol.exchange_symbol.symbol)?;
        let session = self.public_ws_session(subscription)?;
        Ok(format!(
            "independentreserve:{}:{}",
            session.url, session.subscribe_payload
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("independentreserve.subscribe_private_stream")?;
        if subscription
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Spot)
        {
            return self.unsupported_private("independentreserve.non_spot_private_stream");
        }
        let session = self.private_ws_session(subscription)?;
        Ok(format!(
            "independentreserve:{}:{}:{}",
            session.url, session.account_id, session.subscribe_payload
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<IndependentReservePublicWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        self.ensure_supported_quote(&subscription.symbol.exchange_symbol.symbol)?;
        IndependentReservePublicWsSession::new(
            self.exchange_id.clone(),
            self.config.public_ws_url.clone(),
            subscription,
        )
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<IndependentReservePrivateWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("independentreserve.private_ws_session")?;
        let (tenant_id, account_id) = self.context_account(
            &subscription.context,
            "independentreserve.private_ws_session",
        )?;
        IndependentReservePrivateWsSession::new(
            self.exchange_id.clone(),
            self.config.private_ws_url.clone(),
            subscription,
            tenant_id,
            account_id,
        )
    }
}

impl IndependentReservePublicWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = independentreserve_public_subscribe_payload(&subscription)?;
        Ok(Self {
            url,
            exchange_id,
            subscription,
            subscribe_payload,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn handle_text_message(&self, text: &str) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
        match parse_independentreserve_public_stream_message(
            &self.exchange_id,
            &self.subscription,
            &parse_ws_text(&self.exchange_id, text)?,
        )? {
            IndependentReservePublicStreamMessage::OrderBook(book) => {
                Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(book)])
            }
            IndependentReservePublicStreamMessage::OrderBookEvent(_) => Ok(Vec::new()),
            IndependentReservePublicStreamMessage::Heartbeat => {
                Ok(vec![ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: self.exchange_id.clone(),
                    received_at: Utc::now(),
                }])
            }
            IndependentReservePublicStreamMessage::SubscriptionAck { .. }
            | IndependentReservePublicStreamMessage::Ignored => Ok(Vec::new()),
        }
    }
}

impl IndependentReservePrivateWsSession {
    pub fn new(
        exchange_id: ExchangeId,
        url: String,
        subscription: PrivateStreamSubscription,
        tenant_id: TenantId,
        account_id: AccountId,
    ) -> ExchangeApiResult<Self> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        let subscribe_payload = independentreserve_private_subscribe_payload(&subscription)?;
        Ok(Self {
            url,
            exchange_id,
            tenant_id,
            account_id,
            subscription,
            subscribe_payload,
        })
    }

    pub fn initial_requests(&self) -> Vec<Value> {
        vec![self.subscribe_payload.clone()]
    }

    pub fn handle_text_message(&self, text: &str) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
        match parse_independentreserve_private_stream_message(
            &self.exchange_id,
            self.tenant_id.clone(),
            self.account_id.clone(),
            &self.subscription,
            &parse_ws_text(&self.exchange_id, text)?,
        )? {
            IndependentReservePrivateStreamMessage::Events(events) => Ok(events),
            IndependentReservePrivateStreamMessage::Heartbeat => {
                Ok(vec![ExchangeStreamEvent::Heartbeat {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: self.exchange_id.clone(),
                    received_at: Utc::now(),
                }])
            }
            IndependentReservePrivateStreamMessage::SubscriptionAck { .. }
            | IndependentReservePrivateStreamMessage::Ignored => Ok(Vec::new()),
        }
    }
}

pub fn independentreserve_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::Ack,
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn independentreserve_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = independentreserve_public_channel(subscription)?;
    Ok(json!({
        "Event": "Subscribe",
        "Data": [channel],
    }))
}

pub fn independentreserve_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = independentreserve_public_channel(subscription)?;
    Ok(json!({
        "Event": "Unsubscribe",
        "Data": [channel],
    }))
}

pub fn independentreserve_public_query_subscription(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    Ok(format!(
        "?subscribe={}",
        independentreserve_public_channel(subscription)?
    ))
}

pub fn independentreserve_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let (base, _) = split_symbol_assets(&subscription.symbol.exchange_symbol.symbol);
    let prefix = match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbook",
        PublicStreamKind::Trades => "ticker",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => {
            return Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
                operation: "independentreserve.candle_stream",
            })
        }
    };
    Ok(format!(
        "{}-{}",
        prefix,
        independentreserve_asset(&base).to_ascii_lowercase()
    ))
}

fn independentreserve_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "trades",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "accounts",
        PrivateStreamKind::Positions => {
            return Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
                operation: "independentreserve.position_stream",
            })
        }
    };
    Ok(json!({
        "event": "subscribe",
        "channel": channel,
        "account": subscription.account_id.to_string(),
        "marketType": "spot",
    }))
}

pub fn parse_independentreserve_public_stream_message(
    exchange_id: &ExchangeId,
    subscription: &PublicStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<IndependentReservePublicStreamMessage> {
    if is_heartbeat(value) {
        return Ok(IndependentReservePublicStreamMessage::Heartbeat);
    }
    if is_ack(value) {
        return Ok(IndependentReservePublicStreamMessage::SubscriptionAck {
            channel: channel(value),
        });
    }
    if is_public_orderbook_event(value) {
        return Ok(IndependentReservePublicStreamMessage::OrderBookEvent(
            parse_independentreserve_orderbook_event(exchange_id, value)?,
        ));
    }
    if value.get("BuyOrders").is_some()
        || value.get("SellOrders").is_some()
        || value.get("bids").is_some()
        || value
            .get("Data")
            .is_some_and(|data| data.get("BuyOrders").is_some() || data.get("bids").is_some())
    {
        let book = parse_orderbook_snapshot(exchange_id, subscription.symbol.clone(), value)?;
        return Ok(IndependentReservePublicStreamMessage::OrderBook(
            OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                order_book: book,
            },
        ));
    }
    Ok(IndependentReservePublicStreamMessage::Ignored)
}

pub fn parse_independentreserve_orderbook_event(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<IndependentReserveOrderBookEvent> {
    let event_name = value
        .get("Event")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_stream_error(exchange_id.clone(), "IR ws event missing Event", value)
        })?;
    let kind = match event_name {
        "NewOrder" => IndependentReserveOrderBookEventKind::NewOrder,
        "OrderChanged" => IndependentReserveOrderBookEventKind::OrderChanged,
        "OrderCanceled" => IndependentReserveOrderBookEventKind::OrderCanceled,
        _ => {
            return Err(parse_stream_error(
                exchange_id.clone(),
                "IR ws event is not an orderbook event",
                value,
            ))
        }
    };
    let channel = value
        .get("Channel")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_stream_error(
                exchange_id.clone(),
                "IR orderbook event missing Channel",
                value,
            )
        })?
        .to_string();
    let nonce = string_or_number(value.get("Nonce").or_else(|| value.get("nonce")))
        .ok_or_else(|| {
            parse_stream_error(
                exchange_id.clone(),
                "IR orderbook event missing Nonce",
                value,
            )
        })?
        .parse::<u64>()
        .map_err(|error| {
            parse_stream_error(
                exchange_id.clone(),
                &format!("IR orderbook event invalid Nonce: {error}"),
                value,
            )
        })?;
    let data = value
        .get("Data")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let mut prices = BTreeMap::new();
    if let Some(price_map) = data.get("Price").or_else(|| data.get("price")) {
        if let Some(object) = price_map.as_object() {
            for (currency, value) in object {
                if let Some(price) = decimal_as_f64(Some(value)) {
                    prices.insert(currency.to_ascii_uppercase(), price);
                }
            }
        } else if let Some(price) = decimal_as_f64(Some(price_map)) {
            prices.insert("UNKNOWN".to_string(), price);
        }
    }
    Ok(IndependentReserveOrderBookEvent {
        kind,
        channel,
        nonce,
        order_type: text_field(data, &["OrderType", "orderType"]),
        order_guid: text_field(data, &["OrderGuid", "orderGuid"]),
        client_id: text_field(data, &["ClientId", "clientId"]),
        prices,
        volume: decimal_as_f64(data.get("Volume").or_else(|| data.get("volume"))),
        event_time: value
            .get("Time")
            .or_else(|| value.get("time"))
            .and_then(parse_timestamp_value),
    })
}

pub fn parse_independentreserve_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    subscription: &PrivateStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<IndependentReservePrivateStreamMessage> {
    if is_heartbeat(value) {
        return Ok(IndependentReservePrivateStreamMessage::Heartbeat);
    }
    if is_ack(value) {
        return Ok(IndependentReservePrivateStreamMessage::SubscriptionAck {
            channel: channel(value),
        });
    }
    let data = value
        .get("Data")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let events = match subscription.kind {
        PrivateStreamKind::Orders => parse_order(exchange_id, None, data)?
            .map(ExchangeStreamEvent::OrderUpdate)
            .into_iter()
            .collect(),
        PrivateStreamKind::Fills => parse_fills(exchange_id, tenant_id, account_id, None, data)?
            .into_iter()
            .map(ExchangeStreamEvent::Fill)
            .collect(),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => {
            vec![ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                balances: parse_balances(exchange_id, tenant_id, account_id, &[], data)?,
            })]
        }
        PrivateStreamKind::Positions => Vec::new(),
    };
    if events.is_empty() {
        Ok(IndependentReservePrivateStreamMessage::Ignored)
    } else {
        Ok(IndependentReservePrivateStreamMessage::Events(events))
    }
}

fn parse_ws_text(exchange_id: &ExchangeId, text: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(text).map_err(|error| {
        let mut exchange_error = ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            error.to_string(),
            Utc::now(),
        );
        exchange_error.raw = Some(Value::String(text.to_string()));
        rustcta_exchange_api::ExchangeApiError::Exchange(exchange_error)
    })
}

fn is_heartbeat(value: &Value) -> bool {
    channel(value)
        .as_deref()
        .is_some_and(|channel| channel.contains("heartbeat"))
        || value
            .get("event")
            .or_else(|| value.get("Event"))
            .and_then(Value::as_str)
            .is_some_and(|event| {
                matches!(
                    event.to_ascii_lowercase().as_str(),
                    "heartbeat" | "ping" | "pong"
                )
            })
}

fn is_ack(value: &Value) -> bool {
    value
        .get("event")
        .or_else(|| value.get("Event"))
        .and_then(Value::as_str)
        .is_some_and(|event| {
            matches!(
                event.to_ascii_lowercase().as_str(),
                "subscriptions" | "subscribed" | "subscribe" | "subscriptionack" | "ack"
            )
        })
        || value
            .get("success")
            .or_else(|| value.get("Success"))
            .and_then(Value::as_bool)
            .is_some_and(|success| success)
}

fn is_public_orderbook_event(value: &Value) -> bool {
    value
        .get("Event")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .is_some_and(|event| matches!(event, "NewOrder" | "OrderChanged" | "OrderCanceled"))
}

fn channel(value: &Value) -> Option<String> {
    value
        .get("channel")
        .or_else(|| value.get("Channel"))
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .map(|channel| channel.to_ascii_lowercase())
}

fn text_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn parse_stream_error(
    exchange_id: ExchangeId,
    message: &str,
    raw: &Value,
) -> rustcta_exchange_api::ExchangeApiError {
    let mut exchange_error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        message.to_string(),
        Utc::now(),
    );
    exchange_error.raw = Some(raw.clone());
    rustcta_exchange_api::ExchangeApiError::Exchange(exchange_error)
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{
        PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
        RequestContext,
    };
    use rustcta_types::{AccountId, ExchangeId};

    use crate::adapters::independentreserve::parser::symbol_scope;

    use super::{
        independentreserve_check_nonce_continuity, independentreserve_private_stream_capabilities,
        independentreserve_public_order_book_ws_policy,
        independentreserve_public_query_subscription, independentreserve_public_subscribe_payload,
        independentreserve_public_unsubscribe_payload,
        parse_independentreserve_private_stream_message,
        parse_independentreserve_public_stream_message, IndependentReserveNonceContinuity,
        IndependentReserveOrderBookEventKind, IndependentReservePrivateStreamMessage,
        IndependentReservePublicStreamMessage,
    };

    fn public_subscription(exchange: &ExchangeId) -> PublicStreamSubscription {
        PublicStreamSubscription {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(chrono::Utc::now()),
            symbol: symbol_scope(exchange, "XBT_SGD").expect("symbol"),
            kind: PublicStreamKind::OrderBookDelta,
        }
    }

    #[test]
    fn independentreserve_private_capabilities_should_cover_orders_fills_balances() {
        let capabilities = independentreserve_private_stream_capabilities();

        assert!(capabilities.supports_orders);
        assert!(capabilities.supports_fills);
        assert!(capabilities.supports_balances);
        assert!(!capabilities.supports_positions);
    }

    #[test]
    fn independentreserve_public_order_book_policy_should_cover_nonce_resync() {
        let policy = independentreserve_public_order_book_ws_policy();
        assert_eq!(policy.url, "wss://websockets.independentreserve.com");
        assert_eq!(policy.channel_template, "orderbook-{primary}");
        assert_eq!(policy.query_template, "?subscribe=orderbook-{primary}");
        assert_eq!(policy.subscribe_event, "Subscribe");
        assert_eq!(policy.subscribe_data, "[\"orderbook-{primary}\"]");
        assert_eq!(policy.unsubscribe_event, "Unsubscribe");
        assert_eq!(policy.interval_ms, None);
        assert_eq!(policy.depth, None);
        assert_eq!(policy.heartbeat_ms, Some(60_000));
        assert_eq!(policy.sequence_field, "Nonce");
        assert_eq!(policy.checksum, None);
        assert!(policy.update_semantics.contains("NewOrder"));
        assert!(policy.resync.contains("/Public/GetOrderBook"));

        assert_eq!(
            independentreserve_check_nonce_continuity(None, 100),
            IndependentReserveNonceContinuity::First
        );
        assert_eq!(
            independentreserve_check_nonce_continuity(Some(100), 101),
            IndependentReserveNonceContinuity::Continuous
        );
        assert_eq!(
            independentreserve_check_nonce_continuity(Some(100), 103),
            IndependentReserveNonceContinuity::Gap {
                expected: 101,
                actual: 103
            }
        );
        assert!(independentreserve_check_nonce_continuity(Some(100), 99).requires_reconnect());
    }

    #[test]
    fn independentreserve_public_subscriptions_should_match_official_format() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let subscription = public_subscription(&exchange);
        let payload = independentreserve_public_subscribe_payload(&subscription).expect("payload");
        assert_eq!(payload["Event"], "Subscribe");
        assert_eq!(payload["Data"][0], "orderbook-xbt");

        let unsubscribe =
            independentreserve_public_unsubscribe_payload(&subscription).expect("payload");
        assert_eq!(unsubscribe["Event"], "Unsubscribe");
        assert_eq!(unsubscribe["Data"][0], "orderbook-xbt");
        assert_eq!(
            independentreserve_public_query_subscription(&subscription).expect("query"),
            "?subscribe=orderbook-xbt"
        );
    }

    #[test]
    fn independentreserve_public_subscription_ack_fixture_should_parse() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let subscription = public_subscription(&exchange);
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/ws/public_subscriptions.json"
        ))
        .expect("subscriptions fixture");

        let message =
            parse_independentreserve_public_stream_message(&exchange, &subscription, &value)
                .expect("message");
        assert!(matches!(
            message,
            IndependentReservePublicStreamMessage::SubscriptionAck { .. }
        ));
    }

    #[test]
    fn independentreserve_public_order_book_new_order_fixture_should_parse_event() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let subscription = public_subscription(&exchange);
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/ws/public_orderbook_new_order.json"
        ))
        .expect("ws orderbook fixture");
        let message =
            parse_independentreserve_public_stream_message(&exchange, &subscription, &value)
                .expect("message");

        match message {
            IndependentReservePublicStreamMessage::OrderBookEvent(event) => {
                assert_eq!(event.kind, IndependentReserveOrderBookEventKind::NewOrder);
                assert_eq!(event.channel, "orderbook-xbt");
                assert_eq!(event.nonce, 42);
                assert_eq!(
                    event.order_guid.as_deref(),
                    Some("dbe7b832-b9b7-4eac-84ce-9f49c2a93b87")
                );
                assert_eq!(event.client_id.as_deref(), Some("client-42"));
                assert_eq!(event.prices.get("SGD").copied(), Some(2453.0));
                assert_eq!(event.volume, Some(1.0));
                assert!(!event.is_delete());
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn independentreserve_public_order_book_change_and_cancel_should_parse_semantics() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let subscription = public_subscription(&exchange);
        let changed: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/ws/public_orderbook_order_changed.json"
        ))
        .expect("changed fixture");
        let canceled: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/ws/public_orderbook_order_canceled.json"
        ))
        .expect("canceled fixture");

        let changed =
            parse_independentreserve_public_stream_message(&exchange, &subscription, &changed)
                .expect("changed message");
        match changed {
            IndependentReservePublicStreamMessage::OrderBookEvent(event) => {
                assert_eq!(
                    event.kind,
                    IndependentReserveOrderBookEventKind::OrderChanged
                );
                assert_eq!(event.nonce, 43);
                assert_eq!(event.volume, Some(0.25));
                assert!(!event.is_delete());
            }
            other => panic!("unexpected change message: {other:?}"),
        }

        let canceled =
            parse_independentreserve_public_stream_message(&exchange, &subscription, &canceled)
                .expect("canceled message");
        match canceled {
            IndependentReservePublicStreamMessage::OrderBookEvent(event) => {
                assert_eq!(
                    event.kind,
                    IndependentReserveOrderBookEventKind::OrderCanceled
                );
                assert_eq!(event.nonce, 44);
                assert!(event.is_delete());
            }
            other => panic!("unexpected cancel message: {other:?}"),
        }
    }

    #[test]
    fn independentreserve_private_fill_ws_fixture_should_parse_event() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange");
        let tenant = rustcta_exchange_api::TenantId::new("tenant").expect("tenant");
        let account = AccountId::new("account").expect("account");
        let subscription = PrivateStreamSubscription {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext {
                tenant_id: Some(tenant.clone()),
                account_id: Some(account.clone()),
                ..RequestContext::new(chrono::Utc::now())
            },
            exchange: exchange.clone(),
            market_type: Some(rustcta_types::MarketType::Spot),
            account_id: account.clone(),
            kind: PrivateStreamKind::Fills,
        };
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/ws/private_fill.json"
        ))
        .expect("ws fill fixture");

        let message = parse_independentreserve_private_stream_message(
            &exchange,
            tenant,
            account,
            &subscription,
            &value,
        )
        .expect("message");

        match message {
            IndependentReservePrivateStreamMessage::Events(events) => assert_eq!(events.len(), 1),
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
