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

use super::parser::{independentreserve_asset, parse_orderbook_snapshot, split_symbol_assets};
use super::private_parser::{parse_balances, parse_fills, parse_order};
use super::IndependentReserveGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub enum IndependentReservePublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Heartbeat,
    OrderBook(OrderBookResponse),
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

fn independentreserve_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let (base, quote) = split_symbol_assets(&subscription.symbol.exchange_symbol.symbol);
    let channel = match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbook",
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => {
            return Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
                operation: "independentreserve.candle_stream",
            })
        }
    };
    Ok(json!({
        "event": "subscribe",
        "channel": channel,
        "primaryCurrencyCode": independentreserve_asset(&base),
        "secondaryCurrencyCode": independentreserve_asset(&quote),
    }))
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
    if channel(value)
        .as_deref()
        .is_some_and(|channel| channel.contains("orderbook") || channel.contains("book"))
        || value.get("BuyOrders").is_some()
        || value.get("bids").is_some()
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
            .is_some_and(|event| matches!(event.to_ascii_lowercase().as_str(), "ping" | "pong"))
}

fn is_ack(value: &Value) -> bool {
    value
        .get("event")
        .or_else(|| value.get("Event"))
        .and_then(Value::as_str)
        .is_some_and(|event| {
            matches!(
                event.to_ascii_lowercase().as_str(),
                "subscribed" | "subscribe" | "subscriptionack" | "ack"
            )
        })
        || value
            .get("success")
            .or_else(|| value.get("Success"))
            .and_then(Value::as_bool)
            .is_some_and(|success| success)
}

fn channel(value: &Value) -> Option<String> {
    value
        .get("channel")
        .or_else(|| value.get("Channel"))
        .or_else(|| value.get("topic"))
        .and_then(Value::as_str)
        .map(|channel| channel.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{PrivateStreamKind, PrivateStreamSubscription, RequestContext};
    use rustcta_types::{AccountId, ExchangeId};

    use super::{
        independentreserve_private_stream_capabilities,
        parse_independentreserve_private_stream_message, IndependentReservePrivateStreamMessage,
    };

    #[test]
    fn independentreserve_private_capabilities_should_cover_orders_fills_balances() {
        let capabilities = independentreserve_private_stream_capabilities();

        assert!(capabilities.supports_orders);
        assert!(capabilities.supports_fills);
        assert!(capabilities.supports_balances);
        assert!(!capabilities.supports_positions);
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
