#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_market_id, parse_orderbook_snapshot, symbol_from_market_id};
use super::private_parser::{parse_fill, parse_order_state};
use super::signing::websocket_subscribe_payload;
use super::BtcMarketsGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub enum BtcMarketsPublicStreamMessage {
    SubscriptionAck,
    Heartbeat,
    OrderBook(OrderBookResponse),
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BtcMarketsPrivateStreamMessage {
    SubscriptionAck,
    Heartbeat,
    Events(Vec<ExchangeStreamEvent>),
    Ignored,
}

#[derive(Debug, Clone)]
pub struct BtcMarketsWsSession {
    pub url: String,
    pub subscribe_payload: Value,
}

impl BtcMarketsGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let session = self.public_ws_session(subscription)?;
        Ok(format!(
            "btcmarkets-ws:{}:{}",
            session.url, session.subscribe_payload
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("btcmarkets.subscribe_private_stream")?;
        let session = self.private_ws_session(subscription)?;
        Ok(format!(
            "btcmarkets-ws:{}:{}",
            session.url, session.subscribe_payload
        ))
    }

    pub fn public_ws_session(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<BtcMarketsWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let channel = match subscription.kind {
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "orderbook",
            PublicStreamKind::Trades => "trade",
            PublicStreamKind::Ticker => "tick",
            PublicStreamKind::Candles { .. } => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "btcmarkets.ws_candles",
                });
            }
        };
        let payload = websocket_subscribe_payload(
            None,
            None,
            "",
            vec![channel.to_string()],
            vec![normalize_market_id(
                &subscription.symbol.exchange_symbol.symbol,
            )?],
        )?;
        Ok(BtcMarketsWsSession {
            url: self.config.ws_url.clone(),
            subscribe_payload: payload,
        })
    }

    pub fn private_ws_session(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<BtcMarketsWsSession> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let channels = match subscription.kind {
            PrivateStreamKind::Orders | PrivateStreamKind::Fills => vec!["orderChange".to_string()],
            PrivateStreamKind::Balances | PrivateStreamKind::Account => {
                vec!["fundChange".to_string()]
            }
            PrivateStreamKind::Positions => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "btcmarkets.ws_positions",
                });
            }
        };
        let market_ids = subscription
            .market_type
            .filter(|market_type| *market_type == MarketType::Spot)
            .map(|_| Vec::new())
            .unwrap_or_default();
        let payload = websocket_subscribe_payload(
            self.config.api_key.as_deref(),
            self.config.api_secret.as_deref(),
            &Utc::now().timestamp_millis().to_string(),
            channels,
            market_ids,
        )?;
        Ok(BtcMarketsWsSession {
            url: self.config.ws_url.clone(),
            subscribe_payload: payload,
        })
    }
}

pub fn btcmarkets_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
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

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<BtcMarketsPublicStreamMessage> {
    match value.get("messageType").and_then(Value::as_str) {
        Some("heartbeat") => Ok(BtcMarketsPublicStreamMessage::Heartbeat),
        Some("subscribe") | Some("subscribed") => {
            Ok(BtcMarketsPublicStreamMessage::SubscriptionAck)
        }
        Some("orderbook") => {
            let market_id = value
                .get("marketId")
                .and_then(Value::as_str)
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "btcmarkets ws orderbook missing marketId".to_string(),
                })?;
            let symbol = symbol_from_market_id(exchange_id, market_id)?;
            Ok(BtcMarketsPublicStreamMessage::OrderBook(
                OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                    order_book: parse_orderbook_snapshot(exchange_id, symbol, value)?,
                },
            ))
        }
        _ => Ok(BtcMarketsPublicStreamMessage::Ignored),
    }
}

pub fn parse_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<BtcMarketsPrivateStreamMessage> {
    match value.get("messageType").and_then(Value::as_str) {
        Some("heartbeat") => Ok(BtcMarketsPrivateStreamMessage::Heartbeat),
        Some("subscribe") | Some("subscribed") => {
            Ok(BtcMarketsPrivateStreamMessage::SubscriptionAck)
        }
        Some("orderChange") => {
            let mut events = vec![ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                None,
                value,
            )?)];
            for trade in value
                .get("trades")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let mut enriched = trade.clone();
                if let Some(object) = enriched.as_object_mut() {
                    for field in ["marketId", "side", "orderId", "clientOrderId"] {
                        if let Some(parent) = value.get(field) {
                            object
                                .entry(field.to_string())
                                .or_insert_with(|| parent.clone());
                        }
                    }
                }
                events.push(ExchangeStreamEvent::Fill(parse_fill(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    None,
                    &enriched,
                )?));
            }
            Ok(BtcMarketsPrivateStreamMessage::Events(events))
        }
        Some("fundChange") => Ok(BtcMarketsPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            },
        ])),
        _ => Ok(BtcMarketsPrivateStreamMessage::Ignored),
    }
}

pub fn heartbeat_payload() -> Value {
    json!({"messageType": "heartbeat"})
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{AccountId, ExchangeStreamEvent, TenantId};
    use rustcta_types::ExchangeId;
    use serde_json::Value;

    use super::{
        parse_private_stream_message, parse_public_stream_message, BtcMarketsPrivateStreamMessage,
        BtcMarketsPublicStreamMessage,
    };

    fn fixture(name: &str) -> Value {
        let text = match name {
            "ws_orderbook.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/ws_orderbook.json")
            }
            "ws_order_change.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/btcmarkets/ws_order_change.json"
            ),
            _ => panic!("unknown btcmarkets ws fixture {name}"),
        };
        serde_json::from_str(text).expect("btcmarkets ws fixture")
    }

    #[test]
    fn public_orderbook_event_should_parse_snapshot() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let message =
            parse_public_stream_message(&exchange, &fixture("ws_orderbook.json")).unwrap();
        match message {
            BtcMarketsPublicStreamMessage::OrderBook(book) => {
                assert_eq!(book.order_book.bids.len(), 2);
            }
            other => panic!("unexpected message {other:?}"),
        }
    }

    #[test]
    fn private_order_change_should_emit_order_and_fill_events() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let message = parse_private_stream_message(
            &exchange,
            TenantId::new("tenant-a").unwrap(),
            AccountId::new("acct-a").unwrap(),
            &fixture("ws_order_change.json"),
        )
        .unwrap();
        match message {
            BtcMarketsPrivateStreamMessage::Events(events) => {
                assert!(matches!(events[0], ExchangeStreamEvent::OrderUpdate(_)));
                assert!(events
                    .iter()
                    .any(|event| matches!(event, ExchangeStreamEvent::Fill(_))));
            }
            other => panic!("unexpected message {other:?}"),
        }
    }
}
