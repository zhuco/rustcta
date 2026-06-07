#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PositionsResponse, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_order, parse_positions};
use super::DigiFinexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::StreamReconnectPolicy;

#[derive(Debug, Clone, PartialEq)]
pub enum DigiFinexPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    SubscriptionAck { id: Option<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DigiFinexPrivateStreamMessage {
    Events(Vec<ExchangeStreamEvent>),
    SubscriptionAck { id: Option<String> },
    Pong,
}

impl DigiFinexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let payload = digifinex_public_subscribe_payload(&subscription, "1")?;
        let channel = payload
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .or_else(|| payload.get("channel").and_then(Value::as_str))
            .unwrap_or("unknown");
        let url = if subscription.symbol.market_type == MarketType::Perpetual {
            &self.config.swap_public_ws_url
        } else {
            &self.config.spot_public_ws_url
        };
        Ok(format!("digifinex:{}:{channel}", url.trim_end_matches('/')))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        self.private_credentials("digifinex.subscribe_private_stream")?;
        let payload = digifinex_private_subscribe_payload(&subscription, market_type, "1")?;
        let channel = payload
            .get("method")
            .and_then(Value::as_str)
            .and_then(|method| method.split_once('.').map(|(channel, _)| channel))
            .or_else(|| payload.get("method").and_then(Value::as_str))
            .or_else(|| payload.get("channel").and_then(Value::as_str))
            .or_else(|| {
                payload
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_str)
            })
            .unwrap_or("unknown");
        let url = if market_type == MarketType::Perpetual {
            &self.config.swap_private_ws_url
        } else {
            &self.config.spot_private_ws_url
        };
        Ok(format!(
            "digifinex:{}:{channel}:{}",
            url.trim_end_matches('/'),
            subscription.account_id
        ))
    }
}

pub fn digifinex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: &str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let symbol = normalize_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                "orderbook"
            } else {
                "depth"
            }
        }
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => "kline",
    };
    if subscription.symbol.market_type == MarketType::Perpetual {
        Ok(json!({
            "id": id,
            "method": format!("{channel}.subscribe"),
            "params": [symbol],
        }))
    } else {
        Ok(json!({
            "id": id,
            "method": format!("{channel}.subscribe"),
            "params": [symbol],
        }))
    }
}

pub fn digifinex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    id: &str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let channel = match (&subscription.kind, market_type) {
        (PrivateStreamKind::Orders, _) => "order",
        (PrivateStreamKind::Fills, _) => "trade",
        (PrivateStreamKind::Balances | PrivateStreamKind::Account, _) => "balance",
        (PrivateStreamKind::Positions, MarketType::Perpetual) => "position",
        (PrivateStreamKind::Positions, MarketType::Spot) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "digifinex.spot_positions_stream",
            })
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "digifinex.private_stream_kind",
            })
        }
    };
    Ok(json!({
        "id": id,
        "method": format!("{channel}.subscribe"),
        "params": [],
    }))
}

pub fn digifinex_ws_ping_payload(market_type: MarketType) -> Value {
    if market_type == MarketType::Perpetual {
        json!({"method": "server.ping", "params": []})
    } else {
        json!({"method": "server.ping", "params": []})
    }
}

pub fn digifinex_stream_reconnect_policy(market_type: MarketType) -> StreamReconnectPolicy {
    StreamReconnectPolicy {
        ping_interval_ms: if market_type == MarketType::Perpetual {
            20_000
        } else {
            25_000
        },
        pong_timeout_ms: 10_000,
        stale_message_ms: 35_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn parse_digifinex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<DigiFinexPublicStreamMessage> {
    if is_pong(value) {
        return Ok(DigiFinexPublicStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(DigiFinexPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_str).map(str::to_string),
        });
    }
    let method = value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if method.contains("depth") || method.contains("orderbook") || value.get("bids").is_some() {
        let data = value
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .unwrap_or(value);
        return Ok(DigiFinexPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol_hint, data)?,
        ));
    }
    Err(ExchangeApiError::Unsupported {
        operation: "digifinex.public_stream_message",
    })
}

pub fn parse_digifinex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<DigiFinexPrivateStreamMessage> {
    if is_pong(value) {
        return Ok(DigiFinexPrivateStreamMessage::Pong);
    }
    if is_ack(value) {
        return Ok(DigiFinexPrivateStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_str).map(str::to_string),
        });
    }
    let method = value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let data = value
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.first())
        .unwrap_or(value);
    if method.contains("balance") {
        return Ok(DigiFinexPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    &[],
                    &json!({ "data": data }),
                )?,
            }),
        ]));
    }
    if method.contains("position") {
        return Ok(DigiFinexPrivateStreamMessage::Events(vec![
            ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &json!({ "data": [data.clone()] }),
                )?,
            }),
        ]));
    }
    if method.contains("order") {
        if let Some(order) = parse_order(exchange_id, symbol_hint.as_ref(), market_type, data)? {
            return Ok(DigiFinexPrivateStreamMessage::Events(vec![
                ExchangeStreamEvent::OrderUpdate(order),
            ]));
        }
    }
    Err(ExchangeApiError::Unsupported {
        operation: "digifinex.private_stream_message",
    })
}

pub fn order_book_stream_event(
    exchange_id: ExchangeId,
    order_book: rustcta_types::OrderBookSnapshot,
) -> ExchangeStreamEvent {
    ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id, None),
        order_book,
    })
}

pub fn heartbeat_event(exchange_id: ExchangeId) -> ExchangeStreamEvent {
    ExchangeStreamEvent::Heartbeat {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id,
        received_at: Utc::now(),
    }
}

fn is_ack(value: &Value) -> bool {
    value.get("result").is_some()
        || value
            .get("code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code == 0)
}

fn is_pong(value: &Value) -> bool {
    value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.eq_ignore_ascii_case("server.pong"))
        || value
            .as_str()
            .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
        || value.get("pong").is_some()
}
