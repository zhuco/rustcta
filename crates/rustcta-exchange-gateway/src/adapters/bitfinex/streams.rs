use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, TenantId};
use serde_json::{json, Value};

use super::parser::{normalize_bitfinex_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_order, parse_positions};
use super::signing::bitfinex_ws_auth_signature;
use super::BitfinexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const BITFINEX_WS_HEARTBEAT_TIMEOUT_SECONDS: u64 = 30;

impl BitfinexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.public_streams_disabled",
            });
        }
        let payload = bitfinex_public_subscribe_payload(&subscription)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.public_ws_url,
            "payload": payload,
            "unsubscribe": bitfinex_public_unsubscribe_payload(&subscription)?,
            "heartbeat": bitfinex_ws_heartbeat_spec(),
            "resync": {
                "order_book": "rest_snapshot_after_reconnect_or_sequence_gap"
            }
        })
        .to_string())
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
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.private_streams_disabled",
            });
        }
        let (api_key, api_secret) = self.private_credentials("bitfinex.private_ws_auth")?;
        let nonce = Utc::now().timestamp_micros().to_string();
        let auth = bitfinex_private_auth_payload(api_key, api_secret, &nonce);
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.private_ws_url,
            "mode": "auth_channel_zero",
            "initial_payloads": [auth],
            "stream_filter": bitfinex_private_filter(&subscription),
            "heartbeat": bitfinex_ws_heartbeat_spec(),
            "reconciliation": "REST wallets/positions/orders/trades after reconnect"
        })
        .to_string())
    }
}

pub fn bitfinex_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn bitfinex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "subscribe",
        "channel": bitfinex_public_channel(&subscription.kind)?,
        "symbol": normalize_bitfinex_symbol(
            &subscription.symbol.exchange_symbol.symbol,
            subscription.symbol.market_type
        )?,
        "prec": if matches!(subscription.kind, PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot) { "P0" } else { "" },
        "freq": if matches!(subscription.kind, PublicStreamKind::OrderBookDelta) { "F0" } else { "" },
    }))
}

pub fn bitfinex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "unsubscribe",
        "channel": bitfinex_public_channel(&subscription.kind)?,
        "symbol": normalize_bitfinex_symbol(
            &subscription.symbol.exchange_symbol.symbol,
            subscription.symbol.market_type
        )?,
    }))
}

pub fn bitfinex_private_auth_payload(api_key: &str, api_secret: &str, nonce: &str) -> Value {
    json!({
        "event": "auth",
        "apiKey": api_key,
        "authNonce": nonce,
        "authPayload": format!("AUTH{nonce}"),
        "authSig": bitfinex_ws_auth_signature(api_secret, nonce),
    })
}

pub fn bitfinex_ws_heartbeat_spec() -> Value {
    json!({
        "server_heartbeat": "hb",
        "server_event": "heartbeat",
        "timeout_seconds": BITFINEX_WS_HEARTBEAT_TIMEOUT_SECONDS,
        "resubscribe_on_reconnect": true,
    })
}

pub fn parse_bitfinex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if is_heartbeat(value) || is_subscription_event(value) {
        return Ok(None);
    }
    let Some(array) = value.as_array() else {
        return Ok(None);
    };
    let Some(payload) = array.get(1) else {
        return Ok(None);
    };
    if payload.as_str() == Some("hb") {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    if payload
        .as_array()
        .is_some_and(|rows| rows.first().is_some_and(Value::is_array))
    {
        let book = parse_orderbook_snapshot(exchange_id, symbol_hint, payload)?;
        return Ok(Some(ExchangeStreamEvent::OrderBookSnapshot(
            rustcta_exchange_api::OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                order_book: book,
            },
        )));
    }
    Ok(None)
}

pub enum BitfinexPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Balance(rustcta_exchange_api::BalancesResponse),
    Position(rustcta_exchange_api::PositionsResponse),
    Heartbeat,
    Ignored,
}

pub fn parse_bitfinex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type_hint: MarketType,
    value: &Value,
) -> ExchangeApiResult<BitfinexPrivateStreamMessage> {
    if is_heartbeat(value) || is_subscription_event(value) {
        return Ok(BitfinexPrivateStreamMessage::Ignored);
    }
    let Some(array) = value.as_array() else {
        return Ok(BitfinexPrivateStreamMessage::Ignored);
    };
    if array.get(1).and_then(Value::as_str) == Some("hb") {
        return Ok(BitfinexPrivateStreamMessage::Heartbeat);
    }
    let event = array.get(1).and_then(Value::as_str).unwrap_or_default();
    let payload = array.get(2).unwrap_or(&Value::Null);
    match event {
        "on" | "ou" | "oc" => Ok(BitfinexPrivateStreamMessage::Order(parse_order(
            exchange_id,
            market_type_hint,
            payload,
        )?)),
        "ws" | "wu" => Ok(BitfinexPrivateStreamMessage::Balance(
            rustcta_exchange_api::BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type_hint,
                    &json!([payload.clone()]),
                )?,
            },
        )),
        "ps" | "pu" | "pc" => Ok(BitfinexPrivateStreamMessage::Position(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &json!([payload.clone()]),
                )?,
            },
        )),
        _ => Ok(BitfinexPrivateStreamMessage::Ignored),
    }
}

fn bitfinex_public_channel(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok("book"),
        PublicStreamKind::Trades => Ok("trades"),
        PublicStreamKind::Ticker => Ok("ticker"),
        PublicStreamKind::Candles { .. } => Ok("candles"),
    }
}

fn bitfinex_private_filter(subscription: &PrivateStreamSubscription) -> Value {
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Positions => {
            json!(["trading"])
        }
        PrivateStreamKind::Balances | PrivateStreamKind::Account => json!(["wallet", "balance"]),
    }
}

fn is_heartbeat(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("heartbeat")
}

fn is_subscription_event(value: &Value) -> bool {
    matches!(
        value.get("event").and_then(Value::as_str),
        Some("subscribed" | "unsubscribed" | "info" | "auth")
    )
}
