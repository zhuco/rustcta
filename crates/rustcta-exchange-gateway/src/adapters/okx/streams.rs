#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_okx_symbol_for_market, parse_orderbook_snapshot};
use super::OkxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const OKX_WS_IDLE_TIMEOUT_MS: i64 = 30_000;
const OKX_WS_RECOMMENDED_PING_MS: i64 = 25_000;

impl OkxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: self.profile_operation(
                    "okx.public_streams_disabled",
                    "okxus.public_streams_disabled",
                    "myokx.public_streams_disabled",
                ),
            });
        }
        Ok(
            okx_public_ws_session(&self.exchange_id, &self.config.public_ws_url, &subscription)?
                .to_string(),
        )
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.unsupported_private(self.profile_operation(
            "okx.subscribe_private_stream",
            "okxus.subscribe_private_stream",
            "myokx.subscribe_private_stream",
        ))
    }
}

pub fn okx_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    okx_public_subscription_payload("subscribe", subscription)
}

pub fn okx_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    okx_public_subscription_payload("unsubscribe", subscription)
}

pub fn okx_public_ws_session(
    exchange_id: &ExchangeId,
    public_ws_url: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "exchange": exchange_id.to_string(),
        "url": public_ws_url,
        "payload": okx_public_subscribe_payload(subscription)?,
        "unsubscribe": okx_public_unsubscribe_payload(subscription)?,
        "heartbeat": okx_ws_heartbeat_spec(),
        "resync": {
            "order_book": "REST /api/v5/market/books snapshot after reconnect or seqId gap"
        }
    }))
}

pub fn okx_public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => "books5".to_string(),
        PublicStreamKind::OrderBookDelta => "books".to_string(),
        PublicStreamKind::Ticker => "bbo-tbt".to_string(),
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Candles { interval } => format!("candle{interval}"),
    }
}

pub fn parse_okx_public_orderbook_message(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").cloned().unwrap_or_else(|| value.clone());
    parse_orderbook_snapshot(exchange_id, symbol, &data)
}

pub fn parse_okx_public_orderbook_event(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::ExchangeStreamEvent> {
    let book = parse_okx_public_orderbook_message(exchange_id, symbol, value)?;
    Ok(
        rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book: book,
        }),
    )
}

pub fn okx_ws_heartbeat_spec() -> Value {
    json!({
        "mode": "application_ping_pong",
        "client_ping": "ping",
        "server_pong": "pong",
        "idle_timeout_ms": OKX_WS_IDLE_TIMEOUT_MS,
        "recommended_client_ping_interval_ms": OKX_WS_RECOMMENDED_PING_MS,
        "resubscribe_on_reconnect": true
    })
}

fn okx_public_subscription_payload(
    op: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    Ok(json!({
        "op": op,
        "args": [{
            "channel": okx_public_channel(subscription),
            "instId": normalize_okx_symbol_for_market(
                &subscription.symbol.exchange_symbol.symbol,
                subscription.symbol.market_type
            )?
        }]
    }))
}
