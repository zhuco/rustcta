#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::ParadexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl ParadexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Ok(format!(
            "paradex:{}:{}",
            self.config.public_ws_url,
            paradex_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_streams_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: "paradex.private_streams_require_jwt_or_stark_auth",
            });
        }
        Ok(format!(
            "paradex:{}:{}",
            self.config.private_ws_url,
            paradex_private_channel(&subscription.kind)?
        ))
    }
}

pub fn paradex_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: true,
            supports_account: false,
            order_event_kinds: Vec::new(),
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn paradex_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": { "channel": paradex_public_channel(subscription) },
        "id": 1
    })
}

pub fn paradex_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "unsubscribe",
        "params": { "channel": paradex_public_channel(subscription) },
        "id": 1
    })
}

pub fn paradex_private_subscribe_payload(kind: &PrivateStreamKind) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": { "channel": paradex_private_channel(kind)? },
        "id": 1
    }))
}

pub fn paradex_private_unsubscribe_payload(kind: &PrivateStreamKind) -> ExchangeApiResult<Value> {
    Ok(json!({
        "jsonrpc": "2.0",
        "method": "unsubscribe",
        "params": { "channel": paradex_private_channel(kind)? },
        "id": 1
    }))
}

pub fn paradex_ping_payload() -> Value {
    json!({ "jsonrpc": "2.0", "method": "ping", "id": 1 })
}

pub fn paradex_heartbeat_policy_ms() -> (i64, i64, i64) {
    (30_000, 10_000, 60_000)
}

pub fn paradex_public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription
        .symbol
        .exchange_symbol
        .symbol
        .trim()
        .to_ascii_uppercase();
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("order_book.{symbol}")
        }
        PublicStreamKind::Trades => format!("trades.{symbol}"),
        PublicStreamKind::Ticker => format!("markets_summary.{symbol}"),
        PublicStreamKind::Candles { interval } => format!("bbo.{symbol}.{interval}"),
    }
}

pub fn paradex_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders => Ok("user.orders"),
        PrivateStreamKind::Fills => Ok("user.trades"),
        PrivateStreamKind::Balances => Ok("user.balance"),
        PrivateStreamKind::Positions => Ok("user.positions"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "paradex.private_stream_kind",
        }),
    }
}
