#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::normalize_bithumb_symbol;
use super::BithumbGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const BITHUMB_WS_PING_INTERVAL_MS: i64 = 30_000;
const BITHUMB_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const BITHUMB_WS_STALE_MESSAGE_MS: i64 = 60_000;

impl BithumbGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bithumb.public_streams_disabled",
            });
        }
        Ok(format!(
            "bithumb:{}:{}",
            self.config.public_ws_url,
            bithumb_public_channel(&subscription)?
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_stream_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bithumb.private_stream_requires_jwt_credentials",
            });
        }
        Ok(format!(
            "bithumb-private:{}:{}",
            self.config.private_ws_url,
            bithumb_private_channel(&subscription)
        ))
    }
}

pub fn bithumb_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![
                PrivateOrderStreamEventKind::New,
                PrivateOrderStreamEventKind::Fill,
                PrivateOrderStreamEventKind::Cancel,
            ],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn bithumb_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "type": bithumb_public_channel(subscription)?,
        "symbols": [legacy_ws_symbol(&subscription.symbol.exchange_symbol.symbol)?],
    }))
}

pub fn bithumb_private_subscribe_payload(subscription: &PrivateStreamSubscription) -> Value {
    json!({
        "type": bithumb_private_channel(subscription),
        "auth": "jwt_bearer_header",
    })
}

pub fn bithumb_ping_payload() -> Value {
    json!({ "type": "ping" })
}

pub fn bithumb_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        BITHUMB_WS_PING_INTERVAL_MS,
        BITHUMB_WS_PONG_TIMEOUT_MS,
        BITHUMB_WS_STALE_MESSAGE_MS,
    )
}

pub fn bithumb_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    Ok(match subscription.kind {
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Trades => "transaction",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbookdepth",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bithumb.websocket_candles",
            })
        }
    })
}

fn bithumb_private_channel(subscription: &PrivateStreamSubscription) -> &'static str {
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "myOrder",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "myAsset",
        PrivateStreamKind::Positions => "unsupportedPositions",
    }
}

fn legacy_ws_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_bithumb_symbol(symbol)?;
    let mut parts = normalized.split('-');
    let quote = parts.next().unwrap_or_default();
    let base = parts.next().unwrap_or_default();
    Ok(format!("{base}_{quote}"))
}
