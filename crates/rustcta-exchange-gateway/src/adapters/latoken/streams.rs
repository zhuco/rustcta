#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderBookSnapshot;
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, split_symbol};
use super::signing::{ws_auth_signature, LatokenDigest};
use super::LatokenGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl LatokenGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "latoken.public_streams_stomp_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "latoken.private_streams_user_id_auth_unverified",
        })
    }
}

pub fn latoken_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn public_book_destination(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
    Ok(format!("/v1/book/{base}/{quote}"))
}

pub fn public_trade_destination(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
    Ok(format!("/v1/trade/{base}/{quote}"))
}

pub fn stomp_subscribe_frame(subscription_id: &str, destination: &str) -> String {
    format!("SUBSCRIBE\nid:{subscription_id}\ndestination:{destination}\n\n\u{0}")
}

pub fn stomp_unsubscribe_frame(subscription_id: &str) -> String {
    format!("UNSUBSCRIBE\nid:{subscription_id}\n\n\u{0}")
}

pub fn ws_auth_headers(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: &str,
) -> ExchangeApiResult<Value> {
    let signature = ws_auth_signature(api_secret, timestamp_ms, LatokenDigest::Sha256)?;
    Ok(json!({
        "X-LA-APIKEY": api_key,
        "X-LA-DIGEST": "HMAC-SHA256",
        "X-LA-SIGNATURE": signature,
        "X-LA-SIGDATA": timestamp_ms
    }))
}

pub fn parse_public_book_message(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("payload").unwrap_or(value);
    parse_orderbook_snapshot(exchange_id, symbol, 1000, payload)
}

pub fn rest_reconciliation_fallback() -> &'static str {
    "private state remains REST request-spec-only: /v2/auth/order/active and /v2/auth/trade are the documented reconciliation paths after a separate private REST validation task"
}

pub fn heartbeat_policy() -> &'static str {
    "LATOKEN STOMP docs do not publish a fixed heartbeat interval; runtime should reconnect and resubscribe when nonce sequence gaps are detected"
}
