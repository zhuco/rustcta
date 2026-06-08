#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{hibachi_order_book_channel, hibachi_trade_channel};
use super::HibachiGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const HIBACHI_WS_PING_INTERVAL_MS: i64 = 30_000;
const HIBACHI_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const HIBACHI_WS_STALE_MESSAGE_MS: i64 = 90_000;

impl HibachiGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.public_stream_runtime_spec_only",
            });
        }
        Ok(format!(
            "hibachi:{}:{}",
            self.config.public_ws_url,
            hibachi_public_subscribe_payload(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "hibachi.private_stream_runtime_spec_only",
        })
    }
}

pub fn hibachi_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn hibachi_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "method": "subscribe",
        "channel": public_channel(subscription),
    })
}

pub fn hibachi_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "method": "unsubscribe",
        "channel": public_channel(subscription),
    })
}

pub fn hibachi_keepalive_payload() -> Value {
    json!({ "method": "ping" })
}

pub fn hibachi_private_account_ws_url(base_url: &str, account_id: &str) -> String {
    format!("{}?accountId={account_id}", base_url.trim_end_matches('/'))
}

pub fn hibachi_stream_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        HIBACHI_WS_PING_INTERVAL_MS,
        HIBACHI_WS_PONG_TIMEOUT_MS,
        HIBACHI_WS_STALE_MESSAGE_MS,
    )
}

fn public_channel(subscription: &PublicStreamSubscription) -> String {
    let symbol = subscription.symbol.exchange_symbol.symbol.as_str();
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            hibachi_order_book_channel(symbol)
        }
        PublicStreamKind::Trades => hibachi_trade_channel(symbol),
        PublicStreamKind::Ticker => format!("prices/{symbol}"),
        PublicStreamKind::Candles { interval } => format!("klines/{symbol}/{interval}"),
    }
}
