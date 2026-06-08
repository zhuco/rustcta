#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::MangoMarketsGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const MANGO_WS_PING_INTERVAL_MS: i64 = 30_000;
const MANGO_WS_PONG_TIMEOUT_MS: i64 = 45_000;
const MANGO_WS_STALE_MESSAGE_MS: i64 = 90_000;

impl MangoMarketsGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "mango_markets.public_stream_requires_solana_account_subscription_resync",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "mango_markets.private_stream_requires_wallet_owned_account_audit",
        })
    }
}

pub fn mango_markets_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn mango_markets_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    let account = subscription.symbol.exchange_symbol.symbol.trim();
    json!({
        "jsonrpc": "2.0",
        "id": format!("mango-markets-{}", account),
        "method": "accountSubscribe",
        "params": [
            account,
            {
                "encoding": "base64",
                "commitment": "confirmed"
            }
        ],
        "gateway_note": public_kind_note(&subscription.kind)
    })
}

pub fn mango_markets_public_unsubscribe_payload(subscription_id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": format!("mango-markets-unsubscribe-{}", subscription_id),
        "method": "accountUnsubscribe",
        "params": [subscription_id]
    })
}

pub fn mango_markets_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        MANGO_WS_PING_INTERVAL_MS,
        MANGO_WS_PONG_TIMEOUT_MS,
        MANGO_WS_STALE_MESSAGE_MS,
    )
}

fn public_kind_note(kind: &PublicStreamKind) -> &'static str {
    match kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "fixture-only; real order book requires book side and event queue account mapping"
        }
        PublicStreamKind::Trades => "fixture-only; real trades require event queue decoding",
        PublicStreamKind::Ticker => "fixture-only; real ticker requires oracle/account decoding",
        PublicStreamKind::Candles { .. } => "unsupported; Mango v4 does not expose native candles",
    }
}
