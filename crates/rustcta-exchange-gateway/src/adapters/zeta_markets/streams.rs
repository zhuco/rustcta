#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::ZetaMarketsGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

const ZETA_MARKETS_REST_RECONCILE_INTERVAL_MS: i64 = 30_000;

impl ZetaMarketsGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "zeta_markets.public_streams_require_solana_sdk_account_mapping",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "zeta_markets.private_streams_require_wallet_owned_margin_account",
        })
    }
}

pub fn zeta_markets_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn zeta_markets_rest_reconciliation_payload(symbol: &str) -> Value {
    json!({
        "exchange": "zeta_markets",
        "mode": "rest_reconciliation",
        "symbol": symbol.trim().to_ascii_uppercase(),
        "orderbook_path": format!("/v2/orderbook?ticker_id={}", symbol.trim().to_ascii_uppercase()),
        "interval_ms": ZETA_MARKETS_REST_RECONCILE_INTERVAL_MS,
        "stream_runtime": "unsupported"
    })
}

pub fn zeta_markets_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        ZETA_MARKETS_REST_RECONCILE_INTERVAL_MS,
        ZETA_MARKETS_REST_RECONCILE_INTERVAL_MS * 2,
        ZETA_MARKETS_REST_RECONCILE_INTERVAL_MS * 3,
    )
}
