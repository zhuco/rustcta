#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};

use super::BitteamGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BitteamGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitteam.public_streams_unverified",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitteam.private_streams_unverified",
        })
    }
}

pub fn bitteam_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn bitteam_rest_reconciliation_fallback() -> &'static str {
    "private REST read-only fallback over /trade/api/ccxt/order, /trade/api/ccxt/ordersOfUser, and /trade/api/ccxt/tradesOfUser; guarded by BITTEAM_PRIVATE_REST_ENABLED plus Basic auth credentials"
}
