#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};

use super::BtcboxGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BtcboxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "btcbox.public_streams.unsupported_no_official_ws",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        Err(ExchangeApiError::Unsupported {
            operation: "btcbox.private_streams.unsupported_no_official_ws",
        })
    }
}

pub fn btcbox_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn btcbox_rest_reconciliation_fallback() -> &'static str {
    "No official BTCBOX WebSocket API is verified; private reconciliation is REST-only after a future read-only private REST validation task."
}
