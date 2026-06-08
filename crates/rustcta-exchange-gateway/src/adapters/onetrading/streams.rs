#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::onetrading_symbol;
use super::OneTradingGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl OneTradingGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "onetrading.public_streams_disabled",
            });
        }
        Ok(format!(
            "onetrading:{}:{}",
            self.config.public_ws_url,
            onetrading_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation:
                "onetrading.private_streams_use_rest_reconciliation_until_ws_auth_is_verified",
        })
    }
}

pub fn onetrading_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn onetrading_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "SUBSCRIBE",
        "channels": [
            {
                "name": onetrading_public_channel(subscription),
                "instrument_codes": [onetrading_symbol(&subscription.symbol)]
            }
        ]
    })
}

pub fn onetrading_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "UNSUBSCRIBE",
        "channels": [
            {
                "name": onetrading_public_channel(subscription),
                "instrument_codes": [onetrading_symbol(&subscription.symbol)]
            }
        ]
    })
}

pub fn onetrading_private_auth_payload(api_token: &str) -> Value {
    json!({
        "type": "AUTHENTICATE",
        "api_token": api_token
    })
}

pub fn onetrading_ping_payload() -> Value {
    json!({ "type": "PING" })
}

pub fn onetrading_reconnect_policy_ms() -> (u64, u64, u64) {
    (30_000, 45_000, 60_000)
}

fn onetrading_public_channel(subscription: &PublicStreamSubscription) -> &'static str {
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "ORDER_BOOK",
        PublicStreamKind::Ticker => "BOOK_TICKER",
        PublicStreamKind::Trades => "TRADING",
        PublicStreamKind::Candles { .. } => "PRICE_TICKS",
    }
}
