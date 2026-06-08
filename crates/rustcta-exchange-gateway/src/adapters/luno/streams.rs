#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::LunoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl LunoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "luno.public_streams_disabled",
            });
        }
        Ok(format!(
            "luno:{}/{}:{}",
            self.config.public_ws_url,
            super::parser::luno_symbol(&subscription.symbol),
            luno_public_channel(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams || !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "luno.private_streams_require_basic_auth",
            });
        }
        Ok(format!("luno:{}", self.config.private_ws_url))
    }
}

pub fn luno_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
        supports_positions: false,
        supports_account: false,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
        ],
        supports_client_order_id: false,
        supports_exchange_order_id: true,
    }
}

pub fn luno_stream_url(base_ws_url: &str, pair: &str) -> String {
    format!(
        "{}/{}",
        base_ws_url.trim_end_matches('/'),
        pair.trim()
            .replace(['/', '-', '_'], "")
            .to_ascii_uppercase()
    )
}

pub fn luno_ws_auth_payload(api_key_id: &str, api_key_secret: &str) -> Value {
    json!({
        "api_key_id": api_key_id,
        "api_key_secret": api_key_secret
    })
}

fn luno_public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "orderbook".to_string()
        }
        PublicStreamKind::Candles { interval } => format!("candles:{interval}"),
    }
}
