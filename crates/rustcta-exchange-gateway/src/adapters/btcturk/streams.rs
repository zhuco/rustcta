#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::BtcTurkGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BtcTurkGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.public_streams_disabled",
            });
        }
        Ok(format!(
            "btcturk:{}:{}",
            self.config.public_ws_url,
            btcturk_public_channel(&subscription)
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
                operation: "btcturk.private_streams_require_ws_api_key",
            });
        }
        Ok(format!("btcturk:{}", self.config.private_ws_url))
    }
}

pub fn btcturk_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn btcturk_subscribe_payload(channel: &str, pair_symbol: &str) -> Value {
    json!([151, {
        "type": 151,
        "channel": channel,
        "event": pair_symbol.trim().replace(['/', '-', '_'], "").to_ascii_uppercase(),
        "join": true
    }])
}

pub fn btcturk_ws_auth_payload(
    public_key: &str,
    timestamp_ms: i64,
    nonce: i64,
    signature: &str,
) -> Value {
    json!([114, {
        "type": 114,
        "publicKey": public_key,
        "timestamp": timestamp_ms,
        "nonce": nonce,
        "signature": signature
    }])
}

fn btcturk_public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trade".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "orderbook".to_string()
        }
        PublicStreamKind::Candles { interval } => format!("ohlc:{interval}"),
    }
}
