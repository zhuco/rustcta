#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::parser::normalize_paymium_symbol;
use super::PaymiumGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl PaymiumGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        normalize_paymium_symbol(&subscription.symbol.exchange_symbol.symbol)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.public_socketio_runtime_unverified",
            });
        }
        Ok(format!(
            "paymium:{}:{}",
            self.config.websocket_public_url,
            paymium_public_stream_name(&subscription.kind)?
        ))
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
            operation: "paymium.user_socket_requires_channel_id_from_private_rest",
        })
    }
}

pub fn paymium_public_socket_descriptor() -> Value {
    json!({
        "url": "https://paymium.com/public",
        "path": "/ws/socket.io",
        "event": "stream"
    })
}

pub fn paymium_user_socket_descriptor(user_channel_id: &str) -> Value {
    json!({
        "url": "https://paymium.com/user",
        "path": "/ws/socket.io",
        "emit": ["channel", user_channel_id],
        "event": "stream"
    })
}

fn paymium_public_stream_name(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    Ok(match kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "bids_asks",
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.public_socketio_candles_unverified",
            })
        }
    })
}
