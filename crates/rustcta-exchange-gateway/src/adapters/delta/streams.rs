#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::parser::normalize_delta_symbol;
use super::signing::sign_ws_auth;
use super::DeltaGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl DeltaGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_stream {
            return Err(ExchangeApiError::Unsupported {
                operation: "delta.subscribe_public_stream",
            });
        }
        let payload = delta_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "delta:{}:{}",
            self.config.public_ws_url,
            serde_json::to_string(&payload).unwrap_or_default()
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        if !self.config.private_stream_available() {
            return self.unsupported_private("delta.subscribe_private_stream");
        }
        let auth = self.delta_private_auth_payload()?;
        let subscribe = delta_private_subscribe_payload(&subscription);
        Ok(format!(
            "delta:{}:{}:{}",
            self.config.private_ws_url,
            serde_json::to_string(&auth).unwrap_or_default(),
            serde_json::to_string(&subscribe).unwrap_or_default()
        ))
    }

    pub fn delta_private_auth_payload(&self) -> ExchangeApiResult<Value> {
        let credentials = self.private_credentials("delta.private_ws_auth")?;
        let timestamp = Utc::now().timestamp().to_string();
        Ok(json!({
            "type": "key-auth",
            "payload": {
                "api-key": credentials.api_key,
                "timestamp": timestamp.parse::<i64>().unwrap_or_default(),
                "signature": sign_ws_auth(&credentials.api_secret, &timestamp),
            }
        }))
    }
}

fn delta_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let channel = match subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "l2_orderbook",
        PublicStreamKind::Trades => "all_trades",
        PublicStreamKind::Ticker => "v2/ticker",
        PublicStreamKind::Candles { .. } => "candlestick_1m",
    };
    Ok(json!({
        "type": "subscribe",
        "payload": {
            "channels": [{
                "name": channel,
                "symbols": [normalize_delta_symbol(&subscription.symbol)?]
            }]
        }
    }))
}

fn delta_private_subscribe_payload(subscription: &PrivateStreamSubscription) -> Value {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "v2/user_trades",
        PrivateStreamKind::Balances => "wallet",
        PrivateStreamKind::Positions => "positions",
        PrivateStreamKind::Account => "v2/user_trades",
    };
    json!({
        "type": "subscribe",
        "payload": {
            "channels": [{
                "name": channel,
                "symbols": ["all"]
            }]
        }
    })
}
