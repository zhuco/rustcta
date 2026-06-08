#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::normalize_market_symbol;
use super::BitstampGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BitstampGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitstamp.public_streams_disabled",
            });
        }
        Ok(format!(
            "bitstamp:{}:{}",
            self.config.public_ws_url,
            bitstamp_public_channel(&subscription)?
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
        if !self.config.enabled_private_streams || !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitstamp.private_stream_requires_credentials",
            });
        }
        let value = self
            .send_private_post(
                "bitstamp.websockets_token",
                "/api/v2/websockets_token/",
                &std::collections::HashMap::new(),
            )
            .await?;
        let token = value.get("token").and_then(Value::as_str).ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitstamp websockets_token response missing token".to_string(),
            }
        })?;
        Ok(format!(
            "bitstamp:{}:{}:{}",
            self.config.public_ws_url,
            token,
            bitstamp_private_channel(&subscription.kind)?
        ))
    }
}

pub fn bitstamp_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
        supports_positions: false,
        supports_account: true,
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

pub fn bitstamp_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "bts:subscribe",
        "data": {"channel": bitstamp_public_channel(subscription)?}
    }))
}

pub fn bitstamp_private_subscribe_payload(channel: &str, token: &str) -> Value {
    json!({
        "event": "bts:subscribe",
        "data": {"channel": channel, "auth": token}
    })
}

pub fn bitstamp_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let symbol = normalize_market_symbol(&subscription.symbol.exchange_symbol.symbol);
    Ok(match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => format!("order_book_{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("diff_order_book_{symbol}"),
        PublicStreamKind::Trades => format!("live_trades_{symbol}"),
        PublicStreamKind::Ticker => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitstamp.public_ws_ticker",
            })
        }
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitstamp.public_ws_candles",
            })
        }
    })
}

pub fn bitstamp_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    Ok(match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "private-my_orders",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "private-user",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitstamp.private_ws_positions",
            })
        }
    })
}
