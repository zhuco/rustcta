#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::normalize_aster_symbol;
use super::AsterGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl AsterGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "aster.public_streams_disabled",
            });
        }
        Ok(format!(
            "aster:{}:{}",
            self.config.public_ws_url,
            aster_public_stream_name(&subscription)?
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_perpetual(market_type)?;
        }
        if !self.config.enabled_private_streams || !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "aster.private_stream_requires_v3_api_wallet",
            });
        }
        let listen_key = self.create_listen_key().await?;
        Ok(format!(
            "aster:{}:{}:{}",
            self.config.private_ws_url,
            listen_key,
            aster_private_channel(&subscription.kind)
        ))
    }

    async fn create_listen_key(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(
                "aster.create_listen_key",
                "/fapi/v3/listenKey",
                &HashMap::new(),
            )
            .await?;
        value
            .get("listenKey")
            .or_else(|| value.get("listen_key"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster listenKey response missing listenKey".to_string(),
            })
    }
}

pub fn aster_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn aster_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "SUBSCRIBE",
        "params": [aster_public_stream_name(subscription)?],
        "id": 1
    }))
}

pub fn aster_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "UNSUBSCRIBE",
        "params": [aster_public_stream_name(subscription)?],
        "id": 1
    }))
}

pub fn aster_ping_payload() -> Value {
    json!({ "method": "LIST_SUBSCRIPTIONS", "id": 1 })
}

pub fn aster_public_stream_name(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let symbol =
        normalize_aster_symbol(&subscription.symbol.exchange_symbol.symbol)?.to_ascii_lowercase();
    Ok(match &subscription.kind {
        PublicStreamKind::Trades => format!("{symbol}@aggTrade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::OrderBookDelta => format!("{symbol}@depth"),
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@depth20"),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    })
}

pub fn aster_private_channel(kind: &PrivateStreamKind) -> &'static str {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "ORDER_TRADE_UPDATE",
        PrivateStreamKind::Balances | PrivateStreamKind::Positions | PrivateStreamKind::Account => {
            "ACCOUNT_UPDATE"
        }
    }
}
