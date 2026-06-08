#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::blockchaincom_symbol;
use super::signing::blockchaincom_ws_auth_payload;
use super::BlockchainComGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BlockchainComGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.public_streams_disabled",
            });
        }
        Ok(blockchaincom_public_subscribe_payload(&subscription).to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_stream_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blockchaincom.private_streams_require_api_secret",
            });
        }
        Ok(blockchaincom_private_subscribe_payload(
            &subscription,
            self.config.api_secret.as_deref().unwrap_or_default(),
        )?
        .to_string())
    }
}

pub fn blockchaincom_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::Ack,
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

pub fn blockchaincom_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "action": "subscribe",
        "channel": blockchaincom_public_channel(subscription),
        "symbol": blockchaincom_symbol(&subscription.symbol)
    })
}

pub fn blockchaincom_public_unsubscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "action": "unsubscribe",
        "channel": blockchaincom_public_channel(subscription),
        "symbol": blockchaincom_symbol(&subscription.symbol)
    })
}

pub fn blockchaincom_heartbeat_subscribe_payload() -> Value {
    json!({
        "action": "subscribe",
        "channel": "heartbeat"
    })
}

pub fn blockchaincom_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    api_secret: &str,
) -> ExchangeApiResult<Value> {
    let auth = blockchaincom_ws_auth_payload(api_secret)?;
    let channel = match subscription.kind {
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balances",
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "trading",
        PrivateStreamKind::Positions => "trading",
    };
    Ok(json!({
        "auth": auth,
        "subscribe": {
            "action": "subscribe",
            "channel": channel
        },
        "fallback": "rest_reconciliation"
    }))
}

pub fn blockchaincom_sequence_gap_requires_restart(previous: u64, next: u64) -> bool {
    next != previous.saturating_add(1)
}

pub fn blockchaincom_reconnect_policy_ms() -> (u64, u64, u64) {
    (5_000, 15_000, 60_000)
}

fn blockchaincom_public_channel(subscription: &PublicStreamSubscription) -> String {
    match &subscription.kind {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "l2".to_string(),
        PublicStreamKind::Candles { interval } => format!("prices_{interval}"),
    }
}
