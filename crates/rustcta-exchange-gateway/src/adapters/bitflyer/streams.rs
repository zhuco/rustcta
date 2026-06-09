#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_product_code;

#[derive(Debug, Clone, PartialEq)]
pub struct BitflyerWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitflyerPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub protocol: &'static str,
    pub snapshot_channel_template: &'static str,
    pub delta_channel_template: &'static str,
    pub fixed_update_interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence_field: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub rest_snapshot_endpoint: &'static str,
    pub resync_strategy: &'static str,
}

pub fn bitflyer_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: false,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn bitflyer_public_order_book_ws_policy() -> BitflyerPublicOrderBookWsPolicy {
    BitflyerPublicOrderBookWsPolicy {
        url: "wss://ws.lightstream.bitflyer.com/json-rpc",
        protocol: "json_rpc_2",
        snapshot_channel_template: "lightning_board_snapshot_{product_code}",
        delta_channel_template: "lightning_board_{product_code}",
        fixed_update_interval_ms: None,
        depth: None,
        sequence_field: None,
        checksum: None,
        rest_snapshot_endpoint: "/v1/getboard",
        resync_strategy: "subscribe to lightning_board_snapshot_{product_code}, then apply lightning_board_{product_code}; on disconnect, stale stream or suspected loss, rebuild from REST GET /v1/getboard and fresh WS snapshot because no sequence/checksum is documented",
    }
}

pub fn public_subscription_spec(
    subscription: &PublicStreamSubscription,
    url: &str,
) -> ExchangeApiResult<BitflyerWsSubscriptionSpec> {
    if !matches!(
        subscription.symbol.market_type,
        MarketType::Spot | MarketType::Perpetual
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitflyer.public_stream.market_type",
        });
    }
    let product = normalize_product_code(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => format!("lightning_board_snapshot_{product}"),
        PublicStreamKind::OrderBookDelta => format!("lightning_board_{product}"),
        PublicStreamKind::Trades => format!("lightning_executions_{product}"),
        PublicStreamKind::Ticker => format!("lightning_ticker_{product}"),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitflyer.public_stream.candles",
            })
        }
    };
    Ok(BitflyerWsSubscriptionSpec {
        url: url.to_string(),
        channel: channel.clone(),
        subscribe_payload: json!({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": { "channel": channel },
        }),
        unsubscribe_payload: json!({
            "jsonrpc": "2.0",
            "method": "unsubscribe",
            "params": { "channel": channel },
        }),
    })
}

pub fn private_auth_payload(api_key: &str, timestamp: &str, nonce: &str, signature: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "auth",
        "params": {
            "api_key": api_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "signature": signature
        }
    })
}

impl super::BitflyerGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_subscription_spec(&subscription, &self.config.websocket_url)?;
        Ok(format!("bitflyer:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitflyer.private_stream.disabled_rest_reconciliation_fallback",
            });
        }
        Ok(format!("bitflyer:private:{}", subscription.account_id))
    }
}
