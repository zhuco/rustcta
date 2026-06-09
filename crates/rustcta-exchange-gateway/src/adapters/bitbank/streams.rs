#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_bitbank_pair;

#[derive(Debug, Clone, PartialEq)]
pub struct BitbankWsSubscriptionSpec {
    pub url: String,
    pub room: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbankPublicOrderBookWsPolicy {
    pub snapshot_room_template: &'static str,
    pub delta_room_template: &'static str,
    pub normal_depth_per_side: u32,
    pub fixed_update_interval_ms: Option<u64>,
    pub snapshot_sequence_field: &'static str,
    pub delta_sequence_field: &'static str,
    pub sequence_is_contiguous: bool,
    pub rebuild_strategy: &'static str,
}

pub fn public_order_book_ws_policy() -> BitbankPublicOrderBookWsPolicy {
    BitbankPublicOrderBookWsPolicy {
        snapshot_room_template: "depth_whole_{pair}",
        delta_room_template: "depth_diff_{pair}",
        normal_depth_per_side: 200,
        fixed_update_interval_ms: None,
        snapshot_sequence_field: "sequenceId",
        delta_sequence_field: "s",
        sequence_is_contiguous: false,
        rebuild_strategy: "buffer depth_diff, apply updates with s > depth_whole.sequenceId, resubscribe or REST snapshot on reconnect",
    }
}

pub fn bitbank_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![],
            supports_client_order_id: false,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn public_subscription_spec(
    subscription: &PublicStreamSubscription,
    url: &str,
) -> ExchangeApiResult<BitbankWsSubscriptionSpec> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitbank.public_stream.market_type",
        });
    }
    let pair = normalize_bitbank_pair(&subscription.symbol.exchange_symbol.symbol)?;
    let room = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => format!("depth_whole_{pair}"),
        PublicStreamKind::OrderBookDelta => format!("depth_diff_{pair}"),
        PublicStreamKind::Trades => format!("transactions_{pair}"),
        PublicStreamKind::Ticker => format!("ticker_{pair}"),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.public_stream.candles",
            })
        }
    };
    Ok(BitbankWsSubscriptionSpec {
        url: url.to_string(),
        room: room.clone(),
        subscribe_payload: json!(["join-room", room]),
        unsubscribe_payload: json!(["leave-room", room]),
    })
}

impl super::BitbankGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_subscription_spec(&subscription, &self.config.public_websocket_url)?;
        Ok(format!("bitbank:{}:{}", spec.url, spec.room))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.private_stream.disabled_rest_reconciliation_fallback",
            });
        }
        Ok(format!("bitbank:private:{}", subscription.account_id))
    }
}
