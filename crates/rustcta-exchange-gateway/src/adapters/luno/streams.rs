#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::LunoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LunoPublicOrderBookWsPolicy {
    pub url_template: &'static str,
    pub auth_required: bool,
    pub auth_payload_fields: [&'static str; 2],
    pub initial_message: &'static str,
    pub update_message: &'static str,
    pub fixed_update_interval_ms: Option<u64>,
    pub sequence_field: &'static str,
    pub sequence_continuity: &'static str,
    pub checksum: Option<&'static str>,
    pub rest_snapshot_endpoint: &'static str,
    pub resync: &'static str,
}

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

pub fn luno_public_order_book_ws_policy() -> LunoPublicOrderBookWsPolicy {
    LunoPublicOrderBookWsPolicy {
        url_template: "wss://ws.luno.com/api/1/stream/{pair}",
        auth_required: true,
        auth_payload_fields: ["api_key_id", "api_key_secret"],
        initial_message: "full_order_book_state",
        update_message: "atomic_order_book_updates",
        fixed_update_interval_ms: None,
        sequence_field: "sequence",
        sequence_continuity: "each update sequence n must be applied to local state n-1; any gap, duplicate, or regression requires state reinitialisation",
        checksum: None,
        rest_snapshot_endpoint: "GET /api/1/orderbook?pair={pair}",
        resync: "close and reconnect the pair stream to receive a fresh full order book; REST /api/1/orderbook can be used as a fallback snapshot before replaying only verified contiguous stream state",
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

pub fn luno_ws_sequence(value: &Value) -> ExchangeApiResult<u64> {
    let sequence = value
        .get("sequence")
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "luno ws message missing sequence".to_string(),
        })?;
    if let Some(sequence) = sequence.as_u64() {
        return Ok(sequence);
    }
    sequence
        .as_str()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "luno ws sequence must be an integer string".to_string(),
        })?
        .parse::<u64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid luno ws sequence: {error}"),
        })
}

pub fn luno_ws_sequence_is_contiguous(previous: Option<u64>, next: u64) -> bool {
    match previous {
        Some(previous) => previous.checked_add(1) == Some(next),
        None => true,
    }
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
