#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_market_symbol, parse_orderbook_snapshot};
use super::BitstampGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitstampPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub protocol: &'static str,
    pub snapshot_channel_template: &'static str,
    pub delta_channel_template: &'static str,
    pub fixed_update_interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence_field: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub rest_snapshot_endpoint: &'static str,
    pub order_data_gap_recovery_endpoint: &'static str,
    pub resync_strategy: &'static str,
}

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

pub fn bitstamp_public_order_book_ws_policy() -> BitstampPublicOrderBookWsPolicy {
    BitstampPublicOrderBookWsPolicy {
        url: "wss://ws.bitstamp.net",
        protocol: "websocket_v2_json",
        snapshot_channel_template: "order_book_{market_symbol}",
        delta_channel_template: "diff_order_book_{market_symbol}",
        fixed_update_interval_ms: None,
        depth: None,
        sequence_field: None,
        checksum: None,
        rest_snapshot_endpoint: "GET /api/v2/order_book/{market_symbol}/",
        order_data_gap_recovery_endpoint: "POST /api/v2/order_data/",
        resync_strategy: "rebuild from REST order_book snapshot and use public order_data with market/since_id/until_id for WebSocket gap recovery; reconnect or rebuild on stale stream because public WS order_book/diff_order_book has no documented sequence or checksum",
    }
}

impl BitstampPublicOrderBookWsPolicy {
    pub fn as_json(&self) -> Value {
        json!({
            "url": self.url,
            "protocol": self.protocol,
            "channels": {
                "snapshot": self.snapshot_channel_template,
                "delta": self.delta_channel_template,
            },
            "fixed_update_interval_ms": self.fixed_update_interval_ms,
            "depth": self.depth,
            "sequence_field": self.sequence_field,
            "checksum": self.checksum,
            "resync": {
                "rest_snapshot_endpoint": self.rest_snapshot_endpoint,
                "order_data_gap_recovery_endpoint": self.order_data_gap_recovery_endpoint,
                "strategy": self.resync_strategy,
            }
        })
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

pub fn parse_bitstamp_public_order_book_message(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitstamp public WS message missing channel".to_string(),
        })?;
    if !channel.starts_with("order_book_") && !channel.starts_with("diff_order_book_") {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitstamp.public_ws_non_order_book_channel",
        });
    }
    let data = value
        .get("data")
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitstamp public order book WS message missing data".to_string(),
        })?;
    parse_orderbook_snapshot(exchange_id, symbol, None, data)
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
