#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_market_symbol;
use super::UpbitGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl UpbitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let payload = upbit_public_subscribe_payload(&subscription, "rustcta")?;
        Ok(format!(
            "upbit:{}:{}",
            self.config.public_ws_url,
            payload[1]["type"].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_streams("upbit.subscribe_private_stream")?;
        if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "upbit.private_ws_market_type",
            });
        }
        let payload = upbit_private_subscribe_payload(&subscription, "rustcta")?;
        Ok(format!(
            "upbit:{}:{}:{}",
            self.config.private_ws_url,
            payload[1]["type"].as_str().unwrap_or("unknown"),
            subscription.account_id
        ))
    }
}

pub fn upbit_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn upbit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    ticket: &str,
) -> ExchangeApiResult<Value> {
    let market = normalize_market_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let stream_type = match &subscription.kind {
        PublicStreamKind::Trades => "trade",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "orderbook",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "upbit.public_ws_candles",
            });
        }
    };
    Ok(json!([
        {"ticket": ticket},
        {"type": stream_type, "codes": [market], "is_only_realtime": true},
        {"format": "DEFAULT"}
    ]))
}

pub fn upbit_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    ticket: &str,
) -> ExchangeApiResult<Value> {
    let stream_type = match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "myOrder",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "myAsset",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "upbit.private_ws_positions",
            });
        }
    };
    Ok(json!([
        {"ticket": ticket},
        {"type": stream_type},
        {"format": "DEFAULT"}
    ]))
}

pub fn upbit_heartbeat_policy_ms() -> (u64, u64) {
    (30_000, 90_000)
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpbitStreamMessage {
    Snapshot(Value),
    Realtime(Value),
    Error {
        name: Option<String>,
        message: Option<String>,
    },
    Other(Value),
}

pub fn parse_upbit_stream_message(value: &Value) -> UpbitStreamMessage {
    if let Some(error) = value.get("error") {
        return UpbitStreamMessage::Error {
            name: error
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string),
        };
    }
    match value.get("stream_type").and_then(Value::as_str) {
        Some("SNAPSHOT") => UpbitStreamMessage::Snapshot(value.clone()),
        Some("REALTIME") => UpbitStreamMessage::Realtime(value.clone()),
        _ => UpbitStreamMessage::Other(value.clone()),
    }
}
