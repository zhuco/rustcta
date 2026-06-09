#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PublicStreamKind, PublicStreamSubscription,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_bitget_symbol, parse_orderbook_snapshot};
use super::BitgetGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub struct BitgetPublicWsSpec {
    pub url: String,
    pub subscribe: Value,
    pub unsubscribe: Value,
    pub channel: String,
    pub resync_endpoint: &'static str,
}

impl BitgetGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitget.public_streams_disabled",
            });
        }
        let spec = bitget_public_ws_spec(&self.config, &subscription)?;
        Ok(format!("bitget:{}:{}", spec.url, spec.channel))
    }
}

pub fn bitget_public_ws_spec(
    config: &super::config::BitgetGatewayConfig,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<BitgetPublicWsSpec> {
    let channel = bitget_public_channel(subscription, &config.public_order_book_channel)?;
    Ok(BitgetPublicWsSpec {
        url: config.public_ws_url.clone(),
        subscribe: bitget_public_subscribe_payload(subscription, &channel)?,
        unsubscribe: bitget_public_unsubscribe_payload(subscription, &channel)?,
        channel,
        resync_endpoint: match subscription.symbol.market_type {
            MarketType::Spot => "/api/v2/spot/market/orderbook",
            MarketType::Perpetual => "/api/v2/mix/market/orderbook",
            _ => unreachable!("checked by adapter"),
        },
    })
}

pub fn bitget_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    channel: &str,
) -> ExchangeApiResult<Value> {
    bitget_public_payload(subscription, channel, "subscribe")
}

pub fn bitget_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    channel: &str,
) -> ExchangeApiResult<Value> {
    bitget_public_payload(subscription, channel, "unsubscribe")
}

pub fn bitget_public_channel(
    subscription: &PublicStreamSubscription,
    configured_order_book_channel: &str,
) -> ExchangeApiResult<String> {
    match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            normalize_bitget_order_book_channel(configured_order_book_channel)
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitget.public_stream_kind",
        }),
    }
}

pub fn parse_bitget_public_order_book_event(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<OrderBookSnapshot>> {
    if parse_bitget_control_message(value).is_some() {
        return Ok(None);
    }
    let has_levels = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .is_some_and(|data| data.get("bids").is_some() && data.get("asks").is_some());
    if !has_levels {
        return Ok(None);
    }
    parse_orderbook_snapshot(exchange_id, symbol, value).map(Some)
}

pub fn parse_bitget_control_message(value: &Value) -> Option<&'static str> {
    if value.as_str() == Some("pong") {
        return Some("pong");
    }
    if value.as_str() == Some("ping") {
        return Some("ping");
    }
    match value.get("event").and_then(Value::as_str) {
        Some("subscribe") => Some("subscribed"),
        Some("unsubscribe") => Some("unsubscribed"),
        Some("error") => Some("error"),
        Some("login") => Some("login"),
        _ => None,
    }
}

pub fn bitget_pong_payload() -> Value {
    json!("pong")
}

fn bitget_public_payload(
    subscription: &PublicStreamSubscription,
    channel: &str,
    op: &'static str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let inst_type = match subscription.symbol.market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual => "USDT-FUTURES",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitget.public_stream_market_type",
            })
        }
    };
    Ok(json!({
        "op": op,
        "args": [{
            "instType": inst_type,
            "channel": normalize_bitget_order_book_channel(channel)?,
            "instId": normalize_bitget_symbol(&subscription.symbol.exchange_symbol.symbol)?,
        }]
    }))
}

fn normalize_bitget_order_book_channel(channel: &str) -> ExchangeApiResult<String> {
    let normalized = channel.trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "books" | "books1" | "books5" | "books15"
    ) {
        Ok(normalized)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "bitget public order book channel must be books/books1/books5/books15, got {channel}"
            ),
        })
    }
}
