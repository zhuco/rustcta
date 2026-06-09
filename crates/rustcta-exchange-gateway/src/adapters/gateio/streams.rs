#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PublicStreamKind, PublicStreamSubscription,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_gateio_symbol, parse_orderbook_snapshot};
use super::GateIoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub struct GateIoPublicWsSpec {
    pub url: String,
    pub subscribe: Value,
    pub unsubscribe: Value,
    pub channel: &'static str,
    pub interval: String,
    pub depth: Option<String>,
    pub resync_endpoint: &'static str,
}

impl GateIoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "gateio.public_streams_disabled",
            });
        }
        let spec = gateio_public_ws_spec(&self.config, &subscription)?;
        Ok(format!("gateio:{}:{}", spec.url, spec.channel))
    }
}

pub fn gateio_public_ws_spec(
    config: &super::config::GateIoGatewayConfig,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<GateIoPublicWsSpec> {
    let interval = normalize_gateio_order_book_interval(&config.public_order_book_interval)?;
    let channel = gateio_order_book_update_channel(subscription.symbol.market_type)?;
    let depth = (subscription.symbol.market_type == MarketType::Perpetual)
        .then(|| gateio_order_book_depth(&interval).to_string());
    Ok(GateIoPublicWsSpec {
        url: match subscription.symbol.market_type {
            MarketType::Spot => config.spot_public_ws_url.clone(),
            MarketType::Perpetual => config.futures_public_ws_url.clone(),
            _ => unreachable!("checked by adapter"),
        },
        subscribe: gateio_public_subscribe_payload(subscription, &interval)?,
        unsubscribe: gateio_public_unsubscribe_payload(subscription, &interval)?,
        channel,
        interval,
        depth,
        resync_endpoint: match subscription.symbol.market_type {
            MarketType::Spot => "/spot/order_book",
            MarketType::Perpetual => "/futures/usdt/order_book",
            _ => unreachable!("checked by adapter"),
        },
    })
}

pub fn gateio_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    interval: &str,
) -> ExchangeApiResult<Value> {
    gateio_public_payload(subscription, interval, "subscribe")
}

pub fn gateio_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    interval: &str,
) -> ExchangeApiResult<Value> {
    gateio_public_payload(subscription, interval, "unsubscribe")
}

pub fn parse_gateio_public_order_book_event(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<OrderBookSnapshot>> {
    if parse_gateio_control_message(value).is_some() {
        return Ok(None);
    }
    let Some(normalized) = gateio_order_book_result(value) else {
        return Ok(None);
    };
    parse_orderbook_snapshot(exchange_id, symbol, &normalized).map(Some)
}

pub fn parse_gateio_control_message(value: &Value) -> Option<&'static str> {
    match value.get("event").and_then(Value::as_str) {
        Some("subscribe") => Some("subscribed"),
        Some("unsubscribe") => Some("unsubscribed"),
        Some("error") => Some("error"),
        _ => None,
    }
}

fn gateio_public_payload(
    subscription: &PublicStreamSubscription,
    interval: &str,
    event: &'static str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {}
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "gateio.public_stream_kind",
            })
        }
    }
    let interval = normalize_gateio_order_book_interval(interval)?;
    let symbol = normalize_gateio_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = gateio_order_book_update_channel(subscription.symbol.market_type)?;
    let payload = match (subscription.symbol.market_type, event) {
        (MarketType::Spot, _) => json!([symbol, interval]),
        (MarketType::Perpetual, "subscribe") => {
            json!([symbol, interval, gateio_order_book_depth(&interval)])
        }
        (MarketType::Perpetual, _) => json!([symbol, interval]),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "gateio.public_stream_market_type",
            })
        }
    };
    Ok(json!({
        "time": Utc::now().timestamp(),
        "channel": channel,
        "event": event,
        "payload": payload
    }))
}

fn gateio_order_book_update_channel(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("spot.order_book_update"),
        MarketType::Perpetual => Ok("futures.order_book_update"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "gateio.public_stream_market_type",
        }),
    }
}

fn normalize_gateio_order_book_interval(interval: &str) -> ExchangeApiResult<String> {
    let normalized = interval.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "20ms" | "100ms") {
        Ok(normalized)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "gateio order book update interval must be 20ms or 100ms, got {interval}"
            ),
        })
    }
}

fn gateio_order_book_depth(interval: &str) -> &'static str {
    if interval.eq_ignore_ascii_case("20ms") {
        "20"
    } else {
        "100"
    }
}

fn gateio_order_book_result(value: &Value) -> Option<Value> {
    let data = value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let mut object = data.as_object()?.clone();
    if !object.contains_key("bids") {
        if let Some(bids) = object.get("b").cloned() {
            object.insert("bids".to_string(), bids);
        }
    }
    if !object.contains_key("asks") {
        if let Some(asks) = object.get("a").cloned() {
            object.insert("asks".to_string(), asks);
        }
    }
    if !object.contains_key("u") {
        if let Some(sequence) = object.get("id").or_else(|| object.get("U")).cloned() {
            object.insert("u".to_string(), sequence);
        }
    }
    (object.contains_key("bids") && object.contains_key("asks")).then(|| Value::Object(object))
}
