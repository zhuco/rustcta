#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::parser::{normalize_depth, normalize_p2b_symbol};
use super::P2bGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const P2B_PUBLIC_WS_URL: &str = "wss://apiws.p2pb2b.com/";
pub const P2B_PUBLIC_DEPTH_DEFAULT_LIMIT: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub channel: &'static str,
    pub supported_limits: std::ops::RangeInclusive<u32>,
    pub default_limit: u32,
    pub interval_ms: u64,
    pub full_refresh_interval_ms: u64,
    pub sequence: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub resync: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2bDepthUpdateKind {
    Full,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bDepthLevel {
    pub price: String,
    pub quantity: String,
}

impl P2bDepthLevel {
    pub fn is_delete(&self) -> bool {
        self.quantity
            .parse::<f64>()
            .map(|quantity| quantity == 0.0)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bDepthUpdate {
    pub kind: P2bDepthUpdateKind,
    pub symbol: String,
    pub bids: Vec<P2bDepthLevel>,
    pub asks: Vec<P2bDepthLevel>,
}

impl P2bDepthUpdate {
    pub fn is_full(&self) -> bool {
        self.kind == P2bDepthUpdateKind::Full
    }

    pub fn deleted_bid_prices(&self) -> Vec<&str> {
        self.bids
            .iter()
            .filter(|level| level.is_delete())
            .map(|level| level.price.as_str())
            .collect()
    }

    pub fn deleted_ask_prices(&self) -> Vec<&str> {
        self.asks
            .iter()
            .filter(|level| level.is_delete())
            .map(|level| level.price.as_str())
            .collect()
    }
}

impl P2bGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let payload = p2b_public_depth_subscribe_payload(
            &subscription,
            Some(P2B_PUBLIC_DEPTH_DEFAULT_LIMIT),
            1,
        )?;
        Ok(format!(
            "p2b:{P2B_PUBLIC_WS_URL}:{}:{}",
            payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            payload
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "p2b.private_streams_unverified",
        })
    }
}

pub fn p2b_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn p2b_public_order_book_ws_policy() -> P2bPublicOrderBookWsPolicy {
    P2bPublicOrderBookWsPolicy {
        url: P2B_PUBLIC_WS_URL,
        channel: "depth.subscribe",
        supported_limits: 1..=100,
        default_limit: P2B_PUBLIC_DEPTH_DEFAULT_LIMIT,
        interval_ms: 1_000,
        full_refresh_interval_ms: 60_000,
        sequence: None,
        checksum: None,
        update_semantics: "depth.update params are [full, {bids, asks}, market]; full=true rebuilds the book, full=false is a best-effort changed-level update; amount 0 deletes the price level",
        resync: "fetch REST /api/v2/public/book buy/sell snapshot on connect or reconnect, then resubscribe; because no sequence/checksum is documented, treat stale connections or suspected loss as requiring REST rebuild",
    }
}

pub fn p2b_public_depth_subscribe_payload(
    subscription: &PublicStreamSubscription,
    limit: Option<u32>,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "depth.subscribe",
        PublicStreamKind::Trades => {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.public_ws_trades_out_of_scope",
            });
        }
        PublicStreamKind::Ticker => {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.public_ws_ticker_out_of_scope",
            });
        }
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.public_ws_candles_out_of_scope",
            });
        }
    };
    Ok(json!({
        "method": method,
        "params": [
            normalize_p2b_symbol(&subscription.symbol.exchange_symbol.symbol)?,
            normalize_depth(limit),
            "0"
        ],
        "id": id
    }))
}

pub fn p2b_public_depth_unsubscribe_payload(id: u64) -> Value {
    json!({
        "method": "depth.unsubscribe",
        "params": [],
        "id": id
    })
}

pub fn p2b_ping_payload(id: u64) -> Value {
    json!({
        "method": "server.ping",
        "params": [],
        "id": id
    })
}

pub fn parse_p2b_depth_update(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<P2bDepthUpdate> {
    if value
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| *method == "depth.update")
        .is_none()
    {
        return Err(parse_error(
            exchange_id.clone(),
            "P2B websocket message is not depth.update",
            value,
        ));
    }
    let params = value
        .get("params")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "P2B depth.update missing params array",
                value,
            )
        })?;
    let full = params.first().and_then(Value::as_bool).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B depth.update missing full flag",
            value,
        )
    })?;
    let levels = params.get(1).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B depth.update missing level payload",
            value,
        )
    })?;
    let symbol = params
        .get(2)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "P2B depth.update missing market",
                value,
            )
        })
        .and_then(normalize_p2b_symbol)?;
    Ok(P2bDepthUpdate {
        kind: if full {
            P2bDepthUpdateKind::Full
        } else {
            P2bDepthUpdateKind::Incremental
        },
        symbol,
        bids: parse_depth_levels(exchange_id, levels.get("bids"))?,
        asks: parse_depth_levels(exchange_id, levels.get("asks"))?,
    })
}

pub fn p2b_rest_reconciliation_fallback() -> &'static str {
    "private REST request-spec-only fallback over /api/v2/orders and /api/v2/account/market_deals; disabled until read-only credentials are verified"
}

pub fn p2b_public_order_book_rebuild_strategy() -> &'static str {
    "public order book rebuild: call REST /api/v2/public/book twice with side=buy and side=sell, offset=0, limit=1..100; reconnect/resubscribe and rebuild after disconnect, stale stream, or any suspected missed depth.update because P2B publishes no sequence/checksum"
}

fn parse_depth_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<P2bDepthLevel>> {
    let Some(levels) = levels else {
        return Ok(Vec::new());
    };
    let levels = levels.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B depth.update side levels must be arrays",
            levels,
        )
    })?;
    levels
        .iter()
        .map(|level| parse_depth_level(exchange_id, level))
        .collect()
}

fn parse_depth_level(exchange_id: &ExchangeId, level: &Value) -> ExchangeApiResult<P2bDepthLevel> {
    if let Some(array) = level.as_array() {
        let price = array
            .first()
            .and_then(value_as_string)
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid P2B depth price", level))?;
        let quantity = array
            .get(1)
            .and_then(value_as_string)
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid P2B depth quantity", level))?;
        return Ok(P2bDepthLevel { price, quantity });
    }
    let price = level
        .get("price")
        .or_else(|| level.get("rate"))
        .and_then(value_as_string)
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid P2B depth price", level))?;
    let quantity = level
        .get("amount")
        .or_else(|| level.get("quantity"))
        .or_else(|| level.get("volume"))
        .or_else(|| level.get("left"))
        .and_then(value_as_string)
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid P2B depth quantity", level))?;
    Ok(P2bDepthLevel { price, quantity })
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn parse_error(exchange: ExchangeId, message: impl Into<String>, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}
