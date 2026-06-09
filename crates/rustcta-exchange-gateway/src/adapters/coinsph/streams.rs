#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PublicStreamKind, PublicStreamSubscription, SymbolScope,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookLevel, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{
    normalize_coinsph_symbol, parse_error, parse_orderbook_snapshot, validation_error,
};
use super::CoinsPhGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};

pub const COINSPH_PUBLIC_WS_URL: &str = "wss://wsapi.pro.coins.ph/openapi/quote/ws/v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinsPhPublicOrderBookWsPolicy {
    pub book_ticker_stream_template: &'static str,
    pub partial_depth_stream_template: &'static str,
    pub partial_depth_levels: &'static [u32],
    pub partial_depth_intervals_ms: &'static [u64],
    pub diff_depth_stream_template: &'static str,
    pub diff_depth_intervals_ms: &'static [u64],
    pub rest_snapshot_endpoint: &'static str,
    pub rest_snapshot_limits: &'static [u32],
    pub snapshot_sequence_field: &'static str,
    pub first_update_field: &'static str,
    pub final_update_field: &'static str,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub resync: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinsPhWsSubscriptionSpec {
    pub url: &'static str,
    pub stream: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinsPhPublicStreamMessage {
    BookTicker(OrderBookSnapshot),
    PartialDepth(OrderBookSnapshot),
    DiffDepth(OrderBookDelta),
    SubscriptionAck,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoinsPhDepthReplayDecision {
    Stale,
    FirstApplicable,
    Contiguous,
    Gap,
}

impl CoinsPhGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let spec = coinsph_public_subscription_spec(&subscription)?;
        Ok(format!("coinsph:{}:{}", spec.url, spec.stream))
    }
}

pub fn coinsph_public_order_book_ws_policy() -> CoinsPhPublicOrderBookWsPolicy {
    CoinsPhPublicOrderBookWsPolicy {
        book_ticker_stream_template: "{symbol}@bookTicker",
        partial_depth_stream_template: "{symbol}@depth{levels}@{interval_ms}ms",
        partial_depth_levels: &[5, 10, 20, 200],
        partial_depth_intervals_ms: &[100, 1000],
        diff_depth_stream_template: "{symbol}@depth@{interval_ms}ms",
        diff_depth_intervals_ms: &[100, 1000],
        rest_snapshot_endpoint: "GET /openapi/quote/v1/depth",
        rest_snapshot_limits: &[5, 10, 20, 50, 100],
        snapshot_sequence_field: "lastUpdateId",
        first_update_field: "U",
        final_update_field: "u",
        checksum: None,
        update_semantics: "absolute quantity at changed price levels; quantity 0 removes the level",
        resync: "buffer diff depth events, fetch REST snapshot, drop events where u <= lastUpdateId, apply the first event satisfying U <= lastUpdateId + 1 <= u, then require next U == previous u + 1; reconnect and rebuild on gaps",
    }
}

pub fn coinsph_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<CoinsPhWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinsph.public_stream.non_spot_market",
        });
    }
    let stream = match &subscription.kind {
        PublicStreamKind::Ticker => coinsph_book_ticker_stream(&subscription.symbol)?,
        PublicStreamKind::OrderBookSnapshot => {
            coinsph_partial_depth_stream(&subscription.symbol, 20, 100)?
        }
        PublicStreamKind::OrderBookDelta => coinsph_diff_depth_stream(&subscription.symbol, 100)?,
        PublicStreamKind::Trades => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinsph.public_stream.trades_not_in_orderbook_scope",
            });
        }
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinsph.public_stream.candles_not_in_orderbook_scope",
            });
        }
    };
    Ok(CoinsPhWsSubscriptionSpec {
        url: COINSPH_PUBLIC_WS_URL,
        subscribe_payload: coinsph_subscribe_payload(&stream, 1),
        unsubscribe_payload: coinsph_unsubscribe_payload(&stream, 1),
        stream,
    })
}

pub fn coinsph_book_ticker_stream(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    Ok(format!(
        "{}@bookTicker",
        normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?.to_ascii_lowercase()
    ))
}

pub fn coinsph_partial_depth_stream(
    symbol: &SymbolScope,
    levels: u32,
    interval_ms: u64,
) -> ExchangeApiResult<String> {
    let levels = match levels {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        _ => 200,
    };
    let interval_ms = if levels == 200 { 1000 } else { interval_ms };
    let interval_ms = match interval_ms {
        0..=100 => 100,
        _ => 1000,
    };
    Ok(format!(
        "{}@depth{}@{}ms",
        normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?.to_ascii_lowercase(),
        levels,
        interval_ms
    ))
}

pub fn coinsph_diff_depth_stream(
    symbol: &SymbolScope,
    interval_ms: u64,
) -> ExchangeApiResult<String> {
    let interval_ms = match interval_ms {
        0..=100 => 100,
        _ => 1000,
    };
    Ok(format!(
        "{}@depth@{}ms",
        normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?.to_ascii_lowercase(),
        interval_ms
    ))
}

pub fn coinsph_subscribe_payload(stream: &str, id: u64) -> Value {
    json!({
        "method": "SUBSCRIBE",
        "params": [stream],
        "id": id,
    })
}

pub fn coinsph_unsubscribe_payload(stream: &str, id: u64) -> Value {
    json!({
        "method": "UNSUBSCRIBE",
        "params": [stream],
        "id": id,
    })
}

pub fn parse_coinsph_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinsPhPublicStreamMessage> {
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(CoinsPhPublicStreamMessage::SubscriptionAck);
    }
    let payload = value.get("data").unwrap_or(value);
    let event_type = payload
        .get("e")
        .and_then(Value::as_str)
        .or_else(|| event_type_from_stream(value))
        .unwrap_or_default();
    if event_type.eq_ignore_ascii_case("bookTicker") || event_type.ends_with("@bookTicker") {
        return parse_coinsph_book_ticker(exchange_id, symbol, payload)
            .map(CoinsPhPublicStreamMessage::BookTicker);
    }
    if payload.get("U").is_some() || event_type.eq_ignore_ascii_case("depthUpdate") {
        return parse_coinsph_depth_delta(exchange_id, &symbol, payload)
            .map(CoinsPhPublicStreamMessage::DiffDepth);
    }
    if payload.get("lastUpdateId").is_some() || event_type.contains("@depth") {
        return parse_orderbook_snapshot(exchange_id, symbol, payload)
            .map(CoinsPhPublicStreamMessage::PartialDepth);
    }
    Ok(CoinsPhPublicStreamMessage::Ignored)
}

pub fn parse_coinsph_book_ticker(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Coins.ph bookTicker parser requires canonical_symbol".to_string(),
            })?;
    let bid = OrderBookLevel::new(
        number_from_fields(exchange_id, value, &["b", "bidPrice"])?,
        number_from_fields(exchange_id, value, &["B", "bidQty", "bidQuantity"])?,
    )
    .map_err(validation_error)?;
    let ask = OrderBookLevel::new(
        number_from_fields(exchange_id, value, &["a", "askPrice"])?,
        number_from_fields(exchange_id, value, &["A", "askQty", "askQuantity"])?,
    )
    .map_err(validation_error)?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        vec![bid],
        vec![ask],
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = first_u64(value, &["u", "lastUpdateId"]);
    snapshot.exchange_timestamp =
        first_i64(value, &["E", "T", "timestamp"]).and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_coinsph_depth_delta(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Coins.ph diff-depth parser requires canonical_symbol".to_string(),
            })?;
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        Utc::now(),
    );
    delta.bids = parse_delta_levels(exchange_id, value.get("b").or_else(|| value.get("bids")))?;
    delta.asks = parse_delta_levels(exchange_id, value.get("a").or_else(|| value.get("asks")))?;
    delta.first_sequence = first_u64(value, &["U", "firstUpdateId"]);
    delta.last_sequence = first_u64(value, &["u", "lastUpdateId", "finalUpdateId"]);
    delta.exchange_timestamp =
        first_i64(value, &["E", "T", "timestamp"]).and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn coinsph_depth_replay_decision(
    snapshot_last_update_id: u64,
    previous_applied_u: Option<u64>,
    first_update_id: u64,
    final_update_id: u64,
) -> CoinsPhDepthReplayDecision {
    if final_update_id <= snapshot_last_update_id {
        return CoinsPhDepthReplayDecision::Stale;
    }
    if let Some(previous_applied_u) = previous_applied_u {
        if first_update_id == previous_applied_u + 1 {
            CoinsPhDepthReplayDecision::Contiguous
        } else {
            CoinsPhDepthReplayDecision::Gap
        }
    } else if first_update_id <= snapshot_last_update_id + 1
        && final_update_id >= snapshot_last_update_id + 1
    {
        CoinsPhDepthReplayDecision::FirstApplicable
    } else {
        CoinsPhDepthReplayDecision::Gap
    }
}

fn event_type_from_stream(value: &Value) -> Option<&str> {
    value.get("stream").and_then(Value::as_str)
}

fn parse_delta_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid Coins.ph delta level", level)
            })?;
            let price = array
                .first()
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid delta price", level))?;
            let quantity = array
                .get(1)
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid delta quantity", level))?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
}

fn number_from_fields(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_f64))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "missing numeric bookTicker field",
                value,
            )
        })
}

fn first_u64(value: &Value, fields: &[&str]) -> Option<u64> {
    fields
        .iter()
        .find_map(|field| value.get(*field))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn first_i64(value: &Value, fields: &[&str]) -> Option<i64> {
    fields
        .iter()
        .find_map(|field| value.get(*field))
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}
