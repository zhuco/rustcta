#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PublicStreamKind, PublicStreamSubscription,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookLevel, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::normalize_mexc_symbol_for_market;
use super::MexcGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};

const MEXC_SPOT_PUBLIC_WS_URL: &str = "wss://wbs-api.mexc.com/ws";
const MEXC_CONTRACT_PUBLIC_WS_URL: &str = "wss://contract.mexc.com/edge";

#[derive(Debug, Clone, PartialEq)]
pub struct MexcWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MexcBookTicker {
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub symbol: String,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
}

impl MexcGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        let spec = mexc_public_subscription_spec(&subscription)?;
        Ok(format!("mexc:{}:{}", spec.url, spec.channel))
    }
}

pub fn mexc_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    match subscription.symbol.market_type {
        MarketType::Spot => mexc_spot_public_subscription_spec(subscription),
        MarketType::Perpetual => mexc_contract_public_subscription_spec(subscription),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "mexc.public_stream.market_type",
        }),
    }
}

pub fn mexc_ping_payload() -> Value {
    json!({ "method": "PING" })
}

pub fn parse_mexc_spot_aggre_depth_delta(
    exchange_id: &ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let payload = value
        .get("publicAggreDepths")
        .or_else(|| value.get("publicaggredepths"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws depth parser requires canonical_symbol".to_string(),
            })?;
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    );
    delta.bids = parse_delta_levels(
        payload
            .get("bids")
            .or_else(|| payload.get("b"))
            .or_else(|| payload.get("bid")),
    )?;
    delta.asks = parse_delta_levels(
        payload
            .get("asks")
            .or_else(|| payload.get("a"))
            .or_else(|| payload.get("ask")),
    )?;
    delta.first_sequence = first_u64(payload, &["fromVersion", "fromversion", "from_version"]);
    delta.last_sequence = first_u64(
        payload,
        &["toVersion", "toversion", "to_version", "version"],
    );
    delta.exchange_timestamp = first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
        .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn parse_mexc_spot_limit_depth_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value
        .get("publicLimitDepths")
        .or_else(|| value.get("publiclimitdepths"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws limit-depth parser requires canonical_symbol".to_string(),
            })?;
    let bids = parse_snapshot_levels(
        payload
            .get("bids")
            .or_else(|| payload.get("b"))
            .or_else(|| payload.get("bid")),
    )?;
    let asks = parse_snapshot_levels(
        payload
            .get("asks")
            .or_else(|| payload.get("a"))
            .or_else(|| payload.get("ask")),
    )?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = first_u64(payload, &["version", "toVersion", "toversion"]);
    snapshot.exchange_timestamp = first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
        .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_mexc_spot_book_ticker(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<MexcBookTicker> {
    let payload = value
        .get("publicBookTicker")
        .or_else(|| value.get("publicbookticker"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let symbol = first_string(value, &["symbol", "s"])
        .or_else(|| first_string(payload, &["symbol", "s"]))
        .unwrap_or_default();
    Ok(MexcBookTicker {
        exchange_id: exchange_id.clone(),
        market_type,
        symbol,
        bid_price: required_f64(payload, &["bidPrice", "bidprice", "bid_price", "b"])?,
        bid_quantity: required_f64(payload, &["bidQuantity", "bidquantity", "bid_qty", "B"])?,
        ask_price: required_f64(payload, &["askPrice", "askprice", "ask_price", "a"])?,
        ask_quantity: required_f64(payload, &["askQuantity", "askquantity", "ask_qty", "A"])?,
        sequence: first_u64(payload, &["version", "toVersion", "toversion"]),
        exchange_timestamp: first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
            .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    })
}

fn mexc_spot_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    let symbol = normalize_mexc_symbol_for_market(
        &subscription.symbol.exchange_symbol.symbol,
        MarketType::Spot,
    )?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => {
            format!("spot@public.aggre.deals.v3.api.pb@100ms@{symbol}")
        }
        PublicStreamKind::Ticker => {
            format!("spot@public.aggre.bookTicker.v3.api.pb@10ms@{symbol}")
        }
        PublicStreamKind::OrderBookDelta => {
            format!("spot@public.aggre.depth.v3.api.pb@10ms@{symbol}")
        }
        PublicStreamKind::OrderBookSnapshot => {
            format!("spot@public.limit.depth.v3.api.pb@{symbol}@20")
        }
        PublicStreamKind::Candles { interval } => {
            format!("spot@public.kline.v3.api.pb@{symbol}@{interval}")
        }
    };
    Ok(json_subscription(
        MEXC_SPOT_PUBLIC_WS_URL.to_string(),
        channel,
    ))
}

fn mexc_contract_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    let symbol = normalize_mexc_symbol_for_market(
        &subscription.symbol.exchange_symbol.symbol,
        MarketType::Perpetual,
    )?;
    let (method, param) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta => {
            ("sub.depth", json!({ "symbol": symbol, "compress": true }))
        }
        PublicStreamKind::OrderBookSnapshot => {
            ("sub.depth.full", json!({ "symbol": symbol, "limit": 20 }))
        }
        PublicStreamKind::Ticker => ("sub.ticker", json!({ "symbol": symbol })),
        PublicStreamKind::Trades => ("sub.deal", json!({ "symbol": symbol })),
        PublicStreamKind::Candles { interval } => (
            "sub.kline",
            json!({ "symbol": symbol, "interval": interval }),
        ),
    };
    Ok(MexcWsSubscriptionSpec {
        url: MEXC_CONTRACT_PUBLIC_WS_URL.to_string(),
        channel: method.to_string(),
        subscribe_payload: json!({ "method": method, "param": param }),
        unsubscribe_payload: json!({ "method": method.replacen("sub.", "unsub.", 1), "param": param }),
    })
}

fn json_subscription(url: String, channel: String) -> MexcWsSubscriptionSpec {
    MexcWsSubscriptionSpec {
        url,
        channel: channel.clone(),
        subscribe_payload: json!({
            "method": "SUBSCRIPTION",
            "params": [channel],
        }),
        unsubscribe_payload: json!({
            "method": "UNSUBSCRIPTION",
            "params": [channel],
        }),
    }
}

fn parse_delta_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc ws depth message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
}

fn parse_snapshot_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc ws depth message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn level_price_quantity(value: &Value) -> ExchangeApiResult<(f64, f64)> {
    if let Some(array) = value.as_array() {
        let price =
            value_to_f64(array.first()).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws level missing price".to_string(),
            })?;
        let quantity =
            value_to_f64(array.get(1)).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws level missing quantity".to_string(),
            })?;
        return Ok((price, quantity));
    }
    let price = required_f64(value, &["price", "p"])?;
    let quantity = required_f64(value, &["quantity", "q", "qty", "v"])?;
    Ok((price, quantity))
}

fn required_f64(value: &Value, keys: &[&str]) -> ExchangeApiResult<f64> {
    value_to_f64(keys.iter().find_map(|key| value.get(*key))).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("mexc ws message missing numeric field {keys:?}"),
        }
    })
}

fn value_to_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn first_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str).map(str::to_string))
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
