#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_binance_symbol, parse_orderbook_snapshot};
use super::BinanceGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const BINANCE_WS_PING_INTERVAL_MS: i64 = 20_000;
const BINANCE_WS_PONG_TIMEOUT_MS: i64 = 60_000;
const BINANCE_WS_CONNECTION_TTL_MS: i64 = 86_400_000;

impl BinanceGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "binance.public_streams_disabled",
            });
        }
        Ok(binance_public_ws_session(
            &self.exchange_id,
            binance_public_ws_url(&self.config, subscription.symbol.market_type)?,
            &subscription,
        )?
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.unsupported_private("binance.subscribe_private_stream")
    }
}

pub fn binance_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    binance_public_subscription_payload("SUBSCRIBE", subscription)
}

pub fn binance_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    binance_public_subscription_payload("UNSUBSCRIBE", subscription)
}

pub fn binance_public_ws_session(
    exchange_id: &ExchangeId,
    public_ws_url: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "exchange": exchange_id.to_string(),
        "url": public_ws_url,
        "payload": binance_public_subscribe_payload(subscription)?,
        "unsubscribe": binance_public_unsubscribe_payload(subscription)?,
        "heartbeat": binance_ws_heartbeat_spec(),
        "resync": {
            "order_book": "REST depth snapshot + buffered depth deltas; resubscribe after reconnect"
        }
    }))
}

pub fn binance_public_stream_name(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let symbol =
        normalize_binance_symbol(&subscription.symbol.exchange_symbol.symbol)?.to_ascii_lowercase();
    Ok(match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@depth20@100ms"),
        PublicStreamKind::OrderBookDelta => format!("{symbol}@depth@100ms"),
        PublicStreamKind::Ticker => format!("{symbol}@bookTicker"),
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    })
}

pub fn binance_partial_depth_stream_name(
    symbol: &str,
    levels: u32,
    update_speed_ms: u32,
) -> ExchangeApiResult<String> {
    if !matches!(levels, 5 | 10 | 20) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("Binance partial depth levels must be 5, 10, or 20, got {levels}"),
        });
    }
    if !matches!(update_speed_ms, 100 | 1000) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "Binance partial depth update speed must be 100ms or 1000ms, got {update_speed_ms}ms"
            ),
        });
    }
    let symbol = normalize_binance_symbol(symbol)?.to_ascii_lowercase();
    if update_speed_ms == 100 {
        Ok(format!("{symbol}@depth{levels}@100ms"))
    } else {
        Ok(format!("{symbol}@depth{levels}"))
    }
}

pub fn parse_binance_public_depth_message(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("data").unwrap_or(value);
    if is_book_ticker(payload) {
        let snapshot_like = json!({
            "lastUpdateId": payload.get("u").cloned().unwrap_or(Value::Null),
            "bids": [[
                payload.get("b").cloned().unwrap_or(Value::Null),
                payload.get("B").cloned().unwrap_or(Value::Null)
            ]],
            "asks": [[
                payload.get("a").cloned().unwrap_or(Value::Null),
                payload.get("A").cloned().unwrap_or(Value::Null)
            ]]
        });
        return parse_orderbook_snapshot(exchange_id, symbol, &snapshot_like);
    }
    parse_orderbook_snapshot(exchange_id, symbol, payload)
}

pub fn parse_binance_public_depth_event(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::ExchangeStreamEvent> {
    let book = parse_binance_public_depth_message(exchange_id, symbol, value)?;
    Ok(
        rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            order_book: book,
        }),
    )
}

pub fn binance_ws_heartbeat_spec() -> Value {
    json!({
        "mode": "server_ping_frame_client_pong_frame",
        "ping_interval_ms": BINANCE_WS_PING_INTERVAL_MS,
        "pong_timeout_ms": BINANCE_WS_PONG_TIMEOUT_MS,
        "connection_ttl_ms": BINANCE_WS_CONNECTION_TTL_MS,
        "requires_pong_payload_echo": true,
        "resubscribe_on_reconnect": true
    })
}

fn binance_public_subscription_payload(
    method: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    Ok(json!({
        "method": method,
        "params": [binance_public_stream_name(subscription)?],
        "id": 1
    }))
}

fn binance_public_ws_url(
    config: &super::BinanceGatewayConfig,
    market_type: MarketType,
) -> ExchangeApiResult<&str> {
    match market_type {
        MarketType::Spot => Ok(&config.spot_public_ws_url),
        MarketType::Perpetual => Ok(&config.futures_public_ws_url),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "binance.unsupported_market_type",
        }),
    }
}

fn is_book_ticker(payload: &Value) -> bool {
    payload.get("b").is_some()
        && payload.get("B").is_some()
        && payload.get("a").is_some()
        && payload.get("A").is_some()
        && payload.get("bids").is_none()
        && payload.get("asks").is_none()
        && payload.get("b").is_some_and(Value::is_string)
}
