use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_bybit_symbol, parse_orderbook_snapshot};
use super::signing::sign_ws_auth;
use super::BybitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BybitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let payload = bybit_public_subscribe_payload(&subscription)?;
        let url = bybit_public_ws_url(&self.config.public_ws_url, subscription.symbol.market_type);
        Ok(format!(
            "{}:{}:{}",
            self.exchange_id,
            url,
            payload["args"][0].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let operation = self.profile_operation(
            "bybit.subscribe_private_stream",
            "bybiteu.subscribe_private_stream",
        );
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let expires = Utc::now().timestamp_millis() + 60_000;
        let auth = bybit_private_auth_payload(api_key, api_secret, expires)?;
        let subscribe = bybit_private_subscribe_payload(&subscription)?;
        Ok(format!(
            "{}:{}:{}:{}",
            self.exchange_id,
            self.config.private_ws_url,
            auth["op"].as_str().unwrap_or("auth"),
            subscribe["args"][0].as_str().unwrap_or("unknown")
        ))
    }
}

pub fn bybit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let symbol = normalize_bybit_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let topic = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!(
                "orderbook.{}.{symbol}",
                bybit_public_orderbook_depth(subscription.symbol.market_type, &subscription.kind)
            )
        }
        PublicStreamKind::Trades => format!("publicTrade.{symbol}"),
        PublicStreamKind::Ticker => format!("tickers.{symbol}"),
        PublicStreamKind::Candles { interval } => format!("kline.{interval}.{symbol}"),
    };
    Ok(json!({
        "op": "subscribe",
        "args": [topic]
    }))
}

pub fn bybit_public_ws_url(configured_url: &str, market_type: MarketType) -> String {
    let product = match market_type {
        MarketType::Spot => "spot",
        MarketType::Option => "option",
        MarketType::Futures | MarketType::Perpetual => "linear",
        MarketType::Margin => "spot",
    };
    replace_bybit_public_product(configured_url, product)
}

fn replace_bybit_public_product(configured_url: &str, product: &str) -> String {
    let marker = "/v5/public/";
    let Some((prefix, suffix)) = configured_url.split_once(marker) else {
        return configured_url.to_string();
    };
    let trailing = suffix
        .find(['?', '#'])
        .map(|index| &suffix[index..])
        .unwrap_or_default();
    format!("{prefix}{marker}{product}{trailing}")
}

fn bybit_public_orderbook_depth(market_type: MarketType, kind: &PublicStreamKind) -> u32 {
    match (market_type, kind) {
        (MarketType::Option, PublicStreamKind::OrderBookSnapshot) => 25,
        (MarketType::Option, PublicStreamKind::OrderBookDelta) => 100,
        (_, PublicStreamKind::OrderBookSnapshot) => 1,
        (_, PublicStreamKind::OrderBookDelta) => 50,
        _ => 50,
    }
}

pub fn parse_bybit_public_stream_events(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_bybit_ws_pong(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_bybit_ws_ack(value) {
        return Ok(Vec::new());
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if topic.starts_with("orderbook.") {
        let payload = bybit_stream_orderbook_payload(value);
        let order_book = parse_orderbook_snapshot(exchange_id, symbol, &payload)?;
        return Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(
            OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                order_book,
            },
        )]);
    }
    Err(ExchangeApiError::Unsupported {
        operation: "bybit.public_stream_topic",
    })
}

fn bybit_stream_orderbook_payload(value: &Value) -> Value {
    let data = value.get("data").unwrap_or(value);
    json!({
        "b": nonzero_levels(data.get("b")),
        "a": nonzero_levels(data.get("a")),
        "u": data
            .get("seq")
            .or_else(|| data.get("u"))
            .cloned()
            .unwrap_or(Value::Null),
        "ts": value
            .get("cts")
            .or_else(|| value.get("ts"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn nonzero_levels(levels: Option<&Value>) -> Value {
    Value::Array(
        levels
            .and_then(Value::as_array)
            .map(|levels| {
                levels
                    .iter()
                    .filter(|level| !level_quantity_is_zero(level))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    )
}

fn level_quantity_is_zero(level: &Value) -> bool {
    level
        .as_array()
        .and_then(|values| values.get(1))
        .and_then(|value| match value {
            Value::String(text) => text.parse::<f64>().ok(),
            Value::Number(number) => number.as_f64(),
            _ => None,
        })
        .is_some_and(|quantity| quantity == 0.0)
}

pub fn bybit_private_auth_payload(
    api_key: &str,
    api_secret: &str,
    expires_ms: i64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "auth",
        "args": [api_key, expires_ms, sign_ws_auth(api_secret, expires_ms)?]
    }))
}

pub fn bybit_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let topic = match subscription.kind {
        PrivateStreamKind::Orders => "order",
        PrivateStreamKind::Fills => "execution",
        PrivateStreamKind::Balances => "wallet",
        PrivateStreamKind::Positions => "position",
        PrivateStreamKind::Account => "wallet",
    };
    Ok(json!({
        "op": "subscribe",
        "args": [topic]
    }))
}

pub fn bybit_heartbeat_payload() -> Value {
    json!({"op": "ping"})
}

pub fn classify_ws_control(value: &Value) -> ExchangeApiResult<&'static str> {
    if value.get("op").and_then(Value::as_str) == Some("pong") {
        return Ok("pong");
    }
    if value.get("success").and_then(Value::as_bool) == Some(true) {
        return Ok("ack");
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("Bybit WS control failure: {value}"),
    })
}

fn is_bybit_ws_pong(value: &Value) -> bool {
    value.get("op").and_then(Value::as_str) == Some("pong")
        || value
            .get("ret_msg")
            .and_then(Value::as_str)
            .is_some_and(|message| message.eq_ignore_ascii_case("pong"))
}

fn is_bybit_ws_ack(value: &Value) -> bool {
    value.get("success").and_then(Value::as_bool) == Some(true)
        || value.get("op").and_then(Value::as_str) == Some("subscribe")
}
