use chrono::Utc;
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PositionsResponse, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_bybit_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::signing::sign_ws_auth;
use super::BybitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};

#[derive(Debug, Clone, PartialEq)]
pub struct BybitPrivateWsResubscribePlan {
    pub auth_payload: Value,
    pub subscribe_payloads: Vec<Value>,
    pub topics: Vec<String>,
    pub rest_resync_operations: Vec<&'static str>,
}

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
            .get("u")
            .cloned()
            .unwrap_or(Value::Null),
        "seq": data
            .get("seq")
            .cloned()
            .unwrap_or(Value::Null),
        "ts": value
            .get("cts")
            .or_else(|| value.get("ts"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

pub fn parse_bybit_orderbook_delta(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let data = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit order book delta parser requires canonical_symbol".to_string(),
            })?;
    let update_id = data
        .get("seq")
        .or_else(|| data.get("u"))
        .and_then(value_as_u64)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("bybit order book delta missing sequence seq/u: {value}"),
        })?;
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    )
    .with_sequences(Some(update_id), Some(update_id));
    delta.bids = parse_delta_levels(data.get("b"))?;
    delta.asks = parse_delta_levels(data.get("a"))?;
    delta.exchange_timestamp = value
        .get("cts")
        .or_else(|| value.get("ts"))
        .and_then(value_as_i64)
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

fn parse_delta_levels(levels: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    let Some(levels) = levels.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("invalid bybit order book level: {level}"),
                })?;
            let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid bybit order book price: {level}"),
                }
            })?;
            let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid bybit order book quantity: {level}"),
                }
            })?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
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

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
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
    bybit_private_subscribe_payloads(subscription)?
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeApiError::Unsupported {
            operation: "bybit.private_stream.empty_topics",
        })
}

pub fn bybit_private_subscribe_payloads(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Vec<Value>> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    Ok(bybit_private_topics(&subscription.kind)
        .into_iter()
        .map(|topic| {
            json!({
                "op": "subscribe",
                "args": [topic]
            })
        })
        .collect())
}

pub fn bybit_private_resubscribe_plan(
    api_key: &str,
    api_secret: &str,
    expires_ms: i64,
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<BybitPrivateWsResubscribePlan> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let topics = bybit_private_topics(&subscription.kind);
    Ok(BybitPrivateWsResubscribePlan {
        auth_payload: bybit_private_auth_payload(api_key, api_secret, expires_ms)?,
        subscribe_payloads: topics
            .iter()
            .map(|topic| {
                json!({
                    "op": "subscribe",
                    "args": [topic],
                })
            })
            .collect(),
        topics: topics.into_iter().map(str::to_string).collect(),
        rest_resync_operations: bybit_private_resync_operations(&subscription.kind),
    })
}

pub fn parse_bybit_private_stream_events(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if topic.is_empty() {
        return parse_bybit_private_control_event(exchange_id, value);
    }
    let data = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("bybit private stream message missing data array: {value}"),
        }
    })?;
    let tenant_id =
        subscription
            .context
            .tenant_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit private stream parser requires context.tenant_id".to_string(),
            })?;
    let account_id = subscription.account_id.clone();
    let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
    let wrapped = json!({ "result": { "list": data } });

    if topic == "order" || topic.starts_with("order.") {
        return data
            .iter()
            .map(|row| {
                parse_order_state(exchange_id, symbol_hint, row)
                    .map(ExchangeStreamEvent::OrderUpdate)
            })
            .collect();
    }
    if topic == "execution" || topic.starts_with("execution.") {
        let mut events = Vec::new();
        for row in data {
            let symbol = symbol_hint
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| bybit_symbol_scope(exchange_id, market_type, row))?;
            let single = json!({ "result": { "list": [row.clone()] } });
            events.extend(
                parse_fills(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    &symbol,
                    &single,
                )?
                .into_iter()
                .map(ExchangeStreamEvent::Fill),
            );
        }
        return Ok(events);
    }
    if topic == "position" || topic.starts_with("position.") {
        return Ok(vec![ExchangeStreamEvent::PositionSnapshot(
            PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                positions: parse_positions(exchange_id, tenant_id, account_id, &[], &wrapped)?,
            },
        )]);
    }
    if topic == "wallet" {
        return Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
            BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type,
                    &[],
                    &wrapped,
                )?,
            },
        )]);
    }
    Err(ExchangeApiError::Unsupported {
        operation: "bybit.private_stream_topic",
    })
}

fn bybit_symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol_text = value.get("symbol").and_then(Value::as_str).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("bybit private stream row missing symbol: {value}"),
        }
    })?;
    let normalized = normalize_bybit_symbol(symbol_text)?;
    let (base, quote) = split_bybit_symbol(&normalized);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            })?,
    })
}

fn split_bybit_symbol(symbol: &str) -> (&str, &str) {
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            return (base, quote);
        }
    }
    (symbol, "USDT")
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

fn bybit_private_topics(kind: &PrivateStreamKind) -> Vec<&'static str> {
    match kind {
        PrivateStreamKind::Orders => vec!["order"],
        PrivateStreamKind::Fills => vec!["execution"],
        PrivateStreamKind::Balances => vec!["wallet"],
        PrivateStreamKind::Positions => vec!["position"],
        PrivateStreamKind::Account => vec!["order", "execution", "position", "wallet"],
    }
}

fn bybit_private_resync_operations(kind: &PrivateStreamKind) -> Vec<&'static str> {
    match kind {
        PrivateStreamKind::Orders => vec!["get_open_orders", "query_order"],
        PrivateStreamKind::Fills => vec!["get_recent_fills", "query_order"],
        PrivateStreamKind::Balances => vec!["get_balances"],
        PrivateStreamKind::Positions => vec!["get_positions"],
        PrivateStreamKind::Account => vec![
            "get_balances",
            "get_positions",
            "get_open_orders",
            "get_recent_fills",
            "query_order",
        ],
    }
}

fn parse_bybit_private_control_event(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_bybit_ws_pong(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_bybit_ws_ack(value) || value.get("op").and_then(Value::as_str) == Some("auth") {
        classify_ws_control(value)?;
        return Ok(Vec::new());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("Bybit private WS control failure: {value}"),
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
