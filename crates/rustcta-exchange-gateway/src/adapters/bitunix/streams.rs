#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderBookResponse, PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_bitunix_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::signing::sign_request;
use super::BitunixGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BITUNIX_FUTURES_WS_PING_INTERVAL_SECONDS: u64 = 15;
pub const BITUNIX_FUTURES_WS_MAX_MISSED_PINGS: u64 = 2;

impl BitunixGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.public_streams_disabled",
            });
        }
        match subscription.symbol.market_type {
            MarketType::Spot => {
                let (api_key, api_secret) =
                    self.private_credentials("bitunix.spot_ws_public_request")?;
                let timestamp = Utc::now().timestamp_millis().to_string();
                let nonce = bitunix_ws_nonce();
                let payload = bitunix_spot_ws_request_payload(
                    &subscription,
                    api_key,
                    api_secret,
                    &nonce,
                    &timestamp,
                    1,
                )?;
                Ok(json!({
                    "exchange": self.exchange_id.to_string(),
                    "market_type": "spot",
                    "mode": "request_response",
                    "url": self.config.spot_ws_base_url,
                    "payload": payload,
                    "heartbeat": bitunix_spot_ws_heartbeat_spec(),
                })
                .to_string())
            }
            MarketType::Perpetual => {
                let payload = bitunix_futures_public_subscribe_payload(&subscription)?;
                Ok(json!({
                    "exchange": self.exchange_id.to_string(),
                    "market_type": "perpetual",
                    "mode": "subscribe",
                    "url": self.config.futures_public_ws_url,
                    "payload": payload,
                    "unsubscribe": bitunix_futures_public_unsubscribe_payload(&subscription)?,
                    "heartbeat": bitunix_futures_ws_heartbeat_spec(),
                })
                .to_string())
            }
            _ => unreachable!("checked by ensure_supported_market"),
        }
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        if market_type == MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.spot_private_stream",
            });
        }
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.private_streams_disabled",
            });
        }
        let (api_key, api_secret) = self.private_credentials("bitunix.subscribe_private_stream")?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let nonce = bitunix_ws_nonce();
        let login = bitunix_futures_ws_login_payload(api_key, api_secret, &nonce, &timestamp)?;
        let subscribe = bitunix_futures_private_subscribe_payload(&subscription)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "login_subscribe",
            "url": self.config.futures_private_ws_url,
            "initial_payloads": [login, subscribe],
            "heartbeat": bitunix_futures_ws_heartbeat_spec(),
            "account_id": subscription.account_id.to_string(),
        })
        .to_string())
    }
}

pub fn bitunix_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn bitunix_futures_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [bitunix_futures_public_channel(subscription)?],
    }))
}

pub fn bitunix_futures_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "unsubscribe",
        "args": [bitunix_futures_public_channel(subscription)?],
    }))
}

pub fn bitunix_futures_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "ch": bitunix_futures_private_channel(&subscription.kind)?,
        }],
    }))
}

pub fn bitunix_futures_ws_login_payload(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    timestamp: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "sign": sign_request(api_key, api_secret, nonce, timestamp, "", "")?,
        }],
    }))
}

pub fn bitunix_futures_ws_ping_payload(unix_seconds: i64) -> Value {
    json!({
        "op": "ping",
        "ping": unix_seconds,
    })
}

pub fn bitunix_futures_ws_heartbeat_spec() -> Value {
    json!({
        "client_ping_interval_seconds": BITUNIX_FUTURES_WS_PING_INTERVAL_SECONDS,
        "max_missed_pongs": BITUNIX_FUTURES_WS_MAX_MISSED_PINGS,
        "client_ping_template": bitunix_futures_ws_ping_payload(0),
        "pong_fields": ["pong", "ping"],
    })
}

pub fn bitunix_spot_ws_heartbeat_spec() -> Value {
    json!({
        "mode": "authenticated_request_response_ping",
        "method": "ping",
        "timestamp_unit": "milliseconds",
    })
}

pub fn bitunix_spot_ws_request_payload(
    subscription: &PublicStreamSubscription,
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    timestamp: &str,
    id: u64,
) -> ExchangeApiResult<Value> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.spot_ws_market_type",
        });
    }
    let (method, mut params) = bitunix_spot_ws_public_params(subscription)?;
    params.insert("apiKey".to_string(), json!(api_key));
    params.insert("timestamp".to_string(), json!(timestamp));
    params.insert("nonce".to_string(), json!(nonce));
    let sign_payload = spot_ws_sign_payload(&params);
    params.insert(
        "sign".to_string(),
        json!(sign_request(
            api_key,
            api_secret,
            nonce,
            timestamp,
            &sign_payload,
            ""
        )?),
    );
    Ok(json!({
        "id": id,
        "method": method,
        "params": Value::Object(params),
    }))
}

pub fn parse_bitunix_futures_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if is_futures_heartbeat(value) {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    if is_subscription_ack(value) {
        return Ok(None);
    }
    let ch = value.get("ch").and_then(Value::as_str).unwrap_or_default();
    if ch.starts_with("depth_") {
        let data = value.get("data").unwrap_or(value);
        let normalized = json!({
            "data": {
                "bids": data.get("b").cloned().unwrap_or(Value::Array(Vec::new())),
                "asks": data.get("a").cloned().unwrap_or(Value::Array(Vec::new())),
                "ts": value.get("ts").cloned().unwrap_or(Value::Null),
            }
        });
        let order_book = parse_orderbook_snapshot(exchange_id, symbol_hint.clone(), &normalized)?;
        return Ok(Some(ExchangeStreamEvent::OrderBookSnapshot(
            OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                order_book,
            },
        )));
    }
    Err(ExchangeApiError::Unsupported {
        operation: "bitunix.futures_public_stream_channel",
    })
}

pub fn parse_bitunix_futures_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_futures_heartbeat(value) {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_subscription_ack(value) || value.get("op").and_then(Value::as_str) == Some("login") {
        return Ok(Vec::new());
    }
    let ch = value.get("ch").and_then(Value::as_str).unwrap_or_default();
    let data = value.get("data").unwrap_or(value);
    match ch {
        "order" => {
            let mut events = vec![ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                None,
                MarketType::Perpetual,
                data,
            )?)];
            if bitunix_order_push_contains_fill(data) {
                events.extend(
                    parse_fills(
                        exchange_id,
                        tenant_id,
                        account_id,
                        None,
                        MarketType::Perpetual,
                        &json!({ "data": [data.clone()] }),
                    )?
                    .into_iter()
                    .map(ExchangeStreamEvent::Fill),
                );
            }
            Ok(events)
        }
        "balance" => Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
            BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    MarketType::Perpetual,
                    &[],
                    &json!({ "data": [data.clone()] }),
                )?,
            },
        )]),
        "position" => Ok(vec![ExchangeStreamEvent::PositionSnapshot(
            PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &json!({ "data": [data.clone()] }),
                )?,
            },
        )]),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitunix.futures_private_stream_channel",
        }),
    }
}

fn bitunix_futures_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    if subscription.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.futures_public_stream_market_type",
        });
    }
    let symbol = normalize_bitunix_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let ch = match &subscription.kind {
        PublicStreamKind::Trades => "trade".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta => "depth_books".to_string(),
        PublicStreamKind::OrderBookSnapshot => "depth_book15".to_string(),
        PublicStreamKind::Candles { interval } => {
            format!(
                "market_kline_{}",
                normalize_futures_kline_interval(interval)?
            )
        }
    };
    Ok(json!({
        "symbol": symbol,
        "ch": ch,
    }))
}

fn bitunix_futures_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("order"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("balance"),
        PrivateStreamKind::Positions => Ok("position"),
    }
}

fn bitunix_spot_ws_public_params(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<(&'static str, serde_json::Map<String, Value>)> {
    let mut params = serde_json::Map::new();
    params.insert(
        "symbol".to_string(),
        json!(normalize_bitunix_symbol(
            &subscription.symbol.exchange_symbol.symbol
        )?),
    );
    let method = match &subscription.kind {
        PublicStreamKind::Ticker => "market.last_price",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            params.insert("precision".to_string(), json!(5));
            "market.depth"
        }
        PublicStreamKind::Candles { interval } => {
            params.insert(
                "interval".to_string(),
                json!(normalize_spot_kline_interval(interval)?),
            );
            "market.kline"
        }
        PublicStreamKind::Trades => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.spot_public_trades_ws",
            });
        }
    };
    Ok((method, params))
}

fn normalize_futures_kline_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval {
        "1m" | "1min" => Ok("1min"),
        "5m" | "5min" => Ok("5min"),
        "15m" | "15min" => Ok("15min"),
        "30m" | "30min" => Ok("30min"),
        "1h" | "60min" => Ok("1h"),
        "4h" | "240min" => Ok("4h"),
        "1d" | "D" => Ok("1day"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitunix.futures_kline_interval",
        }),
    }
}

fn normalize_spot_kline_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval {
        "1m" | "1min" => Ok("1min"),
        "3m" | "3min" => Ok("3min"),
        "5m" | "5min" => Ok("5min"),
        "15m" | "15min" => Ok("15min"),
        "30m" | "30min" => Ok("30min"),
        "1h" | "60min" => Ok("60min"),
        "4h" | "240min" => Ok("240min"),
        "1d" | "D" => Ok("D"),
        "1w" | "W" => Ok("W"),
        "1M" | "M" => Ok("M"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitunix.spot_kline_interval",
        }),
    }
}

fn spot_ws_sign_payload(params: &serde_json::Map<String, Value>) -> String {
    let mut pairs = params
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "apiKey" | "timestamp" | "nonce" | "sign"))
        .filter_map(|(key, value)| value_to_sign_text(value).map(|text| (key.as_str(), text)))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}{value}"))
        .collect::<String>()
}

fn value_to_sign_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn is_futures_heartbeat(value: &Value) -> bool {
    value.get("op").and_then(Value::as_str) == Some("ping")
        && (value.get("pong").is_some() || value.get("ping").is_some())
}

fn is_subscription_ack(value: &Value) -> bool {
    matches!(
        value.get("op").and_then(Value::as_str),
        Some("subscribe") | Some("unsubscribe")
    ) || value.get("event").and_then(Value::as_str) == Some("subscribe")
}

fn bitunix_order_push_contains_fill(value: &Value) -> bool {
    value.get("tradeId").is_some()
        || value.get("dealId").is_some()
        || value.get("fillId").is_some()
        || value.get("fee").is_some()
        || value.get("roleType").is_some()
}

fn bitunix_ws_nonce() -> String {
    format!(
        "{:032x}",
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_millis())
    )
}
