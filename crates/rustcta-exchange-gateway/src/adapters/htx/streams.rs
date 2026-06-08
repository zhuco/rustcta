use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, PositionsResponse,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, parse_orderbook_response};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_positions};
use super::signing::{auth_params, sign_v2};
use super::HtxGatewayAdapter;
use crate::adapters::response_metadata;

#[derive(Debug, Clone, PartialEq)]
pub enum HtxPublicStreamMessage {
    SubscriptionAck { channel: Option<String> },
    Pong(i64),
    OrderBook(rustcta_exchange_api::OrderBookResponse),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum HtxPrivateStreamMessage {
    AuthAck,
    SubscriptionAck { topic: Option<String> },
    Pong(i64),
    Events(Vec<ExchangeStreamEvent>),
    Raw(Value),
}

impl HtxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let payload = public_subscribe_payload(&subscription)?;
        let url = if subscription.symbol.market_type == MarketType::Spot {
            &self.config.spot_public_ws_url
        } else {
            &self.config.linear_public_ws_url
        };
        Ok(format!(
            "{}:{}:{}",
            self.exchange_id,
            url,
            payload["sub"].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        let (api_key, api_secret) = self.private_credentials("htx.subscribe_private_stream")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        let url = private_ws_url(
            &self.config.spot_private_ws_url,
            &self.config.private_ws_url,
            market_type,
        );
        let login = private_ws_auth_payload_for_url(api_key, api_secret, &url, market_type, None)?;
        let subscribe = private_subscribe_payload(&subscription)?;
        Ok(format!(
            "{}:{}:{}:{}",
            self.exchange_id,
            url,
            login
                .get("op")
                .or_else(|| login.get("action"))
                .and_then(Value::as_str)
                .unwrap_or("auth"),
            subscribe
                .get("topic")
                .or_else(|| subscribe.get("ch"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }
}

pub fn public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_symbol(&subscription.symbol)?;
    let channel = match (&subscription.kind, subscription.symbol.market_type) {
        (
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta,
            MarketType::Spot,
        ) => {
            format!("market.{symbol}.depth.step0")
        }
        (
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta,
            MarketType::Perpetual,
        ) => {
            format!("market.{symbol}.depth.size_20.high_freq")
        }
        (PublicStreamKind::Trades, _) => format!("market.{symbol}.trade.detail"),
        (PublicStreamKind::Ticker, _) => format!("market.{symbol}.detail"),
        (PublicStreamKind::Candles { interval }, _) => format!("market.{symbol}.kline.{interval}"),
        (_, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "htx.public_stream_kind",
            })
        }
    };
    Ok(
        json!({ "sub": channel, "id": subscription.context.request_id.clone().unwrap_or_else(|| "rustcta".to_string()) }),
    )
}

pub fn private_ws_auth_payload(
    api_key: &str,
    api_secret: &str,
    timestamp: Option<&str>,
) -> ExchangeApiResult<Value> {
    private_ws_auth_payload_for_url(
        api_key,
        api_secret,
        "wss://api.hbdm.com/linear-swap-notification",
        MarketType::Perpetual,
        timestamp,
    )
}

pub fn private_ws_auth_payload_for_url(
    api_key: &str,
    api_secret: &str,
    url: &str,
    market_type: MarketType,
    timestamp: Option<&str>,
) -> ExchangeApiResult<Value> {
    let timestamp = timestamp
        .map(ToString::to_string)
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string());
    let (host, path) = ws_host_path(url)?;
    match market_type {
        MarketType::Spot => {
            let mut params = std::collections::BTreeMap::from([
                ("accessKey".to_string(), api_key.to_string()),
                ("signatureMethod".to_string(), "HmacSHA256".to_string()),
                ("signatureVersion".to_string(), "2.1".to_string()),
                ("timestamp".to_string(), timestamp.clone()),
            ]);
            let signature = sign_v2(api_secret, "GET", &host, &path, &params)?;
            params.insert("signature".to_string(), signature);
            Ok(json!({
                "action": "req",
                "ch": "auth",
                "params": params
            }))
        }
        _ => {
            let params = auth_params(api_key, &timestamp);
            let signature = sign_v2(api_secret, "GET", &host, &path, &params)?;
            Ok(json!({
                "op": "auth",
                "type": "api",
                "AccessKeyId": api_key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": timestamp,
                "Signature": signature
            }))
        }
    }
}

pub fn private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    if subscription.market_type == Some(MarketType::Spot) {
        let ch = match &subscription.kind {
            PrivateStreamKind::Orders => "orders#*",
            PrivateStreamKind::Fills => "trade.clearing#*#0",
            PrivateStreamKind::Balances | PrivateStreamKind::Account => "accounts.update#0",
            PrivateStreamKind::Positions => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "htx.spot_private_positions_stream",
                })
            }
        };
        return Ok(json!({ "action": "sub", "ch": ch }));
    }
    let topic = match &subscription.kind {
        PrivateStreamKind::Orders => "orders_cross.*",
        PrivateStreamKind::Fills => "match_orders_cross.*",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "accounts_cross.*",
        PrivateStreamKind::Positions => "positions_cross.*",
    };
    Ok(json!({ "op": "sub", "topic": topic }))
}

fn private_ws_url(
    spot_configured: &str,
    linear_configured: &str,
    market_type: MarketType,
) -> String {
    if market_type == MarketType::Spot {
        spot_configured.to_string()
    } else {
        linear_configured.to_string()
    }
}

fn ws_host_path(url: &str) -> ExchangeApiResult<(String, String)> {
    let parsed = reqwest::Url::parse(url).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("invalid HTX WS URL {url}: {error}"),
    })?;
    let host = parsed
        .host_str()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid HTX WS URL {url}: missing host"),
        })?
        .to_string();
    let path = if parsed.path().is_empty() {
        "/".to_string()
    } else {
        parsed.path().to_string()
    };
    Ok((host, path))
}

pub fn parse_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<HtxPublicStreamMessage> {
    if let Some(ping) = value.get("ping").and_then(Value::as_i64) {
        return Ok(HtxPublicStreamMessage::Pong(ping));
    }
    if value.get("status").and_then(Value::as_str) == Some("ok") && value.get("subbed").is_some() {
        return Ok(HtxPublicStreamMessage::SubscriptionAck {
            channel: value
                .get("subbed")
                .and_then(Value::as_str)
                .map(ToString::to_string),
        });
    }
    if value
        .get("ch")
        .and_then(Value::as_str)
        .is_some_and(|ch| ch.contains(".depth."))
    {
        return Ok(HtxPublicStreamMessage::OrderBook(parse_orderbook_response(
            exchange_id,
            symbol,
            None,
            value,
        )?));
    }
    Ok(HtxPublicStreamMessage::Raw(value.clone()))
}

pub fn parse_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_types::TenantId,
    account_id: rustcta_types::AccountId,
    symbol_hint: Option<SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<HtxPrivateStreamMessage> {
    if let Some(ping) = value.get("ping").and_then(Value::as_i64) {
        return Ok(HtxPrivateStreamMessage::Pong(ping));
    }
    let code_ok = value.get("code").is_none_or(|code| {
        code.as_i64() == Some(200) || code.as_str().is_some_and(|code| code == "200")
    });
    if (value.get("op").and_then(Value::as_str) == Some("auth")
        || value.get("ch").and_then(Value::as_str) == Some("auth"))
        && value.get("err-code").is_none()
        && code_ok
    {
        return Ok(HtxPrivateStreamMessage::AuthAck);
    }
    if value.get("op").and_then(Value::as_str) == Some("sub")
        || value.get("action").and_then(Value::as_str) == Some("sub")
    {
        return Ok(HtxPrivateStreamMessage::SubscriptionAck {
            topic: value
                .get("topic")
                .or_else(|| value.get("ch"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
        });
    }
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let data = value.get("data").unwrap_or(value);
    let events = if topic.contains("orders") && !topic.contains("match") {
        parse_orders_stream(exchange_id, symbol_hint.as_ref(), data)?
    } else if topic.contains("match_orders") {
        parse_fills(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint.as_ref(),
            data,
        )
        .into_iter()
        .map(ExchangeStreamEvent::Fill)
        .collect()
    } else if topic.contains("positions") {
        vec![ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            positions: parse_positions(exchange_id, tenant_id, account_id, data)?,
        })]
    } else if topic.contains("accounts") {
        vec![ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances: parse_balances(
                exchange_id,
                tenant_id,
                account_id,
                MarketType::Perpetual,
                &[],
                data,
            )?,
        })]
    } else {
        return Ok(HtxPrivateStreamMessage::Raw(value.clone()));
    };
    Ok(HtxPrivateStreamMessage::Events(events))
}

pub fn pong_payload(ping: i64) -> Value {
    json!({ "pong": ping })
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn parse_orders_stream(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let rows = value
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(value));
    rows.iter()
        .map(|row| {
            parse_order_state(exchange_id, symbol_hint, row).map(ExchangeStreamEvent::OrderUpdate)
        })
        .collect()
}

#[allow(dead_code)]
fn _timestamp_from_ms(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}
