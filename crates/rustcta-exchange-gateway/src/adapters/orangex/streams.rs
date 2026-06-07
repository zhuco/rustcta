#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::{market_kind, normalize_orangex_symbol};
use super::private_parser::{parse_balances, parse_fill, parse_order_state, parse_positions};
use super::OrangeXGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl OrangeXGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        let payload = orangex_public_subscribe_payload(&subscription, 1)?;
        Ok(format!(
            "orangex:{}:{}:{}",
            self.config.ws_base_url,
            payload
                .get("params")
                .and_then(|params| params.get("channels"))
                .and_then(Value::as_array)
                .and_then(|channels| channels.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            normalize_orangex_symbol(
                &subscription.symbol.exchange_symbol.symbol,
                subscription.symbol.market_type
            )?
        ))
    }
}

pub fn orangex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    let channel = orangex_public_channel(subscription)?;
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "/public/subscribe",
        "params": {
            "channels": [channel],
        },
    }))
}

pub fn orangex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    let channel = orangex_public_channel(subscription)?;
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "/public/unsubscribe",
        "params": {
            "channels": [channel],
        },
    }))
}

pub fn orangex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    access_token: &str,
    id: u64,
) -> ExchangeApiResult<Value> {
    let channel = orangex_private_channel(subscription, market_type)?;
    let token = access_token.trim();
    if token.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "orangex private stream requires access_token".to_string(),
        });
    }
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "/private/subscribe",
        "params": {
            "access_token": token,
            "channels": [channel],
        },
    }))
}

pub fn orangex_private_unsubscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    access_token: &str,
    id: u64,
) -> ExchangeApiResult<Value> {
    let channel = orangex_private_channel(subscription, market_type)?;
    let token = access_token.trim();
    if token.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "orangex private stream requires access_token".to_string(),
        });
    }
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "/private/unsubscribe",
        "params": {
            "access_token": token,
            "channels": [channel],
        },
    }))
}

pub const ORANGEX_WS_TEXT_PING: &str = "PING";

pub fn orangex_public_ping_payload(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "/public/ping",
        "params": {},
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrangeXWsControlMessage {
    Pong,
    SubscriptionAck { channels: Vec<String> },
    SubscriptionEvent { channel: String, data: Value },
}

pub fn parse_orangex_ws_control_message(
    value: &Value,
) -> ExchangeApiResult<Option<OrangeXWsControlMessage>> {
    if value
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong"))
    {
        return Ok(Some(OrangeXWsControlMessage::Pong));
    }

    if value.get("error").is_some() {
        let mut error = rustcta_types::ExchangeError::new(
            rustcta_types::ExchangeId::new("orangex").map_err(validation_error)?,
            rustcta_types::ExchangeErrorClass::Unknown,
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("OrangeX websocket error"),
            chrono::Utc::now(),
        );
        error.code = value
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_i64)
            .map(|code| code.to_string());
        error.raw = Some(value.clone());
        return Err(ExchangeApiError::Exchange(error));
    }

    if value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method == "subscription")
    {
        let params = value.get("params").unwrap_or(&Value::Null);
        let channel = params
            .get("channel")
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "OrangeX subscription message missing channel".to_string(),
            })?;
        return Ok(Some(OrangeXWsControlMessage::SubscriptionEvent {
            channel: channel.to_string(),
            data: params.get("data").cloned().unwrap_or(Value::Null),
        }));
    }

    let Some(result) = value.get("result") else {
        return Ok(None);
    };
    if result.is_object() {
        return Ok(Some(OrangeXWsControlMessage::Pong));
    }
    if let Some(channels) = result.as_array() {
        let channels = channels
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        return Ok(Some(OrangeXWsControlMessage::SubscriptionAck { channels }));
    }
    if result
        .as_str()
        .is_some_and(|text| text.eq_ignore_ascii_case("pong") || text.eq_ignore_ascii_case("ok"))
    {
        return Ok(Some(OrangeXWsControlMessage::Pong));
    }
    Ok(None)
}

pub fn parse_orangex_private_stream_event(
    tenant_id: TenantId,
    account_id: AccountId,
    exchange_id: &rustcta_exchange_api::ExchangeId,
    market_type: MarketType,
    channel: &str,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if channel.starts_with("user.orders.") {
        return Ok(vec![ExchangeStreamEvent::OrderUpdate(parse_order_state(
            exchange_id,
            None,
            data,
        )?)]);
    }

    if channel.starts_with("user.trades.") {
        return Ok(vec![ExchangeStreamEvent::Fill(parse_fill(
            tenant_id,
            account_id,
            exchange_id,
            None,
            data,
        )?)]);
    }

    if channel.starts_with("user.asset.") {
        return Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
            rustcta_exchange_api::BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    tenant_id,
                    account_id,
                    exchange_id,
                    market_type,
                    &[],
                    data,
                )?,
            },
        )]);
    }

    if channel.starts_with("user.changes.") {
        let mut events = Vec::new();
        for order in data
            .get("orders")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            events.push(ExchangeStreamEvent::OrderUpdate(parse_order_state(
                exchange_id,
                None,
                order,
            )?));
        }
        for trade in data
            .get("trades")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            events.push(ExchangeStreamEvent::Fill(parse_fill(
                tenant_id.clone(),
                account_id.clone(),
                exchange_id,
                None,
                trade,
            )?));
        }
        for position in data
            .get("positions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let positions = parse_positions(
                tenant_id.clone(),
                account_id.clone(),
                exchange_id,
                &Value::Array(vec![position.clone()]),
            )?;
            if !positions.is_empty() {
                events.push(ExchangeStreamEvent::PositionSnapshot(
                    rustcta_exchange_api::PositionsResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                        positions,
                    },
                ));
            }
        }
        if events.is_empty() {
            events.push(ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            });
        }
        return Ok(events);
    }

    Ok(vec![ExchangeStreamEvent::Heartbeat {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        received_at: Utc::now(),
    }])
}

fn orangex_public_channel(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let instrument = normalize_orangex_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta => Ok(format!("book.{instrument}.100ms")),
        PublicStreamKind::OrderBookSnapshot => Ok(format!("book.{instrument}.raw")),
        PublicStreamKind::Trades => Ok(format!("trades.{instrument}.raw")),
        PublicStreamKind::Ticker => Ok(format!("ticker.{instrument}.raw")),
        PublicStreamKind::Candles { interval } => Ok(format!(
            "chart.trades.{instrument}.{}",
            normalize_interval(interval)?
        )),
    }
}

fn orangex_private_channel(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    match subscription.kind {
        PrivateStreamKind::Orders => {
            let instrument = subscription
                .context
                .request_id
                .as_deref()
                .and_then(|request_id| request_id.strip_prefix("symbol:"))
                .map(str::to_string);
            if let Some(instrument) = instrument {
                Ok(format!(
                    "user.orders.{}.raw",
                    normalize_orangex_symbol(&instrument, market_type)?
                ))
            } else {
                Ok(format!(
                    "user.changes.{}.{}.raw",
                    market_kind(market_type)?,
                    orangex_private_stream_currency(market_type)?
                ))
            }
        }
        PrivateStreamKind::Fills => Ok(format!(
            "user.changes.{}.{}.raw",
            market_kind(market_type)?,
            orangex_private_stream_currency(market_type)?
        )),
        PrivateStreamKind::Balances => Ok(format!(
            "user.asset.{}",
            orangex_private_stream_currency(market_type)?
        )),
        PrivateStreamKind::Positions | PrivateStreamKind::Account => Ok(format!(
            "user.changes.{}.{}.raw",
            market_kind(market_type)?,
            orangex_private_stream_currency(market_type)?
        )),
    }
}

fn orangex_private_stream_currency(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("SPOT"),
        MarketType::Perpetual => Ok("PERPETUAL"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "orangex.private_stream_market_type",
        }),
    }
}

fn validation_error(message: impl ToString) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.to_string(),
    }
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "1" => Ok("1"),
        "3m" | "3" => Ok("3"),
        "5m" | "5" => Ok("5"),
        "15m" | "15" => Ok("15"),
        "30m" | "30" => Ok("30"),
        "1h" | "60" => Ok("60"),
        "4h" | "240" => Ok("240"),
        "1d" | "1440" => Ok("1D"),
        _ => Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
            operation: "orangex.public_stream_candle_interval",
        }),
    }
}
