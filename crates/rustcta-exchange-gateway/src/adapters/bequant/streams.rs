#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_bequant_symbol, parse_orderbook_snapshot};
use super::signing::basic_auth_value;
use crate::adapters::ensure_exchange_api_schema;
use crate::streams::StreamRuntimeState;

#[derive(Debug, Clone, PartialEq)]
pub struct BequantWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BequantWsAuthSpec {
    pub url: String,
    pub payload: Value,
    pub authorization: String,
}

#[derive(Debug, Clone)]
pub struct BequantWsSession {
    pub spec: BequantWsSubscriptionSpec,
    pub state: StreamRuntimeState,
}

pub fn bequant_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

impl super::BequantGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_subscription_spec(&subscription, &self.config.public_ws_url)?;
        Ok(format!("bequant:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bequant.private_stream.runtime_disabled_rest_reconciliation",
            });
        }
        let stream = match subscription.kind {
            PrivateStreamKind::Orders | PrivateStreamKind::Fills => "spot_subscribe",
            PrivateStreamKind::Balances | PrivateStreamKind::Account => "wallet_subscribe",
            PrivateStreamKind::Positions => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "bequant.private_stream.positions_spot_adapter",
                })
            }
        };
        Ok(format!(
            "bequant:private:{stream}:{}",
            subscription.account_id
        ))
    }
}

pub fn public_subscription_spec(
    subscription: &PublicStreamSubscription,
    public_ws_url: &str,
) -> ExchangeApiResult<BequantWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bequant.public_stream.market_type",
        });
    }
    let symbol = normalize_bequant_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            "orderbook/D20/1000ms"
        }
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "ticker/price/1s",
        PublicStreamKind::Candles { interval } => {
            let _ = interval;
            return Err(ExchangeApiError::Unsupported {
                operation: "bequant.public_stream.candles",
            });
        }
    }
    .to_string();
    Ok(subscription_spec(public_ws_url, channel, symbol))
}

pub fn private_subscription_spec(
    kind: PrivateStreamKind,
    trading_ws_url: &str,
    wallet_ws_url: &str,
) -> ExchangeApiResult<BequantWsSubscriptionSpec> {
    let (url, method, channel) = match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => {
            (trading_ws_url, "spot_subscribe", "spot_reports")
        }
        PrivateStreamKind::Balances | PrivateStreamKind::Account => {
            (wallet_ws_url, "wallet_subscribe", "wallet")
        }
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bequant.private_stream.positions_spot_adapter",
            })
        }
    };
    Ok(BequantWsSubscriptionSpec {
        url: url.to_string(),
        channel: channel.to_string(),
        subscribe_payload: json!({
            "method": method,
            "params": {},
        }),
        unsubscribe_payload: json!({
            "method": method.replace("subscribe", "unsubscribe"),
            "params": {},
        }),
    })
}

pub fn private_auth_spec(
    trading_ws_url: &str,
    api_key: &str,
    api_secret: &str,
) -> BequantWsAuthSpec {
    BequantWsAuthSpec {
        url: trading_ws_url.to_string(),
        payload: json!({
            "method": "login",
            "params": {
                "type": "Basic"
            }
        }),
        authorization: basic_auth_value(api_key, api_secret),
    }
}

pub fn public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    public_ws_url: &str,
) -> ExchangeApiResult<BequantWsSession> {
    let spec = public_subscription_spec(&subscription, public_ws_url)?;
    Ok(BequantWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

pub fn parse_public_orderbook_message(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<OrderBookSnapshot>> {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method != "snapshotOrderbook" && method != "updateOrderbook" {
        return Ok(None);
    }
    let params = value
        .get("params")
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bequant websocket orderbook message missing params".to_string(),
        })?;
    let symbol_param = params
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bequant websocket orderbook message missing symbol".to_string(),
        })?;
    if normalize_bequant_symbol(symbol_param)?
        != normalize_bequant_symbol(&symbol.exchange_symbol.symbol)?
    {
        return Ok(None);
    }
    let rest_shape = json!({
        "ask": params.get("ask").or_else(|| params.get("a")).cloned().unwrap_or(Value::Array(vec![])),
        "bid": params.get("bid").or_else(|| params.get("b")).cloned().unwrap_or(Value::Array(vec![])),
        "timestamp": params.get("timestamp").or_else(|| params.get("t")).cloned().unwrap_or(Value::Null)
    });
    parse_orderbook_snapshot(exchange_id, symbol, &rest_shape).map(Some)
}

pub fn heartbeat_ping_payload() -> Value {
    json!({ "method": "ping" })
}

pub fn heartbeat_pong_method(value: &Value) -> ExchangeApiResult<bool> {
    Ok(value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.eq_ignore_ascii_case("pong")))
}

fn subscription_spec(
    public_ws_url: &str,
    channel: String,
    symbol: String,
) -> BequantWsSubscriptionSpec {
    BequantWsSubscriptionSpec {
        url: public_ws_url.to_string(),
        channel: channel.clone(),
        subscribe_payload: json!({
            "method": "subscribe",
            "params": {
                "symbol": symbol,
                "ch": channel,
            },
        }),
        unsubscribe_payload: json!({
            "method": "unsubscribe",
            "params": {
                "symbol": symbol,
                "ch": channel,
            },
        }),
    }
}
