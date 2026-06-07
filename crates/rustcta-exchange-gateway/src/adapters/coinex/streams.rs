#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_api::{PrivateOrderStreamEventKind, PrivateStreamCapabilities};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_coinex_symbol;
use super::signing::sign_request;
use super::CoinExGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const COINEX_PUBLIC_WS_URL: &str = "wss://socket.coinex.com/v2/spot";
pub const COINEX_PRIVATE_WS_URL: &str = "wss://socket.coinex.com/v2/spot";

impl CoinExGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let policy = coinex_public_ws_policy(&subscription)?;
        let method = policy.subscribe_method().unwrap_or("unknown");
        Ok(format!("coinex:{COINEX_PUBLIC_WS_URL}:{method}"))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinex.subscribe_private_stream")?;
        if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_market_type",
            });
        }
        let subscribe = coinex_private_subscribe_payload(&subscription, 1)?;
        Ok(format!(
            "coinex:{COINEX_PRIVATE_WS_URL}:{}:{}",
            subscribe["method"].as_str().unwrap_or("unknown"),
            subscription.account_id
        ))
    }
}

impl CoinExWsPolicy {
    fn subscribe_method(&self) -> Option<&str> {
        match self {
            Self::Public { subscribe, .. } | Self::Private { subscribe, .. } => {
                subscribe.get("method").and_then(Value::as_str)
            }
        }
    }
}

pub fn coinex_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 10_000, 45_000)
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinExWsPolicy {
    Public {
        url: String,
        subscribe: Value,
        unsubscribe: Value,
        heartbeat: Value,
        resync: CoinExResyncPolicy,
    },
    Private {
        url: String,
        login: Value,
        subscribe: Value,
        unsubscribe: Value,
        heartbeat: Value,
        auth_renewal: CoinExAuthRenewalPolicy,
        reconciliation_fallback: CoinExReconciliationFallback,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExResyncPolicy {
    pub snapshot_endpoint: &'static str,
    pub resubscribe_after_reconnect: bool,
    pub snapshot_required_after_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExAuthRenewalPolicy {
    pub renewal: &'static str,
    pub renewal_interval_ms: i64,
    pub relogin_before_reconnect: bool,
    pub resubscribe_after_relogin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExReconciliationFallback {
    pub query_order_endpoint: &'static str,
    pub open_orders_endpoint: &'static str,
    pub fills_endpoint: &'static str,
    pub use_when_private_ws_stale: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinExStreamControlMessage {
    Pong,
    SubscriptionAck {
        id: Option<u64>,
        method: Option<String>,
    },
    AuthAck {
        id: Option<u64>,
    },
    Error {
        code: Option<i64>,
        message: Option<String>,
    },
    Data(Value),
}

pub fn coinex_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn coinex_public_ws_policy(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<CoinExWsPolicy> {
    Ok(CoinExWsPolicy::Public {
        url: COINEX_PUBLIC_WS_URL.to_string(),
        subscribe: coinex_public_subscribe_payload(subscription, 1)?,
        unsubscribe: coinex_public_unsubscribe_payload(subscription, 2)?,
        heartbeat: coinex_ping_payload(3),
        resync: CoinExResyncPolicy {
            snapshot_endpoint: "/spot/depth",
            resubscribe_after_reconnect: true,
            snapshot_required_after_reconnect: true,
        },
    })
}

pub fn coinex_private_ws_policy(
    subscription: &PrivateStreamSubscription,
    api_key: &str,
    api_secret: &str,
    timestamp_ms: i64,
) -> ExchangeApiResult<CoinExWsPolicy> {
    Ok(CoinExWsPolicy::Private {
        url: COINEX_PRIVATE_WS_URL.to_string(),
        login: coinex_private_login_payload(api_key, api_secret, timestamp_ms, 1)?,
        subscribe: coinex_private_subscribe_payload(subscription, 2)?,
        unsubscribe: coinex_private_unsubscribe_payload(subscription, 3)?,
        heartbeat: coinex_ping_payload(4),
        auth_renewal: CoinExAuthRenewalPolicy {
            renewal: "relogin",
            renewal_interval_ms: 30 * 60 * 1000,
            relogin_before_reconnect: true,
            resubscribe_after_relogin: true,
        },
        reconciliation_fallback: CoinExReconciliationFallback {
            query_order_endpoint: "/spot/order-status",
            open_orders_endpoint: "/spot/pending-order",
            fills_endpoint: "/spot/finished-order",
            use_when_private_ws_stale: true,
        },
    })
}

pub fn coinex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let market = normalize_coinex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let (method, params) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            ("depth.subscribe", json!([market, 50, "0", true]))
        }
        PublicStreamKind::Trades => ("deals.subscribe", json!([market])),
        PublicStreamKind::Ticker => ("state.subscribe", json!([market])),
        PublicStreamKind::Candles { interval } => ("kline.subscribe", json!([market, interval])),
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params,
    }))
}

pub fn coinex_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    Ok(match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "depth.subscribe",
        PublicStreamKind::Trades => "deals.subscribe",
        PublicStreamKind::Ticker => "state.subscribe",
        PublicStreamKind::Candles { .. } => "kline.subscribe",
    })
}

pub fn coinex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "depth.unsubscribe"
        }
        PublicStreamKind::Trades => "deals.unsubscribe",
        PublicStreamKind::Ticker => "state.unsubscribe",
        PublicStreamKind::Candles { .. } => "kline.unsubscribe",
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": [normalize_coinex_symbol(&subscription.symbol.exchange_symbol.symbol)?],
    }))
}

pub fn coinex_private_login_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: i64,
    id: u64,
) -> ExchangeApiResult<Value> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.private_ws_missing_credentials",
        });
    }
    let timestamp = timestamp_ms.to_string();
    let signature = sign_request(api_secret, "GET", "/v2/ws/auth", "", &timestamp);
    Ok(json!({
        "id": id,
        "method": "server.sign",
        "params": {
            "access_id": api_key,
            "signed_str": signature,
            "timestamp": timestamp_ms
        }
    }))
}

pub fn coinex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "order.subscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.subscribe",
        PrivateStreamKind::Fills => "deal.subscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": ["SPOT"],
    }))
}

pub fn coinex_private_channel(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    Ok(match subscription.kind {
        PrivateStreamKind::Orders => "order.subscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.subscribe",
        PrivateStreamKind::Fills => "deal.subscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    })
}

pub fn coinex_private_unsubscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "order.unsubscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.unsubscribe",
        PrivateStreamKind::Fills => "deal.unsubscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": ["SPOT"],
    }))
}

pub fn coinex_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "server.ping",
        "params": []
    })
}

pub fn parse_coinex_stream_control(value: &Value) -> CoinExStreamControlMessage {
    if value.get("method").and_then(Value::as_str) == Some("server.pong")
        || value.get("message").and_then(Value::as_str) == Some("pong")
    {
        return CoinExStreamControlMessage::Pong;
    }
    if value.get("error").is_some()
        || value
            .get("code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
    {
        let error = value.get("error").unwrap_or(value);
        return CoinExStreamControlMessage::Error {
            code: error
                .get("code")
                .or_else(|| value.get("code"))
                .and_then(value_as_i64),
            message: error
                .get("message")
                .or_else(|| error.get("msg"))
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string),
        };
    }
    let id = value.get("id").and_then(Value::as_u64);
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    if method.as_deref() == Some("server.sign")
        || value.get("result").and_then(Value::as_str) == Some("success")
    {
        return CoinExStreamControlMessage::AuthAck { id };
    }
    if id.is_some()
        && (value.get("result").is_some()
            || method
                .as_deref()
                .is_some_and(|text| text.ends_with(".subscribe")))
    {
        return CoinExStreamControlMessage::SubscriptionAck { id, method };
    }
    CoinExStreamControlMessage::Data(value.clone())
}

pub fn coinex_ws_now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
