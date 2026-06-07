#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AuthRenewalKind, AuthRenewalPolicy, BalancesResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, HeartbeatCapability,
    HeartbeatDirection, HeartbeatPolicy, OrderBookResponse, PositionsResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, ReconnectCapability,
    StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};
use uuid::Uuid;

use super::parser::{normalize_blofin_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_order_state, parse_positions};
use super::signing::sign_request;
use super::BlofinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BLOFIN_WS_HEARTBEAT_INTERVAL_SECONDS: u64 = 25;
pub const BLOFIN_WS_MAX_MISSED_PINGS: u64 = 2;

impl BlofinGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(
            subscription.symbol.market_type,
            "blofin.spot_public_ws_unsupported",
        )?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "blofin.public_streams_disabled",
            });
        }
        let payload = blofin_public_subscribe_payload(&subscription)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "subscribe",
            "url": self.config.public_ws_url,
            "payload": payload,
            "unsubscribe": blofin_public_unsubscribe_payload(&subscription)?,
            "heartbeat": blofin_ws_heartbeat_spec(),
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_perpetual(
            subscription.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_private_ws_unsupported",
        )?;
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "blofin.private_streams_disabled",
            });
        }
        let (api_key, api_secret, passphrase) =
            self.private_credentials("blofin.subscribe_private_stream")?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let nonce = Uuid::new_v4().to_string();
        let login = blofin_ws_login_payload(api_key, api_secret, passphrase, &nonce, &timestamp)?;
        let subscribe = blofin_private_subscribe_payload(&subscription)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "login_subscribe",
            "url": self.config.private_ws_url,
            "initial_payloads": [login, subscribe],
            "heartbeat": blofin_ws_heartbeat_spec(),
            "account_id": subscription.account_id.to_string(),
        })
        .to_string())
    }
}

pub fn blofin_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: false,
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
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn blofin_stream_runtime_capability(private_streams: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("blofin private stream credentials disabled")
        },
        supports_subscribe: true,
        supports_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(BLOFIN_WS_HEARTBEAT_INTERVAL_SECONDS * 1_000),
            timeout_ms: Some(10_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private_streams,
            credential_scopes: if private_streams {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: None,
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: (BLOFIN_WS_HEARTBEAT_INTERVAL_SECONDS * 1_000) as i64,
            pong_timeout_ms: 10_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::ReLogin,
            renew_before_expiry_ms: 0,
            renewal_interval_ms: None,
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

pub fn blofin_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [blofin_public_channel(subscription)?],
    }))
}

pub fn blofin_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "unsubscribe",
        "args": [blofin_public_channel(subscription)?],
    }))
}

pub fn blofin_public_funding_rate_subscribe_payload(inst_id: &str) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "channel": "funding-rate",
            "instId": normalize_blofin_symbol(inst_id)?,
        }],
    }))
}

pub fn blofin_public_books5_subscribe_payload(inst_id: &str) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [{
            "channel": "books5",
            "instId": normalize_blofin_symbol(inst_id)?,
        }],
    }))
}

pub fn blofin_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "subscribe",
        "args": [blofin_private_channel(&subscription.kind)?],
    }))
}

pub fn blofin_private_algo_orders_subscribe_payload(
    inst_id: Option<&str>,
) -> ExchangeApiResult<Value> {
    let mut channel = serde_json::Map::new();
    channel.insert("channel".to_string(), json!("orders-algo"));
    if let Some(inst_id) = inst_id {
        channel.insert(
            "instId".to_string(),
            json!(normalize_blofin_symbol(inst_id)?),
        );
    }
    Ok(json!({
        "op": "subscribe",
        "args": [Value::Object(channel)],
    }))
}

pub fn blofin_ws_login_payload(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    nonce: &str,
    timestamp: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp,
            "sign": sign_request(api_secret, "/users/self/verify", "GET", timestamp, nonce, "")?,
            "nonce": nonce,
        }],
    }))
}

pub fn blofin_ws_ping_payload() -> &'static str {
    "ping"
}

pub fn blofin_ws_heartbeat_spec() -> Value {
    json!({
        "client_ping_interval_seconds": BLOFIN_WS_HEARTBEAT_INTERVAL_SECONDS,
        "max_missed_pongs": BLOFIN_WS_MAX_MISSED_PINGS,
        "client_ping_payload": blofin_ws_ping_payload(),
        "server_pong_payload": "pong",
    })
}

pub fn parse_blofin_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if is_heartbeat(value) || value.as_str() == Some("pong") {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    if is_control_event(value) {
        return Ok(None);
    }
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if channel == "books" || channel == "books5" {
        let normalized = json!({ "data": [value.get("data").cloned().unwrap_or(Value::Null)] });
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
        operation: "blofin.public_stream_channel",
    })
}

pub fn parse_blofin_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if is_heartbeat(value) || value.as_str() == Some("pong") {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if is_control_event(value) {
        return Ok(Vec::new());
    }
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    match channel {
        "orders" => parse_data_array(value)
            .iter()
            .map(|order| {
                parse_order_state(exchange_id, None, order).map(ExchangeStreamEvent::OrderUpdate)
            })
            .collect(),
        "positions" => Ok(vec![ExchangeStreamEvent::PositionSnapshot(
            PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions: parse_positions(exchange_id, tenant_id, account_id, value)?,
            },
        )]),
        "account" => Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
            BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &[],
                    &account_details_payload(value),
                )?,
            },
        )]),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "blofin.private_stream_channel",
        }),
    }
}

fn blofin_public_channel(subscription: &PublicStreamSubscription) -> ExchangeApiResult<Value> {
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "books",
        PublicStreamKind::Trades => "trades",
        PublicStreamKind::Ticker => "tickers",
        PublicStreamKind::Candles { interval } => {
            return Ok(json!({
                "channel": blofin_candle_channel(interval),
                "instId": normalize_blofin_symbol(&subscription.symbol.exchange_symbol.symbol)?,
            }))
        }
    };
    Ok(json!({
        "channel": channel,
        "instId": normalize_blofin_symbol(&subscription.symbol.exchange_symbol.symbol)?,
    }))
}

fn blofin_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<Value> {
    let channel = match kind {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "account",
        PrivateStreamKind::Positions => "positions",
        PrivateStreamKind::Fills => {
            return Err(ExchangeApiError::Unsupported {
                operation: "blofin.private_fills_stream_channel_unavailable",
            });
        }
    };
    Ok(json!({ "channel": channel }))
}

fn blofin_candle_channel(interval: &str) -> String {
    let normalized = match interval {
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "15m" => "15m",
        "30m" => "30m",
        "1h" | "1H" => "1H",
        "2h" | "2H" => "2H",
        "4h" | "4H" => "4H",
        "6h" | "6H" => "6H",
        "8h" | "8H" => "8H",
        "12h" | "12H" => "12H",
        "1d" | "1D" => "1D",
        "3d" | "3D" => "3D",
        "1w" | "1W" => "1W",
        "1M" | "1mo" => "1M",
        other => other,
    };
    format!("candle{normalized}")
}

fn is_heartbeat(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("pong")
        || value.get("op").and_then(Value::as_str) == Some("pong")
}

fn is_control_event(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str).is_some()
        && value.get("data").is_none()
        && value.get("arg").is_some()
}

fn parse_data_array(value: &Value) -> Vec<Value> {
    value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn account_details_payload(value: &Value) -> Value {
    let details = value
        .get("data")
        .and_then(|data| data.get("details"))
        .cloned()
        .or_else(|| {
            value
                .get("data")
                .and_then(Value::as_array)
                .map(|items| Value::Array(items.clone()))
        })
        .unwrap_or_else(|| Value::Array(Vec::new()));
    json!({ "data": details })
}
