#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, CapabilitySupport, CredentialScope, ExchangeApiError,
    ExchangeApiResult, ExchangeStreamEvent, HeartbeatCapability, HeartbeatDirection,
    HeartbeatPolicy, OrderBookResponse, PositionsResponse, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    normalize_hyperliquid_coin, parse_balance_and_positions, parse_fills, parse_order_status,
    parse_orderbook_snapshot, symbol_scope,
};
use super::HyperliquidGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const HYPERLIQUID_WS_HEARTBEAT_INTERVAL_SECONDS: u64 = 30;
pub const HYPERLIQUID_WS_MAX_MISSED_PONGS: u64 = 3;

impl HyperliquidGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(
            subscription.symbol.market_type,
            "hyperliquid.spot_public_ws_unsupported",
        )?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "hyperliquid.public_streams_disabled",
            });
        }
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "subscribe",
            "url": self.config.ws_url,
            "payload": hyperliquid_public_subscribe_payload(&subscription)?,
            "unsubscribe": hyperliquid_public_unsubscribe_payload(&subscription)?,
            "heartbeat": hyperliquid_ws_heartbeat_spec(),
            "resync": { "kind": "rest_snapshot", "endpoint": "/info", "type": "l2Book" },
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_optional_perpetual(
            subscription.market_type,
            "hyperliquid.spot_private_ws_unsupported",
        )?;
        if !self.config.private_streams_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hyperliquid.private_streams_disabled",
            });
        }
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "subscribe",
            "url": self.config.ws_url,
            "payload": hyperliquid_private_subscribe_payload(
                &subscription,
                &self.account_address("hyperliquid.subscribe_private_stream")?
            )?,
            "heartbeat": hyperliquid_ws_heartbeat_spec(),
            "account_id": subscription.account_id.to_string(),
            "resync": {
                "kind": "rest_reconciliation",
                "endpoints": ["clearinghouseState", "openOrders", "userFillsByTime"]
            },
        })
        .to_string())
    }
}

pub fn hyperliquid_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
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

pub fn hyperliquid_stream_runtime_capability(private_streams: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("hyperliquid private stream requires account address")
        },
        supports_subscribe: true,
        supports_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(HYPERLIQUID_WS_HEARTBEAT_INTERVAL_SECONDS * 1_000),
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
            required: false,
            credential_scopes: if private_streams {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: None,
            uses_listen_key: false,
            requires_relogin_on_reconnect: false,
        },
        public_private_separate_connections: false,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: (HYPERLIQUID_WS_HEARTBEAT_INTERVAL_SECONDS * 1_000) as i64,
            pong_timeout_ms: 10_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::None,
            renew_before_expiry_ms: 0,
            renewal_interval_ms: None,
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        reconnect_requires_login: false,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

pub fn hyperliquid_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "subscribe",
        "subscription": public_subscription(&subscription.symbol.exchange_symbol.symbol, &subscription.kind)?,
    }))
}

pub fn hyperliquid_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "unsubscribe",
        "subscription": public_subscription(&subscription.symbol.exchange_symbol.symbol, &subscription.kind)?,
    }))
}

pub fn hyperliquid_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    user: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "subscribe",
        "subscription": private_subscription(&subscription.kind, user),
    }))
}

pub fn hyperliquid_private_unsubscribe_payload(
    subscription: &PrivateStreamSubscription,
    user: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "unsubscribe",
        "subscription": private_subscription(&subscription.kind, user),
    }))
}

pub fn hyperliquid_ws_heartbeat_spec() -> Value {
    json!({
        "mode": "client_ping_json",
        "ping": { "method": "ping" },
        "expected": "pong",
        "interval_ms": HYPERLIQUID_WS_HEARTBEAT_INTERVAL_SECONDS * 1_000,
        "timeout_ms": 10_000,
        "max_missed_pongs": HYPERLIQUID_WS_MAX_MISSED_PONGS,
    })
}

pub fn parse_hyperliquid_stream_event(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: Option<rustcta_exchange_api::TenantId>,
    account_id: Option<rustcta_exchange_api::AccountId>,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if value.get("channel").and_then(Value::as_str) == Some("pong") || value.get("pong").is_some() {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    match value.get("channel").and_then(Value::as_str) {
        Some("l2Book") => {
            let data = value.get("data").unwrap_or(value);
            let coin = data
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN");
            let scope = symbol_scope(exchange_id, coin)?;
            Ok(Some(ExchangeStreamEvent::OrderBookSnapshot(
                OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    order_book: parse_orderbook_snapshot(exchange_id, scope, data)?,
                },
            )))
        }
        Some("orderUpdates") => {
            let data = value.get("data").unwrap_or(value);
            let update = data
                .as_array()
                .and_then(|items| items.first())
                .unwrap_or(data);
            let order = update.get("order").unwrap_or(update);
            let status = update
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("open");
            Ok(Some(ExchangeStreamEvent::OrderUpdate(
                parse_order_status(exchange_id, &json!({ "status": status, "order": order }))?
                    .ok_or_else(|| ExchangeApiError::Serialization {
                        message: "hyperliquid orderUpdates missing order".to_string(),
                    })?,
            )))
        }
        Some("userFills") => {
            let Some(tenant_id) = tenant_id else {
                return Ok(None);
            };
            let Some(account_id) = account_id else {
                return Ok(None);
            };
            let fills = value
                .get("data")
                .and_then(|data| data.get("fills"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            let mut fills = parse_fills(exchange_id, tenant_id, account_id, &fills)?;
            Ok(fills.pop().map(ExchangeStreamEvent::Fill))
        }
        Some("webData2") | Some("userEvents") => {
            let Some(tenant_id) = tenant_id else {
                return Ok(None);
            };
            let Some(account_id) = account_id else {
                return Ok(None);
            };
            let data = value.get("data").unwrap_or(value);
            let positions =
                parse_balance_and_positions(exchange_id, tenant_id, account_id, data)?.1;
            Ok(Some(ExchangeStreamEvent::PositionSnapshot(
                PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    positions,
                },
            )))
        }
        _ => Ok(None),
    }
}

fn public_subscription(symbol: &str, kind: &PublicStreamKind) -> ExchangeApiResult<Value> {
    let coin = normalize_hyperliquid_coin(symbol)?;
    Ok(match kind {
        PublicStreamKind::Trades => json!({ "type": "trades", "coin": coin }),
        PublicStreamKind::Ticker => json!({ "type": "bbo", "coin": coin }),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            json!({ "type": "l2Book", "coin": coin })
        }
        PublicStreamKind::Candles { interval } => {
            json!({ "type": "candle", "coin": coin, "interval": interval })
        }
    })
}

fn private_subscription(kind: &PrivateStreamKind, user: &str) -> Value {
    let user = user.to_ascii_lowercase();
    match kind {
        PrivateStreamKind::Orders => json!({ "type": "orderUpdates", "user": user }),
        PrivateStreamKind::Fills => json!({ "type": "userFills", "user": user }),
        PrivateStreamKind::Balances | PrivateStreamKind::Positions | PrivateStreamKind::Account => {
            json!({ "type": "userEvents", "user": user })
        }
    }
}
