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

use super::parser::{normalize_bydfi_symbol, parse_orderbook_snapshot};
use super::private_parser::{parse_balances, parse_order_state, parse_positions};
use super::signing::sign_ws_login;
use super::BydfiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BYDFI_PUBLIC_WS_PING_INTERVAL_SECONDS: u64 = 30;
pub const BYDFI_PUBLIC_WS_MAX_MISSED_PONGS: u64 = 2;

impl BydfiGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(subscription.symbol.market_type, "bydfi.spot_public_stream")?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bydfi.public_streams_disabled",
            });
        }
        let payload = bydfi_public_subscribe_payload(&subscription, 1)?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "subscribe",
            "url": self.config.public_ws_url,
            "path_stream": bydfi_public_stream_name(&subscription)?,
            "payload": payload,
            "unsubscribe": bydfi_public_unsubscribe_payload(&subscription, 2)?,
            "heartbeat": bydfi_public_ws_heartbeat_spec(),
            "resync": {
                "order_book": "REST GET /v1/fapi/market/depth"
            }
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if subscription
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bydfi.private_stream_non_perpetual",
            });
        }
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bydfi.private_ws_url_undocumented",
            });
        }
        let (api_key, api_secret) = self.private_credentials("bydfi.private_ws_login")?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let url = self
            .config
            .private_ws_url
            .as_deref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bydfi.private_ws_url_undocumented",
            })?;
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "market_type": "perpetual",
            "mode": "login_only",
            "url": url,
            "initial_payloads": [
                bydfi_private_ws_login_payload(api_key, api_secret, &timestamp, 1)?
            ],
            "stream": bydfi_private_stream_name(&subscription.kind),
            "account_id": subscription.account_id.to_string(),
            "resync": {
                "orders": "REST GET /v2/fapi/trade/open_order",
                "balances": "REST GET /v1/fapi/account/balance",
                "positions": "REST GET /v2/fapi/trade/positions"
            }
        })
        .to_string())
    }
}

pub fn bydfi_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id,
        "method": "SUBSCRIBE",
        "params": [bydfi_public_stream_name(subscription)?],
    }))
}

pub fn bydfi_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id,
        "method": "UNSUBSCRIBE",
        "params": [bydfi_public_stream_name(subscription)?],
    }))
}

pub fn bydfi_public_ws_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "ping",
    })
}

pub fn bydfi_public_ws_heartbeat_spec() -> Value {
    json!({
        "client_ping_interval_seconds": BYDFI_PUBLIC_WS_PING_INTERVAL_SECONDS,
        "max_missed_pongs": BYDFI_PUBLIC_WS_MAX_MISSED_PONGS,
        "client_ping_template": bydfi_public_ws_ping_payload(0),
        "pong_result": "pong",
        "server_retention_seconds": 1200,
    })
}

pub fn bydfi_private_ws_login_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_millis: &str,
    id: u64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "id": id,
        "method": "LOGIN",
        "params": {
            "apiKey": api_key,
            "timestamp": timestamp_millis.parse::<i64>().unwrap_or_default(),
            "sign": sign_ws_login(api_key, api_secret, timestamp_millis)?,
        }
    }))
}

pub fn bydfi_private_stream_capabilities() -> PrivateStreamCapabilities {
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
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn parse_bydfi_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if value.get("result").and_then(Value::as_str) == Some("pong") {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    if value.get("result").is_some() && value.get("id").is_some() {
        return Ok(None);
    }
    if value.get("e").and_then(Value::as_str) == Some("depthUpdate") {
        let normalized = json!({
            "lastUpdateId": value.get("u").cloned().unwrap_or(Value::Null),
            "E": value.get("E").cloned().unwrap_or(Value::Null),
            "bids": value.get("b").cloned().unwrap_or(Value::Array(Vec::new())),
            "asks": value.get("a").cloned().unwrap_or(Value::Array(Vec::new())),
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
        operation: "bydfi.public_stream_message",
    })
}

pub fn parse_bydfi_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match value.get("e").and_then(Value::as_str) {
        Some("ACCOUNT_UPDATE") => {
            let account = value.get("a").unwrap_or(value);
            let mut events = Vec::new();
            if account.get("B").is_some() {
                events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    balances: parse_balances(
                        exchange_id,
                        tenant_id.clone(),
                        account_id.clone(),
                        &[],
                        &json!({ "data": account.get("B").cloned().unwrap_or(Value::Array(Vec::new())) }),
                    )?,
                }));
            }
            if account.get("p").is_some() {
                events.push(ExchangeStreamEvent::PositionSnapshot(
                    PositionsResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(exchange_id.clone(), None),
                        positions: parse_positions(
                            exchange_id,
                            tenant_id,
                            account_id,
                            &json!({ "data": account.get("p").cloned().unwrap_or(Value::Array(Vec::new())) }),
                        )?,
                    },
                ));
            }
            Ok(events)
        }
        Some("ORDER_TRADE_UPDATE") => Ok(vec![ExchangeStreamEvent::OrderUpdate(
            parse_order_state(exchange_id, None, value.get("o").unwrap_or(value))?,
        )]),
        _ if value.get("method").and_then(Value::as_str) == Some("LOGIN") => Ok(Vec::new()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bydfi.private_stream_message",
        }),
    }
}

fn bydfi_public_stream_name(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let symbol = normalize_bydfi_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let suffix = match &subscription.kind {
        PublicStreamKind::Trades => "trade".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta => "depth".to_string(),
        PublicStreamKind::OrderBookSnapshot => "depth10".to_string(),
        PublicStreamKind::Candles { interval } => {
            format!("kline_{}", interval.to_ascii_lowercase())
        }
    };
    Ok(format!("{symbol}@{suffix}"))
}

fn bydfi_private_stream_name(kind: &PrivateStreamKind) -> &'static str {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "ORDER_TRADE_UPDATE",
        PrivateStreamKind::Balances | PrivateStreamKind::Positions | PrivateStreamKind::Account => {
            "ACCOUNT_UPDATE"
        }
    }
}
