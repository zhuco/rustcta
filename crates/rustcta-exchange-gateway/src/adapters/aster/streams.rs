#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, PositionsResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{normalize_aster_symbol, parse_side, required_str, string_or_number};
use super::private_parser::parse_order_state;
use super::AsterGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, PositionSide, SchemaVersion,
};

impl AsterGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_perpetual(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "aster.public_streams_disabled",
            });
        }
        Ok(format!(
            "aster:{}:{}",
            self.config.public_ws_url,
            aster_public_stream_name(&subscription)?
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_perpetual(market_type)?;
        }
        if !self.config.enabled_private_streams || !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "aster.private_stream_requires_v3_api_wallet",
            });
        }
        let listen_key = self.create_listen_key().await?;
        Ok(format!(
            "aster:{}:{}:{}",
            self.config.private_ws_url,
            listen_key,
            aster_private_channel(&subscription.kind)
        ))
    }

    pub(super) async fn create_listen_key(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(
                "aster.create_listen_key",
                "/fapi/v3/listenKey",
                &HashMap::new(),
            )
            .await?;
        value
            .get("listenKey")
            .or_else(|| value.get("listen_key"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster listenKey response missing listenKey".to_string(),
            })
    }

    pub(super) async fn renew_listen_key(&self, listen_key: &str) -> ExchangeApiResult<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());
        self.send_signed_put("aster.renew_listen_key", "/fapi/v3/listenKey", &params)
            .await?;
        Ok(())
    }

    pub(super) async fn delete_listen_key(&self, listen_key: &str) -> ExchangeApiResult<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());
        self.send_signed_delete("aster.delete_listen_key", "/fapi/v3/listenKey", &params)
            .await?;
        Ok(())
    }
}

pub fn aster_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
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

pub fn aster_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "SUBSCRIBE",
        "params": [aster_public_stream_name(subscription)?],
        "id": 1
    }))
}

pub fn aster_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "method": "UNSUBSCRIBE",
        "params": [aster_public_stream_name(subscription)?],
        "id": 1
    }))
}

pub fn aster_ping_payload() -> Value {
    json!({ "method": "LIST_SUBSCRIPTIONS", "id": 1 })
}

pub fn aster_public_stream_name(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    let symbol =
        normalize_aster_symbol(&subscription.symbol.exchange_symbol.symbol)?.to_ascii_lowercase();
    Ok(match &subscription.kind {
        PublicStreamKind::Trades => format!("{symbol}@aggTrade"),
        PublicStreamKind::Ticker => format!("{symbol}@bookTicker"),
        PublicStreamKind::OrderBookDelta => format!("{symbol}@depth@100ms"),
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@depth20@100ms"),
        PublicStreamKind::Candles { interval } => format!("{symbol}@kline_{interval}"),
    })
}

pub fn aster_partial_depth_stream_name(
    symbol: impl AsRef<str>,
    depth: u32,
) -> ExchangeApiResult<String> {
    let symbol = normalize_aster_symbol(symbol.as_ref())?.to_ascii_lowercase();
    match depth {
        5 | 10 | 20 => Ok(format!("{symbol}@depth{depth}@100ms")),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("aster partial-depth stream supports depth 5, 10, or 20; got {other}"),
        }),
    }
}

pub fn aster_private_channel(kind: &PrivateStreamKind) -> &'static str {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => "ORDER_TRADE_UPDATE",
        PrivateStreamKind::Balances | PrivateStreamKind::Positions | PrivateStreamKind::Account => {
            "ACCOUNT_UPDATE"
        }
    }
}

pub fn parse_aster_private_stream_events(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let event_type = value.get("e").and_then(Value::as_str).unwrap_or_default();
    match event_type {
        "ORDER_TRADE_UPDATE" => {
            parse_aster_order_trade_update(exchange_id, subscription, symbol_hint, value)
        }
        "ACCOUNT_UPDATE" => parse_aster_account_update(exchange_id, subscription, value),
        "listenKeyExpired" => Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("aster private stream event unsupported: {event_type}"),
        }),
    }
}

fn parse_aster_order_trade_update(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let order_payload = value.get("o").unwrap_or(value);
    let mut events = vec![ExchangeStreamEvent::OrderUpdate(parse_order_state(
        exchange_id,
        symbol_hint,
        order_payload,
    )?)];
    if let Some(fill) = parse_aster_ws_fill(exchange_id, subscription, symbol_hint, order_payload)?
    {
        events.push(ExchangeStreamEvent::Fill(fill));
    }
    Ok(events)
}

fn parse_aster_ws_fill(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    order_payload: &Value,
) -> ExchangeApiResult<Option<Fill>> {
    let execution_type = order_payload
        .get("x")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let quantity = order_payload
        .get("l")
        .or_else(|| order_payload.get("lastFilledQty"))
        .and_then(value_as_f64)
        .unwrap_or(0.0);
    if !execution_type.eq_ignore_ascii_case("TRADE") || quantity <= 0.0 {
        return Ok(None);
    }
    let exchange_symbol_text = required_str(exchange_id, order_payload, "s")?.to_ascii_uppercase();
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| linear_symbol_scope(exchange_id, &exchange_symbol_text))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster private fill parser requires canonical symbol".to_string(),
            })?;
    let is_maker = order_payload
        .get("m")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(Some(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: subscription
            .context
            .tenant_id
            .clone()
            .unwrap_or_else(|| TenantId::new("default").expect("tenant")),
        account_id: subscription.account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(order_payload.get("i")),
        client_order_id: string_or_number(order_payload.get("c")),
        fill_id: string_or_number(order_payload.get("t")),
        side: parse_side(required_str(exchange_id, order_payload, "S")?)?,
        position_side: parse_aster_position_side(
            order_payload.get("ps").and_then(Value::as_str),
            0.0,
        ),
        status: FillStatus::Confirmed,
        liquidity_role: if is_maker {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        },
        price: order_payload
            .get("L")
            .or_else(|| order_payload.get("lastFilledPrice"))
            .and_then(value_as_f64)
            .unwrap_or(0.0),
        quantity,
        quote_quantity: order_payload
            .get("Y")
            .or_else(|| order_payload.get("lastQuoteQty"))
            .and_then(value_as_f64),
        fee_asset: order_payload
            .get("N")
            .and_then(Value::as_str)
            .map(str::to_string),
        fee_amount: order_payload.get("n").and_then(value_as_f64),
        fee_rate: None,
        realized_pnl: order_payload.get("rp").and_then(value_as_f64),
        filled_at: order_payload
            .get("T")
            .or_else(|| order_payload.get("E"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    }))
}

fn parse_aster_account_update(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let account = value.get("a").unwrap_or(value);
    let mut events = Vec::new();
    if let Some(balance_rows) = account.get("B").and_then(Value::as_array) {
        let balances = balance_rows
            .iter()
            .filter_map(parse_aster_ws_balance)
            .collect::<Vec<_>>();
        if !balances.is_empty() {
            events.push(ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                balances: vec![ExchangeBalance {
                    schema_version: SchemaVersion::current(),
                    tenant_id: subscription
                        .context
                        .tenant_id
                        .clone()
                        .unwrap_or_else(|| TenantId::new("default").expect("tenant")),
                    account_id: subscription.account_id.clone(),
                    exchange_id: exchange_id.clone(),
                    market_type: MarketType::Perpetual,
                    balances,
                    observed_at: event_time(value),
                }],
            }));
        }
    }
    if let Some(position_rows) = account.get("P").and_then(Value::as_array) {
        let positions = position_rows
            .iter()
            .filter_map(|row| parse_aster_ws_position(exchange_id, subscription, row).transpose())
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        if !positions.is_empty() {
            events.push(ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(
                    exchange_id.clone(),
                    subscription.context.request_id.clone(),
                ),
                positions,
            }));
        }
    }
    Ok(events)
}

fn parse_aster_ws_balance(row: &Value) -> Option<AssetBalance> {
    let asset = row
        .get("a")
        .or_else(|| row.get("asset"))
        .and_then(Value::as_str)?
        .to_ascii_uppercase();
    let total = row
        .get("wb")
        .or_else(|| row.get("walletBalance"))
        .and_then(value_as_f64)
        .unwrap_or(0.0);
    let available = row
        .get("cw")
        .or_else(|| row.get("crossWalletBalance"))
        .and_then(value_as_f64)
        .unwrap_or(total);
    let locked = (total - available).max(0.0);
    AssetBalance::new(asset, total, available, locked).ok()
}

fn parse_aster_ws_position(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    row: &Value,
) -> ExchangeApiResult<Option<ExchangePosition>> {
    let exchange_symbol_text = required_str(exchange_id, row, "s")?.to_ascii_uppercase();
    let quantity = row
        .get("pa")
        .or_else(|| row.get("positionAmt"))
        .and_then(value_as_f64)
        .unwrap_or(0.0);
    if quantity == 0.0 {
        return Ok(None);
    }
    let (base, quote) = split_linear_symbol(&exchange_symbol_text);
    let canonical_symbol =
        CanonicalSymbol::new(base, quote).map_err(|error| ExchangeApiError::InvalidRequest {
            message: error.to_string(),
        })?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        exchange_symbol_text,
    )
    .map_err(|error| ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    })?;
    Ok(Some(ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id: subscription
            .context
            .tenant_id
            .clone()
            .unwrap_or_else(|| TenantId::new("default").expect("tenant")),
        account_id: subscription.account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol: Some(exchange_symbol),
        side: parse_aster_position_side(row.get("ps").and_then(Value::as_str), quantity),
        quantity: quantity.abs(),
        entry_price: row
            .get("ep")
            .or_else(|| row.get("entryPrice"))
            .and_then(value_as_f64),
        mark_price: row
            .get("mp")
            .or_else(|| row.get("markPrice"))
            .and_then(value_as_f64),
        liquidation_price: None,
        unrealized_pnl: row
            .get("up")
            .or_else(|| row.get("unrealizedProfit"))
            .and_then(value_as_f64),
        leverage: None,
        observed_at: event_time(row),
    }))
}

fn linear_symbol_scope(exchange_id: &ExchangeId, symbol: &str) -> ExchangeApiResult<SymbolScope> {
    let (base, quote) = split_linear_symbol(symbol);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            })?,
    })
}

fn split_linear_symbol(symbol: &str) -> (&str, &str) {
    if let Some(base) = symbol.strip_suffix("USDT") {
        (base, "USDT")
    } else if let Some(base) = symbol.strip_suffix("USD") {
        (base, "USD")
    } else {
        (symbol, "USD")
    }
}

fn parse_aster_position_side(value: Option<&str>, quantity: f64) -> PositionSide {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("LONG") => PositionSide::Long,
        Some("SHORT") => PositionSide::Short,
        Some("BOTH") | Some("NET") => {
            if quantity < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            }
        }
        _ if quantity < 0.0 => PositionSide::Short,
        _ => PositionSide::Long,
    }
}

fn event_time(value: &Value) -> DateTime<Utc> {
    value
        .get("E")
        .or_else(|| value.get("T"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

pub fn parse_aster_diff_depth_delta(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster diff depth parser requires canonical_symbol".to_string(),
            })?;
    let first_sequence =
        value
            .get("U")
            .and_then(value_as_u64)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("aster diff depth missing U: {value}"),
            })?;
    let last_sequence =
        value
            .get("u")
            .and_then(value_as_u64)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("aster diff depth missing u: {value}"),
            })?;
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        chrono::Utc::now(),
    )
    .with_sequences(Some(first_sequence), Some(last_sequence))
    .with_previous_sequence(value.get("pu").and_then(value_as_u64));
    delta.bids = parse_depth_delta_levels(value.get("b").or_else(|| value.get("bids")))?;
    delta.asks = parse_depth_delta_levels(value.get("a").or_else(|| value.get("asks")))?;
    delta.exchange_timestamp = value
        .get("E")
        .or_else(|| value.get("T"))
        .and_then(value_as_i64)
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis);
    Ok(delta)
}

fn parse_depth_delta_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("aster invalid depth level: {level}"),
                })?;
            let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("aster invalid depth price: {level}"),
                }
            })?;
            let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("aster invalid depth quantity: {level}"),
                }
            })?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
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
