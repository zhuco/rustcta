#![allow(dead_code)]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    OrderState, PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangePosition, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::{json, Value};

use super::parser::{normalize_xt_symbol, split_xt_symbol};
use super::XtGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::StreamReconnectPolicy;

#[derive(Debug, Clone, PartialEq)]
pub enum XtPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    Trades(Vec<XtPublicTrade>),
    SubscriptionAck { id: Option<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum XtPrivateStreamMessage {
    Events(Vec<ExchangeStreamEvent>),
    SubscriptionAck { id: Option<String> },
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub struct XtPublicTrade {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

impl XtGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let payload = xt_public_subscribe_payload(&subscription, "1")?;
        let topic = payload["params"][0].as_str().unwrap_or("unknown");
        let url = if subscription.symbol.market_type == MarketType::Perpetual {
            &self.config.futures_public_ws_url
        } else {
            &self.config.spot_public_ws_url
        };
        Ok(format!("xt:{}:{}", url.trim_end_matches('/'), topic))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let listen_key = self.get_stream_listen_key(market_type).await?;
        let payload = xt_private_subscribe_payload(&subscription, market_type, &listen_key, "1")?;
        let topic = payload["params"][0].as_str().unwrap_or("unknown");
        let url = if market_type == MarketType::Perpetual {
            &self.config.futures_private_ws_url
        } else {
            &self.config.spot_private_ws_url
        };
        Ok(format!(
            "xt:{}:{}:{}",
            url.trim_end_matches('/'),
            topic,
            subscription.account_id
        ))
    }

    async fn get_stream_listen_key(&self, market_type: MarketType) -> ExchangeApiResult<String> {
        let value = match market_type {
            MarketType::Spot => {
                self.send_signed_post("xt.spot_ws_token", "/v4/ws-token", &HashMap::new())
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_get(
                    "xt.futures_listen_key",
                    "/future/user/v1/user/listen-key",
                    &HashMap::new(),
                )
                .await?
            }
            _ => unreachable!("checked by caller"),
        };
        value
            .get("result")
            .and_then(|result| {
                result
                    .get("accessToken")
                    .or_else(|| result.get("listenKey"))
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("xt listen key response missing token: {value}"),
            })
    }
}

pub fn xt_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: enabled,
        supports_fills: enabled,
        supports_balances: enabled,
        supports_positions: enabled,
        supports_account: enabled,
        order_event_kinds: if enabled {
            vec![
                PrivateOrderStreamEventKind::New,
                PrivateOrderStreamEventKind::PartialFill,
                PrivateOrderStreamEventKind::Fill,
                PrivateOrderStreamEventKind::Cancel,
                PrivateOrderStreamEventKind::Reject,
                PrivateOrderStreamEventKind::Expired,
                PrivateOrderStreamEventKind::BalanceUpdate,
            ]
        } else {
            Vec::new()
        },
        supports_client_order_id: enabled,
        supports_exchange_order_id: enabled,
    }
}

pub fn xt_ws_ping_text() -> &'static str {
    "ping"
}

pub fn xt_stream_reconnect_policy(market_type: MarketType) -> StreamReconnectPolicy {
    let ping_interval_ms = if market_type == MarketType::Spot {
        50_000
    } else {
        25_000
    };
    StreamReconnectPolicy {
        ping_interval_ms,
        pong_timeout_ms: 10_000,
        stale_message_ms: ping_interval_ms + 10_000,
        reconnect_backoff_ms: 1_000,
        max_reconnect_attempts: None,
    }
}

pub fn xt_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: &str,
) -> ExchangeApiResult<Value> {
    let symbol = xt_stream_symbol(&subscription.symbol.exchange_symbol.symbol);
    let topic = match &subscription.kind {
        PublicStreamKind::OrderBookDelta => format!("depth_update@{symbol}"),
        PublicStreamKind::OrderBookSnapshot => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("depth@{symbol},50,1000ms")
            } else {
                format!("depth@{symbol},20")
            }
        }
        PublicStreamKind::Trades => format!("trade@{symbol}"),
        PublicStreamKind::Ticker => {
            if subscription.symbol.market_type == MarketType::Perpetual {
                format!("agg_ticker@{symbol}")
            } else {
                format!("ticker@{symbol}")
            }
        }
        PublicStreamKind::Candles { interval } => format!("kline@{symbol},{interval}"),
    };
    Ok(json!({
        "id": id,
        "method": if subscription.symbol.market_type == MarketType::Perpetual { "SUBSCRIBE" } else { "subscribe" },
        "params": [topic],
    }))
}

pub fn xt_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    market_type: MarketType,
    listen_key: &str,
    id: &str,
) -> ExchangeApiResult<Value> {
    let topic = match (&subscription.kind, market_type) {
        (PrivateStreamKind::Orders, MarketType::Spot) => "order".to_string(),
        (PrivateStreamKind::Fills, MarketType::Spot) => "trade".to_string(),
        (PrivateStreamKind::Balances | PrivateStreamKind::Account, MarketType::Spot) => {
            "balance".to_string()
        }
        (PrivateStreamKind::Positions, MarketType::Spot) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "xt.spot_positions_stream",
            })
        }
        (PrivateStreamKind::Orders, MarketType::Perpetual) => format!("order@{listen_key}"),
        (PrivateStreamKind::Fills, MarketType::Perpetual) => format!("trade@{listen_key}"),
        (PrivateStreamKind::Balances | PrivateStreamKind::Account, MarketType::Perpetual) => {
            format!("balance@{listen_key}")
        }
        (PrivateStreamKind::Positions, MarketType::Perpetual) => format!("position@{listen_key}"),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "xt.private_stream_market_type",
            })
        }
    };
    if market_type == MarketType::Perpetual {
        Ok(json!({
            "id": id,
            "method": "SUBSCRIBE",
            "params": [topic],
        }))
    } else {
        Ok(json!({
            "id": id,
            "method": "subscribe",
            "listenKey": listen_key,
            "params": [topic],
        }))
    }
}

pub fn parse_xt_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<XtPublicStreamMessage> {
    if value.as_str() == Some("pong") {
        return Ok(XtPublicStreamMessage::Pong);
    }
    if value.get("code").and_then(Value::as_i64) == Some(0) {
        return Ok(XtPublicStreamMessage::SubscriptionAck {
            id: text(value.get("id")),
        });
    }
    let topic = value
        .get("topic")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if topic.contains("depth") {
        return Ok(XtPublicStreamMessage::OrderBook(parse_book(
            exchange_id,
            symbol_hint,
            value,
        )?));
    }
    if topic.contains("trade") {
        return Ok(XtPublicStreamMessage::Trades(vec![parse_trade(
            exchange_id,
            symbol_hint,
            value,
        )?]));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported xt public stream message",
        value,
    ))
}

pub fn parse_xt_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<XtPrivateStreamMessage> {
    if value.as_str() == Some("pong") {
        return Ok(XtPrivateStreamMessage::Pong);
    }
    if value.get("code").and_then(Value::as_i64) == Some(0) {
        return Ok(XtPrivateStreamMessage::SubscriptionAck {
            id: text(value.get("id")),
        });
    }
    let topic = value
        .get("topic")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let data = value.get("data").unwrap_or(value);
    let events = if topic.contains("order") {
        vec![ExchangeStreamEvent::OrderUpdate(parse_order(
            exchange_id,
            market_type,
            symbol_hint.as_ref(),
            data,
        )?)]
    } else if topic.contains("trade") {
        vec![ExchangeStreamEvent::Fill(parse_fill(
            exchange_id,
            tenant_id,
            account_id,
            market_type,
            symbol_hint.as_ref(),
            data,
        )?)]
    } else if topic.contains("balance") {
        vec![ExchangeStreamEvent::BalanceSnapshot(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            balances: vec![ExchangeBalance {
                schema_version: SchemaVersion::current(),
                tenant_id,
                account_id,
                exchange_id: exchange_id.clone(),
                market_type,
                balances: vec![AssetBalance::new(
                    required_text(exchange_id, data, &["c", "coin"])?,
                    number(data.get("b")).unwrap_or(0.0),
                    number(data.get("b")).unwrap_or(0.0) - number(data.get("f")).unwrap_or(0.0),
                    number(data.get("f")).unwrap_or(0.0),
                )
                .map_err(validation_error)?],
                observed_at: Utc::now(),
            }],
        })]
    } else if topic.contains("position") {
        vec![ExchangeStreamEvent::PositionSnapshot(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange_id.clone(), None),
            positions: vec![parse_position(
                exchange_id,
                tenant_id,
                account_id,
                market_type,
                data,
            )?],
        })]
    } else {
        return Err(parse_error(
            exchange_id.clone(),
            "unsupported xt private stream message",
            value,
        ));
    };
    Ok(XtPrivateStreamMessage::Events(events))
}

fn parse_book(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = levels(exchange_id, data.get("b"))?;
    let asks = levels(exchange_id, data.get("a"))?;
    let canonical =
        symbol_hint
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "xt stream book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol_hint.market_type,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol_hint.exchange_symbol);
    snapshot.sequence = data.get("u").and_then(number_u64);
    snapshot.exchange_timestamp = data
        .get("t")
        .and_then(number_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

fn parse_trade(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<XtPublicTrade> {
    let data = value.get("data").unwrap_or(value);
    Ok(XtPublicTrade {
        symbol: symbol_hint,
        trade_id: text(data.get("i")),
        side: if data.get("b").and_then(Value::as_bool) == Some(false) {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        price: required_text(exchange_id, data, &["p", "price"])?,
        quantity: required_text(exchange_id, data, &["q", "quantity"])?,
        traded_at: data
            .get("t")
            .and_then(number_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_order(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol_hint: Option<&rustcta_exchange_api::SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_data(exchange_id, market_type, data))?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text(data.get("ci").or_else(|| data.get("clientOrderId"))),
        exchange_order_id: text(data.get("i").or_else(|| data.get("orderId"))),
        side: side_from_text(data.get("sd").or_else(|| data.get("orderSide"))),
        position_side: Some(position_side(
            data.get("positionSide").and_then(Value::as_str),
        )),
        order_type: order_type(data.get("tp").and_then(Value::as_str)),
        time_in_force: Some(TimeInForce::GTC),
        status: order_status(data.get("st").and_then(Value::as_str)),
        quantity: required_text(exchange_id, data, &["oq", "quantity"])?,
        price: text(data.get("p").or_else(|| data.get("price"))),
        filled_quantity: required_text(exchange_id, data, &["eq", "executedQty"])
            .unwrap_or_else(|_| "0".to_string()),
        average_fill_price: text(data.get("ap").or_else(|| data.get("avgPrice"))),
        reduce_only: false,
        post_only: false,
        created_at: data
            .get("t")
            .and_then(number_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: Utc::now(),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    symbol_hint: Option<&rustcta_exchange_api::SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_data(exchange_id, market_type, data))?;
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "xt fill requires canonical_symbol".to_string(),
            })?;
    let price = number(data.get("price")).unwrap_or(0.0);
    let quantity = number(data.get("quantity")).unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol: canonical,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: text(data.get("orderId")),
        client_order_id: text(data.get("clientOrderId")),
        fill_id: text(data.get("tradeId")),
        side: side_from_text(data.get("orderSide")),
        position_side: position_side(data.get("positionSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: if data.get("isMaker").and_then(Value::as_bool) == Some(true) {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: text(data.get("feeCoin")),
        fee_amount: number(data.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: data
            .get("timestamp")
            .and_then(number_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    data: &Value,
) -> ExchangeApiResult<ExchangePosition> {
    let symbol = symbol_from_data(exchange_id, market_type, data)?;
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "xt position requires canonical_symbol".to_string(),
            })?;
    Ok(ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol: canonical,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: position_side(data.get("positionSide").and_then(Value::as_str)),
        quantity: number(data.get("positionSize")).unwrap_or(0.0),
        entry_price: number(data.get("entryPrice")),
        mark_price: number(data.get("markPrice")),
        liquidation_price: number(data.get("liquidationPrice")),
        unrealized_pnl: number(data.get("unrealizedPnl")),
        leverage: number(data.get("leverage")),
        observed_at: Utc::now(),
    })
}

fn levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "xt book missing levels", &Value::Null))?
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "xt book level is not an array", level)
            })?;
            OrderBookLevel::new(
                number(array.first()).unwrap_or(0.0),
                number(array.get(1)).unwrap_or(0.0),
            )
            .map_err(validation_error)
        })
        .collect()
}

fn symbol_from_data(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    data: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::SymbolScope> {
    let raw = text(data.get("s").or_else(|| data.get("symbol"))).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "xt stream message missing symbol",
            data,
        )
    })?;
    let normalized = normalize_xt_symbol(&raw, market_type)?;
    let (base, quote) = split_xt_symbol(&normalized)
        .ok_or_else(|| parse_error(exchange_id.clone(), "xt stream symbol split failed", data))?;
    Ok(rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
                .map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, &normalized)
            .map_err(validation_error)?,
    })
}

fn xt_stream_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .trim_start_matches("SPOT_")
        .trim_start_matches("PERP_")
        .replace('_', "")
        .to_ascii_lowercase()
        .replace("usdt", "_usdt")
}

fn side_from_text(value: Option<&Value>) -> OrderSide {
    match value
        .and_then(Value::as_str)
        .unwrap_or("BUY")
        .to_ascii_uppercase()
        .as_str()
    {
        "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or("BOTH").to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" | "NET" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("LIMIT").to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("NEW").to_ascii_uppercase().as_str() {
        "PARTIALLY_FILLED" | "PARTIAL_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::New,
    }
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<String> {
    fields
        .iter()
        .find_map(|field| text(value.get(*field)))
        .ok_or_else(|| parse_error(exchange_id.clone(), "xt stream missing field", value))
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number(value: Option<&Value>) -> Option<f64> {
    text(value)?.parse().ok()
}

fn number_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn number_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
