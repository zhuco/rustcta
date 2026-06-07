#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, Fill, MarketType, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_bingx_symbol, parse_error, parse_orderbook_snapshot};
use super::private_parser::{parse_fills, parse_order_state};
use super::BingxGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub enum BingxPublicStreamMessage {
    OrderBook(OrderBookSnapshot),
    SubscriptionAck { id: Option<String> },
    Ping,
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BingxPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Fills(Vec<Fill>),
    Account(Value),
    SubscriptionAck { id: Option<String> },
    Ping,
    Pong,
}

impl BingxGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        let payload = bingx_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "bingx:{}:{}",
            self.ws_url(subscription.symbol.market_type),
            payload
                .get("dataType")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let listen_key = self.bingx_listen_key().await?;
        let payload = bingx_private_subscribe_payload(&subscription)?;
        let url = format!("{}?listenKey={listen_key}", self.ws_url(market_type));
        Ok(format!(
            "bingx:{url}:{}",
            payload
                .get("dataType")
                .and_then(Value::as_str)
                .unwrap_or("listenKey")
        ))
    }

    async fn bingx_listen_key(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(
                "bingx.generate_listen_key",
                "/openApi/user/auth/userDataStream",
                &std::collections::HashMap::new(),
            )
            .await?;
        value
            .get("data")
            .and_then(|data| data.get("listenKey"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("BingX listenKey response missing listenKey: {value}"),
            })
    }

    fn ws_url(&self, market_type: MarketType) -> &str {
        if market_type == MarketType::Spot {
            &self.config.spot_ws_url
        } else {
            &self.config.swap_ws_url
        }
    }
}

pub fn bingx_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn bingx_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_bingx_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let data_type = match &subscription.kind {
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::OrderBookDelta => {
            if subscription.symbol.market_type == MarketType::Spot {
                format!("{symbol}@depth")
            } else {
                format!("{symbol}@depth50")
            }
        }
        PublicStreamKind::OrderBookSnapshot => format!("{symbol}@depth5@500ms"),
        PublicStreamKind::Candles { interval } => {
            format!("{symbol}@kline_{}", normalize_interval(interval)?)
        }
    };
    Ok(json!({
        "id": subscription
            .context
            .request_id
            .clone()
            .unwrap_or_else(|| format!("bingx-{}", Utc::now().timestamp_millis())),
        "reqType": "sub",
        "dataType": data_type,
    }))
}

pub fn bingx_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    let data_type = match (subscription.market_type, &subscription.kind) {
        (Some(MarketType::Spot), PrivateStreamKind::Orders | PrivateStreamKind::Fills) => {
            "spot.executionReport"
        }
        (Some(MarketType::Spot), _) => "spot.executionReport",
        (_, PrivateStreamKind::Orders | PrivateStreamKind::Fills) => "ORDER_TRADE_UPDATE",
        (
            _,
            PrivateStreamKind::Balances | PrivateStreamKind::Positions | PrivateStreamKind::Account,
        ) => "ACCOUNT_UPDATE",
    };
    Ok(json!({
        "id": subscription
            .context
            .request_id
            .clone()
            .unwrap_or_else(|| format!("bingx-private-{}", Utc::now().timestamp_millis())),
        "reqType": "sub",
        "dataType": data_type,
    }))
}

pub fn bingx_heartbeat_response(text: &str) -> Option<&'static str> {
    matches!(text.trim(), "Ping" | "ping").then_some("Pong")
}

pub fn parse_bingx_public_stream_message(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BingxPublicStreamMessage> {
    if is_subscription_ack(value) {
        return Ok(BingxPublicStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_str).map(str::to_string),
        });
    }
    if value.get("pong").is_some() {
        return Ok(BingxPublicStreamMessage::Pong);
    }
    if value.get("ping").is_some() {
        return Ok(BingxPublicStreamMessage::Ping);
    }
    let data_type = value
        .get("dataType")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if data_type.contains("@depth")
        || value
            .get("data")
            .and_then(|data| data.get("bids"))
            .is_some()
    {
        return Ok(BingxPublicStreamMessage::OrderBook(
            parse_orderbook_snapshot(exchange_id, symbol, value)?,
        ));
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported BingX public stream message",
        value,
    ))
}

pub fn parse_bingx_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol_hint: Option<SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<BingxPrivateStreamMessage> {
    if is_subscription_ack(value) {
        return Ok(BingxPrivateStreamMessage::SubscriptionAck {
            id: value.get("id").and_then(Value::as_str).map(str::to_string),
        });
    }
    if value.get("pong").is_some() {
        return Ok(BingxPrivateStreamMessage::Pong);
    }
    if value.get("ping").is_some() {
        return Ok(BingxPrivateStreamMessage::Ping);
    }
    let data = value.get("data").unwrap_or(value);
    let event_type = data
        .get("e")
        .or_else(|| value.get("e"))
        .and_then(Value::as_str)
        .or_else(|| value.get("dataType").and_then(Value::as_str))
        .unwrap_or_default();
    if event_type == "ACCOUNT_UPDATE" || data.get("a").is_some() {
        return Ok(BingxPrivateStreamMessage::Account(data.clone()));
    }
    let order_payload = normalized_order_payload(data);
    let order = parse_order_state(
        exchange_id,
        symbol_hint.as_ref(),
        market_type,
        &order_payload,
    )?;
    if is_trade_update(data) {
        let fills = parse_fills(
            exchange_id,
            tenant_id,
            account_id,
            symbol_hint.as_ref(),
            market_type,
            &json!({ "data": { "fills": [normalized_fill_payload(data)] } }),
        )?;
        if !fills.is_empty() {
            return Ok(BingxPrivateStreamMessage::Fills(fills));
        }
    }
    Ok(BingxPrivateStreamMessage::Order(order))
}

fn normalized_order_payload(value: &Value) -> Value {
    let order = value.get("o").unwrap_or(value);
    json!({
        "symbol": order.get("s").or_else(|| order.get("symbol")).cloned().unwrap_or(Value::Null),
        "clientOrderId": order.get("c").or_else(|| order.get("clientOrderId")).cloned().unwrap_or(Value::Null),
        "orderId": order.get("i").or_else(|| order.get("orderId")).cloned().unwrap_or(Value::Null),
        "side": order.get("S").or_else(|| order.get("side")).cloned().unwrap_or(Value::String("BUY".to_string())),
        "type": order.get("o").or_else(|| order.get("type")).cloned().unwrap_or(Value::String("LIMIT".to_string())),
        "origQty": order.get("q").or_else(|| order.get("origQty")).cloned().unwrap_or(Value::String("0".to_string())),
        "price": order.get("p").or_else(|| order.get("price")).cloned().unwrap_or(Value::Null),
        "avgPrice": order.get("ap").or_else(|| order.get("avgPrice")).cloned().unwrap_or(Value::Null),
        "executedQty": order.get("z").or_else(|| order.get("executedQty")).cloned().unwrap_or(Value::String("0".to_string())),
        "status": order.get("X").or_else(|| order.get("status")).cloned().unwrap_or(Value::String("UNKNOWN".to_string())),
        "positionSide": order.get("ps").or_else(|| order.get("positionSide")).cloned().unwrap_or(Value::String("BOTH".to_string())),
        "time": order.get("T").or_else(|| value.get("T")).or_else(|| value.get("E")).cloned().unwrap_or(Value::Null),
        "updateTime": order.get("T").or_else(|| value.get("T")).or_else(|| value.get("E")).cloned().unwrap_or(Value::Null),
    })
}

fn normalized_fill_payload(value: &Value) -> Value {
    let order = value.get("o").unwrap_or(value);
    json!({
        "symbol": order.get("s").or_else(|| order.get("symbol")).cloned().unwrap_or(Value::Null),
        "orderId": order.get("i").or_else(|| order.get("orderId")).cloned().unwrap_or(Value::Null),
        "clientOrderId": order.get("c").or_else(|| order.get("clientOrderId")).cloned().unwrap_or(Value::Null),
        "id": order.get("t").or_else(|| order.get("tradeId")).cloned().unwrap_or(Value::Null),
        "side": order.get("S").or_else(|| order.get("side")).cloned().unwrap_or(Value::String("BUY".to_string())),
        "positionSide": order.get("ps").or_else(|| order.get("positionSide")).cloned().unwrap_or(Value::String("BOTH".to_string())),
        "price": order.get("L").or_else(|| order.get("ap")).or_else(|| order.get("price")).cloned().unwrap_or(Value::String("0".to_string())),
        "qty": order.get("l").or_else(|| order.get("z")).or_else(|| order.get("quantity")).cloned().unwrap_or(Value::String("0".to_string())),
        "commission": order.get("n").or_else(|| order.get("commission")).cloned().unwrap_or(Value::Null),
        "commissionAsset": order.get("N").or_else(|| order.get("commissionAsset")).cloned().unwrap_or(Value::Null),
        "time": order.get("T").or_else(|| value.get("T")).or_else(|| value.get("E")).cloned().unwrap_or(Value::Null),
    })
}

fn is_subscription_ack(value: &Value) -> bool {
    value
        .get("code")
        .and_then(|code| code.as_i64().or_else(|| code.as_str()?.parse().ok()))
        == Some(0)
        && value.get("id").is_some()
}

fn is_trade_update(value: &Value) -> bool {
    let order = value.get("o").unwrap_or(value);
    order
        .get("x")
        .or_else(|| order.get("executionType"))
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("TRADE"))
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim() {
        "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d" | "1w" => {
            Ok(interval.trim().to_string())
        }
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported BingX kline interval {other}"),
        }),
    }
}
