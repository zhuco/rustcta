#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

pub fn private_rpc_method(value: &Value) -> Option<&str> {
    value.get("method").and_then(Value::as_str)
}

pub fn private_event_channel(value: &Value) -> Option<&str> {
    value
        .get("params")
        .and_then(|params| params.get("channel"))
        .and_then(Value::as_str)
        .or_else(|| value.get("channel").and_then(Value::as_str))
}

pub fn parse_replace_order_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = value
        .get("result")
        .and_then(|result| result.get("order"))
        .or_else(|| value.get("result"))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "derive replace ack missing result.order".to_string(),
        })?;
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.exchange_symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text(order, "label").or_else(|| text(order, "client_order_id")),
        exchange_order_id: text(order, "order_id").or_else(|| text(order, "id")),
        side: parse_side(text(order, "direction").as_deref())?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(text(order, "order_type").as_deref()),
        time_in_force: None,
        status: parse_status(
            text(order, "order_status")
                .or_else(|| text(order, "status"))
                .as_deref(),
        ),
        quantity: text(order, "amount")
            .or_else(|| text(order, "quantity"))
            .unwrap_or_else(|| "0".to_string()),
        price: text(order, "limit_price").or_else(|| text(order, "price")),
        filled_quantity: text(order, "filled_amount")
            .or_else(|| text(order, "filled_quantity"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: text(order, "average_price")
            .or_else(|| text(order, "average_fill_price")),
        reduce_only: order
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: order
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: None,
        updated_at: now,
    })
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown Derive order side {other}"),
        }),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "active" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        "partially_filled" | "partial_fill" => OrderStatus::PartiallyFilled,
        "new" | "pending" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}

fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|field| {
            field
                .as_str()
                .map(str::to_string)
                .or_else(|| field.as_i64().map(|number| number.to_string()))
                .or_else(|| field.as_u64().map(|number| number.to_string()))
                .or_else(|| field.as_f64().map(|number| number.to_string()))
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
