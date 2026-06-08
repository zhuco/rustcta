use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::{parse_datetime_value, string_or_number};

pub fn parse_spot_order_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    let side = match value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown HitBTC order side {other}"),
            })
        }
    };
    Ok(rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: string_or_number(value.get("id")),
        side,
        position_side: Some(PositionSide::None),
        order_type: match value.get("type").and_then(Value::as_str).unwrap_or("limit") {
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        },
        time_in_force: None,
        status: parse_status(value.get("status").and_then(Value::as_str)),
        quantity: string_or_number(value.get("quantity")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(value.get("quantity_cumulative"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("price_average")),
        reduce_only: false,
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value.get("created_at").and_then(parse_datetime_value),
        updated_at: value
            .get("updated_at")
            .and_then(parse_datetime_value)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default() {
        "new" | "suspended" => OrderStatus::New,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}
