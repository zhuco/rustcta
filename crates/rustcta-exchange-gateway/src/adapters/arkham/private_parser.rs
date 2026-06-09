#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeId, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

pub fn parse_arkham_order(
    exchange_id: &ExchangeId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_object(exchange_id, symbol_hint, object_payload(value)?)
}

pub fn parse_arkham_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    array_payload(value)
        .unwrap_or(&[])
        .iter()
        .map(|row| parse_order_object(exchange_id, symbol, row))
        .collect()
}

pub fn parse_arkham_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "arkham get_recent_fills requires canonical_symbol".to_string(),
            })?;
    array_payload(value)
        .unwrap_or(&[])
        .iter()
        .map(|row| {
            let price = number(row, &["price", "avgPrice", "averagePrice"])
                .ok_or_else(|| parse_error("arkham fill missing price", row))?;
            let quantity = number(row, &["size", "quantity", "qty", "filledSize"])
                .ok_or_else(|| parse_error("arkham fill missing quantity", row))?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: text(row, &["orderId", "order_id"]),
                client_order_id: text(row, &["clientOrderId", "client_order_id"]),
                fill_id: text(row, &["id", "tradeId", "fillId"]),
                side: parse_side(text(row, &["side"]).as_deref())?,
                position_side: if symbol.market_type == MarketType::Spot {
                    PositionSide::None
                } else {
                    PositionSide::Net
                },
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(text(row, &["liquidity", "role"]).as_deref()),
                price,
                quantity,
                quote_quantity: number(row, &["quoteQuantity", "quote_quantity", "notional"])
                    .or_else(|| Some(price * quantity)),
                fee_asset: text(row, &["feeAsset", "feeCurrency", "fee_asset"]),
                fee_amount: number(row, &["fee", "feeAmount", "fee_amount"]),
                fee_rate: None,
                realized_pnl: number(row, &["realizedPnl", "realized_pnl"]),
                filled_at: timestamp(row, &["filledAt", "time", "createdAt", "timestamp"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: text(value, &["clientOrderId", "client_order_id"]),
        exchange_order_id: text(value, &["id", "orderId", "order_id"]),
        side: parse_side(text(value, &["side"]).as_deref())?,
        position_side: Some(if symbol.market_type == MarketType::Spot {
            PositionSide::None
        } else {
            PositionSide::Net
        }),
        order_type: parse_order_type(text(value, &["type", "orderType", "order_type"]).as_deref()),
        time_in_force: parse_time_in_force(
            text(value, &["timeInForce", "time_in_force"]).as_deref(),
        ),
        status: parse_status(text(value, &["status", "state"]).as_deref()),
        quantity: text(value, &["size", "quantity", "qty"]).unwrap_or_else(|| "0".to_string()),
        price: text(value, &["price", "limitPrice"]),
        filled_quantity: text(value, &["filledSize", "filled_quantity", "executedSize"])
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: text(value, &["averagePrice", "avgPrice", "average_fill_price"]),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                matches!(
                    parse_time_in_force(text(value, &["timeInForce", "time_in_force"]).as_deref()),
                    Some(TimeInForce::GTX)
                )
            }),
        created_at: timestamp(value, &["createdAt", "created_at", "time"]),
        updated_at: timestamp(value, &["updatedAt", "updated_at", "createdAt"])
            .unwrap_or_else(Utc::now),
    })
}

fn object_payload(value: &Value) -> ExchangeApiResult<&Value> {
    let data = value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("order"))
        .unwrap_or(value);
    if data.is_object() {
        return Ok(data);
    }
    data.as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "arkham order response missing object".to_string(),
        })
}

fn array_payload(value: &Value) -> Option<&[Value]> {
    let data = value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("trades"))
        .or_else(|| value.get("fills"))
        .unwrap_or(value);
    data.as_array().map(Vec::as_slice)
}

fn text(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| string_or_number(value.get(*field)))
        .filter(|text| !text.trim().is_empty())
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| numeric_value(value.get(*field)))
}

fn numeric_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| parse_datetime(value.get(*field)))
}

fn parse_datetime(value: Option<&Value>) -> Option<DateTime<Utc>> {
    match value? {
        Value::String(text) => DateTime::parse_from_rfc3339(text)
            .map(|time| time.with_timezone(&Utc))
            .ok()
            .or_else(|| text.parse::<i64>().ok().and_then(timestamp_from_integer)),
        Value::Number(number) => number.as_i64().and_then(timestamp_from_integer),
        _ => None,
    }
}

fn timestamp_from_integer(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000_000 {
        DateTime::<Utc>::from_timestamp_micros(value)
    } else if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        DateTime::<Utc>::from_timestamp(value, 0)
    }
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown Arkham order side {other}"),
        }),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    let value = value.unwrap_or_default().to_ascii_lowercase();
    if value.contains("market") {
        OrderType::Market
    } else {
        OrderType::Limit
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "gtc" | "limitgtc" => Some(TimeInForce::GTC),
        "ioc" | "limitioc" | "marketioc" => Some(TimeInForce::IOC),
        "fok" | "limitfok" => Some(TimeInForce::FOK),
        "gtx" | "postonly" | "limitpostonly" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "new" => OrderStatus::New,
        "open" | "active" | "working" => OrderStatus::Open,
        "partiallyfilled" | "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "closed" => OrderStatus::Filled,
        "pendingcancel" | "pending_cancel" | "canceling" | "cancelling" => {
            OrderStatus::PendingCancel
        }
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn parse_error(message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("{message}: {value}"),
    }
}
