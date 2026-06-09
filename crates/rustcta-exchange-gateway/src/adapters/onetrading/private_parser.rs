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

pub fn parse_onetrading_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_object(exchange_id, symbol_hint, object_payload(value)?)
}

pub fn parse_onetrading_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    array_payload(value)
        .unwrap_or(&[])
        .iter()
        .map(|row| parse_order_object(exchange_id, Some(symbol), row))
        .collect()
}

pub fn parse_onetrading_recent_fills(
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
                message: "onetrading get_recent_fills requires canonical_symbol".to_string(),
            })?;
    array_payload(value)
        .unwrap_or(&[])
        .iter()
        .map(|row| {
            let price = number(row, &["price", "trade_price", "avg_price"])
                .ok_or_else(|| parse_error("onetrading fill missing price", row))?;
            let quantity = number(row, &["amount", "filled_amount", "quantity", "size"])
                .ok_or_else(|| parse_error("onetrading fill missing amount", row))?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: text(row, &["order_id", "orderId"]),
                client_order_id: text(row, &["client_id", "client_order_id", "clientOrderId"]),
                fill_id: text(row, &["id", "trade_id", "tradeId"]),
                side: parse_side(text(row, &["side"]).as_deref())?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(
                    text(row, &["liquidity", "liquidity_role"]).as_deref(),
                ),
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: text(row, &["fee_currency", "fee_asset"]),
                fee_amount: number(row, &["fee", "fee_amount"]),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp(row, &["time", "created_at", "createdAt", "timestamp"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_hint
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "onetrading order response requires symbol hint".to_string(),
        })?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text(value, &["client_id", "client_order_id", "clientOrderId"]),
        exchange_order_id: text(value, &["id", "order_id", "orderId"]),
        side: parse_side(text(value, &["side"]).as_deref())?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(text(value, &["type", "order_type"]).as_deref()),
        time_in_force: parse_time_in_force(
            text(value, &["time_in_force", "timeInForce"]).as_deref(),
        ),
        status: parse_status(text(value, &["status", "state"]).as_deref()),
        quantity: text(value, &["amount", "quantity", "size"]).unwrap_or_else(|| "0".to_string()),
        price: text(value, &["price", "limit_price"]),
        filled_quantity: text(
            value,
            &["filled_amount", "filled", "executed_amount", "filled_size"],
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: text(value, &["average_price", "avg_price", "averageFillPrice"]),
        reduce_only: false,
        post_only: matches!(
            parse_time_in_force(text(value, &["time_in_force", "timeInForce"]).as_deref()),
            Some(TimeInForce::GTX)
        ),
        created_at: timestamp(value, &["time", "created_at", "createdAt", "created"]),
        updated_at: timestamp(value, &["updated_at", "updatedAt", "time"]).unwrap_or_else(Utc::now),
    })
}

fn object_payload(value: &Value) -> ExchangeApiResult<&Value> {
    let data = value
        .get("data")
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    if data.is_object() {
        return Ok(data);
    }
    data.as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "onetrading order response missing object".to_string(),
        })
}

fn array_payload(value: &Value) -> Option<&[Value]> {
    let data = value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("trades"))
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
    if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        DateTime::<Utc>::from_timestamp(value, 0)
    }
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown OneTrading order side {other}"),
        }),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "GOOD_TILL_CANCELLED" | "GTC" => Some(TimeInForce::GTC),
        "IMMEDIATE_OR_CANCEL" | "IOC" => Some(TimeInForce::IOC),
        "FILL_OR_KILL" | "FOK" => Some(TimeInForce::FOK),
        "GOOD_TILL_CROSSING" | "POST_ONLY" | "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "OPEN" | "ACTIVE" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PARTIALLYFILLED" => OrderStatus::PartiallyFilled,
        "FILLED" | "CLOSED" => OrderStatus::Filled,
        "CANCELLING" | "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MAKER" => LiquidityRole::Maker,
        "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn parse_error(message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("{message}: {value}"),
    }
}
