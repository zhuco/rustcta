#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::bitso_canonical_pair;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances = value
        .get("payload")
        .and_then(|payload| payload.get("balances"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso balance fixture missing payload.balances".to_string(),
        })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("currency").and_then(Value::as_str))
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = order_rows(value)?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("oid").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = fill_rows(value)?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("tid").and_then(Value::as_i64))
        .map(|id| id.to_string())
        .collect())
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_or_self(value);
    let symbol = bitso_symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text(
        order
            .get("original_amount")
            .or_else(|| order.get("major"))
            .or_else(|| order.get("amount")),
    )
    .unwrap_or_else(|| "0".to_string());
    let unfilled = decimal(order.get("unfilled_amount"))?;
    let filled_quantity = match (decimal_from_text(&quantity), unfilled) {
        (Some(original), Some(remaining)) if original >= remaining => {
            trim_decimal(original - remaining)
        }
        _ => text(
            order
                .get("filled_amount")
                .or_else(|| order.get("filled_quantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
    };
    let status = parse_order_status(
        order.get("status").and_then(Value::as_str),
        &quantity,
        &filled_quantity,
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text(order.get("origin_id")),
        exchange_order_id: text(order.get("oid")),
        side: parse_side(order.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(order.get("time_in_force").and_then(Value::as_str)),
        status,
        quantity,
        price: text(order.get("price")).filter(|price| !is_zero_decimal(price)),
        filled_quantity,
        average_fill_price: text(
            order
                .get("avg_price")
                .or_else(|| order.get("average_price"))
                .or_else(|| order.get("average_fill_price")),
        ),
        reduce_only: false,
        post_only: order
            .get("time_in_force")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("postonly")),
        created_at: order.get("created_at").and_then(parse_timestamp),
        updated_at: order
            .get("updated_at")
            .or_else(|| order.get("created_at"))
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let mut orders = Vec::new();
    for order in order_rows(value)? {
        let parsed = parse_order_state(exchange_id, symbol_hint, order)?;
        if symbol_hint.is_none()
            || symbol_hint.is_some_and(|symbol| {
                symbol
                    .exchange_symbol
                    .symbol
                    .eq_ignore_ascii_case(parsed.exchange_symbol.symbol.as_str())
            })
        {
            orders.push(parsed);
        }
    }
    Ok(orders)
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let mut fills = Vec::new();
    for row in fill_rows(value)? {
        let symbol = bitso_symbol_scope(exchange_id, Some(symbol_hint), row)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitso fill requires canonical_symbol".to_string(),
                })?;
        let price = decimal(row.get("price"))?.ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso fill missing price".to_string(),
        })?;
        let quantity =
            decimal(row.get("major").or_else(|| row.get("amount")))?.ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "bitso fill missing major quantity".to_string(),
                }
            })?;
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol,
            exchange_symbol: Some(symbol.exchange_symbol),
            order_id: text(row.get("oid")),
            client_order_id: text(row.get("origin_id")),
            fill_id: text(row.get("tid")),
            side: parse_side(row.get("side").and_then(Value::as_str)),
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Unknown,
            price,
            quantity,
            quote_quantity: decimal(row.get("minor"))
                .or_else(|_| ExchangeApiResult::Ok(Some(price * quantity)))?,
            fee_asset: text(row.get("fees_currency")).map(|asset| asset.to_ascii_uppercase()),
            fee_amount: decimal(row.get("fees_amount"))?,
            fee_rate: None,
            realized_pnl: None,
            filled_at: row
                .get("created_at")
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

fn order_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .get("payload")
        .and_then(Value::as_array)
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .map(Vec::as_slice)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso open orders fixture missing payload array".to_string(),
        })
}

fn fill_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .get("payload")
        .and_then(Value::as_array)
        .or_else(|| value.get("fills").and_then(Value::as_array))
        .map(Vec::as_slice)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitso fills fixture missing payload array".to_string(),
        })
}

fn first_order_or_self(value: &Value) -> &Value {
    value
        .get("payload")
        .and_then(|payload| {
            payload
                .as_array()
                .and_then(|orders| orders.first())
                .or_else(|| payload.as_object().map(|_| payload))
        })
        .or_else(|| value.get("order"))
        .unwrap_or(value)
}

fn bitso_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    row: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let book = text(row.get("book")).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "bitso private order/fill missing book".to_string(),
    })?;
    let (base, quote) = bitso_canonical_pair(&book)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(&base, &quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, book)
            .map_err(validation_error)?,
    })
}

fn parse_order_status(status: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "active" => {
            if decimal_from_text(filled_quantity).is_some_and(|filled| filled > 0.0) {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        "completed" | "complete" | "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "pending" | "pending_open" => OrderStatus::New,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => {
            if decimal_from_text(quantity) == decimal_from_text(filled_quantity)
                && decimal_from_text(quantity).is_some_and(|value| value > 0.0)
            {
                OrderStatus::Filled
            } else {
                OrderStatus::Unknown
            }
        }
    }
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "immediateorcancel" | "ioc" => Some(TimeInForce::IOC),
        "fillorkill" | "fok" => Some(TimeInForce::FOK),
        "postonly" => Some(TimeInForce::GTX),
        "goodtillcancelled" | "gtc" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn decimal(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    Ok(match value {
        Some(Value::Number(value)) => value.as_f64(),
        Some(Value::String(value)) if value.trim().is_empty() => None,
        Some(Value::String(value)) => {
            Some(
                value
                    .trim()
                    .parse::<f64>()
                    .map_err(|error| ExchangeApiError::InvalidRequest {
                        message: format!("invalid Bitso decimal {value}: {error}"),
                    })?,
            )
        }
        Some(_) | None => None,
    })
}

fn decimal_from_text(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
}

fn trim_decimal(value: f64) -> String {
    let text = format!("{value:.16}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn is_zero_decimal(value: &str) -> bool {
    decimal_from_text(value).is_some_and(|value| value == 0.0)
}

fn parse_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|value| value.with_timezone(&Utc))
    } else {
        value.as_i64().and_then(|millis| {
            DateTime::<Utc>::from_timestamp_millis(millis)
                .or_else(|| DateTime::<Utc>::from_timestamp(millis, 0))
        })
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
