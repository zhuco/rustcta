#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{normalize_hyperliquid_coin, symbol_scope};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let summary = value.get("marginSummary").unwrap_or(value);
    let total: f64 = summary
        .get("accountValue")
        .and_then(decimal_as_f64)
        .unwrap_or(0.0);
    let used: f64 = summary
        .get("totalMarginUsed")
        .and_then(decimal_as_f64)
        .unwrap_or(0.0);
    let available: f64 = value
        .get("withdrawable")
        .and_then(decimal_as_f64)
        .unwrap_or_else(|| (total - used).max(0.0));
    let include_usdc = requested_assets.is_empty()
        || requested_assets
            .iter()
            .any(|asset| asset.eq_ignore_ascii_case("USDC"));
    let balances = if include_usdc {
        vec![AssetBalance::new("USDC", total, available, used).map_err(validation_error)?]
    } else {
        Vec::new()
    };
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let Some(items) = value.get("assetPositions").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .filter_map(|item| item.get("position").or(Some(item)))
        .filter_map(|position| {
            parse_position(exchange_id, &tenant_id, &account_id, position).transpose()
        })
        .collect()
}

pub fn parse_fee_snapshots(symbols: &[SymbolScope]) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: "0.0001".to_string(),
            taker_rate: "0.00035".to_string(),
            source: Some(
                "hyperliquid.default_fee_schedule_until_user_fee_endpoint_is_wired".to_string(),
            ),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value
        .as_array()
        .ok_or_else(|| parse_error("Hyperliquid openOrders response must be an array"))?;
    items
        .iter()
        .map(|order| {
            parse_order_state(exchange_id, fallback_symbol, order, Some(OrderStatus::Open))
        })
        .collect()
}

pub fn parse_historical_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let orders = value
        .as_array()
        .ok_or_else(|| parse_error("Hyperliquid historicalOrders response must be an array"))?;
    let Some(item) = orders.first() else {
        return Ok(None);
    };
    let order = item.get("order").unwrap_or(item);
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .map(map_order_status)
        .unwrap_or(OrderStatus::New);
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        order,
        Some(status),
    )?))
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value
        .as_array()
        .ok_or_else(|| parse_error("Hyperliquid userFills response must be an array"))?;
    fills
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                fill,
            )
        })
        .collect()
}

pub fn parse_order_ack(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    request_side: OrderSide,
    request_type: OrderType,
    request_quantity: &str,
    request_price: Option<String>,
) -> ExchangeApiResult<OrderState> {
    let status = first_status(value);
    let (exchange_order_id, order_status) =
        if let Some(resting) = status.and_then(|status| status.get("resting")) {
            (string_or_number(resting.get("oid")), OrderStatus::Open)
        } else if let Some(filled) = status.and_then(|status| status.get("filled")) {
            (string_or_number(filled.get("oid")), OrderStatus::Filled)
        } else if status.and_then(|status| status.get("error")).is_some() {
            (None, OrderStatus::Rejected)
        } else {
            (None, OrderStatus::New)
        };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id,
        side: request_side,
        position_side: Some(PositionSide::Net),
        order_type: request_type,
        time_in_force: None,
        status: order_status,
        quantity: request_quantity.to_string(),
        price: request_price,
        filled_quantity: status
            .and_then(|status| status.get("filled"))
            .and_then(|filled| string_or_number(filled.get("totalSz")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: status
            .and_then(|status| status.get("filled"))
            .and_then(|filled| string_or_number(filled.get("avgPx"))),
        reduce_only: false,
        post_only: request_type == OrderType::PostOnly,
        created_at: None,
        updated_at: Utc::now(),
    })
}

pub fn parse_cancel_ack(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> ExchangeApiResult<OrderState> {
    let status = first_status(value);
    let cancelled = status
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("success"))
        || status
            .and_then(|status| status.get("success"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: if cancelled {
            OrderStatus::Cancelled
        } else {
            OrderStatus::New
        },
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    })
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
    status: Option<OrderStatus>,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("cloid")),
        exchange_order_id: string_or_number(value.get("oid")),
        side: parse_side(value.get("side").and_then(Value::as_str).unwrap_or("B")),
        position_side: Some(PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: status.unwrap_or(OrderStatus::Open),
        quantity: string_or_number(value.get("sz"))
            .or_else(|| string_or_number(value.get("origSz")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("limitPx")).or_else(|| string_or_number(value.get("px"))),
        filled_quantity: string_or_number(value.get("filled")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPx")),
        reduce_only: false,
        post_only: false,
        created_at: value
            .get("timestamp")
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("statusTimestamp")
            .or_else(|| value.get("timestamp"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    position: &Value,
) -> ExchangeApiResult<Option<ExchangePosition>> {
    let coin = required_str(exchange_id, position, "coin")?;
    let quantity_signed: f64 = position.get("szi").and_then(decimal_as_f64).unwrap_or(0.0);
    if quantity_signed == 0.0 {
        return Ok(None);
    }
    let coin = normalize_hyperliquid_coin(coin)?;
    Ok(Some(ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: CanonicalSymbol::new(&coin, "USDC").map_err(validation_error)?,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
                .map_err(validation_error)?,
        ),
        side: if quantity_signed > 0.0 {
            PositionSide::Long
        } else {
            PositionSide::Short
        },
        quantity: quantity_signed.abs(),
        entry_price: position.get("entryPx").and_then(decimal_as_f64),
        mark_price: position.get("markPx").and_then(decimal_as_f64),
        liquidation_price: position.get("liquidationPx").and_then(decimal_as_f64),
        unrealized_pnl: position.get("unrealizedPnl").and_then(decimal_as_f64),
        leverage: position
            .get("leverage")
            .and_then(|leverage| leverage.get("value"))
            .and_then(decimal_as_f64),
        observed_at: Utc::now(),
    }))
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| parse_error("Hyperliquid fill missing canonical symbol"))?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("oid")),
        client_order_id: string_or_number(value.get("cloid")),
        fill_id: string_or_number(value.get("tid")).or_else(|| string_or_number(value.get("hash"))),
        side: parse_side(value.get("side").and_then(Value::as_str).unwrap_or("B")),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: if value.get("crossed").and_then(Value::as_bool) == Some(true) {
            LiquidityRole::Taker
        } else {
            LiquidityRole::Maker
        },
        price: value.get("px").and_then(decimal_as_f64).unwrap_or(0.0),
        quantity: value.get("sz").and_then(decimal_as_f64).unwrap_or(0.0),
        quote_quantity: None,
        fee_asset: value
            .get("feeToken")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some("USDC".to_string())),
        fee_amount: value.get("fee").and_then(decimal_as_f64),
        fee_rate: None,
        realized_pnl: value.get("closedPnl").and_then(decimal_as_f64),
        filled_at: value
            .get("time")
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_payload(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolScope> {
    let coin = value
        .get("coin")
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error("Hyperliquid payload missing coin"))?;
    symbol_scope(exchange_id, coin)
}

fn decimal_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn required_str<'a>(
    _exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| parse_error(format!("Hyperliquid payload missing {field}")))
}

fn parse_error(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::Serialization {
        message: message.into(),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn first_status(value: &Value) -> Option<&Value> {
    value
        .get("response")
        .and_then(|response| response.get("data"))
        .and_then(|data| data.get("statuses"))
        .and_then(Value::as_array)
        .and_then(|statuses| statuses.first())
}

fn parse_side(side: &str) -> OrderSide {
    match side {
        "B" | "buy" | "Buy" => OrderSide::Buy,
        _ => OrderSide::Sell,
    }
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "open" | "resting" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "triggered" => OrderStatus::New,
        _ => OrderStatus::New,
    }
}
