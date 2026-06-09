#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion,
};
use serde_json::Value;

use super::parser::foxbit_canonical_pair;

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances = value
        .get("data")
        .or_else(|| value.get("balances"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit balance fixture missing balances array".to_string(),
        })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("currency_symbol").or_else(|| row.get("currency")))
        .filter_map(Value::as_str)
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = value
        .get("data")
        .or_else(|| value.get("orders"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit open orders fixture missing orders array".to_string(),
        })?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("order_id")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = value
        .get("data")
        .or_else(|| value.get("fills"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "foxbit fills fixture missing fills array".to_string(),
        })?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("trade_id")))
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect())
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_row_or_self(value, "order")?;
    let symbol = private_symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text_any(order, &["quantity", "original_quantity", "amount"])
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = text_any(order, &["filled_quantity", "executed_quantity", "filled"])
        .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text_any(order, &["client_order_id", "client_id"]),
        exchange_order_id: text_any(order, &["id", "order_id"]),
        side: parse_side(order),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order),
        time_in_force: None,
        status: parse_status(order),
        quantity,
        price: text_any(order, &["price", "limit_price"]),
        filled_quantity,
        average_fill_price: text_any(order, &["average_price", "avg_price"]),
        reduce_only: false,
        post_only: false,
        created_at: timestamp_any(order, &["created_at", "created_at_timestamp", "created"]),
        updated_at: timestamp_any(order, &["updated_at", "updated_at_timestamp", "updated"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value, "orders")?
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .filter_map(|order| match order {
            Ok(order)
                if matches!(
                    order.status,
                    OrderStatus::New | OrderStatus::Open | OrderStatus::PartiallyFilled
                ) =>
            {
                Some(Ok(order))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    rows(value, "fills")?
        .iter()
        .map(|fill| parse_fill(exchange_id, &tenant_id, &account_id, symbol_hint, fill))
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol_hint: &SymbolScope,
    fill: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = private_symbol_scope(exchange_id, Some(symbol_hint), fill)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "foxbit fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_any(fill, &["price", "unit_price"])
        .ok_or_else(|| parse_error(exchange_id.clone(), "Foxbit fill missing price", fill))?;
    let quantity = decimal_any(fill, &["quantity", "amount"])
        .ok_or_else(|| parse_error(exchange_id.clone(), "Foxbit fill missing quantity", fill))?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: text_any(fill, &["order_id", "id_order"]),
        client_order_id: text_any(fill, &["client_order_id", "client_id"]),
        fill_id: text_any(fill, &["id", "trade_id", "fill_id"]),
        side: parse_side(fill),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(fill),
        price,
        quantity,
        quote_quantity: decimal_any(fill, &["quote_quantity", "total", "gross_amount"])
            .or_else(|| Some(price * quantity)),
        fee_asset: text_any(fill, &["fee_currency", "fee_asset"])
            .map(|asset| asset.to_ascii_uppercase()),
        fee_amount: decimal_any(fill, &["fee", "fee_amount"]),
        fee_rate: None,
        realized_pnl: None,
        filled_at: timestamp_any(fill, &["created_at", "executed_at", "timestamp"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn rows<'a>(value: &'a Value, fallback_key: &str) -> ExchangeApiResult<&'a Vec<Value>> {
    value
        .get("data")
        .or_else(|| value.get(fallback_key))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("foxbit response missing {fallback_key} array"),
        })
}

fn first_row_or_self<'a>(value: &'a Value, fallback_key: &str) -> ExchangeApiResult<&'a Value> {
    if value.is_object() && !value.get("data").is_some_and(Value::is_array) {
        return Ok(value
            .get("data")
            .filter(|data| data.is_object())
            .unwrap_or(value));
    }
    rows(value, fallback_key)?
        .first()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("foxbit response missing {fallback_key} object"),
        })
}

fn private_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let symbol_text = text_any(value, &["market_symbol", "symbol", "instrument_symbol"])
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Foxbit response missing market_symbol",
                value,
            )
        })?;
    let (base, quote) = foxbit_canonical_pair(&symbol_text).map_err(|error| match error {
        ExchangeApiError::InvalidRequest { message } => {
            parse_error(exchange_id.clone(), &message, value)
        }
        other => other,
    })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, symbol_text)
            .map_err(validation_error)?,
    })
}

fn parse_side(value: &Value) -> OrderSide {
    match text_any(value, &["side", "order_side"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match text_any(value, &["type", "order_type"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: &Value) -> OrderStatus {
    match text_any(value, &["status", "state"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" | "pending" => OrderStatus::New,
        "open" | "active" | "partially_filled" | "partially-filled" => OrderStatus::Open,
        "filled" | "done" | "completed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "pending_cancel" | "pending-cancel" => OrderStatus::PendingCancel,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: &Value) -> LiquidityRole {
    match text_any(value, &["liquidity", "liquidity_role"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn text_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .or_else(|| value.as_i64().map(|number| number.to_string()))
                .or_else(|| value.as_u64().map(|number| number.to_string()))
                .or_else(|| value.as_f64().map(|number| number.to_string()))
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn decimal_any(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        })
        .filter(|value| value.is_finite())
}

fn timestamp_any(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(timestamp_value)
}

fn timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(text) {
            return Some(timestamp.with_timezone(&Utc));
        }
        if let Ok(number) = text.parse::<i64>() {
            return timestamp_from_i64(number);
        }
    }
    value.as_i64().and_then(timestamp_from_i64)
}

fn timestamp_from_i64(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        Utc.timestamp_millis_opt(value).single()
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message,
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
