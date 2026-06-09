#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion, TenantId,
};
use serde_json::Value;

use super::parser::{coinmate_canonical_pair, coinmate_pair, parse_order_ack_id, value_as_string};

pub fn parse_amend_order_ack_id(value: &Value) -> ExchangeApiResult<String> {
    if value.get("error").and_then(Value::as_bool) == Some(true) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "coinmate amend ack rejected: {}",
                value
                    .get("errorMessage")
                    .and_then(value_as_string)
                    .unwrap_or_else(|| value.to_string())
            ),
        });
    }
    parse_order_ack_id(value)
}

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate balances fixture missing data array".to_string(),
        })?;
    Ok(rows
        .iter()
        .filter_map(|row| row.get("currency").and_then(value_as_string))
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    parse_ids(value, &["id", "orderId"])
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    parse_ids(value, &["transactionId", "id"])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = order_object(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "coinmate order response missing data object".to_string(),
    })?;
    parse_order_object(exchange_id, symbol_hint, order)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate open orders response missing data array".to_string(),
        })?;
    rows.iter()
        .map(|row| parse_order_object(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_recent_fills(
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
                message: "coinmate get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate recent fills response missing data array".to_string(),
        })?;
    rows.iter()
        .map(|row| {
            let price = number(row, &["price"])
                .ok_or_else(|| parse_error("coinmate fill missing price", row))?;
            let quantity = number(row, &["amount"])
                .ok_or_else(|| parse_error("coinmate fill missing amount", row))?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: text(row, &["orderId"]).or_else(|| {
                    parse_side(text(row, &["type", "orderType"]).as_deref())
                        .ok()
                        .and_then(|side| order_id_from_fill_side(row, side))
                }),
                client_order_id: text(row, &["clientOrderId"]),
                fill_id: text(row, &["transactionId", "id"]),
                side: parse_side(text(row, &["type", "orderType"]).as_deref())?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(text(row, &["tradeFeeType"]).as_deref()),
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: text(row, &["feeCurrency"]),
                fee_amount: number(row, &["fee"]),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp(row, &["date"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_ids(value: &Value, fields: &[&str]) -> ExchangeApiResult<Vec<String>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate private fixture missing data array".to_string(),
        })?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            fields
                .iter()
                .find_map(|field| row.get(*field).and_then(value_as_string))
        })
        .collect())
}

fn order_object(value: &Value) -> Option<&Value> {
    let data = value.get("data").unwrap_or(value);
    if data.is_object() {
        Some(data)
    } else {
        data.as_array()?.first()
    }
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text(order, &["original", "amount"]).unwrap_or_else(|| "0".to_string());
    let remaining = text(order, &["amount"]);
    let filled_quantity =
        filled_from_remaining(&quantity, remaining.as_deref()).unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text(order, &["clientOrderId"]),
        exchange_order_id: text(order, &["id", "orderId"]),
        side: parse_side(text(order, &["type", "side"]).as_deref())?,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: parse_status(
            text(order, &["status"]).as_deref(),
            &quantity,
            &filled_quantity,
        ),
        quantity,
        price: text(order, &["price"]),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: timestamp(order, &["date", "createdAt", "created_at"]),
        updated_at: Utc::now(),
    })
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    row: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let pair = text(row, &["currencyPair"]).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "coinmate private response missing currencyPair".to_string(),
    })?;
    let (base, quote) = coinmate_canonical_pair(&pair)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(
            rustcta_types::CanonicalSymbol::new(base, quote).map_err(validation_error)?,
        ),
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            coinmate_pair(&pair),
        )
        .map_err(validation_error)?,
    })
}

fn text(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_string))
        .filter(|value| !value.trim().is_empty())
}

fn number(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field))
        .and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("coinmate unsupported order side {other}"),
        }),
    }
}

fn parse_status(value: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "active" => OrderStatus::Open,
        "filled" | "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ if filled_quantity != "0" && filled_quantity != quantity => OrderStatus::PartiallyFilled,
        _ => OrderStatus::Open,
    }
}

fn filled_from_remaining(quantity: &str, remaining: Option<&str>) -> Option<String> {
    let total = quantity.parse::<f64>().ok()?;
    let remaining = remaining?.parse::<f64>().ok()?;
    Some(trim_decimal(total - remaining))
}

fn trim_decimal(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn order_id_from_fill_side(row: &Value, side: OrderSide) -> Option<String> {
    match side {
        OrderSide::Buy => text(row, &["buyOrderId"]),
        OrderSide::Sell => text(row, &["sellOrderId"]),
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn timestamp(value: &Value, fields: &[&str]) -> Option<chrono::DateTime<Utc>> {
    let raw = fields.iter().find_map(|field| value.get(*field))?;
    raw.as_i64()
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
        .or_else(|| raw.as_str()?.parse().ok())
}

fn parse_error(message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("{message}: {value}"),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
