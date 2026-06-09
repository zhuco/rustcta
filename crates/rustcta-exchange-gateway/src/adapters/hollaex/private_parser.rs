use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeId, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    number_from_value, parse_datetime_value, string_or_number, symbol_scope_from_exchange_symbol,
};

pub fn parse_order_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_object(exchange_id, Some(&symbol), order_object(value)?)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    array_rows(value)
        .map(|rows| {
            rows.iter()
                .map(|row| parse_order_object(exchange_id, Some(symbol), row))
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
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
                message: "hollaex get_recent_fills requires canonical_symbol".to_string(),
            })?;
    array_rows(value)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let price = number(row, &["price", "deal_price", "trade_price"])
                        .ok_or_else(|| parse_error("hollaex fill missing price", row))?;
                    let quantity = number(row, &["size", "amount", "quantity"])
                        .ok_or_else(|| parse_error("hollaex fill missing quantity", row))?;
                    Ok(Fill {
                        schema_version: SchemaVersion::current(),
                        tenant_id: tenant_id.clone(),
                        account_id: account_id.clone(),
                        exchange_id: exchange_id.clone(),
                        market_type: MarketType::Spot,
                        canonical_symbol: canonical_symbol.clone(),
                        exchange_symbol: Some(symbol.exchange_symbol.clone()),
                        order_id: text(row, &["order_id", "orderId"]),
                        client_order_id: text(row, &["client_order_id"]).or_else(|| {
                            row.get("meta")
                                .and_then(|meta| text(meta, &["client_order_id"]))
                        }),
                        fill_id: text(row, &["id", "trade_id", "transaction_id"]),
                        side: parse_side(text(row, &["side"]).as_deref())?,
                        position_side: PositionSide::None,
                        status: FillStatus::Confirmed,
                        liquidity_role: parse_liquidity(
                            text(row, &["liquidity", "role"]).as_deref(),
                        ),
                        price,
                        quantity,
                        quote_quantity: Some(price * quantity),
                        fee_asset: text(row, &["fee_coin", "fee_currency", "fee_asset"]),
                        fee_amount: number(row, &["fee", "fee_amount"]),
                        fee_rate: None,
                        realized_pnl: None,
                        filled_at: timestamp(row, &["timestamp", "created_at", "createdAt"])
                            .unwrap_or_else(Utc::now),
                        received_at: Utc::now(),
                    })
                })
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_hint
        .cloned()
        .or_else(|| {
            text(value, &["symbol", "pair", "market"])
                .and_then(|symbol| symbol_scope_from_exchange_symbol(exchange_id, &symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "hollaex order response missing symbol".to_string(),
        })?;
    let side = match value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown HollaEx order side {other}"),
            })
        }
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value
            .get("meta")
            .and_then(|meta| meta.get("client_order_id"))
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
        quantity: string_or_number(value.get("size")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(value.get("filled"))
            .or_else(|| string_or_number(value.get("filled_size")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average")),
        reduce_only: false,
        post_only: false,
        created_at: value.get("created_at").and_then(parse_datetime_value),
        updated_at: value
            .get("updated_at")
            .and_then(parse_datetime_value)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default() {
        "new" => OrderStatus::New,
        "open" => OrderStatus::Open,
        "pfilled" | "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn order_object(value: &Value) -> ExchangeApiResult<&Value> {
    let data = value.get("data").unwrap_or(value);
    if data.is_object() {
        return Ok(data);
    }
    data.as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "hollaex order response missing object".to_string(),
        })
}

fn array_rows(value: &Value) -> Option<&Vec<Value>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .or_else(|| value.get("result").and_then(Value::as_array))
}

fn text(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| string_or_number(value.get(*field)))
        .filter(|text| !text.trim().is_empty())
}

fn number(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(number_from_value))
}

fn timestamp(value: &Value, fields: &[&str]) -> Option<chrono::DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(parse_datetime_value))
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown HollaEx order side {other}"),
        }),
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
