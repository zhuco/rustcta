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

use super::parser::{mercado_canonical_pair, mercado_symbol};

pub fn parse_balance_assets(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let balances = value
        .get("balances")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado balance fixture missing balances array".to_string(),
        })?;
    Ok(balances
        .iter()
        .filter_map(|row| row.get("asset").or_else(|| row.get("currency")))
        .filter_map(Value::as_str)
        .map(|asset| asset.to_ascii_uppercase())
        .collect())
}

pub fn parse_open_order_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let orders = order_rows(value)?;
    Ok(orders
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("orderId")))
        .filter_map(|value| text(Some(value)))
        .collect())
}

pub fn parse_fill_ids(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let fills = fill_rows(value)?;
    Ok(fills
        .iter()
        .filter_map(|row| row.get("id").or_else(|| row.get("tradeId")))
        .filter_map(|value| text(Some(value)))
        .collect())
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_or_self(value);
    let symbol = mercado_symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text(
        order
            .get("qty")
            .or_else(|| order.get("quantity"))
            .or_else(|| order.get("amount")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = text(
        order
            .get("filledQty")
            .or_else(|| order.get("filledQuantity"))
            .or_else(|| order.get("executedQty")),
    )
    .unwrap_or_else(|| "0".to_string());
    let status = parse_status(
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
        client_order_id: text(
            order
                .get("externalId")
                .or_else(|| order.get("clientOrderId"))
                .or_else(|| order.get("client_order_id")),
        ),
        exchange_order_id: text(order.get("id").or_else(|| order.get("orderId"))),
        side: parse_side(order.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order.get("type").and_then(Value::as_str)),
        time_in_force: Some(TimeInForce::GTC),
        status,
        quantity,
        price: text(
            order
                .get("limitPrice")
                .or_else(|| order.get("price"))
                .or_else(|| order.get("avgPrice")),
        )
        .filter(|price| !is_zero_decimal(price)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: text(
            order
                .get("avgPrice")
                .or_else(|| order.get("averagePrice"))
                .or_else(|| order.get("average_fill_price")),
        ),
        reduce_only: false,
        post_only: order
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("post-only")),
        created_at: order
            .get("createdAt")
            .or_else(|| order.get("created_at"))
            .and_then(parse_timestamp),
        updated_at: order
            .get("updatedAt")
            .or_else(|| order.get("updated_at"))
            .or_else(|| order.get("createdAt"))
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
            || symbol_hint.is_some_and(|symbol| same_symbol(symbol, &parsed.exchange_symbol.symbol))
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
        let symbol = mercado_symbol_scope(exchange_id, Some(symbol_hint), row)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "mercado fill requires canonical_symbol".to_string(),
                })?;
        let price = decimal(
            row.get("price")
                .or_else(|| row.get("unitPrice"))
                .or_else(|| row.get("limitPrice")),
        )?
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado fill missing price".to_string(),
        })?;
        let quantity = decimal(
            row.get("qty")
                .or_else(|| row.get("quantity"))
                .or_else(|| row.get("amount")),
        )?
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado fill missing quantity".to_string(),
        })?;
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol,
            exchange_symbol: Some(symbol.exchange_symbol),
            order_id: text(row.get("orderId").or_else(|| row.get("order_id"))),
            client_order_id: text(
                row.get("externalId")
                    .or_else(|| row.get("clientOrderId"))
                    .or_else(|| row.get("client_order_id")),
            ),
            fill_id: text(row.get("id").or_else(|| row.get("tradeId"))),
            side: parse_side(row.get("side").and_then(Value::as_str)),
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Unknown,
            price,
            quantity,
            quote_quantity: decimal(row.get("grossAmount").or_else(|| row.get("notional")))?
                .or_else(|| Some(price * quantity)),
            fee_asset: text(row.get("feeCurrency").or_else(|| row.get("fee_currency"))),
            fee_amount: decimal(row.get("fee").or_else(|| row.get("feeAmount")))?,
            fee_rate: None,
            realized_pnl: None,
            filled_at: row
                .get("createdAt")
                .or_else(|| row.get("created_at"))
                .or_else(|| row.get("timestamp"))
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

fn order_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .get("orders")
        .or_else(|| value.get("data"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado open orders fixture missing orders array".to_string(),
        })
}

fn fill_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .get("trades")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("data"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mercado fills fixture missing fills/trades array".to_string(),
        })
}

fn first_order_or_self(value: &Value) -> &Value {
    value
        .get("orders")
        .or_else(|| value.get("data"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .and_then(|orders| orders.first())
        .unwrap_or(value)
}

fn mercado_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    row: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let native_symbol = text(
        row.get("instrument")
            .or_else(|| row.get("symbol"))
            .or_else(|| row.get("market")),
    )
    .ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "mercado order row missing instrument".to_string(),
    })?;
    let normalized = mercado_symbol(&native_symbol);
    let (base, quote) = mercado_canonical_pair(&normalized)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn same_symbol(symbol: &SymbolScope, native_symbol: &str) -> bool {
    mercado_symbol(&symbol.exchange_symbol.symbol) == mercado_symbol(native_symbol)
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post-only" | "post_only" | "postonly" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "created" | "new" | "pending" => OrderStatus::New,
        "working" | "open" | "active" => fill_aware_open_status(quantity, filled_quantity),
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "closed" | "completed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => fill_aware_open_status(quantity, filled_quantity),
    }
}

fn fill_aware_open_status(quantity: &str, filled_quantity: &str) -> OrderStatus {
    let quantity = quantity.parse::<f64>().unwrap_or(0.0);
    let filled_quantity = filled_quantity.parse::<f64>().unwrap_or(0.0);
    if quantity > 0.0 && filled_quantity >= quantity {
        OrderStatus::Filled
    } else if filled_quantity > 0.0 {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::Open
    }
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn decimal(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    text(value)
        .map(|value| {
            value
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid mercado decimal {value}: {error}"),
                })
        })
        .transpose()
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

fn parse_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(value) = value.as_str() {
        return DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|time| time.with_timezone(&Utc));
    }
    if let Some(value) = value.as_i64() {
        return DateTime::from_timestamp_millis(value)
            .or_else(|| DateTime::from_timestamp(value, 0));
    }
    None
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
