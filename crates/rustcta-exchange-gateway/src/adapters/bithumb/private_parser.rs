use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    bithumb_symbol_scope, decimal_value_to_f64, first_timestamp, parse_error, required_str,
    schema_version, string_or_number, validation_error, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let accounts = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bithumb accounts response is not an array",
            value,
        )
    })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for account in accounts {
        let asset = required_str(exchange_id, account, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(account.get("balance"))?.unwrap_or(0.0);
        let locked = decimal_value_to_f64(account.get("locked"))?.unwrap_or(0.0);
        let total = available + locked;
        if total > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: schema_version(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_fee_snapshot(
    fallback_symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: fallback_symbol.clone(),
        maker_rate: string_or_number(value.get("maker_bid_fee"))
            .or_else(|| string_or_number(value.get("bid_fee")))
            .unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(value.get("ask_fee")).unwrap_or_else(|| "0".to_string()),
        source: Some("bithumb.orders.chance".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_state(exchange_id, fallback_symbol, value)
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bithumb orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fills_from_orders(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let orders = if let Some(items) = value.as_array() {
        items.clone()
    } else {
        vec![value.clone()]
    };
    let mut fills = Vec::new();
    for order in &orders {
        let symbol = order_symbol(exchange_id, fallback_symbol, order)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bithumb fills require canonical_symbol".to_string(),
                })?;
        if let Some(trades) = order.get("trades").and_then(Value::as_array) {
            for trade in trades {
                let price = decimal_value_to_f64(trade.get("price"))
                    .or_else(|_| decimal_value_to_f64(order.get("price")))?
                    .unwrap_or(0.0);
                let quantity = decimal_value_to_f64(trade.get("volume"))
                    .or_else(|_| decimal_value_to_f64(order.get("executed_volume")))?
                    .unwrap_or(0.0);
                fills.push(Fill {
                    schema_version: schema_version(),
                    tenant_id: tenant_id.clone(),
                    account_id: account_id.clone(),
                    exchange_id: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: canonical_symbol.clone(),
                    exchange_symbol: Some(symbol.exchange_symbol.clone()),
                    order_id: value_as_string(order.get("uuid"))
                        .or_else(|| value_as_string(order.get("order_id"))),
                    client_order_id: value_as_string(order.get("client_order_id")),
                    fill_id: value_as_string(trade.get("uuid"))
                        .or_else(|| value_as_string(trade.get("sequential_id"))),
                    side: parse_side(order.get("side").and_then(Value::as_str)),
                    position_side: PositionSide::None,
                    status: FillStatus::Confirmed,
                    liquidity_role: LiquidityRole::Unknown,
                    price,
                    quantity,
                    quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                    fee_asset: value_as_string(trade.get("funds_currency")),
                    fee_amount: decimal_value_to_f64(trade.get("fee"))?,
                    fee_rate: None,
                    realized_pnl: None,
                    filled_at: first_timestamp(trade, &["created_at", "timestamp"])
                        .unwrap_or_else(Utc::now),
                    received_at: Utc::now(),
                });
            }
        } else if decimal_value_to_f64(order.get("executed_volume"))?.unwrap_or(0.0) > 0.0 {
            let price = decimal_value_to_f64(order.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(order.get("executed_volume"))?.unwrap_or(0.0);
            fills.push(Fill {
                schema_version: schema_version(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol,
                exchange_symbol: Some(symbol.exchange_symbol),
                order_id: value_as_string(order.get("uuid"))
                    .or_else(|| value_as_string(order.get("order_id"))),
                client_order_id: value_as_string(order.get("client_order_id")),
                fill_id: value_as_string(order.get("uuid")).map(|id| format!("{id}:summary")),
                side: parse_side(order.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(order.get("executed_funds"))?,
                fee_asset: None,
                fee_amount: decimal_value_to_f64(order.get("paid_fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(order, &["created_at", "updated_at"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            });
        }
    }
    Ok(fills)
}

pub fn cancel_ack_order(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = order_symbol(exchange_id, fallback_symbol, value)?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("client_order_id")),
        exchange_order_id: value_as_string(value.get("uuid"))
            .or_else(|| value_as_string(value.get("order_id"))),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(
            value
                .get("ord_type")
                .or_else(|| value.get("order_type"))
                .and_then(Value::as_str),
        ),
        time_in_force: Some(TimeInForce::GTC),
        status: value
            .get("state")
            .and_then(Value::as_str)
            .map(parse_order_status)
            .unwrap_or(OrderStatus::New),
        quantity: string_or_number(value.get("volume")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|price| price != "0"),
        filled_quantity: string_or_number(value.get("executed_volume"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(value, &["created_at"]),
        updated_at: first_timestamp(value, &["updated_at", "created_at"]).unwrap_or_else(Utc::now),
    })
}

fn order_symbol(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    let market = required_str(exchange_id, value, "market")?;
    bithumb_symbol_scope(exchange_id, market)
}

fn parse_side(side: Option<&str>) -> OrderSide {
    match side.map(str::to_ascii_lowercase).as_deref() {
        Some("ask") | Some("sell") => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.map(str::to_ascii_lowercase).as_deref() {
        Some("price") | Some("market") => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "wait" | "watch" => OrderStatus::New,
        "done" => OrderStatus::Filled,
        "cancel" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}
