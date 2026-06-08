use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, first_timestamp_millis, normalize_coinone_exchange_symbol,
    normalize_coinone_symbol, parse_error, required_str, string_or_number, validation_error,
    value_as_string, COINONE_QUOTE,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = value
        .get("balances")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "balance response missing rows", value))?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = required_str(exchange_id, row, "currency")
            .or_else(|_| required_str(exchange_id, row, "asset"))?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            row.get("available")
                .or_else(|| row.get("avail"))
                .or_else(|| row.get("normal_balance")),
        )?
        .unwrap_or(0.0);
        let locked = decimal_value_to_f64(row.get("locked").or_else(|| row.get("limit_balance")))?
            .unwrap_or(0.0);
        let total = decimal_value_to_f64(row.get("balance").or_else(|| row.get("total")))?
            .unwrap_or(available + locked);
        if total > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_item_or_self(value);
    let symbol = symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = string_or_number(
        order
            .get("qty")
            .or_else(|| order.get("quantity"))
            .or_else(|| order.get("order_qty")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_or_number(
        order
            .get("executed_qty")
            .or_else(|| order.get("filled_qty"))
            .or_else(|| order.get("executed_quantity")),
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
        client_order_id: value_as_string(
            order
                .get("user_order_id")
                .or_else(|| order.get("client_order_id")),
        ),
        exchange_order_id: value_as_string(order.get("order_id").or_else(|| order.get("id"))),
        side: parse_side(order.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(order.get("time_in_force").and_then(Value::as_str)),
        status,
        quantity,
        price: string_or_number(order.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: average_fill_price(order, &filled_quantity),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp_millis(order, &["created_at", "ordered_at", "timestamp"]),
        updated_at: first_timestamp_millis(order, &["updated_at", "timestamp"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    items_array(value)
        .iter()
        .map(|item| parse_order_state(exchange_id, symbol_hint, item))
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
                message: "coinone recent fills request requires canonical_symbol".to_string(),
            })?;
    items_array(value)
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(
                fill.get("qty")
                    .or_else(|| fill.get("quantity"))
                    .or_else(|| fill.get("executed_qty")),
            )?
            .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: value_as_string(fill.get("user_order_id")),
                fill_id: value_as_string(fill.get("trade_id").or_else(|| fill.get("id"))),
                side: parse_side(fill.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(
                    fill.get("liquidity")
                        .or_else(|| fill.get("is_maker"))
                        .and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(
                    fill.get("amount")
                        .or_else(|| fill.get("quote_qty"))
                        .or_else(|| fill.get("quote_quantity")),
                )?
                .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
                fee_asset: value_as_string(fill.get("fee_currency"))
                    .or_else(|| Some(COINONE_QUOTE.to_string())),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(
                    fill,
                    &["timestamp", "created_at", "executed_at"],
                )
                .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let items = if let Some(items) = value.get("fees").and_then(Value::as_array) {
        items.clone()
    } else if let Some(items) = value.as_array() {
        items.clone()
    } else {
        vec![value.clone()]
    };
    items
        .iter()
        .map(|item| {
            let symbol = if let Some(symbol) = requested_symbols.first() {
                symbol.clone()
            } else {
                symbol_scope(exchange_id, None, item)?
            };
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: string_or_number(
                    item.get("maker_fee_rate")
                        .or_else(|| item.get("maker"))
                        .or_else(|| item.get("maker_fee")),
                )
                .unwrap_or_else(|| "0".to_string()),
                taker_rate: string_or_number(
                    item.get("taker_fee_rate")
                        .or_else(|| item.get("taker"))
                        .or_else(|| item.get("taker_fee")),
                )
                .unwrap_or_else(|| "0".to_string()),
                source: Some("coinone.trade_fee".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

fn first_item_or_self(value: &Value) -> &Value {
    value
        .get("order")
        .or_else(|| value.get("data"))
        .or_else(|| {
            value
                .get("orders")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
        })
        .unwrap_or(value)
}

fn items_array(value: &Value) -> Vec<Value> {
    value
        .get("orders")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("transactions"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| value.as_array().cloned())
        .unwrap_or_default()
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let base = value
        .get("target_currency")
        .or_else(|| value.get("currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| {
            value
                .get("symbol")
                .or_else(|| value.get("market"))
                .and_then(Value::as_str)
                .and_then(|symbol| normalize_coinone_symbol(symbol).ok().map(|(base, _)| base))
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "order missing target_currency", value))?;
    let exchange_symbol = normalize_coinone_exchange_symbol(&format!("{base}-{COINONE_QUOTE}"))?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn parse_status(value: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "live" | "wait" | "pending" => OrderStatus::Open,
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "done" | "complete" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" => OrderStatus::Cancelled,
        "rejected" | "reject" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => {
            let quantity = quantity.parse::<f64>().unwrap_or(0.0);
            let filled = filled_quantity.parse::<f64>().unwrap_or(0.0);
            if quantity > 0.0 && filled >= quantity {
                OrderStatus::Filled
            } else if filled > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Unknown
            }
        }
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "true" => LiquidityRole::Maker,
        "taker" | "false" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_fill_price(value: &Value, filled_quantity: &str) -> Option<String> {
    string_or_number(
        value
            .get("average_price")
            .or_else(|| value.get("avg_price"))
            .or_else(|| value.get("executed_price")),
    )
    .or_else(|| {
        let filled_quantity = filled_quantity.parse::<f64>().ok()?;
        let amount = string_or_number(value.get("executed_amount"))?
            .parse::<f64>()
            .ok()?;
        (filled_quantity > 0.0).then(|| (amount / filled_quantity).to_string())
    })
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}
