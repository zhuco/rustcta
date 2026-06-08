use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, first_timestamp, required_str, split_indodax_symbol,
    symbol_scope_from_exchange_symbol, validation_error, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let balances = value
        .get("return")
        .and_then(|payload| payload.get("balance"))
        .or_else(|| value.get("balance"))
        .and_then(Value::as_object)
        .ok_or_else(|| {
            super::parser::parse_error(
                exchange_id.clone(),
                "Indodax getInfo response missing balance object",
                value,
            )
        })?;
    let holds = value
        .get("return")
        .and_then(|payload| payload.get("balance_hold"))
        .or_else(|| value.get("balance_hold"))
        .and_then(Value::as_object);
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut assets = Vec::new();
    for (asset, total_value) in balances {
        let asset_name = asset.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset_name) {
            continue;
        }
        let total = decimal_as_f64(Some(total_value)).unwrap_or(0.0);
        let locked = holds
            .and_then(|holds| holds.get(asset))
            .and_then(|value| decimal_as_f64(Some(value)))
            .unwrap_or(0.0);
        let available = (total - locked).max(0.0);
        if total > 0.0 || locked > 0.0 || !requested.is_empty() {
            assets.push(
                AssetBalance::new(asset_name, total, available, locked)
                    .map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances: assets,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = value
        .get("return")
        .and_then(|payload| payload.get("order"))
        .unwrap_or(value);
    let symbol = order_symbol(exchange_id, fallback_symbol, order)?;
    let base_field = symbol
        .exchange_symbol
        .symbol
        .split_once('_')
        .map(|(base, _)| base.to_ascii_lowercase())
        .unwrap_or_else(|| "btc".to_string());
    let quantity = value_as_string(order.get("amount"))
        .or_else(|| value_as_string(order.get(format!("order_{base_field}"))))
        .or_else(|| value_as_string(order.get(&base_field)))
        .or_else(|| value_as_string(order.get("remain")))
        .unwrap_or_else(|| "0".to_string());
    let remaining = value_as_string(order.get("remain"))
        .or_else(|| value_as_string(order.get(format!("remain_{base_field}"))))
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = filled_quantity(order, &remaining, &quantity);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: None,
        exchange_order_id: value_as_string(order.get("order_id"))
            .or_else(|| value_as_string(order.get("id"))),
        side: parse_side(order.get("type").and_then(Value::as_str)).unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: if order
            .get("price")
            .and_then(|price| decimal_as_f64(Some(price)))
            .unwrap_or(0.0)
            > 0.0
        {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        time_in_force: None,
        status: parse_status(order),
        quantity,
        price: value_as_string(order.get("price")).filter(|price| price != "0"),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: value_as_string(order.get("price")).filter(|price| price != "0"),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(order, &["submit_time", "created_at"]),
        updated_at: first_timestamp(order, &["finish_time", "updated_at"]).unwrap_or_else(Utc::now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let payload = value.get("return").unwrap_or(value);
    let orders = payload
        .get("orders")
        .or_else(|| payload.get("open_orders"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| payload.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(payload));
    orders
        .iter()
        .filter(|order| !order.is_null())
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let payload = value.get("return").unwrap_or(value);
    let fills = payload
        .as_array()
        .map(Vec::as_slice)
        .or_else(|| {
            payload
                .get("trades")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
        })
        .unwrap_or_else(|| std::slice::from_ref(payload));
    fills
        .iter()
        .filter(|fill| !fill.is_null())
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

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = order_symbol(exchange_id, fallback_symbol, value)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "indodax fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("amount")
            .or_else(|| value.get("btc"))
            .or_else(|| value.get("vol")),
    )
    .unwrap_or(0.0);
    let fee_asset = value
        .get("fee_currency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| {
            Some(
                if parse_side(value.get("type").and_then(Value::as_str)) == Some(OrderSide::Buy) {
                    symbol
                        .exchange_symbol
                        .symbol
                        .split_once('_')
                        .map(|(base, _)| base.to_ascii_uppercase())
                        .unwrap_or_else(|| "BTC".to_string())
                } else {
                    symbol
                        .exchange_symbol
                        .symbol
                        .split_once('_')
                        .map(|(_, quote)| quote.to_ascii_uppercase())
                        .unwrap_or_else(|| "IDR".to_string())
                },
            )
        });
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("order_id")),
        client_order_id: None,
        fill_id: value_as_string(value.get("trade_id"))
            .or_else(|| value_as_string(value.get("tid"))),
        side: parse_side(value.get("type").and_then(Value::as_str)).unwrap_or(OrderSide::Buy),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: LiquidityRole::Unknown,
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset,
        fee_amount: decimal_as_f64(value.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: first_timestamp(value, &["trade_time", "date", "time"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
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
    if let Some(pair) = value.get("pair").and_then(Value::as_str) {
        return symbol_scope_from_exchange_symbol(exchange_id, pair);
    }
    if let Some(symbol) = value.get("symbol").and_then(Value::as_str) {
        return symbol_scope_from_exchange_symbol(exchange_id, symbol);
    }
    let base = required_str(exchange_id, value, "base_currency").ok();
    let quote = required_str(exchange_id, value, "quote_currency").ok();
    if let (Some(base), Some(quote)) = (base, quote) {
        return symbol_scope_from_exchange_symbol(exchange_id, &format!("{base}_{quote}"));
    }
    if let Some((base, quote)) = value
        .get("currency_pair")
        .and_then(Value::as_str)
        .and_then(split_indodax_symbol)
    {
        return symbol_scope_from_exchange_symbol(exchange_id, &format!("{base}_{quote}"));
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "indodax order/fill parser requires pair or request symbol".to_string(),
    })
}

fn parse_side(value: Option<&str>) -> Option<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" | "bid" => Some(OrderSide::Buy),
        "sell" | "ask" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_status(value: &Value) -> OrderStatus {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    match status.as_str() {
        "open" => OrderStatus::Open,
        "filled" | "closed" | "success" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => {
            if decimal_as_f64(value.get("remain")).unwrap_or(0.0) > 0.0 {
                OrderStatus::Open
            } else {
                OrderStatus::Filled
            }
        }
    }
}

fn filled_quantity(order: &Value, remaining: &str, quantity: &str) -> String {
    if let Some(filled) = value_as_string(order.get("filled")) {
        return filled;
    }
    let amount = quantity.parse::<f64>().unwrap_or(0.0);
    let remain = remaining.parse::<f64>().unwrap_or(0.0);
    if amount > 0.0 {
        (amount - remain).max(0.0).to_string()
    } else {
        "0".to_string()
    }
}
