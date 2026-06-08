use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, first_timestamp, normalize_coincheck_symbol, required_str,
    string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_lowercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for (key, amount) in value.as_object().into_iter().flatten() {
        if key == "success" || key.ends_with("_reserved") {
            continue;
        }
        if !requested.is_empty() && !requested.contains(&key.to_ascii_lowercase()) {
            continue;
        }
        let available = decimal_value_to_f64(Some(amount))?.unwrap_or(0.0);
        let reserved_key = format!("{key}_reserved");
        let locked = decimal_value_to_f64(value.get(&reserved_key))?.unwrap_or(0.0);
        if available + locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(
                    key.to_ascii_uppercase(),
                    available + locked,
                    available,
                    locked,
                )
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
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, symbol_hint, value)?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: None,
        exchange_order_id: value_as_string(value.get("id")),
        side: parse_side(value.get("order_type").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_type(value.get("order_type").and_then(Value::as_str)),
        time_in_force: Some(TimeInForce::GTC),
        status: parse_status(value),
        quantity: string_or_number(value.get("pending_amount"))
            .or_else(|| string_or_number(value.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("rate")),
        filled_quantity: string_or_number(value.get("executed_amount"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average_price")),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(value, &["created_at"]),
        updated_at: first_timestamp(value, &["updated_at"]).unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    value
        .get("orders")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
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
                message: "coincheck fills request requires canonical_symbol".to_string(),
            })?;
    value
        .get("transactions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("rate"))?.unwrap_or(0.0);
            let quantity =
                decimal_value_to_f64(fill.get("funds").and_then(|funds| {
                    funds.get(canonical_symbol.base_asset().to_ascii_lowercase())
                }))?
                .or_else(|| decimal_value_to_f64(fill.get("amount")).ok().flatten())
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
                client_order_id: None,
                fill_id: value_as_string(fill.get("id")),
                side: parse_side(fill.get("order_type").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: None,
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["created_at"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let pair = required_str(exchange_id, value, "pair")?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_coincheck_symbol(pair)?,
        )
        .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    if value.unwrap_or_default().contains("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn parse_type(value: Option<&str>) -> OrderType {
    if value.unwrap_or_default().contains("market") {
        OrderType::Market
    } else {
        OrderType::Limit
    }
}

fn parse_status(value: &Value) -> OrderStatus {
    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("canceled"))
    {
        OrderStatus::Cancelled
    } else {
        OrderStatus::New
    }
}
