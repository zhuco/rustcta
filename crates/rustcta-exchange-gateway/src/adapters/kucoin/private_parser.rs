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
    decimal_value_to_f64, first_timestamp_millis, normalize_kucoin_symbol, parse_error,
    required_str, string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let accounts = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "accounts response missing data array",
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
        if account.get("type").and_then(Value::as_str) != Some("trade") {
            continue;
        }
        let asset = required_str(exchange_id, account, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(account.get("available"))?.unwrap_or(0.0);
        let locked = decimal_value_to_f64(account.get("holds"))?.unwrap_or(0.0);
        let total = available + locked;
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
    let quantity = string_or_number(order.get("size")).unwrap_or_else(|| "0".to_string());
    let filled_quantity =
        string_or_number(order.get("dealSize")).unwrap_or_else(|| "0".to_string());
    let status = if order
        .get("isActive")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        OrderStatus::New
    } else {
        let quantity_number = quantity.parse::<f64>().unwrap_or(0.0);
        let filled_number = filled_quantity.parse::<f64>().unwrap_or(0.0);
        if quantity_number > 0.0 && filled_number >= quantity_number {
            OrderStatus::Filled
        } else {
            OrderStatus::Unknown
        }
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(order.get("clientOid")),
        exchange_order_id: value_as_string(order.get("id")),
        side: parse_side(order.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(order.get("timeInForce").and_then(Value::as_str)),
        status,
        quantity,
        price: string_or_number(order.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: average_fill_price(order, &filled_quantity),
        reduce_only: false,
        post_only: order
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: first_timestamp_millis(order, &["createdAt", "orderTime", "ts"]),
        updated_at: first_timestamp_millis(order, &["updatedAt", "ts"]).unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = items_array(value);
    items
        .iter()
        .map(|item| parse_order_state(exchange_id, symbol_hint, item))
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let items = if let Some(items) = value.as_array() {
        items.clone()
    } else {
        vec![value.clone()]
    };
    items
        .iter()
        .map(|item| {
            let symbol = fee_symbol_scope(exchange_id, requested_symbols, item)?;
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: string_or_number(item.get("makerFeeRate"))
                    .or_else(|| string_or_number(item.get("makerFeeCoefficient")))
                    .unwrap_or_else(|| "0".to_string()),
                taker_rate: string_or_number(item.get("takerFeeRate"))
                    .or_else(|| string_or_number(item.get("takerFeeCoefficient")))
                    .unwrap_or_else(|| "0".to_string()),
                source: Some("kucoin.trade_fees".to_string()),
                updated_at: Utc::now(),
            })
        })
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
                message: "kucoin recent fills request requires canonical_symbol".to_string(),
            })?;
    let items = items_array(value);
    items
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(fill.get("size"))?.unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderId")),
                client_order_id: value_as_string(fill.get("clientOid")),
                fill_id: value_as_string(fill.get("tradeId")),
                side: parse_side(fill.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(fill.get("liquidity").and_then(Value::as_str)),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: value_as_string(fill.get("feeCurrency")),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(fill, &["createdAt", "ts"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn first_item_or_self(value: &Value) -> &Value {
    value
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .unwrap_or(value)
}

fn items_array(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .cloned()
        .or_else(|| value.get("items").and_then(Value::as_array).cloned())
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
    let symbol = required_str(exchange_id, value, "symbol")?;
    let normalized = normalize_kucoin_symbol(symbol)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn fee_symbol_scope(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = requested_symbols.first() {
        return Ok(symbol.clone());
    }
    symbol_scope(exchange_id, None, value)
}

fn parse_side(value: Option<&str>) -> OrderSide {
    if value.unwrap_or_default().eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
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
        "gtx" | "poc" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_fill_price(value: &Value, filled_quantity: &str) -> Option<String> {
    let filled_quantity = filled_quantity.parse::<f64>().ok()?;
    let deal_funds = string_or_number(value.get("dealFunds"))?
        .parse::<f64>()
        .ok()?;
    (filled_quantity > 0.0).then(|| (deal_funds / filled_quantity).to_string())
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}
