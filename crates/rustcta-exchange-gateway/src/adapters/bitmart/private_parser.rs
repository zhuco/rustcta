use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiResult, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use super::parser::{data_payload, decimal_as_f64, normalize_symbol, required_str, value_as_i64};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = balance_items(value);
    let balances = items
        .iter()
        .filter_map(|item| {
            let asset = item
                .get("id")
                .or_else(|| item.get("currency"))
                .or_else(|| item.get("coin"))
                .and_then(Value::as_str)?;
            let available = decimal_as_f64(
                item.get("available")
                    .or_else(|| item.get("available_balance"))
                    .or_else(|| item.get("free")),
            )
            .unwrap_or(0.0);
            let locked = decimal_as_f64(
                item.get("frozen")
                    .or_else(|| item.get("frozen_balance"))
                    .or_else(|| item.get("locked")),
            )
            .unwrap_or(0.0);
            let total = decimal_as_f64(item.get("total").or_else(|| item.get("equity")))
                .unwrap_or(available + locked);
            AssetBalance::new(asset, total, available, locked).ok()
        })
        .collect::<Vec<_>>();
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    position_items(value)
        .iter()
        .filter_map(|item| {
            let symbol = item.get("symbol").and_then(Value::as_str)?;
            let (base, quote) = split_perp_symbol(symbol);
            let canonical_symbol = CanonicalSymbol::new(base, quote).ok()?;
            let quantity = decimal_as_f64(
                item.get("position_amount")
                    .or_else(|| item.get("current_amount"))
                    .or_else(|| item.get("size"))
                    .or_else(|| item.get("qty")),
            )
            .unwrap_or(0.0)
            .abs();
            if quantity == 0.0 {
                return None;
            }
            Some(ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol,
                exchange_symbol: ExchangeSymbol::new(
                    exchange_id.clone(),
                    MarketType::Perpetual,
                    symbol.to_ascii_uppercase(),
                )
                .ok(),
                side: parse_position_side(item),
                quantity,
                entry_price: decimal_as_f64(
                    item.get("entry_price")
                        .or_else(|| item.get("avg_open_price"))
                        .or_else(|| item.get("open_price")),
                ),
                mark_price: decimal_as_f64(item.get("mark_price")),
                liquidation_price: decimal_as_f64(item.get("liquidation_price")),
                unrealized_pnl: decimal_as_f64(item.get("unrealized_pnl")),
                leverage: decimal_as_f64(item.get("leverage")),
                observed_at: Utc::now(),
            })
        })
        .collect::<Vec<_>>()
        .pipe(Ok)
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = order_payload(value);
    order_state(exchange_id, symbol, data)
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    order_items(value)
        .iter()
        .map(|item| {
            let symbol_text = required_str(exchange_id, item, "symbol")?;
            let symbol = symbol_scope(exchange_id, market_type, symbol_text)?;
            order_state(exchange_id, symbol, item)
        })
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    fill_items(value)
        .iter()
        .map(|item| {
            let symbol_text = required_str(exchange_id, item, "symbol")?;
            let (base, quote) = split_symbol(symbol_text, market_type);
            let canonical_symbol =
                CanonicalSymbol::new(base, quote).map_err(super::parser::validation_error)?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol,
                exchange_symbol: ExchangeSymbol::new(
                    exchange_id.clone(),
                    market_type,
                    normalize_symbol(symbol_text, market_type)?,
                )
                .ok(),
                order_id: string_field(item, &["order_id", "orderId"]),
                client_order_id: string_field(item, &["client_order_id", "clientOrderId"]),
                fill_id: string_field(item, &["trade_id", "tradeId", "id"]),
                side: parse_side(item),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(item),
                price: decimal_as_f64(item.get("price")).unwrap_or(0.0),
                quantity: decimal_as_f64(
                    item.get("amount")
                        .or_else(|| item.get("qty"))
                        .or_else(|| item.get("size")),
                )
                .unwrap_or(0.0),
                quote_quantity: decimal_as_f64(item.get("notional").or_else(|| item.get("value"))),
                fee_asset: string_field(item, &["fee_currency", "feeCoin", "fee_asset"]),
                fee_amount: decimal_as_f64(item.get("fee").or_else(|| item.get("fee_amount"))),
                fee_rate: None,
                realized_pnl: decimal_as_f64(item.get("realized_pnl")),
                filled_at: item
                    .get("create_time")
                    .or_else(|| item.get("timestamp"))
                    .and_then(value_as_i64)
                    .and_then(DateTime::<Utc>::from_timestamp_millis)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn order_state(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_field(value, &["client_order_id", "clientOrderId", "clientOid"]),
        exchange_order_id: string_field(value, &["order_id", "orderId", "id"]),
        side: parse_side(value),
        position_side: Some(parse_position_side(value)),
        order_type: parse_order_type(value),
        time_in_force: parse_time_in_force(value),
        status: parse_order_status(value),
        quantity: string_field(value, &["size", "amount", "quantity"])
            .unwrap_or_else(|| "0".to_string()),
        price: string_field(value, &["price"]),
        filled_quantity: string_field(value, &["filled_size", "filled_amount", "deal_size"])
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_field(value, &["avg_price", "average_price"]),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value
            .get("create_time")
            .or_else(|| value.get("created_at"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("update_time")
            .or_else(|| value.get("updated_at"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol_text: &str,
) -> ExchangeApiResult<SymbolScope> {
    let (base, quote) = split_symbol(symbol_text, market_type);
    let canonical_symbol =
        CanonicalSymbol::new(base, quote).map_err(super::parser::validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            normalize_symbol(symbol_text, market_type)?,
        )
        .map_err(super::parser::validation_error)?,
    })
}

fn balance_items(value: &Value) -> Vec<&Value> {
    let data = data_payload(value);
    data.get("wallet")
        .or_else(|| data.get("balances"))
        .or_else(|| data.get("assets"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| data.as_array().map(|items| items.iter().collect()))
        .unwrap_or_default()
}

fn position_items(value: &Value) -> Vec<&Value> {
    let data = data_payload(value);
    data.get("positions")
        .or_else(|| data.get("data"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| data.as_array().map(|items| items.iter().collect()))
        .unwrap_or_default()
}

fn order_payload(value: &Value) -> &Value {
    let data = data_payload(value);
    data.get("order").unwrap_or(data)
}

fn order_items(value: &Value) -> Vec<&Value> {
    let data = data_payload(value);
    data.get("orders")
        .or_else(|| data.get("order_list"))
        .or_else(|| data.get("orderList"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| data.as_array().map(|items| items.iter().collect()))
        .unwrap_or_default()
}

fn fill_items(value: &Value) -> Vec<&Value> {
    let data = data_payload(value);
    data.get("trades")
        .or_else(|| data.get("trade_list"))
        .or_else(|| data.get("fills"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| data.as_array().map(|items| items.iter().collect()))
        .unwrap_or_default()
}

fn parse_side(value: &Value) -> OrderSide {
    match string_field(value, &["side"])
        .as_deref()
        .map(str::to_ascii_lowercase)
    {
        Some(side) if side.contains("sell") || side == "2" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_position_side(value: &Value) -> PositionSide {
    match string_field(value, &["position_side", "positionSide", "side"])
        .as_deref()
        .map(str::to_ascii_lowercase)
    {
        Some(side) if side.contains("short") => PositionSide::Short,
        Some(side) if side.contains("long") => PositionSide::Long,
        _ => PositionSide::None,
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match string_field(value, &["type", "order_type", "orderType"])
        .as_deref()
        .map(str::to_ascii_lowercase)
    {
        Some(kind) if kind.contains("market") => OrderType::Market,
        Some(kind) if kind.contains("ioc") => OrderType::IOC,
        Some(kind) if kind.contains("fok") => OrderType::FOK,
        Some(kind) if kind.contains("post") => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &Value) -> Option<TimeInForce> {
    match string_field(value, &["time_in_force", "timeInForce"])
        .as_deref()
        .map(str::to_ascii_uppercase)
    {
        Some(tif) if tif == "IOC" => Some(TimeInForce::IOC),
        Some(tif) if tif == "FOK" => Some(TimeInForce::FOK),
        Some(tif) if tif == "GTX" || tif == "POST_ONLY" => Some(TimeInForce::GTX),
        Some(_) => Some(TimeInForce::GTC),
        None => None,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match string_field(value, &["status", "state"])
        .as_deref()
        .map(str::to_ascii_lowercase)
    {
        Some(status) if status.contains("cancel") => OrderStatus::Cancelled,
        Some(status) if status.contains("filled") || status == "4" => OrderStatus::Filled,
        Some(status) if status.contains("partial") || status == "3" => OrderStatus::PartiallyFilled,
        Some(status) if status.contains("reject") => OrderStatus::Rejected,
        _ => OrderStatus::New,
    }
}

fn parse_liquidity(value: &Value) -> LiquidityRole {
    match string_field(value, &["role", "liquidity", "roleType"])
        .as_deref()
        .map(str::to_ascii_lowercase)
    {
        Some(role) if role.contains("maker") => LiquidityRole::Maker,
        _ => LiquidityRole::Taker,
    }
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| match value {
            Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn split_symbol(symbol: &str, market_type: MarketType) -> (&str, &str) {
    if market_type == MarketType::Spot {
        return symbol.split_once('_').unwrap_or(("BTC", "USDT"));
    }
    split_perp_symbol(symbol)
}

fn split_perp_symbol(symbol: &str) -> (&str, &str) {
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return (base, quote);
            }
        }
    }
    ("BTC", "USDT")
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
