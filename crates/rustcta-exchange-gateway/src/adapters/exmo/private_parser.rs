use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus,
    LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, first_timestamp, normalize_exmo_exchange_symbol, parse_error, split_pair,
    string_or_number, validation_error, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let balances = value
        .get("balances")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "EXMO user_info missing balances",
                value,
            )
        })?;
    let reserved = value
        .get("reserved")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for (asset, available_value) in balances {
        let asset = asset.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(Some(available_value))?.unwrap_or(0.0);
        let locked = decimal_value_to_f64(reserved.get(&asset))?.unwrap_or(0.0);
        rows.push(
            AssetBalance::new(asset, available + locked, available, locked)
                .map_err(validation_error)?,
        );
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances: rows,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_or_self(value);
    let symbol = symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = string_or_number(
        order
            .get("quantity")
            .or_else(|| order.get("original_quantity"))
            .or_else(|| order.get("in_amount")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_or_number(
        order
            .get("executed_quantity")
            .or_else(|| order.get("filled_quantity"))
            .or_else(|| order.get("deal_quantity"))
            .or_else(|| order.get("dealQuantity")),
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
        client_order_id: value_as_string(order.get("client_id")),
        exchange_order_id: value_as_string(order.get("order_id").or_else(|| order.get("id"))),
        side: parse_side(
            order
                .get("type")
                .or_else(|| order.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order.get("type").and_then(Value::as_str)),
        time_in_force: None,
        status,
        quantity,
        price: string_or_number(order.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: average_fill_price(order, &filled_quantity),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(order, &["created", "date", "timestamp"]),
        updated_at: first_timestamp(order, &["updated", "date", "timestamp"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    flatten_pair_rows(value, "orders")
        .iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = flatten_pair_rows(value, "trades");
    rows.iter()
        .map(|fill| parse_fill(exchange_id, &tenant_id, &account_id, symbol_hint, fill))
        .collect()
}

pub fn parse_order_trades_as_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let rows = value
        .get("trades")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut order = value.clone();
    if let Some(first) = rows.first() {
        if order.get("pair").is_none() {
            order["pair"] = first
                .get("pair")
                .cloned()
                .unwrap_or_else(|| Value::String(symbol_hint.exchange_symbol.symbol.clone()));
        }
        if order.get("type").is_none() {
            order["type"] = first
                .get("type")
                .cloned()
                .unwrap_or(Value::String("buy".to_string()));
        }
    }
    let filled_quantity = rows
        .iter()
        .filter_map(|row| decimal_value_to_f64(row.get("quantity")).ok().flatten())
        .sum::<f64>();
    if order.get("quantity").is_none() && filled_quantity > 0.0 {
        order["quantity"] = Value::String(trim_float(filled_quantity));
    }
    order["executed_quantity"] = Value::String(trim_float(filled_quantity));
    order["status"] = if filled_quantity > 0.0 {
        Value::String("filled".to_string())
    } else {
        Value::String("unknown".to_string())
    };
    parse_order_state(exchange_id, Some(symbol_hint), &order)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let settings = value.as_object().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "EXMO pair_settings response must be an object",
            value,
        )
    })?;
    let mut fees = Vec::new();
    for symbol in requested_symbols {
        let pair = normalize_exmo_exchange_symbol(&symbol.exchange_symbol.symbol)?;
        let Some(row) = settings.get(&pair) else {
            continue;
        };
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: percent_to_rate(row.get("commission_maker_percent")),
            taker_rate: percent_to_rate(row.get("commission_taker_percent")),
            source: Some("exmo.pair_settings".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol_hint: &SymbolScope,
    fill: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_scope(exchange_id, Some(symbol_hint), fill)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "EXMO fill parser requires canonical_symbol".to_string(),
            })?;
    let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
    let quantity = decimal_value_to_f64(fill.get("quantity"))?.unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(fill.get("order_id")),
        client_order_id: value_as_string(fill.get("client_id")),
        fill_id: value_as_string(fill.get("trade_id").or_else(|| fill.get("id"))),
        side: parse_side(
            fill.get("type")
                .or_else(|| fill.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(
            fill.get("exec_type").and_then(Value::as_str),
            fill.get("is_maker").and_then(Value::as_bool),
        ),
        price,
        quantity,
        quote_quantity: decimal_value_to_f64(fill.get("amount"))?
            .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(fill.get("commission_currency")),
        fee_amount: decimal_value_to_f64(fill.get("commission_amount"))?,
        fee_rate: decimal_value_to_f64(fill.get("commission_percent"))?.map(|rate| rate / 100.0),
        realized_pnl: None,
        filled_at: first_timestamp(fill, &["date", "timestamp"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn first_order_or_self(value: &Value) -> &Value {
    value
        .get("order")
        .or_else(|| value.get("data"))
        .or_else(|| {
            value
                .get("orders")
                .and_then(Value::as_array)
                .and_then(|orders| orders.first())
        })
        .unwrap_or(value)
}

fn flatten_pair_rows(value: &Value, fallback_field: &str) -> Vec<Value> {
    if let Some(rows) = value.get(fallback_field).and_then(Value::as_array) {
        return rows.clone();
    }
    if let Some(rows) = value.as_array() {
        return rows.clone();
    }
    let Some(object) = value.as_object() else {
        return Vec::new();
    };
    object
        .iter()
        .flat_map(|(pair, rows)| {
            rows.as_array()
                .into_iter()
                .flatten()
                .map(|row| {
                    let mut row = row.clone();
                    if row.get("pair").is_none() {
                        row["pair"] = Value::String(pair.clone());
                    }
                    row
                })
                .collect::<Vec<_>>()
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
    let pair = value
        .get("pair")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "EXMO row missing pair", value))?;
    let (base, quote) = split_pair(pair)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(&base, &quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_exmo_exchange_symbol(pair)?,
        )
        .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" | "market_sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market_buy" | "market_sell" | "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "active" => OrderStatus::Open,
        "new" => OrderStatus::New,
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "closed" | "done" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
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

fn parse_liquidity(exec_type: Option<&str>, is_maker: Option<bool>) -> LiquidityRole {
    match exec_type.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => match is_maker {
            Some(true) => LiquidityRole::Maker,
            Some(false) => LiquidityRole::Taker,
            None => LiquidityRole::Unknown,
        },
    }
}

fn average_fill_price(value: &Value, filled_quantity: &str) -> Option<String> {
    let amount = decimal_value_to_f64(value.get("amount")).ok().flatten()?;
    let filled = filled_quantity.parse::<f64>().ok()?;
    (amount > 0.0 && filled > 0.0).then(|| trim_float(amount / filled))
}

fn percent_to_rate(value: Option<&Value>) -> String {
    decimal_value_to_f64(value)
        .ok()
        .flatten()
        .map(|percent| trim_float(percent / 100.0))
        .unwrap_or_else(|| "0".to_string())
}

fn trim_float(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|value| value == 0.0)
}
