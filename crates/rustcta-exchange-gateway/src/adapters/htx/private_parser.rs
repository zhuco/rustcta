use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance as Balance, ExchangeId,
    ExchangePosition as Position, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId,
};
use serde_json::Value;

use super::parser::{
    parse_error, symbol_scope, text, validation_error, value_as_f64, value_as_i64,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let rows = value
        .get("list")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| value.as_array().map(Vec::as_slice))
        .unwrap_or(&[]);
    let balances = rows
        .iter()
        .filter_map(|row| {
            let asset = text(
                row.get("currency")
                    .or_else(|| row.get("margin_asset"))
                    .or_else(|| row.get("symbol")),
            )?;
            if !assets.is_empty()
                && !assets
                    .iter()
                    .any(|wanted| wanted.eq_ignore_ascii_case(&asset))
            {
                return None;
            }
            let available = row
                .get("available")
                .or_else(|| row.get("available_balance"))
                .or_else(|| row.get("withdraw_available"))
                .or_else(|| row.get("balance"))
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            let total = row
                .get("balance")
                .or_else(|| row.get("margin_balance"))
                .or_else(|| row.get("equity"))
                .and_then(value_as_f64)
                .unwrap_or(available);
            let locked = row
                .get("frozen")
                .or_else(|| row.get("lock"))
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            Some(AssetBalance::new(asset, total, available, locked).map_err(validation_error))
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(vec![Balance {
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
) -> ExchangeApiResult<Vec<Position>> {
    rows(value)
        .iter()
        .filter_map(|row| {
            let symbol_text = text(row.get("contract_code"))?;
            let (base, quote) = split_contract(&symbol_text);
            let quantity = row
                .get("volume")
                .or_else(|| row.get("position_volume"))
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            if quantity == 0.0 {
                return None;
            }
            let side = match text(row.get("direction"))
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str()
            {
                "sell" | "short" => PositionSide::Short,
                "buy" | "long" => PositionSide::Long,
                _ => PositionSide::Net,
            };
            let canonical = CanonicalSymbol::new(base, quote).ok()?;
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text).ok();
            Some(Ok(Position {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: canonical,
                exchange_symbol,
                side,
                quantity,
                entry_price: row
                    .get("cost_open")
                    .or_else(|| row.get("cost_hold"))
                    .and_then(value_as_f64),
                mark_price: row.get("last_price").and_then(value_as_f64),
                liquidation_price: row.get("liquidation_price").and_then(value_as_f64),
                unrealized_pnl: row.get("profit_unreal").and_then(value_as_f64),
                leverage: row.get("lever_rate").and_then(value_as_f64),
                observed_at: Utc::now(),
            }))
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let row = value.get("data").unwrap_or(value);
    let symbol = match symbol_hint {
        Some(symbol) => symbol.clone(),
        None => {
            let symbol_text = text(row.get("contract_code").or_else(|| row.get("symbol")))
                .ok_or_else(|| parse_error(exchange_id.clone(), "HTX order missing symbol", row))?;
            let market_type = if symbol_text.contains('-') {
                MarketType::Perpetual
            } else {
                MarketType::Spot
            };
            let (base, quote) = split_contract(&symbol_text);
            symbol_scope(exchange_id, market_type, &symbol_text, &base, &quote)?
        }
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: text(
            row.get("client_order_id")
                .or_else(|| row.get("client-order-id")),
        ),
        exchange_order_id: text(
            row.get("order_id_str")
                .or_else(|| row.get("order_id"))
                .or_else(|| row.get("id")),
        ),
        side: side(row),
        position_side: position_side(row),
        order_type: order_type(row),
        time_in_force: None,
        status: order_status(row),
        quantity: text(row.get("volume").or_else(|| row.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: text(row.get("price")),
        filled_quantity: text(row.get("trade_volume").or_else(|| row.get("field-amount")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: text(
            row.get("trade_avg_price")
                .or_else(|| row.get("field-cash-amount")),
        ),
        reduce_only: text(row.get("offset")).is_some_and(|value| value == "close"),
        post_only: text(row.get("order_price_type"))
            .is_some_and(|value| value.contains("post_only")),
        created_at: row
            .get("created_at")
            .or_else(|| row.get("created-at"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis),
        updated_at: Utc::now(),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value)
        .iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn cancelled_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
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

pub fn parse_fee_rates(symbols: &[SymbolScope], value: &Value) -> Vec<FeeRateSnapshot> {
    let maker = text(
        value
            .get("maker_fee_rate")
            .or_else(|| value.get("maker-fee-rate")),
    )
    .unwrap_or_else(|| "0.002".to_string());
    let taker = text(
        value
            .get("taker_fee_rate")
            .or_else(|| value.get("taker-fee-rate")),
    )
    .unwrap_or_else(|| "0.002".to_string());
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some("htx".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> Vec<Fill> {
    rows(value)
        .iter()
        .filter_map(|row| {
            let symbol = symbol_hint.cloned().or_else(|| {
                let symbol_text = text(row.get("contract_code").or_else(|| row.get("symbol")))?;
                let market_type = if symbol_text.contains('-') {
                    MarketType::Perpetual
                } else {
                    MarketType::Spot
                };
                let (base, quote) = split_contract(&symbol_text);
                symbol_scope(exchange_id, market_type, &symbol_text, &base, &quote).ok()
            })?;
            let canonical = symbol.canonical_symbol.clone()?;
            Some(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical,
                exchange_symbol: Some(symbol.exchange_symbol),
                order_id: text(
                    row.get("order_id_str")
                        .or_else(|| row.get("order_id"))
                        .or_else(|| row.get("order-id")),
                ),
                client_order_id: text(
                    row.get("client_order_id")
                        .or_else(|| row.get("client-order-id")),
                ),
                fill_id: text(
                    row.get("trade_id")
                        .or_else(|| row.get("trade-id"))
                        .or_else(|| row.get("id")),
                ),
                side: side(row),
                position_side: position_side(row).unwrap_or(PositionSide::Net),
                status: FillStatus::Confirmed,
                liquidity_role: if text(row.get("role"))
                    .is_some_and(|role| role.eq_ignore_ascii_case("maker"))
                {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price: row
                    .get("trade_price")
                    .or_else(|| row.get("price"))
                    .and_then(value_as_f64)
                    .unwrap_or(0.0),
                quantity: row
                    .get("trade_volume")
                    .or_else(|| row.get("filled-amount"))
                    .or_else(|| row.get("amount"))
                    .and_then(value_as_f64)
                    .unwrap_or(0.0),
                quote_quantity: row
                    .get("trade_turnover")
                    .or_else(|| row.get("filled-cash-amount"))
                    .and_then(value_as_f64),
                fee_asset: text(row.get("fee_asset").or_else(|| row.get("fee-currency"))),
                fee_amount: row
                    .get("trade_fee")
                    .or_else(|| row.get("filled-fees"))
                    .and_then(value_as_f64),
                fee_rate: None,
                realized_pnl: row.get("real_profit").and_then(value_as_f64),
                filled_at: row
                    .get("created_at")
                    .or_else(|| row.get("created-at"))
                    .and_then(value_as_i64)
                    .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn rows(value: &Value) -> &[Value] {
    value
        .get("orders")
        .or_else(|| value.get("list"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| value.as_array().map(Vec::as_slice))
        .unwrap_or(&[])
}

fn split_contract(symbol: &str) -> (String, String) {
    let upper = symbol.to_ascii_uppercase().replace('-', "");
    if upper.ends_with("USDT") && upper.len() > 4 {
        (upper[..upper.len() - 4].to_string(), "USDT".to_string())
    } else if upper.ends_with("USD") && upper.len() > 3 {
        (upper[..upper.len() - 3].to_string(), "USD".to_string())
    } else {
        (upper, "USDT".to_string())
    }
}

fn side(value: &Value) -> OrderSide {
    match text(
        value
            .get("direction")
            .or_else(|| value.get("side"))
            .or_else(|| value.get("type")),
    )
    .unwrap_or_default()
    .to_ascii_lowercase()
    .as_str()
    {
        side if side.starts_with("sell") => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn position_side(value: &Value) -> Option<PositionSide> {
    match text(value.get("offset").or_else(|| value.get("direction")))
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" | "short" => Some(PositionSide::Short),
        "buy" | "long" => Some(PositionSide::Long),
        _ => None,
    }
}

fn order_type(value: &Value) -> OrderType {
    let kind = text(value.get("order_price_type").or_else(|| value.get("type")))
        .unwrap_or_default()
        .to_ascii_lowercase();
    if kind.contains("market") {
        OrderType::Market
    } else if kind.contains("ioc") {
        OrderType::IOC
    } else if kind.contains("fok") {
        OrderType::FOK
    } else if kind.contains("post") {
        OrderType::PostOnly
    } else {
        OrderType::Limit
    }
}

fn order_status(value: &Value) -> OrderStatus {
    match text(value.get("status").or_else(|| value.get("state")))
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "submitted" | "created" | "1" => OrderStatus::New,
        "partial-filled" | "partial_filled" | "4" => OrderStatus::PartiallyFilled,
        "filled" | "6" => OrderStatus::Filled,
        "canceled" | "cancelled" | "7" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Open,
    }
}
