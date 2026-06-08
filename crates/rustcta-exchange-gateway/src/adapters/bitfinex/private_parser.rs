use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiResult, Fill, OrderState, Position, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeId, ExchangeSymbol, FillStatus, LiquidityRole, MarketType, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    canonical_from_bitfinex_symbol, market_type_from_symbol, millis, parse_error, validation_error,
    value_as_f64, value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex wallets response missing array",
            value,
        )
    })?;
    let mut balances = Vec::new();
    for row in rows {
        let Some(array) = row.as_array() else {
            continue;
        };
        let wallet_type = array.first().and_then(Value::as_str).unwrap_or_default();
        let market_type = match wallet_type {
            "exchange" => MarketType::Spot,
            "trading" => MarketType::Margin,
            _ => requested_market_type,
        };
        if requested_market_type != market_type
            && !(requested_market_type == MarketType::Perpetual && wallet_type == "trading")
        {
            continue;
        }
        let asset = array
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase();
        let total = array.get(2).and_then(value_as_f64).unwrap_or(0.0);
        let available = array.get(4).and_then(value_as_f64).unwrap_or(total);
        let locked = (total - available).max(0.0);
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
    }
    Ok(vec![Balance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: requested_market_type,
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
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex positions response missing array",
            value,
        )
    })?;
    rows.iter()
        .filter_map(|row| row.as_array())
        .filter(|row| row.first().and_then(Value::as_str).is_some())
        .map(|row| {
            let symbol = row.first().and_then(Value::as_str).unwrap_or("tUNKNOWNUSD");
            let amount = row.get(2).and_then(value_as_f64).unwrap_or(0.0);
            let side = if amount < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            };
            let market_type = market_type_from_symbol(symbol, MarketType::Margin);
            let canonical_symbol = canonical_from_bitfinex_symbol(symbol)?;
            Ok(Position {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), market_type, symbol.to_string())
                        .map_err(validation_error)?,
                ),
                side,
                quantity: amount.abs(),
                entry_price: row.get(3).and_then(value_as_f64),
                mark_price: None,
                liquidation_price: row.get(8).and_then(value_as_f64),
                unrealized_pnl: row.get(6).and_then(value_as_f64),
                leverage: row.get(9).and_then(value_as_f64),
                observed_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    if let Some(order_array) = notification_order_array(value) {
        return parse_order_array(
            exchange_id,
            fallback_market_type,
            &Value::Array(order_array),
        );
    }
    parse_order_array(exchange_id, fallback_market_type, value)
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex orders response missing array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| parse_order(exchange_id, fallback_market_type, row))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex trades response missing array",
            value,
        )
    })?;
    rows.iter()
        .filter_map(Value::as_array)
        .map(|row| {
            let symbol = row.get(1).and_then(Value::as_str).unwrap_or("tUNKNOWNUSD");
            let market_type = market_type_from_symbol(symbol, fallback_market_type);
            let amount = row.get(4).and_then(value_as_f64).unwrap_or(0.0);
            let side = if amount < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol: canonical_from_bitfinex_symbol(symbol)?,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), market_type, symbol.to_string())
                        .map_err(validation_error)?,
                ),
                order_id: value_as_string(row.get(3)),
                client_order_id: value_as_string(row.get(11)),
                fill_id: value_as_string(row.first()),
                side,
                position_side: PositionSide::Net,
                status: FillStatus::Confirmed,
                liquidity_role: if row.get(8).and_then(Value::as_i64) == Some(1) {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price: row.get(5).and_then(value_as_f64).unwrap_or(0.0),
                quantity: amount.abs(),
                quote_quantity: None,
                fee_asset: row
                    .get(10)
                    .and_then(Value::as_str)
                    .map(str::to_ascii_uppercase),
                fee_amount: row.get(9).and_then(value_as_f64).map(f64::abs),
                fee_rate: None,
                realized_pnl: None,
                filled_at: millis(row.get(2)).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_order_array(
    exchange_id: &ExchangeId,
    fallback_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let row = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex order response missing array",
            value,
        )
    })?;
    let symbol = row.get(3).and_then(Value::as_str).unwrap_or("tUNKNOWNUSD");
    let market_type = market_type_from_symbol(symbol, fallback_market_type);
    let amount = row.get(6).and_then(value_as_f64).unwrap_or(0.0);
    let amount_orig = row.get(7).and_then(value_as_f64).unwrap_or(amount);
    let order_type_text = row.get(8).and_then(Value::as_str).unwrap_or("LIMIT");
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_from_bitfinex_symbol(symbol)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol.to_string())
            .map_err(validation_error)?,
        client_order_id: value_as_string(row.get(2)),
        exchange_order_id: value_as_string(row.first()),
        side: if amount_orig < 0.0 {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        position_side: None,
        order_type: parse_order_type(order_type_text),
        time_in_force: parse_time_in_force(order_type_text),
        status: parse_order_status(row.get(13).and_then(Value::as_str).unwrap_or("UNKNOWN")),
        quantity: amount_orig.abs().to_string(),
        price: value_as_string(row.get(16)),
        filled_quantity: (amount_orig.abs() - amount.abs()).max(0.0).to_string(),
        average_fill_price: value_as_string(row.get(17)),
        reduce_only: false,
        post_only: row.get(12).and_then(value_as_i64).unwrap_or(0) & 4096 != 0,
        created_at: millis(row.get(4)),
        updated_at: millis(row.get(5)).unwrap_or_else(Utc::now),
    })
}

fn notification_order_array(value: &Value) -> Option<Vec<Value>> {
    value.as_array()?.iter().find_map(|item| {
        let array = item.as_array()?;
        if array.len() > 16 {
            Some(array.clone())
        } else {
            None
        }
    })
}

fn parse_order_type(value: &str) -> OrderType {
    let normalized = value
        .trim()
        .to_ascii_uppercase()
        .replace("EXCHANGE ", "")
        .replace(" DERIV", "");
    if normalized.contains("MARKET") {
        OrderType::Market
    } else if normalized.contains("IOC") {
        OrderType::IOC
    } else if normalized.contains("FOK") {
        OrderType::FOK
    } else {
        OrderType::Limit
    }
}

fn parse_time_in_force(value: &str) -> Option<TimeInForce> {
    let normalized = value.to_ascii_uppercase();
    if normalized.contains("IOC") {
        Some(TimeInForce::IOC)
    } else if normalized.contains("FOK") {
        Some(TimeInForce::FOK)
    } else {
        Some(TimeInForce::GTC)
    }
}

pub fn parse_order_status(value: &str) -> OrderStatus {
    let normalized = value.to_ascii_uppercase();
    if normalized.starts_with("ACTIVE") {
        OrderStatus::Open
    } else if normalized.starts_with("PARTIALLY") {
        OrderStatus::PartiallyFilled
    } else if normalized.starts_with("EXECUTED") {
        OrderStatus::Filled
    } else if normalized.starts_with("CANCELED") || normalized.starts_with("CANCELLED") {
        OrderStatus::Cancelled
    } else if normalized.contains("INSUFFICIENT") || normalized.contains("REJECT") {
        OrderStatus::Rejected
    } else {
        OrderStatus::Unknown
    }
}
