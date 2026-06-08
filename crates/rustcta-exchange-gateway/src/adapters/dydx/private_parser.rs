#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiResult, OrderState, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::split_market;

fn normalize_dydx_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().to_ascii_uppercase().replace('/', "-");
    if normalized.is_empty() {
        return Err(validation_error("dydx symbol must not be empty"));
    }
    Ok(normalized)
}

fn split_dydx_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let (base, quote) = split_market(symbol);
    if base.is_empty() || quote.is_empty() {
        return Err(validation_error(format!("invalid dydx symbol {symbol}")));
    }
    Ok((base, quote))
}

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let subaccount = value.get("subaccount").unwrap_or(value);
    let equity = field_f64(subaccount, &["equity", "quoteBalance", "total"]);
    let free_collateral = field_f64(subaccount, &["freeCollateral", "available"]);
    let total = equity.unwrap_or(free_collateral.unwrap_or(0.0));
    let available = free_collateral.unwrap_or(total);
    let locked = (total - available).max(0.0);
    let balance = AssetBalance::new("USDC", total, available, locked).map_err(validation_error)?;
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances: vec![balance],
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let positions = value
        .get("positions")
        .or_else(|| value.get("perpetualPositions"))
        .or_else(|| value.get("openPerpetualPositions"))
        .or_else(|| {
            value
                .get("subaccount")
                .and_then(|sub| sub.get("openPerpetualPositions"))
        })
        .unwrap_or(value);
    let mut parsed = if let Some(map) = positions.as_object() {
        map.iter()
            .map(|(ticker, position)| {
                parse_position(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    ticker,
                    position,
                )
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?
    } else if let Some(array) = positions.as_array() {
        array
            .iter()
            .map(|position| {
                let ticker = position
                    .get("market")
                    .or_else(|| position.get("ticker"))
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN-USD");
                parse_position(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    ticker,
                    position,
                )
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?
    } else {
        Vec::new()
    };
    if !requested_symbols.is_empty() {
        parsed.retain(|position| {
            position.exchange_symbol.as_ref().is_some_and(|symbol| {
                requested_symbols
                    .iter()
                    .any(|requested| requested.symbol.eq_ignore_ascii_case(&symbol.symbol))
            })
        });
    }
    Ok(parsed)
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.get("orders").unwrap_or(value);
    let Some(array) = orders.as_array() else {
        return Ok(Vec::new());
    };
    array
        .iter()
        .map(|order| parse_order(exchange_id, symbol, order))
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let ticker = value
        .get("ticker")
        .or_else(|| value.get("market"))
        .and_then(Value::as_str)
        .or_else(|| symbol.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .unwrap_or("UNKNOWN-USD");
    let normalized = normalize_dydx_symbol(ticker)?;
    let (base, quote) = split_dydx_symbol(&normalized)?;
    let side = parse_side(value.get("side").and_then(Value::as_str));
    let status = parse_order_status(value.get("status").and_then(Value::as_str));
    let created_at = parse_time(value.get("createdAt").or_else(|| value.get("created_at")));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            normalized,
        )
        .map_err(validation_error)?,
        client_order_id: value
            .get("clientId")
            .or_else(|| value.get("clientOrderId"))
            .and_then(value_as_string),
        exchange_order_id: value
            .get("id")
            .or_else(|| value.get("orderId"))
            .and_then(value_as_string),
        side,
        position_side: Some(match side {
            OrderSide::Buy => PositionSide::Long,
            OrderSide::Sell => PositionSide::Short,
        }),
        order_type: parse_order_type(value.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(value.get("timeInForce").and_then(Value::as_str)),
        status,
        quantity: value
            .get("size")
            .or_else(|| value.get("quantity"))
            .and_then(value_as_string)
            .unwrap_or_else(|| "0".to_string()),
        price: value.get("price").and_then(value_as_string),
        filled_quantity: value
            .get("totalFilled")
            .or_else(|| value.get("filled"))
            .and_then(value_as_string)
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value
            .get("averageFilledPrice")
            .or_else(|| value.get("avgPrice"))
            .and_then(value_as_string),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at,
        updated_at: parse_time(value.get("updatedAt").or_else(|| value.get("updated_at")))
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value.get("fills").unwrap_or(value);
    let Some(array) = fills.as_array() else {
        return Ok(Vec::new());
    };
    array
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                symbol,
                fill,
            )
        })
        .collect()
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    ticker: &str,
    value: &Value,
) -> ExchangeApiResult<ExchangePosition> {
    let normalized = normalize_dydx_symbol(ticker)?;
    let (base, quote) = split_dydx_symbol(&normalized)?;
    let raw_size = field_f64(value, &["size"]).unwrap_or(0.0);
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .map(parse_position_side)
        .unwrap_or_else(|| {
            if raw_size < 0.0 {
                PositionSide::Short
            } else if raw_size > 0.0 {
                PositionSide::Long
            } else {
                PositionSide::None
            }
        });
    Ok(ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, normalized)
                .map_err(validation_error)?,
        ),
        side,
        quantity: raw_size.abs(),
        entry_price: field_f64(value, &["entryPrice", "entry_price"]),
        mark_price: field_f64(value, &["oraclePrice", "markPrice"]),
        liquidation_price: field_f64(value, &["liquidationPrice"]),
        unrealized_pnl: field_f64(value, &["unrealizedPnl", "unrealizedPnlQuoteQuantums"]),
        leverage: None,
        observed_at: Utc::now(),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let ticker = value
        .get("ticker")
        .or_else(|| value.get("market"))
        .and_then(Value::as_str)
        .or_else(|| symbol.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .unwrap_or("UNKNOWN-USD");
    let normalized = normalize_dydx_symbol(ticker)?;
    let (base, quote) = split_dydx_symbol(&normalized)?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, normalized)
                .map_err(validation_error)?,
        ),
        order_id: value
            .get("orderId")
            .or_else(|| value.get("order_id"))
            .and_then(value_as_string),
        client_order_id: value
            .get("clientId")
            .or_else(|| value.get("clientOrderId"))
            .and_then(value_as_string),
        fill_id: value
            .get("id")
            .or_else(|| value.get("fillId"))
            .and_then(value_as_string),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value.get("liquidity").and_then(Value::as_str)),
        price: field_f64(value, &["price"]).unwrap_or(0.0),
        quantity: field_f64(value, &["size", "quantity"]).unwrap_or(0.0),
        quote_quantity: field_f64(value, &["quoteAmount"]),
        fee_asset: Some("USDC".to_string()),
        fee_amount: field_f64(value, &["fee"]),
        fee_rate: None,
        realized_pnl: field_f64(value, &["realizedPnl"]),
        filled_at: parse_time(value.get("createdAt").or_else(|| value.get("created_at")))
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn field_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_as_f64))
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn parse_side(side: Option<&str>) -> OrderSide {
    match side.unwrap_or_default().to_ascii_uppercase().as_str() {
        "SELL" | "SHORT" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_position_side(side: &str) -> PositionSide {
    match side.to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_order_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default().to_ascii_uppercase().as_str() {
        "OPEN" | "BEST_EFFORT_OPENED" => OrderStatus::Open,
        "FILLED" | "BEST_EFFORT_FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" | "BEST_EFFORT_CANCELED" => OrderStatus::Cancelled,
        "UNTRIGGERED" | "PENDING" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "LIMIT" => OrderType::Limit,
        "STOP_MARKET" => OrderType::StopMarket,
        "STOP_LIMIT" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(time_in_force: Option<&str>) -> Option<TimeInForce> {
    match time_in_force?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "POST_ONLY" | "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_liquidity(liquidity: Option<&str>) -> LiquidityRole {
    match liquidity.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MAKER" => LiquidityRole::Maker,
        "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn parse_time(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn validation_error(error: impl std::fmt::Display) -> rustcta_exchange_api::ExchangeApiError {
    rustcta_exchange_api::ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
