use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    normalize_bitvavo_symbol, number_from_value, parse_error, required_str, string_or_number,
    validation_error, value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "bitvavo balances response is not an array",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = required_str(exchange_id, item, "symbol")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = decimal_value_to_f64(item.get("available"))?.unwrap_or(0.0)
            + decimal_value_to_f64(item.get("inOrder"))?.unwrap_or(0.0);
        let available = decimal_value_to_f64(item.get("available"))?.unwrap_or(0.0);
        let locked = decimal_value_to_f64(item.get("inOrder"))?.unwrap_or(0.0);
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
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
    let market = value
        .get("market")
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .unwrap_or("UNKNOWN-EUR");
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_bitvavo_symbol(market)?,
        )
        .map_err(validation_error)?
    };
    let filled = string_or_number(value.get("filledAmount")).unwrap_or_else(|| "0".to_string());
    let quantity = string_or_number(value.get("amount")).unwrap_or_else(|| filled.clone());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(value.get("clientOrderId")),
        exchange_order_id: value_as_string(value.get("orderId")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value),
        time_in_force: parse_time_in_force(value),
        status: parse_order_status(value, &filled, &quantity),
        quantity,
        price: string_or_number(value.get("price")),
        filled_quantity: filled,
        average_fill_price: string_or_number(value.get("averagePrice")),
        reduce_only: false,
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: first_timestamp(value, &["created", "createdAt"]),
        updated_at: first_timestamp(value, &["updated", "updatedAt"]).unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "bitvavo open orders response is not an array",
                value,
            )
        })?
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let maker = string_or_number(value.get("maker")).unwrap_or_else(|| "0".to_string());
    let taker = string_or_number(value.get("taker")).unwrap_or_else(|| "0".to_string());
    Ok(symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some("bitvavo.account_fees".to_string()),
            updated_at: Utc::now(),
        })
        .collect())
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
                message: "bitvavo get_recent_fills requires canonical_symbol".to_string(),
            })?;
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "bitvavo fills response is not an array",
                value,
            )
        })?
        .iter()
        .map(|fill| {
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderId")),
                client_order_id: value_as_string(fill.get("clientOrderId")),
                fill_id: value_as_string(fill.get("id")),
                side: fill
                    .get("side")
                    .and_then(Value::as_str)
                    .map(parse_side)
                    .transpose()?
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price: decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0),
                quantity: decimal_value_to_f64(fill.get("amount"))?.unwrap_or(0.0),
                quote_quantity: decimal_value_to_f64(fill.get("filledAmountQuote"))?,
                fee_asset: value_as_string(fill.get("feeCurrency")),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestamp"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitvavo order side {other}"),
        }),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    if value
        .get("postOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return OrderType::PostOnly;
    }
    match value
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("limit")
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &Value) -> Option<TimeInForce> {
    match value.get("timeInForce").and_then(Value::as_str) {
        Some("IOC") => Some(TimeInForce::IOC),
        Some("FOK") => Some(TimeInForce::FOK),
        _ => Some(TimeInForce::GTC),
    }
}

fn parse_order_status(value: &Value, filled: &str, quantity: &str) -> OrderStatus {
    match value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" => OrderStatus::New,
        "awaitingtrigger" => OrderStatus::New,
        "filled" => OrderStatus::Filled,
        "partiallyfilled" | "partially_filled" => OrderStatus::PartiallyFilled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ if filled != "0" && filled == quantity => OrderStatus::Filled,
        _ if filled != "0" => OrderStatus::PartiallyFilled,
        _ => OrderStatus::Unknown,
    }
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    number_from_value(value)
        .map(Some)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid bitvavo decimal value {value}"),
        })
}
