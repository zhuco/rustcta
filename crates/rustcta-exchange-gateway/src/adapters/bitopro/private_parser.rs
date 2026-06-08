use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    canonical_from_pair, normalize_bitopro_pair, parse_error, required_str, string_or_number,
    validation_error, value_as_string, value_f64, value_i64,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id, "balance data array", value))?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = required_str(exchange_id, row, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = decimal_value_to_f64(row.get("amount"))?.unwrap_or(0.0);
        let available = decimal_value_to_f64(row.get("available"))?.unwrap_or(0.0);
        let stake = decimal_value_to_f64(row.get("stake"))?.unwrap_or(0.0);
        let locked = (total - available).max(stake).max(0.0);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
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
    let pair = value
        .get("pair")
        .and_then(Value::as_str)
        .map(normalize_bitopro_pair)
        .transpose()?
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .unwrap_or_else(|| "unknown_unknown".to_string());
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
            .map_err(validation_error)?
    };
    let canonical_symbol = if let Some(symbol) = symbol_hint {
        symbol.canonical_symbol.clone()
    } else {
        canonical_from_pair(&pair).ok()
    };
    let order_type_text = value.get("type").and_then(Value::as_str).unwrap_or("LIMIT");
    let time_in_force = value.get("timeInForce").and_then(Value::as_str);
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(value.get("clientId").or_else(|| value.get("clientID"))),
        exchange_order_id: value_as_string(value.get("id").or_else(|| value.get("orderId"))),
        side: value
            .get("action")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order_type_text),
        time_in_force: time_in_force.and_then(parse_time_in_force),
        status: value
            .get("status")
            .and_then(value_i64)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("originalAmount").or_else(|| value.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("price")).unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: string_or_number(value.get("executedAmount"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: non_zero_string(
            string_or_number(value.get("avgExecutionPrice")).unwrap_or_else(|| "0".to_string()),
        ),
        reduce_only: false,
        post_only: time_in_force.is_some_and(|value| value.eq_ignore_ascii_case("POST_ONLY")),
        created_at: first_timestamp_millis(value, &["createdTimestamp", "timestamp"]),
        updated_at: first_timestamp_millis(value, &["updatedTimestamp", "timestamp"])
            .unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    if let Some(rows) = data.as_array() {
        return rows
            .iter()
            .map(|row| parse_order_state(exchange_id, symbol_hint, row))
            .collect();
    }
    if let Some(map) = data.as_object() {
        let mut orders = Vec::new();
        for rows in map.values() {
            let rows = rows.as_array().ok_or_else(|| {
                parse_error(exchange_id, "open order map values are arrays", rows)
            })?;
            for row in rows {
                orders.push(parse_order_state(exchange_id, symbol_hint, row)?);
            }
        }
        return Ok(orders);
    }
    Err(parse_error(exchange_id, "open orders data", value))
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
                message: "bitopro get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id, "trades data array", value))?;
    rows.iter()
        .map(|row| {
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(row.get("orderId")),
                client_order_id: None,
                fill_id: value_as_string(row.get("tradeId")),
                side: parse_side(required_str(exchange_id, row, "action")?)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: row
                    .get("isTaker")
                    .and_then(Value::as_bool)
                    .map(|is_taker| {
                        if is_taker {
                            LiquidityRole::Taker
                        } else {
                            LiquidityRole::Maker
                        }
                    })
                    .unwrap_or(LiquidityRole::Unknown),
                price: decimal_value_to_f64(row.get("price"))?.unwrap_or(0.0),
                quantity: decimal_value_to_f64(row.get("baseAmount"))?.unwrap_or(0.0),
                quote_quantity: decimal_value_to_f64(row.get("quoteAmount"))?,
                fee_asset: value_as_string(row.get("feeSymbol"))
                    .map(|asset| asset.trim().to_ascii_uppercase()),
                fee_amount: decimal_value_to_f64(row.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(row, &["createdTimestamp", "timestamp"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_side(value: &str) -> ExchangeApiResult<OrderSide> {
    match value.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitopro side {value}"),
        }),
    }
}

fn parse_order_type(value: &str) -> OrderType {
    match value.trim().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "STOP_LIMIT" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &str) -> Option<TimeInForce> {
    match value.trim().to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "POST_ONLY" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn map_order_status(status: i64) -> OrderStatus {
    match status {
        -1 => OrderStatus::New,
        0 => OrderStatus::Open,
        1 => OrderStatus::PartiallyFilled,
        2 => OrderStatus::Filled,
        3 => OrderStatus::PartiallyFilled,
        4 | 6 => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn non_zero_string(value: String) -> Option<String> {
    if value == "0" || value == "0.0" || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            value_f64(value).ok_or_else(|| {
                parse_error(&ExchangeId::unchecked("bitopro"), "decimal number", value)
            })
        })
        .transpose()
}

fn first_timestamp_millis(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(value_i64)
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
    })
}
