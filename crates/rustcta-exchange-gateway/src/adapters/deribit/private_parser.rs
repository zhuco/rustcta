use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, ExchangeId, ExchangePosition, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId,
    TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, parse_error, required_str, string_or_number, symbol_scope_from_instrument,
    validation_error, value_as_i64,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let result = value.get("result").unwrap_or(value);
    let rows = if let Some(array) = result.as_array() {
        array.as_slice()
    } else {
        std::slice::from_ref(result)
    };
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut assets = Vec::new();
    for row in rows {
        let asset = row
            .get("currency")
            .and_then(Value::as_str)
            .unwrap_or("USD")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = row
            .get("equity")
            .or_else(|| row.get("balance"))
            .and_then(decimal_as_f64)
            .unwrap_or(0.0);
        let available = row
            .get("available_funds")
            .or_else(|| row.get("available_withdrawal_funds"))
            .and_then(decimal_as_f64)
            .unwrap_or(total);
        let locked = (total - available).max(0.0);
        assets.push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
    }
    Ok(vec![Balance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        balances: assets,
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = value
        .get("result")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit positions response is not an array",
                value,
            )
        })?;
    rows.iter()
        .filter(|position| position.get("size").and_then(decimal_as_f64).unwrap_or(0.0) != 0.0)
        .map(|position| {
            parse_position(exchange_id, tenant_id.clone(), account_id.clone(), position)
        })
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let result = value.get("result").unwrap_or(value);
    if result.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        result,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .get("result")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit orders response is not an array",
                value,
            )
        })?;
    rows.iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fees(symbol: &SymbolScope, value: &Value) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let result = value.get("result").unwrap_or(value);
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate: string_or_number(result.get("maker_commission"))
            .unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(result.get("taker_commission"))
            .unwrap_or_else(|| "0".to_string()),
        source: Some("deribit.private.get_account_summary".to_string()),
        updated_at: Utc::now(),
    }])
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let result = value.get("result").unwrap_or(value);
    let rows = result
        .get("trades")
        .unwrap_or(result)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit fills response is not an array",
                value,
            )
        })?;
    rows.iter()
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let instrument_name = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit order missing instrument_name",
                value,
            )
        })?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_instrument(exchange_id, instrument_name))?;
    let order_type = parse_order_type(value.get("order_type").and_then(Value::as_str));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("label")).filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("order_id")),
        side: parse_side(
            exchange_id,
            value
                .get("direction")
                .and_then(Value::as_str)
                .unwrap_or("buy"),
        )?,
        position_side: Some(PositionSide::Net),
        order_type,
        time_in_force: parse_time_in_force(
            value.get("time_in_force").and_then(Value::as_str),
            order_type,
        ),
        status: parse_order_status(value.get("order_state").and_then(Value::as_str)),
        quantity: string_or_number(value.get("amount")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| value != "market_price"),
        filled_quantity: string_or_number(value.get("filled_amount"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average_price")),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || matches!(order_type, OrderType::PostOnly),
        created_at: value
            .get("creation_timestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("last_update_timestamp")
            .or_else(|| value.get("creation_timestamp"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<ExchangePosition> {
    let instrument_name = required_str(exchange_id, value, "instrument_name")?;
    let symbol = symbol_scope_from_instrument(exchange_id, instrument_name)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deribit position requires canonical_symbol".to_string(),
            })?;
    let signed_size = value.get("size").and_then(decimal_as_f64).unwrap_or(0.0);
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: if signed_size < 0.0 {
            PositionSide::Short
        } else {
            PositionSide::Long
        },
        quantity: signed_size.abs(),
        entry_price: value
            .get("average_price")
            .and_then(decimal_as_f64)
            .filter(|value| *value > 0.0),
        mark_price: value
            .get("mark_price")
            .and_then(decimal_as_f64)
            .filter(|value| *value > 0.0),
        liquidation_price: value
            .get("estimated_liquidation_price")
            .and_then(decimal_as_f64)
            .filter(|value| *value > 0.0),
        unrealized_pnl: value
            .get("floating_profit_loss")
            .or_else(|| value.get("total_profit_loss"))
            .and_then(decimal_as_f64),
        leverage: value
            .get("leverage")
            .and_then(decimal_as_f64)
            .filter(|value| *value > 0.0),
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(position)
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let instrument_name = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "deribit fill missing instrument_name",
                value,
            )
        })?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_instrument(exchange_id, instrument_name))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deribit fill requires canonical_symbol".to_string(),
            })?;
    let price = value.get("price").and_then(decimal_as_f64).unwrap_or(0.0);
    let quantity = value.get("amount").and_then(decimal_as_f64).unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("order_id")),
        client_order_id: string_or_number(value.get("label")),
        fill_id: string_or_number(value.get("trade_id")),
        side: parse_side(
            exchange_id,
            value
                .get("direction")
                .and_then(Value::as_str)
                .unwrap_or("buy"),
        )?,
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("liquidity").and_then(Value::as_str) {
            Some("M") | Some("maker") => LiquidityRole::Maker,
            Some("T") | Some("taker") => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: string_or_number(value.get("fee_currency")),
        fee_amount: value.get("fee").and_then(decimal_as_f64).map(f64::abs),
        fee_rate: None,
        realized_pnl: value.get("profit_loss").and_then(decimal_as_f64),
        filled_at: value
            .get("timestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn parse_side(exchange_id: &ExchangeId, value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported deribit order side",
            &Value::String(value.to_string()),
        )),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "stop_market" => OrderType::StopMarket,
        "stop_limit" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>, order_type: OrderType) -> Option<TimeInForce> {
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("immediate_or_cancel") => Some(TimeInForce::IOC),
        Some("fill_or_kill") => Some(TimeInForce::FOK),
        Some("good_til_cancelled") => Some(TimeInForce::GTC),
        _ if matches!(order_type, OrderType::Limit | OrderType::PostOnly) => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("open").to_ascii_lowercase().as_str() {
        "open" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "untriggered" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}
