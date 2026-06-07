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

use super::parser::normalize_gateio_symbol;

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Gate.io accounts response is not an array",
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
        let asset = required_str(exchange_id, item, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(item.get("available"))?.unwrap_or(0.0);
        let locked = decimal_value_to_f64(item.get("locked"))?.unwrap_or(0.0);
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

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_state(exchange_id, fallback_symbol, value)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Gate.io open orders response is not an array",
            value,
        )
    })?;
    let mut orders = Vec::new();
    for item in items {
        if let Some(nested) = item.get("orders").and_then(Value::as_array) {
            for order in nested {
                orders.push(parse_order_state(exchange_id, fallback_symbol, order)?);
            }
        } else {
            orders.push(parse_order_state(exchange_id, fallback_symbol, item)?);
        }
    }
    Ok(orders)
}

pub fn parse_fees(
    _exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: fallback_symbol.clone(),
        maker_rate: string_or_number(value.get("maker_fee")).unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(value.get("taker_fee")).unwrap_or_else(|| "0".to_string()),
        source: Some("gateio.spot.fee".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Gate.io fills response is not an array",
            value,
        )
    })?;
    fills
        .iter()
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

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, fallback_symbol, value)?;
    let quantity = string_or_number(value.get("amount")).unwrap_or_else(|| "0".to_string());
    let left = decimal_value_to_f64(value.get("left"))?;
    let quantity_number = decimal_text_to_f64(&quantity)?;
    let filled_quantity = left
        .map(|left| (quantity_number - left).max(0.0).to_string())
        .or_else(|| string_or_number(value.get("filled_total")))
        .unwrap_or_else(|| "0".to_string());
    let order_type = parse_order_type(
        value.get("type").and_then(Value::as_str).unwrap_or("limit"),
        value
            .get("time_in_force")
            .or_else(|| value.get("tif"))
            .and_then(Value::as_str),
    );

    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("text")).filter(|value| !value.is_empty()),
        exchange_order_id: value_as_string(value.get("id")).filter(|value| !value.is_empty()),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(|side| parse_side(exchange_id, side))
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: parse_time_in_force(
            value
                .get("time_in_force")
                .or_else(|| value.get("tif"))
                .and_then(Value::as_str),
        ),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_gateio_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity,
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity,
        average_fill_price: string_or_number(value.get("avg_deal_price"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: false,
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: first_timestamp(value, &["create_time_ms", "create_time"]),
        updated_at: first_timestamp(value, &["update_time_ms", "update_time"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_scope(exchange_id, fallback_symbol, value)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio recent fills require canonical_symbol".to_string(),
            })?;
    let price = decimal_value_to_f64(value.get("price"))?.unwrap_or(0.0);
    let quantity = decimal_value_to_f64(value.get("amount"))?.unwrap_or(0.0);
    let quote_quantity = (price > 0.0 && quantity > 0.0).then_some(price * quantity);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("order_id")).filter(|value| !value.is_empty()),
        client_order_id: value_as_string(value.get("text")).filter(|value| !value.is_empty()),
        fill_id: value_as_string(value.get("id")).filter(|value| !value.is_empty()),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(|side| parse_side(exchange_id, side))
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity_role(value.get("role").and_then(Value::as_str)),
        price,
        quantity,
        quote_quantity,
        fee_asset: value_as_string(value.get("fee_currency")).filter(|value| !value.is_empty()),
        fee_amount: decimal_value_to_f64(value.get("fee"))?,
        fee_rate: None,
        realized_pnl: None,
        filled_at: first_timestamp(value, &["create_time_ms", "create_time"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    let symbol = required_str(exchange_id, value, "currency_pair")
        .or_else(|_| required_str(exchange_id, value, "symbol"))?;
    let normalized = normalize_gateio_symbol(symbol)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn parse_side(exchange_id: &ExchangeId, value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "invalid Gate.io order side",
            &Value::String(value.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    match (
        order_type.to_ascii_lowercase().as_str(),
        tif.map(str::to_ascii_lowercase),
    ) {
        ("market", _) => OrderType::Market,
        (_, Some(tif)) if tif == "ioc" => OrderType::IOC,
        (_, Some(tif)) if tif == "fok" => OrderType::FOK,
        (_, Some(tif)) if tif == "poc" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "poc" | "gtx" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn map_gateio_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "open" => OrderStatus::New,
        "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing Gate.io field {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .and_then(|value| string_or_number(Some(value)))
        .map(|value| decimal_text_to_f64(&value).map(Some))
        .unwrap_or(Ok(None))
}

fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Gate.io decimal `{value}`: {error}"),
        })
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(gateio_timestamp))
}

fn gateio_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    let raw = match value {
        Value::String(text) => text.parse::<f64>().ok()?,
        Value::Number(number) => number.as_f64()?,
        _ => return None,
    };
    if raw > 1_000_000_000_000.0 {
        DateTime::<Utc>::from_timestamp_millis(raw as i64)
    } else {
        DateTime::<Utc>::from_timestamp_millis((raw * 1000.0) as i64)
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(rustcta_types::ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: rustcta_types::ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
