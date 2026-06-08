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
    decimal_value_to_f64, normalize_upbit_market, parse_error, required_str, validation_error,
    value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Upbit balances response is not an array",
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
        let available = decimal_value_to_f64_required(exchange_id, item, "balance")?;
        let locked = decimal_value_to_f64_required(exchange_id, item, "locked")?;
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
        .map(normalize_upbit_market)
        .transpose()?
        .unwrap_or_else(|| {
            symbol_hint
                .map(|symbol| symbol.exchange_symbol.symbol.clone())
                .unwrap_or_else(|| "KRW-BTC".to_string())
        });
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, market)
            .map_err(validation_error)?
    };
    let side_text = value.get("side").and_then(Value::as_str).unwrap_or("bid");
    let order_type_text = value
        .get("ord_type")
        .and_then(Value::as_str)
        .unwrap_or("limit");
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(value.get("identifier")),
        exchange_order_id: value_as_string(value.get("uuid")),
        side: parse_side(side_text)?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order_type_text),
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str)),
        status: parse_order_status(value.get("state").and_then(Value::as_str)),
        quantity: string_or_zero(value.get("volume")),
        price: non_zero_string(string_or_zero(value.get("price"))),
        filled_quantity: string_or_zero(value.get("executed_volume")),
        average_fill_price: non_zero_string(string_or_zero(value.get("avg_price"))),
        reduce_only: false,
        post_only: false,
        created_at: parse_rfc3339(value.get("created_at")),
        updated_at: Utc::now(),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Upbit open orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let symbol = symbols
        .first()
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "upbit get_fees requires at least one symbol".to_string(),
        })?;
    let data = value.get("bid_fee").or_else(|| value.get("ask_fee"));
    let fallback_fee = data.and_then(|fee| match fee {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    });
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: value_as_string(value.get("maker_bid_fee"))
            .or_else(|| value_as_string(value.get("bid_fee")))
            .or_else(|| fallback_fee.clone())
            .unwrap_or_else(|| "0.0005".to_string()),
        taker_rate: value_as_string(value.get("taker_bid_fee"))
            .or_else(|| value_as_string(value.get("ask_fee")))
            .or(fallback_fee)
            .unwrap_or_else(|| "0.0005".to_string()),
        source: Some(format!("{exchange_id}.orders/chance")),
        updated_at: Utc::now(),
    }])
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
                message: "upbit get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Upbit fills response is not an array",
            value,
        )
    })?;
    fills
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
                order_id: value_as_string(fill.get("order_uuid").or_else(|| fill.get("uuid"))),
                client_order_id: value_as_string(fill.get("identifier")),
                fill_id: value_as_string(fill.get("uuid").or_else(|| fill.get("trade_uuid"))),
                side: parse_side(required_str(exchange_id, fill, "side")?)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price: decimal_value_to_f64(fill.get("price").unwrap_or(&Value::Null))
                    .unwrap_or(0.0),
                quantity: decimal_value_to_f64(fill.get("volume").unwrap_or(&Value::Null))
                    .unwrap_or(0.0),
                quote_quantity: None,
                fee_asset: value_as_string(
                    fill.get("funds_currency")
                        .or_else(|| fill.get("fee_currency")),
                ),
                fee_amount: fill
                    .get("paid_fee")
                    .or_else(|| fill.get("fee"))
                    .and_then(decimal_value_to_f64),
                fee_rate: None,
                realized_pnl: None,
                filled_at: parse_rfc3339(fill.get("created_at")).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn decimal_value_to_f64_required(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
) -> ExchangeApiResult<f64> {
    value
        .get(field)
        .and_then(decimal_value_to_f64)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                &format!("invalid field {field}"),
                value,
            )
        })
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side {
        "bid" | "buy" => Ok(OrderSide::Buy),
        "ask" | "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported upbit side {other}"),
        }),
    }
}

fn parse_order_type(ord_type: &str) -> OrderType {
    match ord_type {
        "market" | "price" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value {
        Some("ioc") => Some(TimeInForce::IOC),
        Some("fok") => Some(TimeInForce::FOK),
        _ => Some(TimeInForce::GTC),
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default() {
        "wait" | "watch" => OrderStatus::Open,
        "done" => OrderStatus::Filled,
        "cancel" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn string_or_zero(value: Option<&Value>) -> String {
    value_as_string(value).unwrap_or_else(|| "0".to_string())
}

fn non_zero_string(value: String) -> Option<String> {
    if value == "0" || value == "0.0" {
        None
    } else {
        Some(value)
    }
}

fn parse_rfc3339(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let text = value?.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.with_timezone(&Utc))
        .or_else(|| {
            value
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
        })
}
