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
    normalize_coinex_symbol, parse_error, required_str, string_or_number, validation_error,
    value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
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
                "balance response is not an array",
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
        let asset = required_str(exchange_id, item, "ccy")
            .or_else(|_| required_str(exchange_id, item, "asset"))?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            item.get("available")
                .or_else(|| item.get("available_balance")),
        )?
        .unwrap_or(0.0);
        let locked =
            decimal_value_to_f64(item.get("frozen").or_else(|| item.get("locked")))?.unwrap_or(0.0);
        let total = decimal_value_to_f64(item.get("total"))?.unwrap_or(available + locked);
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
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let market = required_str(exchange_id, value, "market")
        .or_else(|_| required_str(exchange_id, value, "symbol"))
        .unwrap_or("UNKNOWN");
    let normalized_market = normalize_coinex_symbol(market)?;
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Spot);
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), market_type, normalized_market)
            .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let option = value
        .get("option")
        .or_else(|| value.get("time_in_force"))
        .and_then(Value::as_str);
    let order_type_text = value.get("type").and_then(Value::as_str).unwrap_or("limit");
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(value.get("client_id")),
        exchange_order_id: value_as_string(value.get("order_id").or_else(|| value.get("id"))),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(if market_type == MarketType::Perpetual {
            PositionSide::Net
        } else {
            PositionSide::None
        }),
        order_type: parse_order_type(order_type_text, option),
        time_in_force: parse_time_in_force(option),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_coinex_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("amount").or_else(|| value.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("price")).unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: string_or_number(
            value
                .get("filled_amount")
                .or_else(|| value.get("deal_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: non_zero_string(
            string_or_number(value.get("avg_price")).unwrap_or_else(|| "0".to_string()),
        ),
        reduce_only: value
            .get("is_reduce_only")
            .or_else(|| value.get("reduce_only"))
            .and_then(value_as_bool)
            .unwrap_or(false),
        post_only: order_type_text.eq_ignore_ascii_case("maker_only")
            || option.is_some_and(|text| text.eq_ignore_ascii_case("maker_only")),
        created_at: first_timestamp_millis(value, &["created_at", "create_time"]),
        updated_at: first_timestamp_millis(value, &["updated_at", "finished_at"]).unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "open orders response is not an array",
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
    let data = value.get("data").unwrap_or(value);
    if let Some(items) = data.as_array() {
        return items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let symbol = symbols
                    .get(index)
                    .cloned()
                    .or_else(|| symbols.first().cloned())
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "coinex get_fees requires at least one symbol".to_string(),
                    })?;
                parse_fee_snapshot(exchange_id, symbol, item)
            })
            .collect();
    }
    let symbol = symbols
        .first()
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinex get_fees requires at least one symbol".to_string(),
        })?;
    Ok(vec![parse_fee_snapshot(exchange_id, symbol, data)?])
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
                message: "coinex get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response is not an array", value))?;
    fills
        .iter()
        .map(|fill| {
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: value_as_string(fill.get("client_id")),
                fill_id: value_as_string(fill.get("deal_id").or_else(|| fill.get("id"))),
                side: parse_side(required_str(exchange_id, fill, "side")?)?,
                position_side: if symbol.market_type == MarketType::Perpetual {
                    PositionSide::Net
                } else {
                    PositionSide::None
                },
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price: decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0),
                quantity: decimal_value_to_f64(
                    fill.get("amount").or_else(|| fill.get("quantity")),
                )?
                .unwrap_or(0.0),
                quote_quantity: None,
                fee_asset: value_as_string(fill.get("fee_ccy").or_else(|| fill.get("fee_asset"))),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(fill, &["created_at", "time"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_fee_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    let maker = string_or_number(
        value
            .get("maker_fee_rate")
            .or_else(|| value.get("maker_fee"))
            .or_else(|| value.get("maker")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing maker rate",
            value,
        )
    })?;
    let taker = string_or_number(
        value
            .get("taker_fee_rate")
            .or_else(|| value.get("taker_fee"))
            .or_else(|| value.get("taker")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing taker rate",
            value,
        )
    })?;
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: maker,
        taker_rate: taker,
        source: Some("coinex.spot_market".to_string()),
        updated_at: Utc::now(),
    })
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported coinex order side {other}"),
        }),
    }
}

fn parse_order_type(order_type: &str, option: Option<&str>) -> OrderType {
    match (
        order_type.trim().to_ascii_lowercase().as_str(),
        option.map(|text| text.trim().to_ascii_lowercase()),
    ) {
        ("market", _) => OrderType::Market,
        ("maker_only", _) => OrderType::PostOnly,
        (_, Some(option)) if option == "maker_only" => OrderType::PostOnly,
        (_, Some(option)) if option == "ioc" => OrderType::IOC,
        (_, Some(option)) if option == "fok" => OrderType::FOK,
        ("ioc", _) => OrderType::IOC,
        ("fok", _) => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(option: Option<&str>) -> Option<TimeInForce> {
    match option?.trim().to_ascii_lowercase().as_str() {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "maker_only" => Some(TimeInForce::GTX),
        _ => Some(TimeInForce::GTC),
    }
}

fn map_coinex_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "open" | "not_deal" | "pending" => OrderStatus::New,
        "part_deal" | "partially_filled" => OrderStatus::PartiallyFilled,
        "done" | "filled" | "finished" => OrderStatus::Filled,
        "cancel" | "canceled" | "cancelled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_bool(value: &Value) -> Option<bool> {
    value.as_bool().or_else(|| match value.as_str()? {
        "true" | "TRUE" | "1" => Some(true),
        "false" | "FALSE" | "0" => Some(false),
        _ => None,
    })
}

fn non_zero_string(value: String) -> Option<String> {
    if value.trim().is_empty() || value.trim() == "0" || value.trim() == "0.0" {
        None
    } else {
        Some(value)
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let text = string_or_number(Some(value)).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("invalid decimal value {value}"),
    })?;
    text.parse::<f64>()
        .map(Some)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid decimal value {text}: {error}"),
        })
}
