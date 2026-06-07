use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderStatus, OrderType, SchemaVersion,
    TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_items, data_payload, decimal_as_f64, is_zero_decimal, parse_error, parse_position_side,
    parse_side, required_str, split_blofin_symbol, string_or_number, validation_error,
    value_as_i64,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = data_items(value).unwrap_or_else(|| std::slice::from_ref(data_payload(value)));
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = item
            .get("currency")
            .or_else(|| item.get("ccy"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = item
            .get("available")
            .or_else(|| item.get("availableBalance"))
            .and_then(decimal_as_f64)
            .unwrap_or(0.0);
        let frozen = item.get("frozen").and_then(decimal_as_f64).unwrap_or(0.0);
        let total = item
            .get("balance")
            .or_else(|| item.get("equity"))
            .and_then(decimal_as_f64)
            .unwrap_or(available + frozen);
        if total > 0.0 || available > 0.0 || frozen > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, frozen).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let items = data_items(value).unwrap_or_else(|| std::slice::from_ref(data_payload(value)));
    let mut positions = Vec::new();
    for item in items {
        let inst_id = required_str(exchange_id, item, "instId")?.to_ascii_uppercase();
        let quantity = item
            .get("positions")
            .or_else(|| item.get("availablePositions"))
            .and_then(decimal_as_f64)
            .unwrap_or(0.0)
            .abs();
        if quantity == 0.0 {
            continue;
        }
        let (base, quote) = split_blofin_symbol(&inst_id)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, inst_id)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(item.get("positionSide").and_then(Value::as_str)),
            quantity,
            entry_price: item
                .get("averagePrice")
                .or_else(|| item.get("entryPrice"))
                .and_then(decimal_as_f64),
            mark_price: item.get("markPrice").and_then(decimal_as_f64),
            liquidation_price: item.get("liquidationPrice").and_then(decimal_as_f64),
            unrealized_pnl: item
                .get("unrealizedPnl")
                .or_else(|| item.get("unrealizedPNL"))
                .and_then(decimal_as_f64),
            leverage: item.get("leverage").and_then(decimal_as_f64),
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_fee_snapshots(symbols: &[SymbolScope]) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: "0".to_string(),
            taker_rate: "0".to_string(),
            source: Some("blofin.trade_fee_endpoint_unavailable_in_openapi".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    if data.is_null() {
        return Ok(None);
    }
    let order = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = data_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BloFin orders response missing data",
            value,
        )
    })?;
    items
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let items = data_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BloFin fills response missing data",
            value,
        )
    })?;
    items
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    let raw_type = string_or_number(value.get("orderType")).unwrap_or_else(|| "limit".to_string());
    let order_type = parse_order_type(&raw_type);
    let tif = parse_time_in_force(&raw_type);
    let canonical_symbol = symbol.canonical_symbol.ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BloFin order missing canonical symbol",
            value,
        )
    })?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOrderId"))
            .filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        side: parse_side(
            exchange_id,
            string_or_number(value.get("side"))
                .as_deref()
                .unwrap_or("buy"),
        )?,
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
        )),
        order_type,
        time_in_force: tif,
        status: string_or_number(value.get("state"))
            .or_else(|| string_or_number(value.get("status")))
            .as_deref()
            .map(map_order_status)
            .unwrap_or(OrderStatus::New),
        quantity: string_or_number(value.get("size")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(value.get("filledSize"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("averagePrice"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: string_or_number(value.get("reduceOnly")).is_some_and(|value| value == "true"),
        post_only: order_type == OrderType::PostOnly,
        created_at: value
            .get("createTime")
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("updateTime")
            .or_else(|| value.get("ts"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
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
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BloFin fill missing canonical symbol",
                value,
            )
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")),
        client_order_id: string_or_number(value.get("clientOrderId")),
        fill_id: string_or_number(value.get("tradeId"))
            .or_else(|| string_or_number(value.get("fillId"))),
        side: parse_side(
            exchange_id,
            string_or_number(value.get("side"))
                .as_deref()
                .unwrap_or("buy"),
        )?,
        position_side: rustcta_types::PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: value
            .get("execType")
            .and_then(Value::as_str)
            .map(|role| {
                if role.eq_ignore_ascii_case("maker") {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                }
            })
            .unwrap_or(LiquidityRole::Unknown),
        price: value.get("price").and_then(decimal_as_f64).unwrap_or(0.0),
        quantity: value
            .get("size")
            .or_else(|| value.get("fillSize"))
            .and_then(decimal_as_f64)
            .unwrap_or(0.0),
        quote_quantity: None,
        fee_asset: Some(
            value
                .get("feeCurrency")
                .and_then(Value::as_str)
                .unwrap_or("USDT")
                .to_string(),
        ),
        fee_amount: value.get("fee").and_then(decimal_as_f64),
        fee_rate: None,
        realized_pnl: value.get("pnl").and_then(decimal_as_f64),
        filled_at: value
            .get("ts")
            .or_else(|| value.get("fillTime"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

pub fn ack_order(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    status: OrderStatus,
) -> OrderState {
    let canonical_symbol = fallback_symbol.canonical_symbol.clone().unwrap_or_else(|| {
        let (base, quote) = split_blofin_symbol(&fallback_symbol.exchange_symbol.symbol)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        CanonicalSymbol::new(base, quote).expect("fallback canonical symbol")
    });
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id: string_or_number(value.get("clientOrderId"))
            .filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        side: rustcta_types::OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status,
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

fn symbol_from_payload(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolScope> {
    let inst_id = required_str(exchange_id, value, "instId")?.to_ascii_uppercase();
    let (base, quote) = split_blofin_symbol(&inst_id)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, inst_id)
            .map_err(validation_error)?,
    })
}

fn parse_order_type(raw: &str) -> OrderType {
    match raw.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(raw: &str) -> Option<TimeInForce> {
    match raw.to_ascii_lowercase().as_str() {
        "post_only" => Some(TimeInForce::GTX),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "limit" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn map_order_status(raw: &str) -> OrderStatus {
    match raw.to_ascii_lowercase().as_str() {
        "live" => OrderStatus::Open,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "partially_canceled" => OrderStatus::Cancelled,
        "failed" | "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}
