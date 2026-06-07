use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, normalize_backpack_symbol, parse_error, parse_market_type, split_symbol_assets,
    string_or_number, validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    let object = value.as_object().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Backpack balances response is not an object",
            value,
        )
    })?;
    for (asset, item) in object {
        let asset = asset.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(item.get("available")).unwrap_or(0.0);
        let locked = decimal_as_f64(item.get("locked")).unwrap_or(0.0);
        let staked = decimal_as_f64(item.get("staked")).unwrap_or(0.0);
        balances.push(
            AssetBalance::new(asset, available + locked + staked, available, locked)
                .map_err(validation_error)?,
        );
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = rows(value);
    let requested = requested
        .iter()
        .map(|symbol| normalize_backpack_symbol(&symbol.symbol))
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for item in rows {
        let symbol_text = item
            .get("symbol")
            .and_then(Value::as_str)
            .map(normalize_backpack_symbol)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Backpack position missing symbol",
                    item,
                )
            })?;
        if !requested.is_empty() && !requested.contains(&symbol_text) {
            continue;
        }
        let quantity = decimal_as_f64(item.get("netQuantity")).unwrap_or(0.0);
        if quantity == 0.0 {
            continue;
        }
        let (base, quote) = split_symbol_assets(&symbol_text);
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: canonical_symbol.clone(),
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                    .map_err(validation_error)?,
            ),
            side: if quantity < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            },
            quantity: quantity.abs(),
            entry_price: decimal_as_f64(item.get("entryPrice")),
            mark_price: decimal_as_f64(item.get("markPrice")),
            liquidation_price: decimal_as_f64(item.get("estLiquidationPrice")),
            unrealized_pnl: decimal_as_f64(item.get("pnlUnrealized")),
            leverage: None,
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_fee_snapshots(
    symbols: &[SymbolScope],
    account: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let spot_maker = bps_to_rate(string_or_number(account.get("spotMakerFee")).as_deref())
        .unwrap_or_else(|| "0".to_string());
    let spot_taker = bps_to_rate(string_or_number(account.get("spotTakerFee")).as_deref())
        .unwrap_or_else(|| "0".to_string());
    let futures_maker = bps_to_rate(string_or_number(account.get("futuresMakerFee")).as_deref())
        .unwrap_or_else(|| spot_maker.clone());
    let futures_taker = bps_to_rate(string_or_number(account.get("futuresTakerFee")).as_deref())
        .unwrap_or_else(|| spot_taker.clone());
    Ok(symbols
        .iter()
        .map(|symbol| {
            let (maker, taker) = if symbol.market_type == MarketType::Perpetual {
                (futures_maker.clone(), futures_taker.clone())
            } else {
                (spot_maker.clone(), spot_taker.clone())
            };
            FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol.clone(),
                maker_rate: maker,
                taker_rate: taker,
                source: Some("backpack.account".to_string()),
                updated_at: Utc::now(),
            }
        })
        .collect())
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    if value.is_null() {
        return Ok(None);
    }
    let item = value
        .get("order")
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        market_type,
        item,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value)
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, market_type, order))
        .collect()
}

pub fn parse_batch_orders(
    exchange_id: &ExchangeId,
    fallback_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value)
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            if item.get("operation").and_then(Value::as_str) == Some("Err") {
                return None;
            }
            let fallback = fallback_symbols.get(idx);
            let market_type = fallback
                .map(|symbol| symbol.market_type)
                .or_else(|| market_type_from_order(item).ok())
                .unwrap_or(MarketType::Spot);
            Some(parse_order_state(exchange_id, fallback, market_type, item))
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_order(exchange_id, market_type, value))?;
    let raw_type = value
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("Limit");
    let side = parse_side(value.get("side").and_then(Value::as_str));
    let status = parse_order_status(value.get("status").and_then(Value::as_str));
    let created_at = value
        .get("createdAt")
        .and_then(|value| match value {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.parse().ok(),
            _ => None,
        })
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientId")),
        exchange_order_id: string_or_number(value.get("id").or_else(|| value.get("orderId"))),
        side,
        position_side: (market_type == MarketType::Perpetual).then_some(PositionSide::Net),
        order_type: parse_order_type(raw_type, value.get("postOnly").and_then(Value::as_bool)),
        time_in_force: parse_time_in_force(value.get("timeInForce").and_then(Value::as_str)),
        status,
        quantity: string_or_number(
            value
                .get("quantity")
                .or_else(|| value.get("executedQuantity"))
                .or_else(|| value.get("quoteQuantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(value.get("executedQuantity"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at,
        updated_at: Utc::now(),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    rows(value)
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                &tenant_id,
                &account_id,
                fallback_symbol,
                market_type,
                fill,
            )
        })
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_order(exchange_id, market_type, value))?;
    let filled_at = parse_fill_time(value.get("timestamp")).unwrap_or_else(Utc::now);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol.ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "backpack fill requires canonical_symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")),
        client_order_id: string_or_number(value.get("clientId")),
        fill_id: string_or_number(value.get("tradeId")),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: if value.get("isMaker").and_then(Value::as_bool) == Some(true) {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        },
        price: decimal_as_f64(value.get("price")).unwrap_or(0.0),
        quantity: decimal_as_f64(value.get("quantity")).unwrap_or(0.0),
        quote_quantity: None,
        fee_asset: value
            .get("feeSymbol")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        fee_amount: decimal_as_f64(value.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at,
        received_at: Utc::now(),
    })
}

pub fn order_state_from_cancel_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
    value: Option<&Value>,
) -> ExchangeApiResult<OrderState> {
    if let Some(value) = value {
        if let Some(order) = parse_order(
            exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            value,
        )? {
            return Ok(order);
        }
    }
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
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
    })
}

fn rows(value: &Value) -> Vec<&Value> {
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.get("rows").and_then(Value::as_array))
        .or_else(|| value.as_array())
        .map(|rows| rows.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn symbol_scope_from_order(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(normalize_backpack_symbol)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Backpack order missing symbol", value))?;
    let (base, quote) = split_symbol_assets(&symbol);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

fn market_type_from_order(value: &Value) -> ExchangeApiResult<MarketType> {
    if let Some(market_type) = value.get("marketType").and_then(Value::as_str) {
        parse_market_type(Some(market_type))
    } else if value
        .get("symbol")
        .and_then(Value::as_str)
        .is_some_and(|symbol| symbol.to_ascii_uppercase().ends_with("_PERP"))
    {
        Ok(MarketType::Perpetual)
    } else {
        Ok(MarketType::Spot)
    }
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or("Bid").to_ascii_lowercase().as_str() {
        "ask" | "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("New").to_ascii_lowercase().as_str() {
        "new" => OrderStatus::New,
        "partiallyfilled" | "partial_fill" | "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "triggerfailed" | "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(value: &str, post_only: Option<bool>) -> OrderType {
    if post_only == Some(true) {
        return OrderType::PostOnly;
    }
    match value.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn parse_fill_time(value: Option<&Value>) -> Option<DateTime<Utc>> {
    match value? {
        Value::Number(number) => number
            .as_i64()
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single()),
        Value::String(text) => NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f"))
            .ok()
            .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)),
        _ => None,
    }
}

fn bps_to_rate(value: Option<&str>) -> Option<String> {
    let bps = value?.parse::<f64>().ok()?;
    Some(format!("{:.8}", bps / 10_000.0))
}
