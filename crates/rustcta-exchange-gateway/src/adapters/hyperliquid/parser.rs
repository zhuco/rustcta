use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use rustcta_exchange_api::OrderState;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let universe = value
        .get("universe")
        .and_then(Value::as_array)
        .or_else(|| {
            value
                .get("meta")
                .and_then(|meta| meta.get("universe"))
                .and_then(Value::as_array)
        })
        .ok_or_else(|| parse_error("hyperliquid meta response missing universe"))?;
    let mut rules = Vec::new();
    for market in universe {
        if market.get("isDelisted").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let coin = required_str(market, "name")?;
        if !requested.is_empty()
            && !requested.iter().any(|requested| {
                requested.market_type == MarketType::Perpetual
                    && to_coin(&requested.exchange_symbol.symbol).eq_ignore_ascii_case(coin)
            })
        {
            continue;
        }
        let sz_decimals = market
            .get("szDecimals")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        let canonical_symbol = CanonicalSymbol::new(coin, "USDC").map_err(validation_error)?;
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
                .map_err(validation_error)?,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset: coin.to_ascii_uppercase(),
            quote_asset: "USDC".to_string(),
            price_increment: None,
            quantity_increment: Some(decimal_increment(sz_decimals)),
            min_price: None,
            max_price: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: Some("10".to_string()),
            max_notional: None,
            price_precision: None,
            quantity_precision: Some(sz_decimals),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: true,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let levels = value
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("hyperliquid l2Book response missing levels"))?;
    let bids = parse_levels(levels.first(), "bids")?;
    let asks = parse_levels(levels.get(1), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hyperliquid order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = value
        .get("time")
        .and_then(Value::as_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(snapshot)
}

pub fn parse_balance_and_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<(ExchangeBalance, Vec<ExchangePosition>)> {
    let observed_at = Utc::now();
    let account_value = number_like(
        value
            .pointer("/marginSummary/accountValue")
            .or_else(|| value.pointer("/crossMarginSummary/accountValue")),
    )
    .unwrap_or(0.0);
    let withdrawable = number_like(value.get("withdrawable")).unwrap_or(account_value);
    let usdc = AssetBalance::new(
        "USDC",
        account_value,
        withdrawable,
        account_value - withdrawable,
    )
    .map_err(validation_error)?;
    let balance = ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances: vec![usdc],
        observed_at,
    };

    let positions = value
        .get("assetPositions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| parse_position(exchange_id, &tenant_id, &account_id, item).transpose())
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok((balance, positions))
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .as_array()
        .ok_or_else(|| parse_error("hyperliquid openOrders response is not an array"))?;
    orders
        .iter()
        .map(|order| parse_order(exchange_id, order, OrderStatus::Open))
        .collect()
}

pub fn parse_order_status(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let status = value.get("status").and_then(Value::as_str);
    if status == Some("unknownOid") || value.get("order").is_none() {
        return Ok(None);
    }
    let order = value
        .get("order")
        .ok_or_else(|| parse_error("hyperliquid orderStatus missing order"))?;
    parse_order(exchange_id, order, parse_status(status)).map(Some)
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value
        .as_array()
        .ok_or_else(|| parse_error("hyperliquid fills response is not an array"))?;
    fills
        .iter()
        .map(|fill| parse_fill(exchange_id, &tenant_id, &account_id, fill))
        .collect()
}

pub fn to_coin(symbol: &str) -> String {
    symbol
        .trim()
        .to_ascii_uppercase()
        .replace("/USDC", "")
        .replace("/USDT", "")
        .replace("-USD", "")
        .replace("-PERP", "")
}

pub fn normalize_hyperliquid_coin(symbol: &str) -> ExchangeApiResult<String> {
    let coin = to_coin(symbol);
    if coin.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "hyperliquid symbol must not be empty".to_string(),
        });
    }
    Ok(coin)
}

pub fn symbol_scope(exchange_id: &ExchangeId, symbol: &str) -> ExchangeApiResult<SymbolScope> {
    let coin = normalize_hyperliquid_coin(symbol)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_from_coin(&coin)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
            .map_err(validation_error)?,
    })
}

pub fn canonical_from_coin(coin: &str) -> ExchangeApiResult<CanonicalSymbol> {
    CanonicalSymbol::new(coin, "USDC").map_err(validation_error)
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangePosition>> {
    let position = value.get("position").unwrap_or(value);
    let coin = required_str(position, "coin")?;
    let signed_size = number_like(position.get("szi")).unwrap_or(0.0);
    if signed_size == 0.0 {
        return Ok(None);
    }
    let side = if signed_size > 0.0 {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    let canonical_symbol = canonical_from_coin(coin)?;
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
                .map_err(validation_error)?,
        ),
        side,
        quantity: signed_size.abs(),
        entry_price: number_like(position.get("entryPx")),
        mark_price: number_like(position.get("markPx")),
        liquidation_price: number_like(position.get("liquidationPx")),
        unrealized_pnl: number_like(position.get("unrealizedPnl")),
        leverage: position
            .get("leverage")
            .and_then(|leverage| number_like(leverage.get("value"))),
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(Some(position))
}

fn parse_order(
    exchange_id: &ExchangeId,
    value: &Value,
    default_status: OrderStatus,
) -> ExchangeApiResult<OrderState> {
    let coin = required_str(value, "coin")?;
    let side = match value.get("side").and_then(Value::as_str).unwrap_or("B") {
        "B" | "buy" | "BUY" => OrderSide::Buy,
        _ => OrderSide::Sell,
    };
    let quantity = value_as_string(value.get("origSz").or_else(|| value.get("sz")))
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = value_as_string(value.get("filledSz")).unwrap_or_else(|| {
        let original = number_like(value.get("origSz").or_else(|| value.get("sz"))).unwrap_or(0.0);
        let remaining = number_like(value.get("sz")).unwrap_or(original);
        (original - remaining).max(0.0).to_string()
    });
    let tif = value.get("tif").and_then(Value::as_str).and_then(parse_tif);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_from_coin(coin)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
            .map_err(validation_error)?,
        client_order_id: value_as_string(value.get("cloid")),
        exchange_order_id: value_as_string(value.get("oid")),
        side,
        position_side: Some(PositionSide::Net),
        order_type: parse_order_type(value),
        time_in_force: tif,
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(|status| parse_status(Some(status)))
            .unwrap_or(default_status),
        quantity,
        price: value_as_string(value.get("limitPx").or_else(|| value.get("px"))),
        filled_quantity,
        average_fill_price: value_as_string(value.get("avgPx")),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: tif == Some(TimeInForce::GTX),
        created_at: timestamp_millis(value.get("timestamp")),
        updated_at: timestamp_millis(value.get("statusTimestamp"))
            .or_else(|| timestamp_millis(value.get("timestamp")))
            .unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let coin = required_str(value, "coin")?;
    let side = match value.get("side").and_then(Value::as_str).unwrap_or("B") {
        "B" => OrderSide::Buy,
        _ => OrderSide::Sell,
    };
    let price = number_like(value.get("px")).ok_or_else(|| parse_error("fill missing px"))?;
    let quantity = number_like(value.get("sz")).ok_or_else(|| parse_error("fill missing sz"))?;
    let filled_at = timestamp_millis(value.get("time")).unwrap_or_else(Utc::now);
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: canonical_from_coin(coin)?,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, coin)
                .map_err(validation_error)?,
        ),
        order_id: value_as_string(value.get("oid")),
        client_order_id: value_as_string(value.get("cloid")),
        fill_id: value_as_string(value.get("tid")).or_else(|| value_as_string(value.get("hash"))),
        side,
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: if value.get("crossed").and_then(Value::as_bool) == Some(true) {
            LiquidityRole::Taker
        } else {
            LiquidityRole::Maker
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value_as_string(value.get("feeToken")).or_else(|| Some("USDC".to_string())),
        fee_amount: number_like(value.get("fee")),
        fee_rate: None,
        realized_pnl: number_like(value.get("closedPnl")),
        filled_at,
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn parse_levels(value: Option<&Value>, field: &str) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("hyperliquid l2Book missing {field}")))?
        .iter()
        .map(|level| {
            let price = number_like(level.get("px").or_else(|| level.get(0)))
                .ok_or_else(|| parse_error(format!("{field} level missing price")))?;
            let quantity = number_like(level.get("sz").or_else(|| level.get(1)))
                .ok_or_else(|| parse_error(format!("{field} level missing size")))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn parse_order_type(value: &Value) -> OrderType {
    match value
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("Limit")
        .to_ascii_lowercase()
        .as_str()
    {
        "market" | "frontendmarket" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_tif(value: &str) -> Option<TimeInForce> {
    match value.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" | "frontendmarket" => Some(TimeInForce::IOC),
        "alo" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("open").to_ascii_lowercase().as_str() {
        "open" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        status if status.contains("cancel") => OrderStatus::Cancelled,
        status if status.contains("reject") => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn timestamp_millis(value: Option<&Value>) -> Option<chrono::DateTime<Utc>> {
    value
        .and_then(Value::as_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
}

fn decimal_increment(decimals: u32) -> String {
    if decimals == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(decimals.saturating_sub(1) as usize))
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_like(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| parse_error(format!("hyperliquid response missing {field}")))
}

fn parse_error(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::Serialization {
        message: message.into(),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
