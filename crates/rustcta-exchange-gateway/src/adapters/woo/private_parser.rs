use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeId, ExchangePosition, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, parse_error, parse_woo_market_symbol, required_str, string_or_number,
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
    let holdings = value
        .get("data")
        .unwrap_or(value)
        .get("holding")
        .or_else(|| value.get("holding"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "woo balances response is not an array",
                value,
            )
        })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for holding in holdings {
        let asset = required_str(exchange_id, holding, "token")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(holding.get("availableBalance"))
            .or_else(|| decimal_as_f64(holding.get("available")))
            .unwrap_or(0.0);
        let locked = decimal_as_f64(holding.get("frozen")).unwrap_or(0.0);
        let total = decimal_as_f64(holding.get("holding"))
            .or_else(|| decimal_as_f64(holding.get("total")))
            .unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![Balance {
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
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = value.get("data").unwrap_or(value);
    let positions = data
        .get("positions")
        .or_else(|| data.get("rows"))
        .unwrap_or(data)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "woo positions response is not an array",
                value,
            )
        })?;
    let requested = requested_symbols
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    positions
        .iter()
        .filter(|position| {
            requested.is_empty()
                || position
                    .get("symbol")
                    .and_then(Value::as_str)
                    .is_some_and(|symbol| requested.contains(&symbol.to_ascii_uppercase()))
        })
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
    let data = value.get("data").unwrap_or(value);
    if data.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(exchange_id, fallback_symbol, data)?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    let orders = data.get("rows").unwrap_or(data).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "woo orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fees(
    _exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = value.get("data").unwrap_or(value);
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate: string_or_number(data.get("makerFee")).unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(data.get("takerFee")).unwrap_or_else(|| "0".to_string()),
        source: Some("woo.trade.tradingFee".to_string()),
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
    let data = value.get("data").unwrap_or(value);
    let fills = data.get("rows").unwrap_or(data).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "woo fills response is not an array",
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "woo order missing symbol", value))?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_exchange_symbol(exchange_id, &exchange_symbol_text))?;
    let order_type = parse_order_type(value.get("type").and_then(Value::as_str).unwrap_or("LIMIT"));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOrderId")).filter(|value| value != "0"),
        exchange_order_id: string_or_number(value.get("orderId")),
        side: parse_side(
            exchange_id,
            required_str(exchange_id, value, "side").unwrap_or("BUY"),
        )?,
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str)),
        order_type,
        time_in_force: time_in_force_from_order_type(order_type),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_woo_order_status)
            .unwrap_or(OrderStatus::New),
        quantity: string_or_number(value.get("quantity").or_else(|| value.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(value.get("executed")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("averageExecutedPrice"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: value
            .get("createdTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("updatedTime")
            .or_else(|| value.get("createdTime"))
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
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let symbol = symbol_scope_from_exchange_symbol(exchange_id, &exchange_symbol_text)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo position requires canonical_symbol".to_string(),
            })?;
    let quantity = decimal_as_f64(value.get("holding")).unwrap_or(0.0).abs();
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: parse_position_side(value.get("positionSide").and_then(Value::as_str))
            .unwrap_or(PositionSide::Net),
        quantity,
        entry_price: decimal_as_f64(
            value
                .get("averageOpenPrice")
                .or_else(|| value.get("settlePrice")),
        ),
        mark_price: decimal_as_f64(value.get("markPrice")),
        liquidation_price: decimal_as_f64(value.get("estLiqPrice")).filter(|price| *price > 0.0),
        unrealized_pnl: decimal_as_f64(value.get("pnl24H")),
        leverage: decimal_as_f64(value.get("leverage")),
        observed_at: value
            .get("timestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
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
    let exchange_symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "woo fill missing symbol", value))?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_exchange_symbol(exchange_id, &exchange_symbol_text))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo fill requires canonical_symbol".to_string(),
            })?;
    let price =
        decimal_as_f64(value.get("executedPrice").or_else(|| value.get("price"))).unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("executedQuantity")
            .or_else(|| value.get("quantity")),
    )
    .unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")),
        client_order_id: string_or_number(value.get("clientOrderId")).filter(|value| value != "0"),
        fill_id: string_or_number(value.get("id").or_else(|| value.get("tradeId"))),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str))
            .unwrap_or(PositionSide::None),
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("isMaker").and_then(Value::as_bool) {
            Some(true) => LiquidityRole::Maker,
            Some(false) => LiquidityRole::Taker,
            None => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: string_or_number(value.get("feeAsset")),
        fee_amount: decimal_as_f64(value.get("fee")).map(f64::abs),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("realizedPnl")),
        filled_at: value
            .get("executedTimestamp")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let (market_type, base, quote) = parse_woo_market_symbol(symbol)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

fn parse_side(exchange_id: &ExchangeId, side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported woo order side",
            &Value::String(side.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str) -> OrderType {
    match order_type.to_ascii_uppercase().as_str() {
        "MARKET" | "ASK" | "BID" => OrderType::Market,
        "IOC" => OrderType::IOC,
        "FOK" => OrderType::FOK,
        "POST_ONLY" | "RPI" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn time_in_force_from_order_type(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::FOK => Some(TimeInForce::FOK),
        OrderType::PostOnly => Some(TimeInForce::GTX),
        OrderType::Limit => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn parse_position_side(side: Option<&str>) -> Option<PositionSide> {
    match side?.to_ascii_uppercase().as_str() {
        "LONG" => Some(PositionSide::Long),
        "SHORT" => Some(PositionSide::Short),
        "BOTH" | "NET" => Some(PositionSide::Net),
        _ => Some(PositionSide::None),
    }
}

fn map_woo_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIAL_FILLED" | "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|value| value == 0.0)
}
