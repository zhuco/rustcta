use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, FundingRateSnapshot, OrderState,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangePosition, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    normalize_okx_symbol_for_market, required_str, split_okx_inst_id, string_or_number,
    validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let accounts = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX balance response is not an array",
            value,
        )
    })?;
    let mut balances = Vec::new();
    for account in accounts {
        let details = account
            .get("details")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "OKX balance missing details", account)
            })?;
        let mut assets = Vec::new();
        for detail in details {
            let asset = required_str(exchange_id, detail, "ccy")?.to_ascii_uppercase();
            let available = decimal_as_f64(
                detail
                    .get("availBal")
                    .or_else(|| detail.get("availEq"))
                    .or_else(|| detail.get("cashBal")),
            )
            .unwrap_or(0.0);
            let locked = decimal_as_f64(detail.get("frozenBal")).unwrap_or(0.0);
            let total = decimal_as_f64(detail.get("cashBal")).unwrap_or(available + locked);
            if total > 0.0 || available > 0.0 || locked > 0.0 {
                assets.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
        if !assets.is_empty() {
            balances.push(Balance {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                balances: assets,
                observed_at: Utc::now(),
            });
        }
    }
    Ok(balances)
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let order = value.as_array().and_then(|items| items.first());
    order
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .transpose()
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX open orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fees(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let fees = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX fee response is not an array",
            value,
        )
    })?;
    fees.iter()
        .map(|item| {
            let symbol = if let Some(symbol) = fallback_symbol {
                symbol.clone()
            } else {
                symbol_scope_from_inst_id(exchange_id, required_str(exchange_id, item, "instId")?)?
            };
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: string_or_number(item.get("maker")).unwrap_or_else(|| "0".to_string()),
                taker_rate: string_or_number(item.get("taker")).unwrap_or_else(|| "0".to_string()),
                source: Some("okx.account.trade-fee".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_market_type: MarketType,
    symbols_filter: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let positions = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX positions response is not an array",
            value,
        )
    })?;
    let wanted = symbols_filter
        .iter()
        .map(|symbol| normalize_okx_symbol_for_market(&symbol.symbol, symbol.market_type))
        .collect::<ExchangeApiResult<std::collections::HashSet<_>>>()?;
    let mut parsed = Vec::new();
    for item in positions {
        let inst_id = required_str(exchange_id, item, "instId")?;
        let market_type = item
            .get("instType")
            .and_then(Value::as_str)
            .map(okx_market_type_from_inst_type)
            .transpose()?
            .unwrap_or_else(|| {
                let inferred = okx_market_type_from_inst_id(inst_id);
                if inferred == MarketType::Spot {
                    fallback_market_type
                } else {
                    inferred
                }
            });
        if market_type != fallback_market_type {
            continue;
        }
        let normalized = normalize_okx_symbol_for_market(inst_id, market_type)?;
        if !wanted.is_empty() && !wanted.contains(&normalized) {
            continue;
        }
        let signed_quantity = decimal_as_f64(
            item.get("pos")
                .or_else(|| item.get("position"))
                .or_else(|| item.get("qty")),
        )
        .unwrap_or(0.0);
        let quantity = signed_quantity.abs();
        if quantity == 0.0 {
            continue;
        }
        let (base, quote) =
            split_okx_inst_id(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("OKX position symbol missing base/quote: {normalized}"),
            })?;
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        let position = ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
                    .map_err(validation_error)?,
            ),
            side: okx_position_side(item, signed_quantity),
            quantity,
            entry_price: decimal_as_f64(item.get("avgPx").or_else(|| item.get("avgCost"))),
            mark_price: decimal_as_f64(item.get("markPx")),
            liquidation_price: decimal_as_f64(item.get("liqPx")),
            unrealized_pnl: decimal_as_f64(item.get("upl").or_else(|| item.get("unrealizedPnl"))),
            leverage: decimal_as_f64(item.get("lever")),
            observed_at: Utc::now(),
        };
        position.validate().map_err(validation_error)?;
        parsed.push(position);
    }
    Ok(parsed)
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
            "OKX fills response is not an array",
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

pub fn parse_funding_rate(
    exchange_id: &ExchangeId,
    fallback_symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FundingRateSnapshot> {
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| parse_error(exchange_id.clone(), "OKX funding response is empty", value))?;
    let funding_rate = string_or_number(row.get("fundingRate")).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OKX funding response missing fundingRate",
            row,
        )
    })?;
    Ok(FundingRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: fallback_symbol,
        funding_rate,
        predicted_funding_rate: string_or_number(row.get("nextFundingRate")),
        funding_time: row
            .get("fundingTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        next_funding_time: row
            .get("nextFundingTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        mark_price: None,
        index_price: None,
        open_interest: None,
        turnover_24h: None,
        volume_24h: None,
        source: Some("okx.public.funding-rate".to_string()),
        updated_at: Utc::now(),
    })
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let inst_id = required_str(exchange_id, value, "instId")?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_inst_id(exchange_id, inst_id))?;
    let order_type = parse_order_type(
        value
            .get("ordType")
            .and_then(Value::as_str)
            .unwrap_or("limit"),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clOrdId")).filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("ordId")).filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: parse_position_side(value.get("posSide").and_then(Value::as_str)),
        order_type,
        time_in_force: parse_time_in_force(value.get("ordType").and_then(Value::as_str)),
        status: value
            .get("state")
            .and_then(Value::as_str)
            .map(map_okx_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("sz")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("px")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(
            value
                .get("accFillSz")
                .or_else(|| value.get("fillSz"))
                .or_else(|| value.get("filledSz")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPx"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: value
            .get("reduceOnly")
            .and_then(value_as_bool)
            .unwrap_or(false),
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: value
            .get("cTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("uTime")
            .or_else(|| value.get("fillTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
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
    let inst_id = required_str(exchange_id, value, "instId")?;
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_inst_id(exchange_id, inst_id))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "OKX fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("fillPx").or_else(|| value.get("px"))).unwrap_or(0.0);
    let quantity = decimal_as_f64(value.get("fillSz").or_else(|| value.get("sz"))).unwrap_or(0.0);
    let quote_quantity = (price > 0.0 && quantity > 0.0).then_some(price * quantity);
    let fee_amount =
        decimal_as_f64(value.get("fee").or_else(|| value.get("fillFee"))).map(f64::abs);
    let filled_at = value
        .get("fillTime")
        .or_else(|| value.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now);
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("ordId")).filter(|value| !value.is_empty()),
        client_order_id: string_or_number(value.get("clOrdId")).filter(|value| !value.is_empty()),
        fill_id: string_or_number(value.get("tradeId").or_else(|| value.get("fillIdx")))
            .filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: parse_position_side(value.get("posSide").and_then(Value::as_str))
            .unwrap_or(PositionSide::None),
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity_role(
            value.get("execType").or_else(|| value.get("liquidity")),
        ),
        price,
        quantity,
        quote_quantity,
        fee_asset: string_or_number(value.get("feeCcy").or_else(|| value.get("fillFeeCcy")))
            .filter(|value| !value.is_empty()),
        fee_amount,
        fee_rate: None,
        realized_pnl: None,
        filled_at,
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn symbol_scope_from_inst_id(
    exchange_id: &ExchangeId,
    inst_id: &str,
) -> ExchangeApiResult<SymbolScope> {
    let market_type = okx_market_type_from_inst_id(inst_id);
    let normalized = normalize_okx_symbol_for_market(inst_id, market_type)?;
    let (base, quote) =
        split_okx_inst_id(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("OKX symbol missing base/quote: {normalized}"),
        })?;
    let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
            .map_err(validation_error)?,
    })
}

fn okx_market_type_from_inst_id(inst_id: &str) -> MarketType {
    let upper = inst_id.to_ascii_uppercase();
    if upper.ends_with("-SWAP") {
        return MarketType::Perpetual;
    }
    let parts = upper.split('-').collect::<Vec<_>>();
    if parts.len() >= 5 && parts.last().is_some_and(|kind| matches!(*kind, "C" | "P")) {
        return MarketType::Option;
    }
    if parts.len() >= 3
        && parts
            .get(2)
            .is_some_and(|expiry| expiry.chars().all(|ch| ch.is_ascii_digit()))
    {
        return MarketType::Futures;
    }
    MarketType::Spot
}

fn okx_market_type_from_inst_type(inst_type: &str) -> ExchangeApiResult<MarketType> {
    match inst_type.to_ascii_uppercase().as_str() {
        "SPOT" | "MARGIN" => Ok(MarketType::Spot),
        "SWAP" => Ok(MarketType::Perpetual),
        "FUTURES" => Ok(MarketType::Futures),
        "OPTION" => Ok(MarketType::Option),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported OKX instType {inst_type}"),
        }),
    }
}

fn okx_position_side(value: &Value, signed_quantity: f64) -> PositionSide {
    match value
        .get("posSide")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        "net" if signed_quantity < 0.0 => PositionSide::Short,
        "net" if signed_quantity > 0.0 => PositionSide::Long,
        "net" => PositionSide::Net,
        _ if signed_quantity < 0.0 => PositionSide::Short,
        _ if signed_quantity > 0.0 => PositionSide::Long,
        _ => PositionSide::Net,
    }
}

fn map_okx_order_status(state: &str) -> OrderStatus {
    match state.trim().to_ascii_lowercase().as_str() {
        "live" => OrderStatus::New,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "mmp_canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_side(exchange_id: &ExchangeId, side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported OKX side",
            &Value::String(side.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str) -> OrderType {
    match order_type.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(order_type: Option<&str>) -> Option<TimeInForce> {
    match order_type.unwrap_or_default().to_ascii_lowercase().as_str() {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "post_only" => Some(TimeInForce::GTX),
        "limit" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn parse_liquidity_role(value: Option<&Value>) -> LiquidityRole {
    match value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "M" | "MAKER" => LiquidityRole::Maker,
        "T" | "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) if !text.is_empty() => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().ok().is_some_and(|value| value == 0.0)
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

fn parse_position_side(pos_side: Option<&str>) -> Option<PositionSide> {
    match pos_side.unwrap_or("net").to_ascii_lowercase().as_str() {
        "long" => Some(PositionSide::Long),
        "short" => Some(PositionSide::Short),
        "net" => Some(PositionSide::Net),
        "" => Some(PositionSide::None),
        _ => Some(PositionSide::None),
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
