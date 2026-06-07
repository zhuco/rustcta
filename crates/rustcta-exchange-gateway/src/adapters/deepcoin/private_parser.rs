use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_payload, decimal_as_f64, parse_error, parse_position_side, parse_side,
    split_deepcoin_symbol, string_or_number, validation_error, value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let account_type = match market_type {
        MarketType::Spot => "spot",
        MarketType::Perpetual => "swapU",
        _ => "unsupported",
    };
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    let payload = data_payload(value);
    let accounts = payload
        .get("accounts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(payload));
    for account in accounts {
        if account
            .get("accountType")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.eq_ignore_ascii_case(account_type))
        {
            continue;
        }
        let details = account
            .get("details")
            .or_else(|| account.get("balances"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_else(|| std::slice::from_ref(account));
        for detail in details {
            let Some(asset) = detail
                .get("ccy")
                .or_else(|| detail.get("coin"))
                .and_then(Value::as_str)
                .map(str::to_ascii_uppercase)
            else {
                continue;
            };
            if !requested.is_empty() && !requested.contains(&asset) {
                continue;
            }
            let available = decimal_as_f64(
                detail
                    .get("availBal")
                    .or_else(|| detail.get("available"))
                    .or_else(|| detail.get("free")),
            )
            .unwrap_or(0.0);
            let locked = decimal_as_f64(
                detail
                    .get("frozenBal")
                    .or_else(|| detail.get("locked"))
                    .or_else(|| detail.get("frozen")),
            )
            .unwrap_or(0.0);
            let total = decimal_as_f64(
                detail
                    .get("bal")
                    .or_else(|| detail.get("balance"))
                    .or_else(|| detail.get("total")),
            )
            .unwrap_or(available + locked);
            if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = data_payload(value);
    let positions = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut output = Vec::new();
    for position in positions {
        let Some(symbol) = value_as_string(position.get("instId")) else {
            continue;
        };
        let (base, quote) = split_deepcoin_symbol(&symbol, MarketType::Perpetual)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let quantity = decimal_as_f64(position.get("pos")).unwrap_or(0.0).abs();
        if quantity == 0.0 {
            continue;
        }
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        output.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(position.get("posSide").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_as_f64(position.get("avgPx")),
            mark_price: decimal_as_f64(position.get("lastPx").or_else(|| position.get("markPx"))),
            liquidation_price: decimal_as_f64(position.get("liqPx")),
            unrealized_pnl: decimal_as_f64(position.get("unrealizedProfit")),
            leverage: decimal_as_f64(position.get("lever")),
            observed_at: Utc::now(),
        });
    }
    Ok(output)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = data_payload(value);
    let items = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut fees = Vec::new();
    for item in items {
        let symbol = symbols
            .first()
            .cloned()
            .ok_or_else(|| parse_error(exchange_id.clone(), "fee response missing symbol", item))?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: string_or_number(
                item.get("makerU")
                    .filter(|_| market_type == MarketType::Perpetual)
                    .or_else(|| item.get("maker")),
            )
            .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(
                item.get("takerU")
                    .filter(|_| market_type == MarketType::Perpetual)
                    .or_else(|| item.get("taker")),
            )
            .unwrap_or_else(|| "0".to_string()),
            source: Some("deepcoin.account.trade-fee".to_string()),
            updated_at: item
                .get("ts")
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
        });
    }
    Ok(fees)
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    let order = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    if order.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        market_type,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = data_payload(value);
    let orders = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, market_type, order))
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_from_payload(exchange_id, fallback_symbol, market_type, value)?;
    let order_type = parse_order_type(value.get("ordType").and_then(Value::as_str));
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("clOrdId")),
        exchange_order_id: value_as_string(value.get("ordId")),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(parse_position_side(
            value.get("posSide").and_then(Value::as_str),
        ))
        .filter(|side| *side != PositionSide::None),
        order_type,
        time_in_force: parse_time_in_force(order_type),
        status: parse_order_status(value.get("state").and_then(Value::as_str)),
        quantity: string_or_number(value.get("sz")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("px")).filter(|value| value != "0"),
        filled_quantity: string_or_number(value.get("accFillSz").or_else(|| value.get("fillSz")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPx").or_else(|| value.get("fillPx")))
            .filter(|value| !value.is_empty() && value != "0"),
        reduce_only: value
            .get("reduceOnly")
            .and_then(|value| {
                value
                    .as_bool()
                    .or_else(|| value.as_str().map(|s| s == "true"))
            })
            .unwrap_or(false),
        post_only: order_type == OrderType::PostOnly,
        created_at: value
            .get("cTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: value
            .get("uTime")
            .or_else(|| value.get("fillTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or(now),
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
    let data = data_payload(value);
    let fills = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    fills
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                market_type,
                fill,
            )
        })
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_from_payload(exchange_id, fallback_symbol, market_type, value)?;
    let price = decimal_as_f64(value.get("fillPx").or_else(|| value.get("px"))).unwrap_or(0.0);
    let quantity = decimal_as_f64(value.get("fillSz").or_else(|| value.get("sz"))).unwrap_or(0.0);
    let mut fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "fill response missing canonical symbol",
                value,
            )
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("ordId")),
        client_order_id: value_as_string(value.get("clOrdId")),
        fill_id: value_as_string(value.get("billId").or_else(|| value.get("tradeId"))),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: parse_position_side(value.get("posSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("execType").and_then(Value::as_str) {
            Some("M") | Some("maker") => LiquidityRole::Maker,
            Some("T") | Some("taker") => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value_as_string(value.get("feeCcy")),
        fee_amount: decimal_as_f64(value.get("fee")).map(f64::abs),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("pnl")),
        filled_at: value
            .get("ts")
            .or_else(|| value.get("fillTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    if fill.position_side == PositionSide::None && fill.market_type == MarketType::Spot {
        fill.position_side = PositionSide::None;
    }
    Ok(fill)
}

fn symbol_from_payload(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    let symbol = value_as_string(value.get("instId")).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order/fill response missing instId",
            value,
        )
    })?;
    let market_type = match value.get("instType").and_then(Value::as_str) {
        Some(inst_type) if inst_type.eq_ignore_ascii_case("SPOT") => MarketType::Spot,
        Some(inst_type) if inst_type.eq_ignore_ascii_case("SWAP") => MarketType::Perpetual,
        _ => market_type,
    };
    let (base, quote) = split_deepcoin_symbol(&symbol, market_type)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "live" | "new" => OrderStatus::Open,
        "partially_filled" | "partial-filled" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" | "postonly" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::PostOnly => Some(TimeInForce::GTX),
        OrderType::Limit => Some(TimeInForce::GTC),
        _ => None,
    }
}

pub fn order_state_from_place_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_as_string(data.get("clOrdId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_string(data.get("ordId")),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::Open,
        quantity: request
            .quote_quantity
            .clone()
            .unwrap_or_else(|| request.quantity.clone()),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

pub fn order_state_from_cancel_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_as_string(data.get("clOrdId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_string(data.get("ordId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
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
