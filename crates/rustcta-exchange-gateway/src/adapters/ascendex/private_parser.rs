use chrono::Utc;
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
    data_payload, decimal_as_f64, first_timestamp_millis, normalize_symbol, parse_error,
    parse_position_side, parse_side, split_ascendex_symbol, split_futures_symbol, string_or_number,
    validation_error, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = data_payload(value);
    let rows = data
        .get("data")
        .or_else(|| data.get("balances"))
        .or_else(|| data.get("collaterals"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("asset")
            .or_else(|| row.get("coin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = decimal_as_f64(
            row.get("totalBalance")
                .or_else(|| row.get("balance"))
                .or_else(|| row.get("equity")),
        )
        .unwrap_or(0.0);
        let available = decimal_as_f64(
            row.get("availableBalance")
                .or_else(|| row.get("availableForTransfer"))
                .or_else(|| row.get("free")),
        )
        .unwrap_or(total);
        let locked = (total - available).max(0.0);
        if total > 0.0 || available > 0.0 || !requested.is_empty() {
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = data_payload(value);
    let contracts = data
        .get("contracts")
        .or_else(|| data.get("data"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut positions = Vec::new();
    for item in contracts {
        let symbol = item
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let quantity = decimal_as_f64(
            item.get("position")
                .or_else(|| item.get("positionQty"))
                .or_else(|| item.get("qty")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        let (base, quote) = split_futures_symbol(&symbol)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(item.get("side").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_as_f64(
                item.get("avgOpenPrice")
                    .or_else(|| item.get("entryPrice"))
                    .or_else(|| item.get("referenceCost")),
            ),
            mark_price: decimal_as_f64(item.get("markPrice")),
            liquidation_price: decimal_as_f64(item.get("liquidationPrice")),
            unrealized_pnl: decimal_as_f64(item.get("unrealizedPnl")),
            leverage: decimal_as_f64(item.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = data_payload(value);
    let rows = data
        .get("productFee")
        .or_else(|| data.get("data").and_then(|data| data.get("productFee")))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut fees = Vec::new();
    for row in rows {
        let symbol = row
            .get("symbol")
            .and_then(Value::as_str)
            .and_then(|raw| {
                requested_symbols.iter().find(|symbol| {
                    normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
                        .is_ok_and(|normalized| normalized.eq_ignore_ascii_case(raw))
                })
            })
            .cloned()
            .or_else(|| requested_symbols.first().cloned())
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "fee response cannot map symbol", row)
            })?;
        let fee = row.get("fee").unwrap_or(row);
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: string_or_number(fee.get("maker")).unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(fee.get("taker")).unwrap_or_else(|| "0".to_string()),
            source: Some("ascendex.fee".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    let order = data
        .get("info")
        .or_else(|| data.get("data"))
        .unwrap_or(data)
        .as_array()
        .and_then(|rows| rows.first())
        .unwrap_or_else(|| data.get("info").unwrap_or(data));
    if order.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        symbol_hint,
        market_type,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = data_payload(value);
    let rows = data
        .get("data")
        .or_else(|| data.get("orders"))
        .or_else(|| data.get("info"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "orders response missing list", value))?;
    rows.iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, market_type, row))
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let raw_symbol = value_as_string(value.get("symbol").or_else(|| value.get("s")))
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "order response missing symbol", value))?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or_else(|| {
            ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol.clone())
                .expect("validated exchange symbol")
        });
    let canonical_symbol = symbol_hint
        .and_then(|symbol| symbol.canonical_symbol.clone())
        .or_else(|| canonical_from_symbol(&raw_symbol, market_type));
    let order_type_text = value
        .get("orderType")
        .or_else(|| value.get("ot"))
        .and_then(Value::as_str)
        .unwrap_or("Limit");
    let (order_type, tif, post_only) = parse_order_type(
        order_type_text,
        value.get("timeInForce").and_then(Value::as_str),
        value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || value
                .get("execInst")
                .and_then(Value::as_str)
                .is_some_and(|exec| exec.to_ascii_lowercase().contains("post")),
    );
    let side = value
        .get("side")
        .or_else(|| value.get("sd"))
        .and_then(Value::as_str)
        .map(|side| parse_side(exchange_id, side))
        .transpose()?
        .unwrap_or(OrderSide::Buy);
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(value.get("id").or_else(|| value.get("clientOrderId"))),
        exchange_order_id: value_as_string(value.get("orderId")),
        side,
        position_side: Some(if market_type == MarketType::Spot {
            PositionSide::None
        } else {
            parse_position_side(value.get("positionSide").and_then(Value::as_str))
        }),
        order_type,
        time_in_force: tif,
        status: value
            .get("status")
            .or_else(|| value.get("st"))
            .and_then(Value::as_str)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("orderQty")
                .or_else(|| value.get("q"))
                .or_else(|| value.get("qty"))
                .or_else(|| value.get("quantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            value
                .get("price")
                .or_else(|| value.get("p"))
                .or_else(|| value.get("orderPrice"))
                .or_else(|| value.get("lastPx")),
        )
        .filter(|price| price != "0"),
        filled_quantity: string_or_number(
            value
                .get("cumFilledQty")
                .or_else(|| value.get("cfq"))
                .or_else(|| value.get("fillQty"))
                .or_else(|| value.get("filledQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("avgPx")
                .or_else(|| value.get("ap"))
                .or_else(|| value.get("avgFilledPx"))
                .or_else(|| value.get("avgFillPrice")),
        )
        .filter(|price| price != "0"),
        reduce_only: value
            .get("execInst")
            .and_then(Value::as_str)
            .is_some_and(|exec| exec.to_ascii_lowercase().contains("reduce")),
        post_only,
        created_at: first_timestamp_millis(value, &["time", "timestamp", "transactTime"]),
        updated_at: first_timestamp_millis(value, &["lastExecTime", "updateTime", "time"])
            .unwrap_or(now),
    })
}

pub fn parse_fills_from_orders(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let orders = parse_orders(exchange_id, Some(symbol_hint), market_type, value)?;
    let mut fills = Vec::new();
    for order in orders {
        let quantity = order.filled_quantity.parse::<f64>().unwrap_or(0.0);
        if quantity <= 0.0 {
            continue;
        }
        let price = order
            .average_fill_price
            .as_deref()
            .or(order.price.as_deref())
            .and_then(|price| price.parse::<f64>().ok())
            .unwrap_or(0.0);
        if price <= 0.0 {
            continue;
        }
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol: order.canonical_symbol.clone().ok_or_else(|| {
                parse_error(exchange_id.clone(), "fill missing canonical symbol", value)
            })?,
            exchange_symbol: Some(order.exchange_symbol.clone()),
            order_id: order.exchange_order_id.clone(),
            client_order_id: order.client_order_id.clone(),
            fill_id: order.exchange_order_id.clone(),
            side: order.side,
            position_side: order.position_side.unwrap_or(PositionSide::None),
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Unknown,
            price,
            quantity,
            quote_quantity: Some(price * quantity),
            fee_asset: None,
            fee_amount: None,
            fee_rate: None,
            realized_pnl: None,
            filled_at: order.updated_at,
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

fn canonical_from_symbol(symbol: &str, market_type: MarketType) -> Option<CanonicalSymbol> {
    let parts = if market_type == MarketType::Spot {
        split_ascendex_symbol(symbol)
    } else {
        split_futures_symbol(symbol)
    }?;
    CanonicalSymbol::new(parts.0, parts.1).ok()
}

fn parse_order_type(
    raw: &str,
    tif: Option<&str>,
    post_only: bool,
) -> (OrderType, Option<TimeInForce>, bool) {
    let tif = match tif.map(str::to_ascii_uppercase).as_deref() {
        Some("IOC") => Some(TimeInForce::IOC),
        Some("FOK") => Some(TimeInForce::FOK),
        Some("GTC") => Some(TimeInForce::GTC),
        _ => None,
    };
    let order_type = if post_only {
        OrderType::PostOnly
    } else {
        match raw.to_ascii_lowercase().as_str() {
            "market" => OrderType::Market,
            "stopmarket" | "stop_market" => OrderType::StopMarket,
            "stoplimit" | "stop_limit" => OrderType::StopLimit,
            _ if tif == Some(TimeInForce::IOC) => OrderType::IOC,
            _ if tif == Some(TimeInForce::FOK) => OrderType::FOK,
            _ => OrderType::Limit,
        }
    };
    let tif = if post_only {
        Some(TimeInForce::GTX)
    } else {
        tif.or(Some(TimeInForce::GTC))
    };
    (order_type, tif, post_only)
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "new" | "open" => OrderStatus::Open,
        "partiallyfilled" | "partially_filled" | "partialfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "pendingnew" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}

pub fn ack_order_from_request(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value)
        .get("info")
        .unwrap_or_else(|| data_payload(value));
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| value_as_string(data.get("id"))),
        exchange_order_id: value_as_string(data.get("orderId")),
        side: request.side,
        position_side: Some(request.position_side.unwrap_or(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::Open,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

pub fn cancelled_order_from_request(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
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
