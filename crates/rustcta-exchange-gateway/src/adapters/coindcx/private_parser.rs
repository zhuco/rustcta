use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangePosition, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, parse_error, rows, string_or_number, symbol_scope,
    timestamp_from_millis_or_seconds, validation_error,
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
    let balances = rows(value)
        .into_iter()
        .filter_map(|item| {
            let asset = item
                .get("currency")
                .or_else(|| item.get("asset"))
                .or_else(|| item.get("coin"))
                .and_then(Value::as_str)?
                .to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&asset) {
                return None;
            }
            let available = decimal_as_f64(
                item.get("balance")
                    .or_else(|| item.get("available_balance"))
                    .or_else(|| item.get("available")),
            )
            .unwrap_or(0.0);
            let locked = decimal_as_f64(
                item.get("locked_balance")
                    .or_else(|| item.get("locked"))
                    .or_else(|| item.get("reserved")),
            )
            .unwrap_or(0.0);
            let total = decimal_as_f64(item.get("total_balance").or_else(|| item.get("total")))
                .unwrap_or(available + locked);
            Some(AssetBalance::new(asset, total, available, locked).map_err(validation_error))
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
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
    requested_symbols: &[rustcta_types::ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let requested = requested_symbols
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for item in rows(value) {
        let symbol = item
            .get("pair")
            .or_else(|| item.get("symbol"))
            .or_else(|| item.get("instrument_name"))
            .or_else(|| item.get("instrument"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "CoinDCX position missing symbol", item)
            })?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|requested| requested.eq_ignore_ascii_case(symbol))
        {
            continue;
        }
        let quantity = decimal_as_f64(
            item.get("active_pos")
                .or_else(|| item.get("quantity"))
                .or_else(|| item.get("size"))
                .or_else(|| item.get("position_size")),
        )
        .unwrap_or(0.0);
        if quantity == 0.0 {
            continue;
        }
        let scope = symbol_scope(exchange_id, MarketType::Perpetual, symbol)?;
        let canonical_symbol =
            scope
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "CoinDCX position requires canonical symbol".to_string(),
                })?;
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(scope.exchange_symbol),
            side: parse_position_side(item, quantity),
            quantity: quantity.abs(),
            entry_price: decimal_as_f64(
                item.get("avg_price")
                    .or_else(|| item.get("entry_price"))
                    .or_else(|| item.get("average_price")),
            ),
            mark_price: decimal_as_f64(item.get("mark_price")),
            liquidation_price: decimal_as_f64(item.get("liquidation_price")),
            unrealized_pnl: decimal_as_f64(
                item.get("unrealized_profit")
                    .or_else(|| item.get("unrealized_pnl")),
            ),
            leverage: decimal_as_f64(item.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_fee_snapshots(symbols: &[SymbolScope]) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: "0".to_string(),
            taker_rate: "0".to_string(),
            source: Some("coindcx.fee_endpoint_unconfirmed_zero_placeholder".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
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
        .get("orders")
        .and_then(Value::as_array)
        .and_then(|orders| orders.first())
        .or_else(|| value.get("order"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    if item.is_null() {
        return Ok(None);
    }
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
        .map(|item| parse_order_state(exchange_id, fallback_symbol, market_type, item))
        .collect()
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
        .map(|item| {
            parse_fill(
                exchange_id,
                &tenant_id,
                &account_id,
                fallback_symbol,
                market_type,
                item,
            )
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
    let side = parse_side(
        value
            .get("side")
            .or_else(|| value.get("order_side"))
            .and_then(Value::as_str),
    );
    let status = parse_order_status(
        value
            .get("status")
            .or_else(|| value.get("order_status"))
            .and_then(Value::as_str),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(
            value
                .get("client_order_id")
                .or_else(|| value.get("client_order_id_string")),
        ),
        exchange_order_id: string_or_number(value.get("id").or_else(|| value.get("order_id"))),
        side,
        position_side: (market_type == MarketType::Perpetual).then_some(PositionSide::Net),
        order_type: parse_order_type(
            value
                .get("order_type")
                .or_else(|| value.get("type"))
                .and_then(Value::as_str),
        ),
        time_in_force: parse_time_in_force(
            value
                .get("time_in_force")
                .or_else(|| value.get("tif"))
                .and_then(Value::as_str),
        ),
        status,
        quantity: string_or_number(
            value
                .get("total_quantity")
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("order_quantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            value
                .get("price_per_unit")
                .or_else(|| value.get("price"))
                .or_else(|| value.get("avg_price")),
        ),
        filled_quantity: string_or_number(
            value
                .get("filled_quantity")
                .or_else(|| value.get("filled_qty"))
                .or_else(|| value.get("executed_quantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("avg_price")
                .or_else(|| value.get("average_price")),
        ),
        reduce_only: false,
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value
            .get("created_at")
            .or_else(|| value.get("createdAt"))
            .or_else(|| value.get("timestamp"))
            .and_then(super::parser::number_i64)
            .and_then(timestamp_from_millis_or_seconds),
        updated_at: Utc::now(),
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
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "CoinDCX fill requires canonical_symbol".to_string(),
            })?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("order_id").or_else(|| value.get("id"))),
        client_order_id: string_or_number(value.get("client_order_id")),
        fill_id: string_or_number(value.get("trade_id").or_else(|| value.get("id"))),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: if value.get("maker").and_then(Value::as_bool) == Some(true) {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        },
        price: decimal_as_f64(
            value
                .get("price")
                .or_else(|| value.get("price_per_unit"))
                .or_else(|| value.get("avg_price")),
        )
        .unwrap_or(0.0),
        quantity: decimal_as_f64(
            value
                .get("quantity")
                .or_else(|| value.get("filled_quantity"))
                .or_else(|| value.get("total_quantity")),
        )
        .unwrap_or(0.0),
        quote_quantity: decimal_as_f64(value.get("amount")),
        fee_asset: value
            .get("fee_currency")
            .and_then(Value::as_str)
            .map(str::to_string),
        fee_amount: decimal_as_f64(value.get("fee_amount").or_else(|| value.get("fee"))),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("realized_pnl")),
        filled_at: value
            .get("timestamp")
            .or_else(|| value.get("created_at"))
            .and_then(super::parser::number_i64)
            .and_then(timestamp_from_millis_or_seconds)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope_from_order(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = value
        .get("market")
        .or_else(|| value.get("pair"))
        .or_else(|| value.get("symbol"))
        .or_else(|| value.get("instrument_name"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "CoinDCX order missing market", value))?;
    symbol_scope(exchange_id, market_type, symbol)
}

fn parse_position_side(value: &Value, quantity: f64) -> PositionSide {
    match value
        .get("side")
        .or_else(|| value.get("position_side"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "short" | "sell" => PositionSide::Short,
        "long" | "buy" => PositionSide::Long,
        _ if quantity < 0.0 => PositionSide::Short,
        _ if quantity > 0.0 => PositionSide::Long,
        _ => PositionSide::Net,
    }
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or("buy").to_ascii_lowercase().as_str() {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "active" | "init" | "created" => OrderStatus::Open,
        "partially_filled" | "partially filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "closed" | "complete" | "completed" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancelled_by_user" => OrderStatus::Cancelled,
        "rejected" | "failed" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit_order").to_ascii_lowercase().as_str() {
        "market" | "market_order" => OrderType::Market,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        "post_only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" | "POST_ONLY" => Some(TimeInForce::GTX),
        _ => None,
    }
}
