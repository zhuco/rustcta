use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiResult, Fill, OrderState, Position, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeId, ExchangeSymbol, FillStatus, LiquidityRole, MarketType, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    canonical_from_alpaca_symbol, decode_error, empty_order_state, f64_from_value, optional_f64,
    optional_string, parse_time, schema_version, string_field, validation_error,
};

pub fn parse_account_balance(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Balance> {
    let cash = optional_f64(value, "cash").unwrap_or(0.0);
    let available = optional_f64(value, "non_marginable_buying_power")
        .or_else(|| optional_f64(value, "buying_power"))
        .unwrap_or(cash);
    let locked = (cash - available).max(0.0);
    let mut balances = vec![
        AssetBalance::new("USD", cash.max(available), available, locked)
            .map_err(validation_error)?,
    ];
    if !assets.is_empty() {
        balances.retain(|balance| {
            assets
                .iter()
                .any(|asset| asset.eq_ignore_ascii_case(&balance.asset))
        });
    }
    Ok(Balance {
        schema_version: schema_version(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    })
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<Position>> {
    let positions = value.as_array().ok_or_else(|| {
        decode_error(
            exchange_id,
            "alpaca positions response must be an array",
            value,
        )
    })?;
    let requested_symbols = requested
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase().replace('-', "/"))
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    for position in positions {
        let asset_class = position
            .get("asset_class")
            .and_then(Value::as_str)
            .unwrap_or("crypto");
        if !asset_class.eq_ignore_ascii_case("crypto") {
            continue;
        }
        let raw_symbol = string_field(exchange_id, position, "symbol")?;
        let canonical = canonical_from_alpaca_symbol(raw_symbol)?;
        let normalized = canonical.to_string();
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| symbol == &normalized || symbol == &normalized.replace('/', ""))
        {
            continue;
        }
        let quantity = optional_f64(position, "qty").unwrap_or(0.0).abs();
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized.clone())
                .map_err(validation_error)?;
        let parsed = Position {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: canonical,
            exchange_symbol: Some(exchange_symbol),
            side: PositionSide::Net,
            quantity,
            entry_price: optional_f64(position, "avg_entry_price"),
            mark_price: optional_f64(position, "current_price"),
            liquidation_price: None,
            unrealized_pnl: optional_f64(position, "unrealized_pl"),
            leverage: None,
            observed_at: Utc::now(),
        };
        parsed.validate().map_err(validation_error)?;
        output.push(parsed);
    }
    Ok(output)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    requested_symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let raw_symbol = optional_string(value, "symbol")
        .or_else(|| requested_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| decode_error(exchange_id, "alpaca order missing symbol", value))?;
    let canonical = canonical_from_alpaca_symbol(&raw_symbol)?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, canonical.to_string())
            .map_err(validation_error)?;
    let order_type = parse_order_type(
        optional_string(value, "type")
            .or_else(|| optional_string(value, "order_type"))
            .as_deref()
            .unwrap_or("limit"),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
        client_order_id: optional_string(value, "client_order_id"),
        exchange_order_id: optional_string(value, "id")
            .or_else(|| optional_string(value, "order_id")),
        side: parse_side(optional_string(value, "side").as_deref().unwrap_or("buy")),
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: optional_string(value, "time_in_force")
            .as_deref()
            .and_then(parse_tif),
        status: parse_status(
            optional_string(value, "status")
                .as_deref()
                .unwrap_or("unknown"),
        ),
        quantity: optional_string(value, "qty")
            .or_else(|| optional_string(value, "notional"))
            .unwrap_or_else(|| "0".to_string()),
        price: optional_string(value, "limit_price")
            .or_else(|| optional_string(value, "stop_price")),
        filled_quantity: optional_string(value, "filled_qty").unwrap_or_else(|| "0".to_string()),
        average_fill_price: optional_string(value, "filled_avg_price"),
        reduce_only: false,
        post_only: false,
        created_at: optional_string(value, "created_at")
            .or_else(|| optional_string(value, "submitted_at"))
            .as_deref()
            .and_then(parse_time),
        updated_at: optional_string(value, "updated_at")
            .as_deref()
            .and_then(parse_time)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    requested_symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        decode_error(
            exchange_id,
            "alpaca orders response must be an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, requested_symbol, order))
        .collect()
}

pub fn parse_cancel_all_count(value: &Value) -> u32 {
    value
        .as_array()
        .map(|items| items.len() as u32)
        .unwrap_or_default()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let activities = value.as_array().ok_or_else(|| {
        decode_error(
            exchange_id,
            "alpaca account activities response must be an array",
            value,
        )
    })?;
    let mut fills = Vec::new();
    for activity in activities {
        let activity_type = optional_string(activity, "activity_type").unwrap_or_default();
        if !activity_type.eq_ignore_ascii_case("FILL") {
            continue;
        }
        let symbol = string_field(exchange_id, activity, "symbol")?;
        let canonical = canonical_from_alpaca_symbol(symbol)?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, canonical.to_string())
                .map_err(validation_error)?;
        let price = activity
            .get("price")
            .and_then(f64_from_value)
            .unwrap_or(0.0);
        let quantity = activity.get("qty").and_then(f64_from_value).unwrap_or(0.0);
        let filled_at = optional_string(activity, "transaction_time")
            .or_else(|| optional_string(activity, "date"))
            .as_deref()
            .and_then(parse_time)
            .unwrap_or_else(Utc::now);
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: canonical,
            exchange_symbol: Some(exchange_symbol),
            order_id: optional_string(activity, "order_id"),
            client_order_id: optional_string(activity, "client_order_id"),
            fill_id: optional_string(activity, "id"),
            side: parse_side(
                optional_string(activity, "side")
                    .as_deref()
                    .unwrap_or("buy"),
            ),
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Unknown,
            price,
            quantity,
            quote_quantity: Some(price * quantity),
            fee_asset: None,
            fee_amount: None,
            fee_rate: None,
            realized_pnl: None,
            filled_at,
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

pub fn cancelled_order(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
) -> OrderState {
    empty_order_state(
        exchange_id,
        &request.symbol,
        request.exchange_order_id.clone(),
        request.client_order_id.clone(),
        OrderStatus::Cancelled,
    )
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "stop_limit" | "stop-limit" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

pub fn parse_side(value: &str) -> OrderSide {
    match value.to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_tif(value: &str) -> Option<TimeInForce> {
    match value.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn parse_status(value: &str) -> OrderStatus {
    match value.to_ascii_lowercase().as_str() {
        "new" | "accepted" | "pending_new" => OrderStatus::New,
        "open" => OrderStatus::Open,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "pending_cancel" => OrderStatus::PendingCancel,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}
