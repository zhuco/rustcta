use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState,
    SymbolRules, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangePosition,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "WOOFi Pro public info missing rows",
                value,
            )
        })?;
    rows.iter()
        .filter(|row| {
            row.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.starts_with("PERP_"))
        })
        .map(|row| parse_market(exchange_id, row))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("symbol")
                .and_then(Value::as_str)
                .and_then(|symbol| canonical_from_orderly_symbol(symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "WOOFi Pro order book requires canonical_symbol or response symbol"
                .to_string(),
        })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("seq")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("holding"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "WOOFi Pro holding response missing holding rows",
                value,
            )
        })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let balances = rows
        .iter()
        .filter_map(|row| {
            let token = row
                .get("token")
                .and_then(Value::as_str)?
                .to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&token) {
                return None;
            }
            Some((token, row))
        })
        .map(|(token, row)| {
            let total = value_as_f64(row.get("holding").unwrap_or(&Value::Null)).unwrap_or(0.0);
            let locked = value_as_f64(row.get("frozen").unwrap_or(&Value::Null)).unwrap_or(0.0);
            let available = (total - locked).max(0.0);
            AssetBalance::new(token, total, available, locked).map_err(validation_error)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
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

pub fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangePosition>> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let symbol = required_str(exchange_id, data, "symbol")?;
    let quantity = decimal_path(data, &["position_qty"])
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    if quantity == 0.0 {
        return Ok(None);
    }
    let side = if quantity > 0.0 {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
        .map_err(validation_error)?;
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: canonical_from_orderly_symbol(symbol)?,
        exchange_symbol: Some(exchange_symbol),
        side,
        quantity: quantity.abs(),
        entry_price: decimal_path(data, &["average_open_price"])
            .and_then(|value| value.parse::<f64>().ok()),
        mark_price: decimal_path(data, &["mark_price"]).and_then(|value| value.parse::<f64>().ok()),
        liquidation_price: decimal_path(data, &["est_liq_price"])
            .and_then(|value| value.parse::<f64>().ok()),
        unrealized_pnl: decimal_path(data, &["unsettled_pnl"])
            .and_then(|value| value.parse::<f64>().ok()),
        leverage: decimal_path(data, &["leverage"]).and_then(|value| value.parse::<f64>().ok()),
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(Some(position))
}

pub fn parse_order_ack(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    fallback_side: OrderSide,
    fallback_order_type: OrderType,
    fallback_quantity: String,
    fallback_price: Option<String>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    parse_order_state(
        exchange_id,
        Some(fallback_symbol),
        Some(fallback_side),
        Some(fallback_order_type),
        Some(fallback_quantity),
        fallback_price,
        data,
    )
}

pub fn parse_single_order(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    parse_order_state(
        exchange_id,
        Some(fallback_symbol),
        None,
        None,
        None,
        None,
        data,
    )
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "WOOFi Pro orders response missing rows",
                value,
            )
        })?;
    rows.iter()
        .map(|row| parse_order_state(exchange_id, fallback_symbol, None, None, None, None, row))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "WOOFi Pro trades response missing rows",
                value,
            )
        })?;
    rows.iter()
        .map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                row,
            )
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| vec![data.clone()]);
    let maker = rows
        .first()
        .and_then(|row| decimal_path(row, &["maker_fee_rate"]))
        .unwrap_or_else(|| "0".to_string());
    let taker = rows
        .first()
        .and_then(|row| decimal_path(row, &["taker_fee_rate"]))
        .unwrap_or_else(|| "0".to_string());
    symbols
        .iter()
        .map(|symbol| {
            if &symbol.exchange != exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "WOOFi Pro fee symbol exchange mismatch: {}",
                        symbol.exchange
                    ),
                });
            }
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol.clone(),
                maker_rate: maker.clone(),
                taker_rate: taker.clone(),
                source: Some("orderly_broker_user_info".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let canonical_symbol = canonical_from_orderly_symbol(symbol_text)?;
    let base_asset = value
        .get("base")
        .or_else(|| value.get("base_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote")
        .or_else(|| value.get("quote_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            symbol_text,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["quote_tick"])
        .or_else(|| decimal_path(value, &["price_tick"]))
        .or_else(|| decimal_path(value, &["tick_size"]));
    let quantity_increment = decimal_path(value, &["base_tick"])
        .or_else(|| decimal_path(value, &["size_tick"]))
        .or_else(|| decimal_path(value, &["lot_size"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(value, &["base_min"]),
        max_quantity: decimal_path(value, &["base_max"]),
        min_notional: decimal_path(value, &["min_notional"])
            .or_else(|| decimal_path(value, &["quote_min"])),
        max_notional: decimal_path(value, &["quote_max"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    fallback_side: Option<OrderSide>,
    fallback_order_type: Option<OrderType>,
    fallback_quantity: Option<String>,
    fallback_price: Option<String>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "WOOFi Pro order missing symbol", value))?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, &symbol_text)
            .map_err(validation_error)?;
    let canonical_symbol = canonical_from_orderly_symbol(&symbol_text).ok();
    let order_type = value
        .get("order_type")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .and_then(parse_order_type)
        .or(fallback_order_type)
        .unwrap_or(OrderType::Limit);
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .and_then(parse_side)
        .or(fallback_side)
        .unwrap_or(OrderSide::Buy);
    let price = decimal_path(value, &["order_price"])
        .or_else(|| decimal_path(value, &["price"]))
        .or(fallback_price);
    let quantity = decimal_path(value, &["order_quantity"])
        .or_else(|| decimal_path(value, &["quantity"]))
        .or(fallback_quantity)
        .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(value.get("client_order_id")),
        exchange_order_id: value_as_string(value.get("order_id")),
        side,
        position_side: Some(PositionSide::Net),
        order_type,
        time_in_force: order_type_to_time_in_force(order_type),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .and_then(parse_order_status)
            .unwrap_or(OrderStatus::Open),
        quantity,
        price,
        filled_quantity: decimal_path(value, &["total_executed_quantity"])
            .or_else(|| decimal_path(value, &["executed_quantity"]))
            .or_else(|| decimal_path(value, &["executed"]))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: decimal_path(value, &["average_executed_price"]),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: order_type == OrderType::PostOnly,
        created_at: value
            .get("created_time")
            .and_then(value_as_i64)
            .and_then(timestamp_millis),
        updated_at: value
            .get("updated_time")
            .or_else(|| value.get("timestamp"))
            .and_then(value_as_i64)
            .and_then(timestamp_millis)
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
    let symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "WOOFi Pro fill missing symbol", value))?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, &symbol_text)
            .map_err(validation_error)?;
    let price = value
        .get("executed_price")
        .and_then(value_as_f64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "WOOFi Pro fill missing price", value))?;
    let quantity = value
        .get("executed_quantity")
        .and_then(value_as_f64)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "WOOFi Pro fill missing quantity",
                value,
            )
        })?;
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .and_then(parse_side)
        .unwrap_or(OrderSide::Buy);
    let fee_amount = value.get("fee").and_then(value_as_f64);
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: canonical_from_orderly_symbol(&symbol_text)?,
        exchange_symbol: Some(exchange_symbol),
        order_id: value_as_string(value.get("order_id")),
        client_order_id: value_as_string(value.get("client_order_id")),
        fill_id: value_as_string(value.get("id"))
            .or_else(|| value_as_string(value.get("match_id"))),
        side,
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("is_maker").and_then(value_as_i64) {
            Some(1) => LiquidityRole::Maker,
            Some(0) => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value
            .get("fee_asset")
            .and_then(Value::as_str)
            .map(str::to_string),
        fee_amount,
        fee_rate: value.get("order_enum_fee_rate").and_then(value_as_f64),
        realized_pnl: value.get("realized_pnl").and_then(value_as_f64),
        filled_at: value
            .get("executed_timestamp")
            .and_then(value_as_i64)
            .and_then(timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn canonical_from_orderly_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let parts = symbol.trim().split('_').collect::<Vec<_>>();
    if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("PERP") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer WOOFi Pro canonical symbol from {symbol}"),
        });
    }
    CanonicalSymbol::new(parts[1], parts[2]).map_err(validation_error)
}

fn parse_side(value: &str) -> Option<OrderSide> {
    match value.trim().to_ascii_uppercase().as_str() {
        "BUY" => Some(OrderSide::Buy),
        "SELL" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_order_type(value: &str) -> Option<OrderType> {
    match value.trim().to_ascii_uppercase().as_str() {
        "MARKET" => Some(OrderType::Market),
        "LIMIT" => Some(OrderType::Limit),
        "POST_ONLY" => Some(OrderType::PostOnly),
        "IOC" => Some(OrderType::IOC),
        "FOK" => Some(OrderType::FOK),
        _ => None,
    }
}

fn parse_order_status(value: &str) -> Option<OrderStatus> {
    match value.trim().to_ascii_uppercase().as_str() {
        "NEW" => Some(OrderStatus::New),
        "OPEN" => Some(OrderStatus::Open),
        "PARTIAL_FILLED" | "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "CANCEL_SENT" | "CANCEL_ALL_SENT" => Some(OrderStatus::PendingCancel),
        "CANCELLED" | "CANCELED" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        "EXPIRED" => Some(OrderStatus::Expired),
        _ => Some(OrderStatus::Unknown),
    }
}

fn order_type_to_time_in_force(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::FOK => Some(TimeInForce::FOK),
        OrderType::PostOnly => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "WOOFi Pro order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(values) => {
                let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(object) => {
                let price = object
                    .get("price")
                    .or_else(|| object.get("p"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level price", level)
                    })?;
                let quantity = object
                    .get("quantity")
                    .or_else(|| object.get("size"))
                    .or_else(|| object.get("q"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "WOOFi Pro order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "WOOFi Pro response success=false",
        value,
    ))
}

fn decimal_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn timestamp_millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

fn precision_hint(value: &str) -> Option<u32> {
    if let Ok(number) = value.parse::<u32>() {
        return Some(number);
    }
    value
        .trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("WOOFi Pro public info missing {field}"),
            value,
        )
    })
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
