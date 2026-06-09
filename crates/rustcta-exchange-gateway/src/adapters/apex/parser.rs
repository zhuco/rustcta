use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let contracts = value
        .get("data")
        .and_then(|data| data.get("contractConfig"))
        .and_then(|config| config.get("perpetualContract"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApeX symbols response missing data.contractConfig.perpetualContract",
                value,
            )
        })?;
    let mut rules = Vec::new();
    for contract in contracts {
        let public_symbol = required_str(exchange_id, contract, "symbol")?;
        let trade_symbol = contract
            .get("crossSymbolName")
            .and_then(Value::as_str)
            .map(apex_trade_symbol)
            .unwrap_or_else(|| apex_trade_symbol(public_symbol));
        if !requested.is_empty()
            && !requested.iter().any(|scope| {
                scope.market_type == MarketType::Perpetual
                    && (scope
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(public_symbol)
                        || scope
                            .exchange_symbol
                            .symbol
                            .eq_ignore_ascii_case(&trade_symbol))
            })
        {
            continue;
        }
        rules.push(parse_symbol_rule(
            exchange_id,
            contract,
            public_symbol,
            &trade_symbol,
        )?);
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("b"), "bids")?;
    let asks = parse_levels(exchange_id, data.get("a"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "apex order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = data.get("u").and_then(number_u64);
    Ok(snapshot)
}

pub fn apex_public_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '/', '_'], "")
        .to_ascii_uppercase()
}

pub fn apex_trade_symbol(symbol: &str) -> String {
    let upper = apex_public_symbol(symbol);
    if upper.ends_with("USDT") && upper.len() > 4 {
        format!("{}-USDT", &upper[..upper.len() - 4])
    } else {
        upper
    }
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_state(exchange_id, symbol, first_order_like(exchange_id, value)?)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    order_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApeX open orders missing orders",
                value,
            )
        })?
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol, order))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    fill_items(value)
        .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX fills missing trades", value))?
        .iter()
        .map(|fill| {
            let price = number_from_value(first_field(
                fill,
                &["price", "fillPrice", "avgPrice", "averagePrice"],
            ))
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX fill missing price", fill))?;
            let quantity = number_from_value(first_field(
                fill,
                &["size", "qty", "quantity", "fillSize", "filledSize"],
            ))
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX fill missing quantity", fill))?;
            let quote_quantity = number_from_value(first_field(
                fill,
                &["quoteQuantity", "quoteQty", "turnover", "notional"],
            ))
            .or(Some(price * quantity));
            let canonical_symbol = symbol.canonical_symbol.clone().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "ApeX fill parser requires canonical_symbol".to_string(),
                }
            })?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol,
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_or_number(first_field(fill, &["orderId", "order_id", "id"])),
                client_order_id: string_or_number(first_field(
                    fill,
                    &["clientOrderId", "client_order_id", "clientId"],
                )),
                fill_id: string_or_number(first_field(
                    fill,
                    &["tradeId", "fillId", "id", "transactionId"],
                )),
                side: parse_side(
                    first_field(fill, &["side", "orderSide"])
                        .and_then(Value::as_str)
                        .unwrap_or("BUY"),
                ),
                position_side: parse_position_side(
                    first_field(fill, &["positionSide"]).and_then(Value::as_str),
                ),
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    first_field(fill, &["liquidity", "liquidityRole", "maker"])
                        .and_then(Value::as_str),
                    first_field(fill, &["isMaker", "maker"]).and_then(Value::as_bool),
                ),
                price,
                quantity,
                quote_quantity,
                fee_asset: string_or_number(first_field(fill, &["feeToken", "feeAsset", "token"])),
                fee_amount: number_from_value(first_field(
                    fill,
                    &["fee", "feeAmount", "commission"],
                )),
                fee_rate: number_from_value(first_field(fill, &["feeRate"])),
                realized_pnl: number_from_value(first_field(fill, &["realizedPnl", "realizedPnl"])),
                filled_at: timestamp_any(fill, &["createdAt", "createdTime", "time", "timestamp"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let now = Utc::now();
    let canonical_symbol = symbol.canonical_symbol.clone();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: string_or_number(first_field(
            value,
            &["clientOrderId", "client_order_id", "clientId"],
        )),
        exchange_order_id: string_or_number(first_field(value, &["id", "orderId", "order_id"])),
        side: parse_side(
            first_field(value, &["side", "orderSide"])
                .and_then(Value::as_str)
                .unwrap_or("BUY"),
        ),
        position_side: Some(parse_position_side(
            first_field(value, &["positionSide"]).and_then(Value::as_str),
        )),
        order_type: parse_order_type(
            first_field(value, &["type", "orderType"])
                .and_then(Value::as_str)
                .unwrap_or("LIMIT"),
        ),
        time_in_force: parse_time_in_force(
            first_field(value, &["timeInForce"]).and_then(Value::as_str),
        ),
        status: parse_order_status(
            first_field(value, &["status", "state"]).and_then(Value::as_str),
        ),
        quantity: string_or_number(first_field(value, &["size", "qty", "quantity", "amount"]))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(first_field(value, &["price", "limitPrice"])),
        filled_quantity: string_or_number(first_field(
            value,
            &["filledSize", "filledQuantity", "cumExecQty", "filled"],
        ))
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(first_field(
            value,
            &["avgPrice", "averagePrice", "averageFillPrice"],
        )),
        reduce_only: first_field(value, &["reduceOnly", "reduce_only"])
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: first_field(value, &["postOnly", "post_only"])
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: timestamp_any(value, &["createdAt", "createdTime", "time", "timestamp"]),
        updated_at: timestamp_any(value, &["updatedAt", "updatedTime", "createdAt", "time"])
            .unwrap_or(now),
    })
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
    public_symbol: &str,
    trade_symbol: &str,
) -> ExchangeApiResult<SymbolRules> {
    let (base_asset, quote_asset) = split_symbol_assets(trade_symbol);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            public_symbol.to_string(),
        )
        .map_err(validation_error)?,
    };
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("stepSize")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minOrderSize")),
        max_quantity: string_or_number(value.get("maxOrderSize")),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(value.get("tickSize"))
            .and_then(|value| precision(&value)),
        quantity_precision: string_or_number(value.get("stepSize"))
            .and_then(|value| precision(&value)),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("ApeX order book missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApeX order book level is not an array",
                row,
            )
        })?;
        let price = number_from_value(values.first())
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX level missing price", row))?;
        let quantity = number_from_value(values.get(1))
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX level missing quantity", row))?;
        levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    let clean = symbol.replace('-', "");
    for quote in ["USDT", "USDC", "USD"] {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                clean[..clean.len() - quote.len()].to_string(),
                quote.to_string(),
            );
        }
    }
    (clean, "USDT".to_string())
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("ApeX payload missing {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn number_u64(value: &Value) -> Option<u64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64(),
        _ => None,
    }
}

fn number_i64(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

fn first_field<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a Value> {
    fields.iter().find_map(|field| value.get(*field))
}

fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn first_order_like<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<&'a Value> {
    let data = data_payload(value);
    if data.is_object() {
        if let Some(order) = data.get("order").filter(|order| order.is_object()) {
            return Ok(order);
        }
        return Ok(data);
    }
    if let Some(first) = data.as_array().and_then(|items| items.first()) {
        return Ok(first);
    }
    Err(parse_error(
        exchange_id.clone(),
        "ApeX order response missing order object",
        value,
    ))
}

fn order_items(value: &Value) -> Option<&Vec<Value>> {
    let data = data_payload(value);
    data.as_array()
        .or_else(|| data.get("orders").and_then(Value::as_array))
        .or_else(|| data.get("list").and_then(Value::as_array))
        .or_else(|| data.get("items").and_then(Value::as_array))
}

fn fill_items(value: &Value) -> Option<&Vec<Value>> {
    let data = data_payload(value);
    data.as_array()
        .or_else(|| data.get("fills").and_then(Value::as_array))
        .or_else(|| data.get("trades").and_then(Value::as_array))
        .or_else(|| data.get("list").and_then(Value::as_array))
        .or_else(|| data.get("items").and_then(Value::as_array))
}

fn parse_side(value: &str) -> OrderSide {
    match value.to_ascii_lowercase().as_str() {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("long") => PositionSide::Long,
        Some("short") => PositionSide::Short,
        Some("net") | Some("both") => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "market" => OrderType::Market,
        "postonly" => OrderType::PostOnly,
        "ioc" | "immediateorcancel" => OrderType::IOC,
        "fok" | "fillorkill" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "gtc" | "goodtilcancel" | "goodtillcancel" => Some(TimeInForce::GTC),
        "ioc" | "immediateorcancel" => Some(TimeInForce::IOC),
        "fok" | "fillorkill" => Some(TimeInForce::FOK),
        "gtx" | "postonly" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value
        .unwrap_or_default()
        .to_ascii_lowercase()
        .replace(['_', '-'], "")
        .as_str()
    {
        "new" | "pending" => OrderStatus::New,
        "open" | "active" => OrderStatus::Open,
        "partiallyfilled" | "partialfilled" | "partfilled" => OrderStatus::PartiallyFilled,
        "filled" | "closed" => OrderStatus::Filled,
        "pendingcancel" | "canceling" | "cancelling" => OrderStatus::PendingCancel,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" | "failed" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: Option<&str>, is_maker: Option<bool>) -> LiquidityRole {
    if let Some(is_maker) = is_maker {
        return if is_maker {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        };
    }
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("maker") | Some("m") => LiquidityRole::Maker,
        Some("taker") | Some("t") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn timestamp_any(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(timestamp_value)
}

fn timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
            return Some(parsed.with_timezone(&Utc));
        }
        if let Ok(number) = text.parse::<i64>() {
            return timestamp_from_number(number);
        }
    }
    number_i64(value).and_then(timestamp_from_number)
}

fn timestamp_from_number(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000_000 {
        DateTime::<Utc>::from_timestamp_micros(value)
    } else if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        DateTime::<Utc>::from_timestamp(value, 0)
    }
}

fn precision(value: &str) -> Option<u32> {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
