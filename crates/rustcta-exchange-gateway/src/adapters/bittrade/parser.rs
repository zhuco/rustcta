use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn normalize_bittrade_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = value
        .trim()
        .replace(['/', '-', '_', ' '], "")
        .to_ascii_lowercase();
    if symbol.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bittrade symbol must not be empty".to_string(),
        });
    }
    Ok(symbol)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| parse_error(exchange_id, "symbols data array", value))?;
    rows.iter()
        .map(|row| {
            let symbol = normalize_bittrade_symbol(required_str(exchange_id, row, "symbol")?)?;
            let base_asset = required_str(exchange_id, row, "base-currency")?.to_ascii_uppercase();
            let quote_asset =
                required_str(exchange_id, row, "quote-currency")?.to_ascii_uppercase();
            let canonical_symbol =
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &symbol)
                    .map_err(validation_error)?;
            let api_trading = row
                .get("api-trading")
                .and_then(Value::as_str)
                .unwrap_or("enabled")
                .eq_ignore_ascii_case("enabled");
            let online = row
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("online")
                .eq_ignore_ascii_case("online");
            let tradable = api_trading && online;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical_symbol),
                    exchange_symbol,
                },
                base_asset,
                quote_asset,
                price_increment: decimal_step(row.get("price-precision")),
                quantity_increment: decimal_step(row.get("amount-precision")),
                min_price: None,
                max_price: None,
                min_quantity: string_or_number(row.get("min-order-amt"))
                    .or_else(|| string_or_number(row.get("limit-order-min-order-amt"))),
                max_quantity: string_or_number(row.get("max-order-amt"))
                    .or_else(|| string_or_number(row.get("limit-order-max-order-amt"))),
                min_notional: string_or_number(row.get("min-order-value")),
                max_notional: None,
                price_precision: row.get("price-precision").and_then(value_u32),
                quantity_precision: row.get("amount-precision").and_then(value_u32),
                supports_market_orders: tradable,
                supports_limit_orders: tradable,
                supports_post_only: false,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_response(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("tick")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let mut bids = parse_levels(exchange_id, data.get("bids"))?;
    let mut asks = parse_levels(exchange_id, data.get("asks"))?;
    if let Some(depth) = depth {
        bids.truncate(depth as usize);
        asks.truncate(depth as usize);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bittrade order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("version")
        .or_else(|| data.get("seqId"))
        .and_then(value_u64);
    snapshot.exchange_timestamp = data
        .get("ts")
        .or_else(|| value.get("ts"))
        .and_then(value_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(snapshot)
}

pub fn parse_bittrade_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_object(exchange_id, value)?;
    parse_order_state_row(exchange_id, symbol_hint, order)
}

pub fn parse_bittrade_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    data_rows(exchange_id, value)?
        .iter()
        .map(|row| parse_order_state_row(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_bittrade_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| parse_error(exchange_id, "canonical symbol for fills", value))?;
    data_rows(exchange_id, value)?
        .iter()
        .map(|fill| {
            let price = value_f64_required(exchange_id, fill, &["price"])?;
            let quantity = value_f64_required(exchange_id, fill, &["filled-amount", "amount"])?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_any(fill, &["order-id", "orderId"]),
                client_order_id: string_any(fill, &["client-order-id", "clientOrderId"]),
                fill_id: string_any(fill, &["id", "match-id", "trade-id"]),
                side: parse_order_side(exchange_id, fill)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(fill),
                price,
                quantity,
                quote_quantity: string_any(fill, &["filled-points", "filled-cash-amount"])
                    .and_then(|value| value.parse().ok())
                    .or_else(|| Some(price * quantity)),
                fee_asset: string_any(fill, &["fee-currency", "filled-fees-currency"]),
                fee_amount: string_any(fill, &["filled-fees", "fee"])
                    .and_then(|value| value.parse().ok()),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp_millis_any(fill, &["created-at", "createdAt"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_order_state_row(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_order_id = string_any(order, &["id", "order-id", "orderId"])
        .ok_or_else(|| parse_error(exchange_id, "order id", order))?;
    let symbol_text = string_any(order, &["symbol"])
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id, "order symbol", order))?;
    let scope = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_bittrade_symbol(exchange_id.clone(), &symbol_text))?;
    let quantity = string_any(order, &["amount", "orderSize", "order-size"])
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_any(order, &["field-amount", "filled-amount", "filledAmount"])
        .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: scope.canonical_symbol.clone(),
        exchange_symbol: scope.exchange_symbol,
        client_order_id: string_any(order, &["client-order-id", "clientOrderId"]),
        exchange_order_id: Some(exchange_order_id),
        side: parse_order_side(exchange_id, order)?,
        position_side: None,
        order_type: parse_order_type(order),
        time_in_force: None,
        status: parse_order_status(order, &filled_quantity),
        quantity,
        price: string_any(order, &["price", "orderPrice", "order-price"]),
        filled_quantity,
        average_fill_price: average_fill_price(order),
        reduce_only: false,
        post_only: false,
        created_at: timestamp_millis_any(order, &["created-at", "orderCreateTime"]),
        updated_at: timestamp_millis_any(order, &["finished-at", "canceled-at", "created-at"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "order book levels", &Value::Null))?;
    rows.iter()
        .map(|row| {
            let level = row
                .as_array()
                .ok_or_else(|| parse_error(exchange_id, "price/amount level", row))?;
            let price = level
                .first()
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level price", row))?;
            let quantity = level
                .get(1)
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level quantity", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn first_order_object<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<&'a Value> {
    if let Some(data) = value.get("data") {
        if data.is_object() {
            return Ok(data);
        }
        if let Some(first) = data.as_array().and_then(|rows| rows.first()) {
            return Ok(first);
        }
    }
    if value.is_object() {
        return Ok(value);
    }
    Err(parse_error(exchange_id, "order object", value))
}

fn data_rows<'a>(exchange_id: &ExchangeId, value: &'a Value) -> ExchangeApiResult<&'a Vec<Value>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id, "data array", value))
}

fn symbol_scope_from_bittrade_symbol(
    exchange_id: ExchangeId,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let normalized = normalize_bittrade_symbol(symbol)?;
    let (base, quote) =
        split_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("bittrade cannot infer canonical symbol from {symbol}"),
        })?;
    let canonical_symbol = CanonicalSymbol::new(&base, &quote).map_err(validation_error)?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &normalized)
        .map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol,
    })
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in ["jpy", "usdt", "btc", "eth", "xrp", "ltc", "bch"] {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return Some((base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
        }
    }
    None
}

fn parse_order_side(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<OrderSide> {
    let side = string_any(value, &["side"])
        .or_else(|| {
            string_any(value, &["type"])
                .and_then(|value| value.split('-').next().map(str::to_string))
        })
        .ok_or_else(|| parse_error(exchange_id, "order side", value))?;
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(exchange_id, "order side", value)),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    let order_type = string_any(value, &["type"]).unwrap_or_default();
    let order_type = order_type.to_ascii_lowercase();
    if order_type.contains("market") {
        OrderType::Market
    } else {
        OrderType::Limit
    }
}

fn parse_order_status(value: &Value, filled_quantity: &str) -> OrderStatus {
    match string_any(value, &["state", "orderStatus"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "created" | "pre-submitted" => OrderStatus::New,
        "submitted" => {
            if filled_quantity.parse::<f64>().unwrap_or(0.0) > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        "partial-filled" | "partial_filled" | "partial-filled-canceling" => {
            OrderStatus::PartiallyFilled
        }
        "filled" => OrderStatus::Filled,
        "cancelling" | "canceling" => OrderStatus::PendingCancel,
        "canceled" | "cancelled" | "partial-canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: &Value) -> LiquidityRole {
    match string_any(value, &["role", "liquidity"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_fill_price(value: &Value) -> Option<String> {
    let filled_amount = string_any(value, &["field-amount", "filled-amount"])?
        .parse::<f64>()
        .ok()?;
    let filled_cash = string_any(value, &["field-cash-amount", "filled-cash-amount"])?
        .parse::<f64>()
        .ok()?;
    if filled_amount > 0.0 {
        Some((filled_cash / filled_amount).to_string())
    } else {
        None
    }
}

fn string_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|field| match field {
            Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn value_f64_required(
    exchange_id: &ExchangeId,
    value: &Value,
    keys: &[&str],
) -> ExchangeApiResult<f64> {
    string_any(value, keys)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            parse_error(
                exchange_id,
                keys.first().copied().unwrap_or("number"),
                value,
            )
        })
}

fn timestamp_millis_any(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_i64))
        .filter(|millis| *millis > 0)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    key: &str,
) -> ExchangeApiResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id, key, value))
}

fn decimal_step(value: Option<&Value>) -> Option<String> {
    let digits = value.and_then(value_u32)? as usize;
    if digits == 0 {
        Some("1".to_string())
    } else {
        Some(format!("0.{}1", "0".repeat(digits.saturating_sub(1))))
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("bittrade parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
