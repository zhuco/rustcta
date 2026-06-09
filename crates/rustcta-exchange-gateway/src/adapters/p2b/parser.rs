use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value_array(value, &["data", "pairs", "result"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B pairs response missing array data",
            value,
        )
    })?;
    markets
        .iter()
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol = first_string(
        value,
        &["symbol", "pair", "name", "ticker_id", "market", "id"],
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "P2B pair missing symbol", value))?;
    let normalized_symbol = normalize_p2b_symbol(&symbol)?;
    let (fallback_base, fallback_quote) = split_pair_symbol(&normalized_symbol)
        .ok_or_else(|| parse_error(exchange_id.clone(), "P2B pair missing assets", value))?;
    let base_asset = first_string(
        value,
        &[
            "stock",
            "base",
            "baseCurrency",
            "base_currency",
            "baseAsset",
            "base_asset",
        ],
    )
    .map(|text| text.to_ascii_uppercase())
    .unwrap_or(fallback_base);
    let quote_asset = first_string(
        value,
        &[
            "money",
            "quote",
            "quoteCurrency",
            "quote_currency",
            "quoteAsset",
            "quote_asset",
        ],
    )
    .map(|text| text.to_ascii_uppercase())
    .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let tradable = is_tradable(value);
    let precision = value.get("precision").unwrap_or(value);
    let limits = value.get("limits").unwrap_or(value);
    let price_precision = integer_from_fields(
        precision,
        &[
            "money",
            "pricePrecision",
            "price_precision",
            "priceScale",
            "price_scale",
        ],
    );
    let quantity_precision = integer_from_fields(
        precision,
        &[
            "stock",
            "amountPrecision",
            "amount_precision",
            "quantityPrecision",
            "quantity_precision",
            "volumePrecision",
            "volume_precision",
        ],
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                normalized_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_from_fields(limits, &["tickSize", "tick_size", "priceStep"])
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_from_fields(
            limits,
            &["stepSize", "step_size", "amountStep", "quantityStep"],
        )
        .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: string_from_fields(limits, &["minPrice", "min_price"]),
        max_price: string_from_fields(limits, &["maxPrice", "max_price"]),
        min_quantity: string_from_fields(
            limits,
            &["minAmount", "min_amount", "minQuantity", "min_quantity"],
        ),
        max_quantity: string_from_fields(
            limits,
            &["maxAmount", "max_amount", "maxQuantity", "max_quantity"],
        ),
        min_notional: string_from_fields(
            limits,
            &["minTotal", "min_total", "minNotional", "min_notional"],
        ),
        max_notional: string_from_fields(
            limits,
            &["maxTotal", "max_total", "maxNotional", "max_notional"],
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: false,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let mut bids = parse_levels(
        exchange_id,
        data.get("bids")
            .or_else(|| data.get("buy"))
            .or_else(|| data.get("buyorders")),
    )?;
    let mut asks = parse_levels(
        exchange_id,
        data.get("asks")
            .or_else(|| data.get("sell"))
            .or_else(|| data.get("sellorders")),
    )?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "P2B order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value_as_u64(
        data.get("sequence")
            .or_else(|| data.get("lastUpdateId"))
            .or_else(|| data.get("last")),
    );
    snapshot.exchange_timestamp = first_timestamp(data, &["timestamp", "time", "updatedAt"])
        .or_else(|| first_timestamp(value, &["timestamp", "time", "updatedAt"]));
    Ok(snapshot)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = order_object(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B order response missing object",
            value,
        )
    })?;
    parse_order_object(exchange_id, symbol_hint, order)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B open orders response missing array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_object(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "p2b get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let rows = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B recent fills response missing array",
            value,
        )
    })?;
    rows.iter()
        .map(|fill| {
            let price = number_from_fields(fill, &["price", "rate"])
                .ok_or_else(|| parse_error(exchange_id.clone(), "P2B fill missing price", fill))?;
            let quantity = number_from_fields(fill, &["amount", "quantity", "volume", "deal"])
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "P2B fill missing quantity", fill)
                })?;
            let side = parse_side(
                first_string(fill, &["side", "type", "orderSide", "deal_type"]).as_deref(),
            )?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: first_string(fill, &["orderId", "order_id", "id_order"]),
                client_order_id: first_string(fill, &["clientOrderId", "client_order_id"]),
                fill_id: first_string(fill, &["id", "dealId", "tradeId", "deal_id"]),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(fill),
                price,
                quantity,
                quote_quantity: number_from_fields(fill, &["total", "quoteQuantity", "quote_qty"]),
                fee_asset: first_string(fill, &["feeCurrency", "fee_asset", "commissionAsset"]),
                fee_amount: number_from_fields(fill, &["fee", "commission", "feeAmount"]),
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["time", "timestamp", "createdAt", "created_at"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_p2b_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed
        .replace(['/', '-'], "_")
        .replace("__", "_")
        .to_ascii_uppercase();
    if upper.contains('_') {
        return Ok(upper);
    }
    let (base, quote) =
        split_compact_symbol(&upper).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer P2B quote asset from symbol {symbol}"),
        })?;
    Ok(format!("{base}_{quote}"))
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(50).clamp(1, 100)
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_from_order(exchange_id, symbol_hint, order)?;
    let quantity = string_from_fields(order, &["amount", "quantity", "volume", "stock"])
        .unwrap_or_else(|| "0".to_string());
    let remaining = string_from_fields(order, &["left", "remaining"]);
    let filled_quantity = string_from_fields(
        order,
        &["dealStock", "filled", "filledAmount", "executedQty"],
    )
    .or_else(|| filled_from_remaining(&quantity, remaining.as_deref()))
    .unwrap_or_else(|| "0".to_string());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: first_string(order, &["clientOrderId", "client_order_id"]),
        exchange_order_id: first_string(order, &["id", "orderId", "order_id"]),
        side: parse_side(first_string(order, &["side", "type", "orderSide"]).as_deref())?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(first_string(order, &["orderType", "order_type"]).as_deref()),
        time_in_force: Some(TimeInForce::GTC),
        status: parse_order_status(
            first_string(order, &["status", "state"]).as_deref(),
            &quantity,
            &filled_quantity,
            remaining.as_deref(),
        ),
        quantity,
        price: string_from_fields(order, &["price", "rate"]).filter(|value| value != "0"),
        filled_quantity,
        average_fill_price: string_from_fields(order, &["avgPrice", "averagePrice"]),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(order, &["createdAt", "created_at", "time", "timestamp"]),
        updated_at: first_timestamp(order, &["updatedAt", "updated_at", "lastUpdateTime"])
            .unwrap_or(now),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .or_else(|| level.get("rate"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("left")
                .or_else(|| level.get("remaining"))
                .or_else(|| level.get("amount"))
                .or_else(|| level.get("quantity"))
                .or_else(|| level.get("volume"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn order_object(value: &Value) -> Option<&Value> {
    if value.as_object().is_some()
        && (value.get("id").is_some()
            || value.get("orderId").is_some()
            || value.get("order_id").is_some())
    {
        return Some(value);
    }
    for field in ["result", "data", "order"] {
        let Some(child) = value.get(field) else {
            continue;
        };
        if child.as_object().is_some() {
            if let Some(inner) = order_object(child) {
                return Some(inner);
            }
        }
        if let Some(first) = child.as_array().and_then(|array| array.first()) {
            return Some(first);
        }
    }
    None
}

fn payload_array(value: &Value) -> Option<&Vec<Value>> {
    if let Some(array) = value.as_array() {
        return Some(array);
    }
    for field in [
        "result", "data", "orders", "items", "records", "deals", "trades",
    ] {
        let Some(child) = value.get(field) else {
            continue;
        };
        if let Some(array) = child.as_array() {
            return Some(array);
        }
        if let Some(array) = payload_array(child) {
            return Some(array);
        }
    }
    None
}

fn symbol_from_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let raw_symbol = first_string(order, &["market", "symbol", "pair"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B order missing market symbol",
            order,
        )
    })?;
    let normalized = normalize_p2b_symbol(&raw_symbol)?;
    let (base, quote) = split_pair_symbol(&normalized).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "P2B order market symbol cannot infer assets",
            order,
        )
    })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(&base, &quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "bid" | "b" => Ok(OrderSide::Buy),
        "sell" | "ask" | "s" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "P2B order/fill side is missing or unsupported".to_string(),
        }),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        "post_only" | "postonly" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(
    status: Option<&str>,
    quantity: &str,
    filled_quantity: &str,
    remaining: Option<&str>,
) -> OrderStatus {
    match status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" | "pending" | "created" => OrderStatus::New,
        "open" | "active" | "placed" | "accepted" => OrderStatus::Open,
        "partially_filled" | "partial" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" | "done" | "closed" | "completed" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" => OrderStatus::Cancelled,
        "pending_cancel" | "pendingcancel" => OrderStatus::PendingCancel,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => inferred_order_status(quantity, filled_quantity, remaining),
    }
}

fn inferred_order_status(
    quantity: &str,
    filled_quantity: &str,
    remaining: Option<&str>,
) -> OrderStatus {
    let quantity = quantity.parse::<f64>().unwrap_or(0.0);
    let filled = filled_quantity.parse::<f64>().unwrap_or(0.0);
    let remaining = remaining.and_then(|value| value.parse::<f64>().ok());
    if quantity > 0.0 && filled >= quantity {
        return OrderStatus::Filled;
    }
    if filled > 0.0 {
        return OrderStatus::PartiallyFilled;
    }
    if remaining.is_some_and(|left| left > 0.0) {
        return OrderStatus::Open;
    }
    OrderStatus::Unknown
}

fn filled_from_remaining(quantity: &str, remaining: Option<&str>) -> Option<String> {
    let remaining = remaining?;
    let quantity = quantity.parse::<f64>().ok()?;
    let remaining = remaining.parse::<f64>().ok()?;
    Some(trim_decimal(quantity - remaining))
}

fn trim_decimal(value: f64) -> String {
    let mut text = format!("{value:.12}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn parse_liquidity_role(fill: &Value) -> LiquidityRole {
    if let Some(is_maker) = fill
        .get("isMaker")
        .or_else(|| fill.get("maker"))
        .and_then(Value::as_bool)
    {
        return if is_maker {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        };
    }
    match first_string(fill, &["role", "liquidity"]).as_deref() {
        Some(role) if role.eq_ignore_ascii_case("maker") => LiquidityRole::Maker,
        Some(role) if role.eq_ignore_ascii_case("taker") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn is_tradable(value: &Value) -> bool {
    if let Some(active) = value
        .get("active")
        .or_else(|| value.get("enabled"))
        .and_then(Value::as_bool)
    {
        return active;
    }
    value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "active" | "enabled" | "online" | "trading"
            )
        })
        .unwrap_or(true)
}

fn value_array<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a Vec<Value>> {
    if let Some(array) = value.as_array() {
        return Some(array);
    }
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(Value::as_array)
}

fn split_pair_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    split_compact_symbol(symbol)
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 10] = [
        "USDT", "USDC", "BUSD", "RUB", "USD", "EUR", "BTC", "ETH", "BNB", "TRY",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn first_string(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_string)
            .filter(|text| !text.trim().is_empty())
    })
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_string))
}

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(number_from_value))
}

fn integer_from_fields(value: &Value, fields: &[&str]) -> Option<u32> {
    fields
        .iter()
        .find_map(|field| value_as_u64(value.get(*field)))
        .map(|value| value as u32)
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(timestamp_value_to_datetime)
}

fn timestamp_value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        if number > 1_000_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(number);
        }
        return Utc.timestamp_opt(number, 0).single();
    }
    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn parse_error(exchange: ExchangeId, message: impl Into<String>, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
