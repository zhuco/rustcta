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
    let pairs = value
        .get("pairs")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "YoBit info response missing pairs object",
                value,
            )
        })?;
    pairs
        .iter()
        .map(|(symbol, market)| parse_symbol_rule(exchange_id, symbol, market))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let normalized_symbol = normalize_yobit_symbol(symbol)?;
    let (fallback_base, fallback_quote) = split_pair_symbol(&normalized_symbol)
        .map(|(base, quote)| (base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "YoBit pair missing assets", value))?;
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
            "decimal_places",
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
    let pair_key = normalize_yobit_symbol(&symbol.exchange_symbol.symbol)?;
    let data = value
        .get(&pair_key)
        .or_else(|| value.as_object().and_then(|pairs| pairs.values().next()))
        .unwrap_or(value);
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
                message: "YOBIT order book request requires canonical_symbol".to_string(),
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
    let (order_id, order) = order_entry(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YoBit order response missing order object",
            value,
        )
    })?;
    parse_order_object(exchange_id, symbol_hint, Some(order_id), order)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let entries = object_entries_payload(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YoBit open orders response missing object payload",
            value,
        )
    })?;
    entries
        .iter()
        .map(|(order_id, order)| {
            parse_order_object(exchange_id, symbol_hint, Some(order_id), order)
        })
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
                message: "yobit get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let entries = object_entries_payload(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YoBit recent fills response missing object payload",
            value,
        )
    })?;
    entries
        .iter()
        .map(|(fill_id, fill)| {
            let price = number_from_fields(fill, &["price", "rate"]).ok_or_else(|| {
                parse_error(exchange_id.clone(), "YoBit fill missing price", fill)
            })?;
            let quantity =
                number_from_fields(fill, &["amount", "quantity", "volume"]).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "YoBit fill missing quantity", fill)
                })?;
            let side = parse_side(first_string(fill, &["type", "side"]).as_deref())?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: first_string(fill, &["order_id", "orderId"]),
                client_order_id: None,
                fill_id: Some(fill_id.to_string()),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: number_from_fields(fill, &["total", "quoteQuantity"]),
                fee_asset: first_string(fill, &["fee_currency", "feeCurrency"]),
                fee_amount: number_from_fields(fill, &["fee", "commission"]),
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestamp", "time"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_yobit_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed
        .replace(['/', '-'], "_")
        .replace("__", "_")
        .to_ascii_lowercase();
    if upper.contains('_') {
        return Ok(upper);
    }
    let (base, quote) =
        split_compact_symbol(&upper).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer YoBit quote asset from symbol {symbol}"),
        })?;
    Ok(format!("{base}_{quote}").to_ascii_lowercase())
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(150).clamp(1, 2000)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YOBIT order book missing levels",
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

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order_id_hint: Option<&str>,
    order: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_from_order(exchange_id, symbol_hint, order)?;
    let quantity = string_from_fields(order, &["start_amount", "quantity", "amount"])
        .unwrap_or_else(|| "0".to_string());
    let remaining = string_from_fields(order, &["remains", "left", "remaining"]).or_else(|| {
        order
            .get("start_amount")
            .and_then(|_| string_from_fields(order, &["amount"]))
    });
    let filled_quantity = string_from_fields(order, &["filled", "filled_amount"])
        .or_else(|| filled_from_remaining(&quantity, remaining.as_deref()))
        .unwrap_or_else(|| "0".to_string());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: None,
        exchange_order_id: first_string(order, &["order_id", "orderId", "id"])
            .or_else(|| order_id_hint.map(ToString::to_string)),
        side: parse_side(first_string(order, &["type", "side"]).as_deref())?,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: parse_order_status(status_string(order).as_deref(), &quantity, &filled_quantity),
        quantity,
        price: string_from_fields(order, &["rate", "price"]).filter(|value| value != "0"),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(order, &["timestamp_created", "created_at", "timestamp"]),
        updated_at: first_timestamp(order, &["timestamp_updated", "updated_at", "timestamp"])
            .unwrap_or(now),
    })
}

fn order_entry(value: &Value) -> Option<(&str, &Value)> {
    if value.as_object().is_some()
        && (value.get("order_id").is_some()
            || value.get("id").is_some()
            || value.get("type").is_some() && value.get("rate").is_some())
    {
        return Some((
            value
                .get("order_id")
                .or_else(|| value.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default(),
            value,
        ));
    }
    object_entries_payload(value).and_then(|entries| entries.first().copied())
}

fn object_entries_payload(value: &Value) -> Option<Vec<(&str, &Value)>> {
    for field in ["return", "result", "data", "orders", "trades"] {
        let Some(child) = value.get(field) else {
            continue;
        };
        if let Some(object) = child.as_object() {
            return Some(
                object
                    .iter()
                    .map(|(key, value)| (key.as_str(), value))
                    .collect(),
            );
        }
        if let Some(entries) = object_entries_payload(child) {
            return Some(entries);
        }
    }
    if let Some(object) = value.as_object() {
        if !object.is_empty() && object.values().all(|child| child.as_object().is_some()) {
            return Some(
                object
                    .iter()
                    .map(|(key, value)| (key.as_str(), value))
                    .collect(),
            );
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
    let raw_symbol = first_string(order, &["pair", "market", "symbol"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YoBit order missing pair symbol",
            order,
        )
    })?;
    let normalized = normalize_yobit_symbol(&raw_symbol)?;
    let (base, quote) = split_pair_symbol(&normalized).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "YoBit order pair symbol cannot infer assets",
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
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "YoBit order/fill side is missing or unsupported".to_string(),
        }),
    }
}

fn parse_order_status(status: Option<&str>, quantity: &str, filled_quantity: &str) -> OrderStatus {
    let inferred = inferred_order_status(quantity, filled_quantity);
    match status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" | "pending" => OrderStatus::New,
        "0" | "open" | "active" | "opened" => {
            if inferred == OrderStatus::PartiallyFilled {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "1" | "filled" | "closed" | "done" => OrderStatus::Filled,
        "2" | "cancelled" | "canceled" | "cancel" => OrderStatus::Cancelled,
        "pending_cancel" | "pendingcancel" => OrderStatus::PendingCancel,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => inferred,
    }
}

fn status_string(order: &Value) -> Option<String> {
    order
        .get("status")
        .or_else(|| order.get("state"))
        .and_then(value_as_string)
}

fn inferred_order_status(quantity: &str, filled_quantity: &str) -> OrderStatus {
    let quantity = quantity.parse::<f64>().unwrap_or(0.0);
    let filled = filled_quantity.parse::<f64>().unwrap_or(0.0);
    if quantity > 0.0 && filled >= quantity {
        return OrderStatus::Filled;
    }
    if filled > 0.0 {
        return OrderStatus::PartiallyFilled;
    }
    OrderStatus::Open
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

fn split_pair_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    split_compact_symbol(symbol)
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 12] = [
        "usdt", "usdc", "busd", "rur", "rub", "usd", "eur", "btc", "eth", "doge", "bnb", "try",
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
