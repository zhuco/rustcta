use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value_array(value, &["data", "pairs", "result"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BIT.TEAM pairs response missing array data",
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
    .ok_or_else(|| parse_error(exchange_id.clone(), "BIT.TEAM pair missing symbol", value))?;
    let normalized_symbol = normalize_bitteam_symbol(&symbol)?;
    let (fallback_base, fallback_quote) = split_pair_symbol(&normalized_symbol)
        .ok_or_else(|| parse_error(exchange_id.clone(), "BIT.TEAM pair missing assets", value))?;
    let base_asset = first_string(
        value,
        &[
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
    let price_precision = integer_from_fields(
        value,
        &[
            "pricePrecision",
            "price_precision",
            "priceScale",
            "price_scale",
        ],
    );
    let quantity_precision = integer_from_fields(
        value,
        &[
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
        price_increment: string_from_fields(value, &["tickSize", "tick_size", "priceStep"])
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_from_fields(
            value,
            &["stepSize", "step_size", "amountStep", "quantityStep"],
        )
        .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: string_from_fields(value, &["minPrice", "min_price"]),
        max_price: string_from_fields(value, &["maxPrice", "max_price"]),
        min_quantity: string_from_fields(
            value,
            &["minAmount", "min_amount", "minQuantity", "min_quantity"],
        ),
        max_quantity: string_from_fields(
            value,
            &["maxAmount", "max_amount", "maxQuantity", "max_quantity"],
        ),
        min_notional: string_from_fields(
            value,
            &["minTotal", "min_total", "minNotional", "min_notional"],
        ),
        max_notional: string_from_fields(
            value,
            &["maxTotal", "max_total", "maxNotional", "max_notional"],
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
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
                message: "BIT.TEAM order book request requires canonical_symbol".to_string(),
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

pub fn parse_private_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_payload(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BIT.TEAM private order response missing order",
            value,
        )
    })?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: first_string(
            order,
            &[
                "clientOrderId",
                "client_order_id",
                "clientOrderID",
                "client_id",
            ],
        ),
        exchange_order_id: first_string(order, &["id", "orderId", "order_id", "exchangeOrderId"]),
        side: parse_order_side(exchange_id, order)?,
        position_side: None,
        order_type: parse_order_type(order),
        time_in_force: None,
        status: parse_order_status(order),
        quantity: string_from_fields(order, &["amount", "quantity", "qty", "origQty"])
            .unwrap_or_else(|| "0".to_string()),
        price: string_from_fields(order, &["price", "limit_price", "rate"]),
        filled_quantity: string_from_fields(order, &["filled", "executedQty", "filledAmount"])
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_from_fields(
            order,
            &["average", "avgPrice", "averagePrice", "avg_price"],
        ),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(order, &["timestamp", "createdAt", "created_at"]),
        updated_at: first_timestamp(order, &["updatedAt", "updated_at", "timestamp"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_private_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = payload_array(value, &["orders", "data", "result"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BIT.TEAM open orders response missing orders",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_private_order_state(exchange_id, symbol, order))
        .collect()
}

pub fn parse_private_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = payload_array(value, &["trades", "fills", "data", "result"]).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BIT.TEAM recent fills response missing fills",
            value,
        )
    })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BIT.TEAM fills require canonical_symbol".to_string(),
            })?;
    fills
        .iter()
        .map(|fill| {
            let price = number_field(exchange_id, fill, &["price", "rate"])?;
            let quantity = number_field(exchange_id, fill, &["amount", "quantity", "qty"])?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: first_string(fill, &["orderId", "order", "order_id"]),
                client_order_id: first_string(fill, &["clientOrderId", "client_order_id"]),
                fill_id: first_string(fill, &["id", "tradeId", "trade_id", "fillId"]),
                side: parse_order_side(exchange_id, fill)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(fill),
                price,
                quantity,
                quote_quantity: number_from_fields(fill, &["cost", "quoteQuantity", "quote_qty"]),
                fee_asset: fee_asset(fill),
                fee_amount: fee_amount(fill),
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestamp", "time", "createdAt"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_bitteam_symbol(symbol: &str) -> ExchangeApiResult<String> {
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
            message: format!("cannot infer BIT.TEAM quote asset from symbol {symbol}"),
        })?;
    Ok(format!("{base}_{quote}"))
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(50).clamp(1, 1000)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BIT.TEAM order book missing levels",
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
                .get("amount")
                .or_else(|| level.get("quantity"))
                .or_else(|| level.get("volume"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
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

fn first_order_payload<'a>(value: &'a Value) -> Option<&'a Value> {
    let payload = value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("payload"))
        .or_else(|| value.get("order"))
        .unwrap_or(value);
    if let Some(array) = payload.as_array() {
        return array.first();
    }
    payload
        .get("orders")
        .and_then(Value::as_array)
        .and_then(|orders| orders.first())
        .or_else(|| payload.get("order"))
        .or(Some(payload))
}

fn payload_array<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a Vec<Value>> {
    let payload = value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("payload"))
        .unwrap_or(value);
    if let Some(array) = payload.as_array() {
        return Some(array);
    }
    fields
        .iter()
        .filter_map(|field| payload.get(*field))
        .find_map(Value::as_array)
}

fn parse_order_side(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<OrderSide> {
    match first_string(value, &["side", "type"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(parse_error(
            exchange_id.clone(),
            format!("BIT.TEAM unsupported order side {other}"),
            value,
        )),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match first_string(value, &["orderType", "type"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match first_string(value, &["status", "state"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" => OrderStatus::New,
        "open" | "active" | "working" => OrderStatus::Open,
        "partial" | "partially_filled" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" | "closed" | "done" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: &Value) -> LiquidityRole {
    match first_string(value, &["liquidity", "takerOrMaker", "role"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
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

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(number_from_value))
}

fn number_field(
    exchange_id: &ExchangeId,
    value: &Value,
    fields: &[&str],
) -> ExchangeApiResult<f64> {
    number_from_fields(value, fields).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!(
                "BIT.TEAM missing numeric field {}",
                fields.first().copied().unwrap_or("unknown")
            ),
            value,
        )
    })
}

fn fee_amount(value: &Value) -> Option<f64> {
    number_from_fields(value, &["feeCost", "fee_amount"]).or_else(|| {
        value
            .get("fee")
            .and_then(|fee| fee.get("cost"))
            .and_then(number_from_value)
    })
}

fn fee_asset(value: &Value) -> Option<String> {
    first_string(value, &["feeAsset", "feeCurrency", "fee_asset"]).or_else(|| {
        value
            .get("fee")
            .and_then(|fee| fee.get("currency"))
            .and_then(value_as_string)
    })
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
