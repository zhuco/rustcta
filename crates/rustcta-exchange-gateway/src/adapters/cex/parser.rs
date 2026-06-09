use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderBookResponse, OrderState,
    ResponseMetadata, SymbolRules, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
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
        .get("data")
        .and_then(|data| data.get("pairs"))
        .or_else(|| value.get("pairs"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "currency_limits missing pairs", value))?;
    pairs
        .iter()
        .map(|pair| parse_symbol_rule(exchange_id, pair))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let base_asset = required_str(exchange_id, value, "symbol1")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "symbol2")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol_text = format!("{base_asset}/{quote_asset}");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                exchange_symbol_text,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: None,
        quantity_increment: None,
        min_price: string_or_number(value.get("minPrice")),
        max_price: string_or_number(value.get("maxPrice")),
        min_quantity: string_or_number(value.get("minLotSize")),
        max_quantity: string_or_number(value.get("maxLotSize")),
        min_notional: string_or_number(value.get("minLotSizeS2")),
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    if let Some(depth) = depth {
        let depth = depth.clamp(1, 100) as usize;
        bids.truncate(depth);
        asks.truncate(depth);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cex order book requires canonical_symbol".to_string(),
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
    snapshot.sequence = value
        .get("seqId")
        .or_else(|| value.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp_ms")
        .and_then(timestamp_value_to_datetime)
        .or_else(|| value.get("timestamp").and_then(timestamp_value_to_datetime));
    Ok(snapshot)
}

pub fn parse_ws_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    if !matches!(
        value.get("e").and_then(Value::as_str),
        Some("order-book-subscribe" | "order_book_subscribe")
    ) {
        return Err(parse_error(
            exchange_id.clone(),
            "CEX.IO websocket message is not order-book-subscribe",
            value,
        ));
    }
    let data = value.get("data").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO websocket snapshot missing data",
            value,
        )
    })?;
    let snapshot = parse_orderbook_snapshot(exchange_id, symbol, None, data)?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order_book: snapshot,
    })
}

pub fn parse_ws_orderbook_increment(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    if !matches!(
        value.get("e").and_then(Value::as_str),
        Some("order-book-increment" | "order_book_increment")
    ) {
        return Err(parse_error(
            exchange_id.clone(),
            "CEX.IO websocket message is not order-book-increment",
            value,
        ));
    }
    let data = value.get("data").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO websocket increment missing data",
            value,
        )
    })?;
    let normalized = serde_json::json!({
        "bids": data.get("bids").cloned().unwrap_or_else(|| serde_json::json!([])),
        "asks": data.get("asks").cloned().unwrap_or_else(|| serde_json::json!([])),
        "seqId": data.get("seqId").cloned().unwrap_or(Value::Null),
        "timestamp": data.get("timestamp").cloned().unwrap_or(Value::Null),
    });
    let snapshot = parse_orderbook_snapshot(exchange_id, symbol, None, &normalized)?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order_book: snapshot,
    })
}

pub fn cex_ws_seq_id(value: &Value) -> Option<u64> {
    value
        .get("seqId")
        .or_else(|| value.get("sequence"))
        .or_else(|| value.get("data").and_then(|data| data.get("seqId")))
        .or_else(|| value.get("data").and_then(|data| data.get("sequence")))
        .and_then(value_as_u64)
}

pub fn cex_ws_orderbook_seq_is_next(previous: Option<u64>, next: u64) -> bool {
    previous.is_none_or(|previous| next == previous.saturating_add(1))
}

pub fn normalize_cex_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cex symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed
        .replace(':', "/")
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    let (base, quote) = if let Some((base, quote)) = normalized.split_once('/') {
        (base.to_string(), quote.to_string())
    } else {
        split_suffix_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer CEX.IO base/quote from symbol {symbol}"),
        })?
    };
    if base.is_empty() || quote.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid CEX.IO symbol {symbol}"),
        });
    }
    Ok((base, quote))
}

pub fn cex_pair_string(symbol: &str) -> ExchangeApiResult<String> {
    let (base, quote) = normalize_cex_symbol(symbol)?;
    Ok(format!("{base}:{quote}"))
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = order_object(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO order response missing order object",
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
            "CEX.IO open orders response missing order array",
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
                message: "cex get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let orders = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO archived orders response missing order array",
            value,
        )
    })?;
    let mut fills = Vec::new();
    for order in orders {
        let quantity = filled_quantity(order)
            .and_then(|quantity| quantity.parse::<f64>().ok())
            .unwrap_or_default();
        if quantity <= 0.0 {
            continue;
        }
        let price = number_from_fields(order, &["price", "avgPrice", "average"])
            .ok_or_else(|| parse_error(exchange_id.clone(), "CEX.IO fill missing price", order))?;
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: canonical_symbol.clone(),
            exchange_symbol: Some(symbol.exchange_symbol.clone()),
            order_id: string_from_fields(order, &["id", "order_id"]),
            client_order_id: string_from_fields(order, &["clientOrderId", "client_order_id"]),
            fill_id: string_from_fields(order, &["trade_id", "tradeId", "id"]),
            side: parse_side(string_from_fields(order, &["type", "side"]).as_deref())?,
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: LiquidityRole::Unknown,
            price,
            quantity,
            quote_quantity: Some(price * quantity),
            fee_asset: string_from_fields(order, &["feeCurrency", "fee_asset"]),
            fee_amount: number_from_fields(order, &["fee", "feeAmount", "fee_amount"]),
            fee_rate: None,
            realized_pnl: None,
            filled_at: timestamp_from_fields(order, &["time", "timestamp", "date"])
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

fn split_suffix_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in [
        "USDT", "USDC", "USD", "EUR", "GBP", "BTC", "ETH", "BCH", "LTC",
    ] {
        let Some(base) = symbol.strip_suffix(quote) else {
            continue;
        };
        if !base.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    None
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order: &Value,
) -> ExchangeApiResult<OrderState> {
    let scope = symbol_hint.cloned().map(Ok).unwrap_or_else(|| {
        let base = string_from_fields(order, &["symbol1", "base", "baseCurrency"])
            .ok_or_else(|| parse_error(exchange_id.clone(), "CEX.IO order missing base", order))?;
        let quote = string_from_fields(order, &["symbol2", "quote", "quoteCurrency"])
            .ok_or_else(|| parse_error(exchange_id.clone(), "CEX.IO order missing quote", order))?;
        symbol_scope(exchange_id.clone(), &base, &quote)
    })?;
    let quantity = string_from_fields(order, &["amount", "quantity", "volume"])
        .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: scope.canonical_symbol.clone(),
        exchange_symbol: scope.exchange_symbol,
        client_order_id: string_from_fields(order, &["clientOrderId", "client_order_id"]),
        exchange_order_id: string_from_fields(order, &["id", "order_id"]),
        side: parse_side(string_from_fields(order, &["type", "side"]).as_deref())?,
        position_side: None,
        order_type: if string_from_fields(order, &["price"]).is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        time_in_force: None,
        status: parse_order_status(order),
        quantity,
        price: string_from_fields(order, &["price"]),
        filled_quantity: filled_quantity(order).unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_from_fields(order, &["avgPrice", "average"]),
        reduce_only: false,
        post_only: false,
        created_at: timestamp_from_fields(order, &["time", "createdAt", "created_at"]),
        updated_at: timestamp_from_fields(order, &["lastTxTime", "timestamp", "updatedAt"])
            .unwrap_or_else(Utc::now),
    })
}

fn order_object(value: &Value) -> Option<&Value> {
    if value.as_object().is_some() {
        if let Some(data) = value.get("data").filter(|data| data.as_object().is_some()) {
            return Some(data);
        }
        if let Some(order) = value
            .get("order")
            .filter(|order| order.as_object().is_some())
        {
            return Some(order);
        }
        return Some(value);
    }
    value.as_array()?.first()
}

fn payload_array(value: &Value) -> Option<&[Value]> {
    if let Some(array) = value.as_array() {
        return Some(array);
    }
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .map(Vec::as_slice)
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
) -> ExchangeApiResult<SymbolScope> {
    let base = base.trim().to_ascii_uppercase();
    let quote = quote.trim().to_ascii_uppercase();
    let canonical_symbol = CanonicalSymbol::new(&base, &quote).map_err(validation_error)?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Spot,
        format!("{base}/{quote}"),
    )
    .map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol,
    })
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("buy" | "bid") => Ok(OrderSide::Buy),
        Some("sell" | "ask") => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "CEX.IO order side must be buy or sell".to_string(),
        }),
    }
}

fn parse_order_status(order: &Value) -> OrderStatus {
    match string_from_fields(order, &["status"])
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "d" | "done" | "filled" | "closed" => OrderStatus::Filled,
        "c" | "cd" | "cancel" | "cancelled" | "canceled" => OrderStatus::Cancelled,
        "a" | "active" | "open" | "pending" => pending_status(order),
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => pending_status(order),
    }
}

fn pending_status(order: &Value) -> OrderStatus {
    let Some(amount) = number_from_fields(order, &["amount", "quantity", "volume"]) else {
        return OrderStatus::Unknown;
    };
    let pending = number_from_fields(order, &["pending", "remaining"]).unwrap_or(amount);
    if pending <= 0.0 {
        OrderStatus::Filled
    } else if pending < amount {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::Open
    }
}

fn filled_quantity(order: &Value) -> Option<String> {
    if let Some(filled) = string_from_fields(order, &["filled", "filledAmount", "filled_amount"]) {
        return Some(filled);
    }
    let amount = number_from_fields(order, &["amount", "quantity", "volume"])?;
    let pending = number_from_fields(order, &["pending", "remaining"]).unwrap_or_else(|| {
        if matches!(parse_order_status(order), OrderStatus::Filled) {
            0.0
        } else {
            amount
        }
    });
    Some(trim_float(amount - pending))
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| string_or_number(value.get(*field)))
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(number_from_value))
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(timestamp_value_to_datetime))
}

fn trim_float(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CEX.IO order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level
                .as_array()
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level", level))?;
            let price = array
                .first()
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {field}"),
            value,
        )
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn timestamp_value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        if number > 1_000_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(number);
        }
        return Utc.timestamp_opt(number, 0).single();
    }
    DateTime::parse_from_rfc3339(value.as_str()?)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message,
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
