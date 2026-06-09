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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymiumBookLevelChange {
    pub price: String,
    pub quantity: String,
}

impl PaymiumBookLevelChange {
    pub fn is_delete(&self) -> bool {
        self.quantity
            .parse::<f64>()
            .map(|quantity| quantity == 0.0)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymiumPublicBookUpdate {
    pub event: &'static str,
    pub sequence: Option<u64>,
    pub checksum: Option<String>,
    pub bids: Vec<PaymiumBookLevelChange>,
    pub asks: Vec<PaymiumBookLevelChange>,
}

impl PaymiumPublicBookUpdate {
    pub fn deleted_bid_prices(&self) -> Vec<&str> {
        self.bids
            .iter()
            .filter(|level| level.is_delete())
            .map(|level| level.price.as_str())
            .collect()
    }

    pub fn deleted_ask_prices(&self) -> Vec<&str> {
        self.asks
            .iter()
            .filter(|level| level.is_delete())
            .map(|level| level.price.as_str())
            .collect()
    }
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let quote_asset = value
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or("EUR")
        .to_ascii_uppercase();
    let canonical_symbol = CanonicalSymbol::new("BTC", &quote_asset).map_err(validation_error)?;
    Ok(vec![SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                format!("BTC/{quote_asset}"),
            )
            .map_err(validation_error)?,
        },
        base_asset: "BTC".to_string(),
        quote_asset,
        price_increment: None,
        quantity_increment: Some("0.00000001".to_string()),
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: Some(8),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    }])
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
                message: "paymium order book requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("at")
        .and_then(timestamp_value_to_datetime)
        .or_else(|| value.get("timestamp").and_then(timestamp_value_to_datetime));
    Ok(snapshot)
}

pub fn parse_public_book_update(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<PaymiumPublicBookUpdate> {
    let payload = public_stream_payload(exchange_id, value)?;
    let has_bids = payload.get("bids").is_some();
    let has_asks = payload.get("asks").is_some();
    if !has_bids && !has_asks {
        return Err(parse_error(
            exchange_id.clone(),
            "Paymium public stream update missing bids/asks",
            value,
        ));
    }
    Ok(PaymiumPublicBookUpdate {
        event: "stream",
        sequence: None,
        checksum: None,
        bids: parse_level_changes(exchange_id, payload.get("bids"))?,
        asks: parse_level_changes(exchange_id, payload.get("asks"))?,
    })
}

pub fn normalize_paymium_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "paymium symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed
        .replace(':', "/")
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    let (base, quote) = if let Some((base, quote)) = normalized.split_once('/') {
        (base.to_string(), quote.to_string())
    } else if let Some(quote) = normalized.strip_prefix("BTC") {
        ("BTC".to_string(), quote.to_string())
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Paymium base/quote from symbol {symbol}"),
        });
    };
    if base != "BTC" || quote != "EUR" {
        return Err(ExchangeApiError::Unsupported {
            operation: "paymium.only_btc_eur_spot_public_rest",
        });
    }
    Ok((base, quote))
}

pub fn paymium_currency_path(symbol: &str) -> ExchangeApiResult<String> {
    let (_base, quote) = normalize_paymium_symbol(symbol)?;
    Ok(quote.to_ascii_lowercase())
}

pub fn parse_private_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order_or_self(value);
    let symbol = private_symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text_any(
        order,
        &[
            "amount",
            "amount_btc",
            "requested_amount",
            "original_amount",
        ],
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = text_any(
        order,
        &[
            "filled_amount",
            "traded_btc",
            "executed_amount",
            "matched_amount",
        ],
    )
    .or_else(|| filled_from_trades(order))
    .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text_any(order, &["client_order_id", "client_id"]),
        exchange_order_id: text_any(order, &["uuid", "id", "order_id"]),
        side: parse_side(order),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order),
        time_in_force: None,
        status: parse_order_status(order, &quantity, &filled_quantity),
        quantity,
        price: text_any(order, &["price", "limit_price"]).filter(|price| !is_zero_decimal(price)),
        filled_quantity,
        average_fill_price: text_any(order, &["average_price", "avg_price", "executed_price"]),
        reduce_only: false,
        post_only: false,
        created_at: timestamp_any(order, &["created_at", "created", "date"]),
        updated_at: timestamp_any(
            order,
            &["updated_at", "executed_at", "cancelled_at", "created_at"],
        )
        .unwrap_or_else(Utc::now),
    })
}

pub fn parse_private_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    order_rows(value)?
        .iter()
        .map(|order| parse_private_order_state(exchange_id, symbol_hint, order))
        .filter_map(|order| match order {
            Ok(order)
                if matches!(
                    order.status,
                    OrderStatus::Open | OrderStatus::New | OrderStatus::PartiallyFilled
                ) =>
            {
                Some(Ok(order))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub fn parse_private_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let mut fills = Vec::new();
    for order in order_rows(value)? {
        let symbol = private_symbol_scope(exchange_id, Some(symbol_hint), order)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "paymium fill requires canonical_symbol".to_string(),
                })?;
        let order_id = text_any(order, &["uuid", "id", "order_id"]);
        let client_order_id = text_any(order, &["client_order_id", "client_id"]);
        for trade in trade_rows(order) {
            let price = decimal_any(trade, &["price", "rate"]).ok_or_else(|| {
                parse_error(exchange_id.clone(), "Paymium fill missing price", trade)
            })?;
            let quantity = decimal_any(trade, &["amount", "amount_btc", "quantity", "btc"])
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "Paymium fill missing amount", trade)
                })?;
            fills.push(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: order_id.clone(),
                client_order_id: client_order_id.clone(),
                fill_id: text_any(trade, &["uuid", "id", "trade_id"]),
                side: parse_side(order),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: decimal_any(trade, &["amount_eur", "quote_amount", "total"])
                    .or_else(|| Some(price * quantity)),
                fee_asset: text_any(trade, &["fee_currency", "fees_currency"])
                    .map(|asset| asset.to_ascii_uppercase()),
                fee_amount: decimal_any(trade, &["fee", "fees_amount", "fee_amount"]),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp_any(trade, &["created_at", "date", "executed_at"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            });
        }
    }
    Ok(fills)
}

fn public_stream_payload<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<&'a Value> {
    if let Some(name) = value.get("name").and_then(Value::as_str) {
        if name != "stream" {
            return Err(parse_error(
                exchange_id.clone(),
                "Paymium socket.io event is not stream",
                value,
            ));
        }
        return value
            .get("args")
            .and_then(Value::as_array)
            .and_then(|args| args.first())
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Paymium socket.io stream event missing first arg payload",
                    value,
                )
            });
    }
    if let Some(event) = value.get("event").and_then(Value::as_str) {
        if event != "stream" {
            return Err(parse_error(
                exchange_id.clone(),
                "Paymium public stream event is not stream",
                value,
            ));
        }
        return value
            .get("data")
            .or_else(|| value.get("payload"))
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Paymium stream envelope missing data payload",
                    value,
                )
            });
    }
    Ok(value)
}

fn parse_level_changes(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<PaymiumBookLevelChange>> {
    let Some(levels) = levels else {
        return Ok(Vec::new());
    };
    let levels = levels.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Paymium public stream levels must be arrays",
            levels,
        )
    })?;
    levels
        .iter()
        .map(|level| parse_level_change(exchange_id, level))
        .collect()
}

fn parse_level_change(
    exchange_id: &ExchangeId,
    level: &Value,
) -> ExchangeApiResult<PaymiumBookLevelChange> {
    if let Some(array) = level.as_array() {
        let price = array.first().and_then(value_as_string).ok_or_else(|| {
            parse_error(exchange_id.clone(), "invalid Paymium level price", level)
        })?;
        let quantity = array.get(1).and_then(value_as_string).ok_or_else(|| {
            parse_error(exchange_id.clone(), "invalid Paymium level quantity", level)
        })?;
        return Ok(PaymiumBookLevelChange { price, quantity });
    }
    let price = level
        .get("price")
        .or_else(|| level.get("rate"))
        .and_then(value_as_string)
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid Paymium level price", level))?;
    let quantity = level
        .get("amount")
        .or_else(|| level.get("quantity"))
        .or_else(|| level.get("volume"))
        .and_then(value_as_string)
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid Paymium level amount", level))?;
    Ok(PaymiumBookLevelChange { price, quantity })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Paymium order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("amount")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level amount", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn order_rows(value: &Value) -> ExchangeApiResult<Vec<&Value>> {
    if let Some(array) = value.as_array() {
        return Ok(array.iter().collect());
    }
    for key in ["orders", "payload", "data"] {
        if let Some(array) = value.get(key).and_then(Value::as_array) {
            return Ok(array.iter().collect());
        }
    }
    if value.as_object().is_some() {
        return Ok(vec![first_order_or_self(value)]);
    }
    Err(parse_error(
        ExchangeId::new("paymium").map_err(validation_error)?,
        "Paymium private order response must be object or array",
        value,
    ))
}

fn first_order_or_self(value: &Value) -> &Value {
    value
        .get("order")
        .or_else(|| {
            value
                .get("payload")
                .filter(|payload| payload.as_object().is_some())
        })
        .or_else(|| value.get("data").filter(|data| data.as_object().is_some()))
        .or_else(|| {
            value
                .get("orders")
                .and_then(Value::as_array)
                .and_then(|orders| orders.first())
        })
        .or_else(|| value.as_array().and_then(|orders| orders.first()))
        .unwrap_or(value)
}

fn trade_rows(order: &Value) -> Vec<&Value> {
    for key in ["trades", "executions", "fills"] {
        if let Some(trades) = order.get(key).and_then(Value::as_array) {
            return trades.iter().collect();
        }
    }
    Vec::new()
}

fn private_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    row: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let quote = text_any(row, &["currency", "quote_currency"])
        .unwrap_or_else(|| "EUR".to_string())
        .to_ascii_uppercase();
    let canonical = CanonicalSymbol::new("BTC", &quote).map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            format!("BTC/{quote}"),
        )
        .map_err(validation_error)?,
    })
}

fn parse_side(order: &Value) -> OrderSide {
    match text_any(order, &["direction", "side"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(order: &Value) -> OrderType {
    let raw = text_any(order, &["type", "order_type"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    if raw.contains("market") {
        OrderType::Market
    } else {
        OrderType::Limit
    }
}

fn parse_order_status(order: &Value, quantity: &str, filled_quantity: &str) -> OrderStatus {
    let raw = text_any(order, &["state", "status"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    match raw.as_str() {
        "active" | "open" | "pending" | "created" => {
            if filled_quantity != "0" {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        "completed" | "executed" | "filled" | "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ if quantity != "0" && quantity == filled_quantity => OrderStatus::Filled,
        _ => OrderStatus::Unknown,
    }
}

fn text_any(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(field).and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_i64().map(|number| number.to_string()))
                .or_else(|| value.as_u64().map(|number| number.to_string()))
                .or_else(|| value.as_f64().map(trim_decimal))
        })
    })
}

fn decimal_any(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(field).and_then(number_from_value))
}

fn filled_from_trades(order: &Value) -> Option<String> {
    let total = trade_rows(order)
        .into_iter()
        .filter_map(|trade| decimal_any(trade, &["amount", "amount_btc", "quantity", "btc"]))
        .sum::<f64>();
    (total > 0.0).then(|| trim_decimal(total))
}

fn timestamp_any(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(field).and_then(timestamp_value_to_datetime))
}

fn trim_decimal(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn is_zero_decimal(text: &str) -> bool {
    text.parse::<f64>()
        .map(|value| value.abs() <= f64::EPSILON)
        .unwrap_or(false)
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
