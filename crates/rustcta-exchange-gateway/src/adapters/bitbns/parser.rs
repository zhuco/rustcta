use chrono::{DateTime, Utc};
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
    let markets = value.get("data").unwrap_or(value);
    match markets {
        Value::Array(items) => items
            .iter()
            .filter(|item| !is_inactive(item))
            .map(|item| parse_symbol_rule(exchange_id, None, item))
            .collect(),
        Value::Object(map) => map
            .iter()
            .filter(|(_, item)| !is_inactive(item))
            .map(|(key, item)| parse_symbol_rule(exchange_id, Some(key.as_str()), item))
            .collect(),
        _ => Err(parse_error(
            exchange_id.clone(),
            "Bitbns markets response must be an array or object",
            value,
        )),
    }
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    key: Option<&str>,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let (base_asset, quote_asset, exchange_symbol) = parse_market_identity(key, value)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    };

    let price_precision = integer_from_fields(
        value,
        if quote_asset == "USDT" {
            &[
                "usdtDecimals",
                "usdt_decimals",
                "price_precision",
                "quote_precision",
            ]
        } else {
            &[
                "inrDecimals",
                "inr_decimals",
                "price_precision",
                "quote_precision",
            ]
        },
    );
    let quantity_precision = integer_from_fields(
        value,
        &[
            "floatPlaces",
            "float_places",
            "quantity_precision",
            "base_precision",
        ],
    );

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset: quote_asset.clone(),
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: quantity_precision.map(increment_from_precision),
        min_price: None,
        max_price: None,
        min_quantity: string_from_fields(value, &["min_quantity", "minQty", "min_qty"]),
        max_quantity: string_from_fields(value, &["max_quantity", "maxQty", "max_qty"]),
        min_notional: string_from_fields(
            value,
            &[
                "min_notional",
                "minOrderValue",
                "min_order_value",
                "minVolume",
                "min_volume",
            ],
        )
        .or_else(|| default_min_notional(&quote_asset).map(str::to_string)),
        max_notional: string_from_fields(
            value,
            &["max_notional", "maxOrderValue", "max_order_value"],
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: usize,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value.get("data").unwrap_or(value);
    let mut bids = parse_levels(exchange_id, book.get("bids").or_else(|| book.get("buy")))?;
    let mut asks = parse_levels(exchange_id, book.get("asks").or_else(|| book.get("sell")))?;
    bids.truncate(depth);
    asks.truncate(depth);
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| canonical_from_exchange_symbol(&symbol.exchange_symbol.symbol).ok())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbns order book request requires canonical_symbol or split exchange symbol"
                .to_string(),
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
    snapshot.exchange_timestamp = book
        .get("timestamp")
        .or_else(|| book.get("time"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bitbns_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    split_symbol(symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("invalid Bitbns spot symbol {symbol}; expected BASE_INR or BASE_USDT"),
    })
}

pub fn normalize_depth(depth: Option<u32>) -> ExchangeApiResult<usize> {
    match depth {
        Some(0) => Err(ExchangeApiError::InvalidRequest {
            message: "bitbns order book depth must be greater than zero".to_string(),
        }),
        Some(depth) => Ok(depth.min(50) as usize),
        None => Ok(20),
    }
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = order_object(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitbns order response missing order object",
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
            "Bitbns open orders response missing data array",
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
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitbns recent fills response missing data array",
            value,
        )
    })?;
    fills
        .iter()
        .map(|fill| parse_fill_object(exchange_id, &tenant_id, &account_id, symbol_hint, fill))
        .collect()
}

fn parse_order_object(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Bitbns private order parsing requires symbol hint".to_string(),
        })?;
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let quantity = string_from_fields(
        value,
        &[
            "btc", "quantity", "qty", "qnty", "amount", "volume", "origQty",
        ],
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "Bitbns order missing quantity", value))?;
    let price = string_from_fields(value, &["rate", "price", "limit_price"]);
    let status = parse_order_status(value);
    let filled_quantity = string_from_fields(
        value,
        &[
            "filled_quantity",
            "filledQuantity",
            "filled_qty",
            "executedQty",
            "executed_quantity",
        ],
    )
    .unwrap_or_else(|| match status {
        OrderStatus::Filled => quantity.clone(),
        _ => "0".to_string(),
    });
    let now = Utc::now();
    let created_at = first_timestamp(value, &["time", "date", "created_at", "createdAt"]);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: string_from_fields(value, &["client_order_id", "clientOrderId"]),
        exchange_order_id: string_from_fields(value, &["entry_id", "order_id", "id"]),
        side: parse_side(value).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Bitbns order missing side/type", value)
        })?,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status,
        quantity,
        price,
        filled_quantity,
        average_fill_price: string_from_fields(
            value,
            &["average_fill_price", "avg_price", "avgRate", "averageRate"],
        ),
        reduce_only: false,
        post_only: false,
        created_at,
        updated_at: first_timestamp(value, &["updated_at", "updatedAt", "time", "date"])
            .unwrap_or(now),
    })
}

fn parse_fill_object(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let canonical_symbol =
        symbol_hint
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Bitbns fill parsing requires canonical_symbol".to_string(),
            })?;
    let price = number_from_fields(value, &["rate", "price"])
        .ok_or_else(|| parse_error(exchange_id.clone(), "Bitbns fill missing price/rate", value))?;
    let quantity = number_from_fields(
        value,
        &["btc", "quantity", "qty", "qnty", "amount", "volume"],
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "Bitbns fill missing quantity", value))?;
    let filled_at = first_timestamp(value, &["time", "date", "created_at", "createdAt"])
        .unwrap_or_else(Utc::now);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol_hint.exchange_symbol.clone()),
        order_id: string_from_fields(value, &["entry_id", "order_id", "id"]),
        client_order_id: string_from_fields(value, &["client_order_id", "clientOrderId"]),
        fill_id: string_from_fields(value, &["trade_id", "fill_id", "entry_id", "id"]),
        side: parse_side(value).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Bitbns fill missing side/type", value)
        })?,
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: LiquidityRole::Unknown,
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: string_from_fields(value, &["fee_asset", "feeAsset"]),
        fee_amount: number_from_fields(value, &["fee", "fee_amount", "feeAmount"]),
        fee_rate: None,
        realized_pnl: None,
        filled_at,
        received_at: Utc::now(),
    })
}

fn parse_market_identity(
    key: Option<&str>,
    value: &Value,
) -> ExchangeApiResult<(String, String, String)> {
    let raw_symbol = string_from_fields(
        value,
        &[
            "symbol",
            "pair",
            "market_symbol",
            "ticker_id",
            "coinMarket",
            "coin_market",
        ],
    )
    .or_else(|| key.map(str::to_string));
    let raw_base = string_from_fields(
        value,
        &["coin", "coinName", "coin_name", "base", "base_currency"],
    );
    let raw_quote = string_from_fields(value, &["market", "quote", "quote_currency", "currency"]);

    let (base, quote) = match (raw_base, raw_quote, raw_symbol) {
        (Some(base), Some(quote), _) => (normalize_asset(&base), normalize_asset(&quote)),
        (_, _, Some(symbol)) => {
            split_symbol(&symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot split Bitbns market symbol {symbol}"),
            })?
        }
        _ => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "Bitbns market missing symbol/base/quote".to_string(),
            })
        }
    };
    let exchange_symbol = format!("{base}_{quote}");
    Ok((base, quote, exchange_symbol))
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('_') {
        if !base.is_empty() && matches!(quote, "INR" | "USDT") {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    ["USDT", "INR"].iter().find_map(|quote| {
        normalized
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.trim_end_matches('_').to_string(), (*quote).to_string()))
    })
}

fn order_object(value: &Value) -> Option<&Value> {
    if value.is_object() {
        if let Some(data) = value.get("data") {
            if data.is_object() {
                return Some(data);
            }
            if data.is_array() {
                return data.as_array().and_then(|orders| orders.first());
            }
        }
        return Some(value);
    }
    payload_array(value).and_then(|orders| orders.first())
}

fn payload_array(value: &Value) -> Option<&Vec<Value>> {
    if let Some(items) = value.as_array() {
        return Some(items);
    }
    value
        .get("data")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("result"))
        .unwrap_or(value)
        .as_array()
}

fn parse_side(value: &Value) -> Option<OrderSide> {
    let side = value
        .get("side")
        .or_else(|| value.get("order_side"))
        .or_else(|| value.get("type"));
    match side? {
        Value::Number(number) => match number.as_i64()? {
            0 => Some(OrderSide::Buy),
            1 => Some(OrderSide::Sell),
            _ => None,
        },
        Value::String(text) => {
            let lower = text.to_ascii_lowercase();
            if lower.contains("buy") || lower == "0" {
                Some(OrderSide::Buy)
            } else if lower.contains("sell") || lower == "1" {
                Some(OrderSide::Sell)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    let status = value.get("status").or_else(|| value.get("order_status"));
    match status {
        Some(Value::Number(number)) => match number.as_i64() {
            Some(-1) => OrderStatus::Cancelled,
            Some(0) => OrderStatus::Open,
            Some(1) => OrderStatus::PartiallyFilled,
            Some(2) => OrderStatus::Filled,
            _ => OrderStatus::Unknown,
        },
        Some(Value::String(text)) => match text.trim().to_ascii_lowercase().as_str() {
            "-1" | "cancelled" | "canceled" => OrderStatus::Cancelled,
            "0" | "open" | "new" | "not processed" => OrderStatus::Open,
            "1" | "partial" | "partially_filled" | "partially filled" => {
                OrderStatus::PartiallyFilled
            }
            "2" | "filled" | "complete" | "completed" | "fully executed" => OrderStatus::Filled,
            "rejected" => OrderStatus::Rejected,
            "expired" => OrderStatus::Expired,
            _ => OrderStatus::Unknown,
        },
        _ => OrderStatus::Unknown,
    }
}

fn canonical_from_exchange_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) = normalize_bitbns_symbol(symbol)?;
    CanonicalSymbol::new(base, quote).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitbns order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(items) => {
                let price = items.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Bitbns level price", level)
                })?;
                let quantity = items.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Bitbns level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(map) => {
                let price = map
                    .get("rate")
                    .or_else(|| map.get("price"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid Bitbns level rate", level)
                    })?;
                let quantity = map
                    .get("btc")
                    .or_else(|| map.get("quantity"))
                    .or_else(|| map.get("qty"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid Bitbns level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "invalid Bitbns order book level",
                level,
            )),
        })
        .collect()
}

fn is_inactive(value: &Value) -> bool {
    value
        .get("status")
        .or_else(|| value.get("isActive"))
        .or_else(|| value.get("is_active"))
        .is_some_and(|status| match status {
            Value::Bool(status) => !*status,
            Value::Number(number) => number.as_i64() == Some(0),
            Value::String(text) => matches!(text.as_str(), "0" | "false" | "False" | "inactive"),
            _ => false,
        })
}

fn normalize_asset(value: &str) -> String {
    value.trim().trim_start_matches('_').to_ascii_uppercase()
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(string_or_number)
}

fn integer_from_fields(value: &Value, fields: &[&str]) -> Option<u32> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(|value| match value {
            Value::String(text) => text.parse().ok(),
            Value::Number(number) => number.as_u64().map(|number| number as u32),
            _ => None,
        })
}

fn string_or_number(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
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
        .filter_map(|field| value.get(*field))
        .find_map(number_from_value)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(timestamp_from_value)
}

fn timestamp_from_value(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(text) => DateTime::parse_from_rfc3339(text)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|timestamp| timestamp.and_utc())
            })
            .or_else(|| text.parse::<i64>().ok().and_then(timestamp_from_i64)),
        Value::Number(number) => number.as_i64().and_then(timestamp_from_i64),
        _ => None,
    }
}

fn timestamp_from_i64(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        DateTime::<Utc>::from_timestamp(value, 0)
    }
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn default_min_notional(quote: &str) -> Option<&'static str> {
    match quote {
        "INR" => Some("10"),
        "USDT" => Some("0.1"),
        _ => None,
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        message.to_string(),
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
