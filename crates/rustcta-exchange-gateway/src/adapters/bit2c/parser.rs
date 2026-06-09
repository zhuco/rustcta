use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bit2cPair {
    pub venue: &'static str,
    pub base: &'static str,
    pub quote: &'static str,
}

pub const SUPPORTED_PAIRS: &[Bit2cPair] = &[
    Bit2cPair {
        venue: "BtcNis",
        base: "BTC",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "EthNis",
        base: "ETH",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "LtcNis",
        base: "LTC",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "UsdcNis",
        base: "USDC",
        quote: "NIS",
    },
];

pub fn symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
) -> ExchangeApiResult<Vec<SymbolRules>> {
    if requested.is_empty() {
        return SUPPORTED_PAIRS
            .iter()
            .map(|pair| pair_to_rules(exchange_id, pair))
            .collect();
    }

    requested
        .iter()
        .map(|symbol| {
            let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
            pair_to_rules(exchange_id, &pair)
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .unwrap_or(CanonicalSymbol::new(pair.base, pair.quote).map_err(validation_error)?);
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
    Ok(snapshot)
}

pub fn parse_bit2c_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_bit2c_order_state_with_side(exchange_id, symbol_hint, value, None)
}

pub fn parse_bit2c_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    if let Some(orders) = value.as_array() {
        return orders
            .iter()
            .map(|order| parse_bit2c_order_state_with_side(exchange_id, Some(symbol), order, None))
            .collect();
    }
    if let Some(orders) = value.get("orders").and_then(Value::as_array) {
        return orders
            .iter()
            .map(|order| parse_bit2c_order_state_with_side(exchange_id, Some(symbol), order, None))
            .collect();
    }

    let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
    let pair_orders = value.get(pair.venue).or_else(|| {
        value
            .as_object()
            .and_then(|object| object.iter().find(|(key, _)| pair_from_symbol(key).is_ok()))
            .map(|(_, value)| value)
    });
    let pair_orders = pair_orders.ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "Bit2C open orders response missing pair bucket".to_string(),
    })?;

    let mut orders = Vec::new();
    for (field, side) in [("bid", OrderSide::Buy), ("bids", OrderSide::Buy)] {
        if let Some(rows) = pair_orders.get(field).and_then(Value::as_array) {
            for row in rows {
                orders.push(parse_bit2c_order_state_with_side(
                    exchange_id,
                    Some(symbol),
                    row,
                    Some(side),
                )?);
            }
        }
    }
    for (field, side) in [("ask", OrderSide::Sell), ("asks", OrderSide::Sell)] {
        if let Some(rows) = pair_orders.get(field).and_then(Value::as_array) {
            for row in rows {
                orders.push(parse_bit2c_order_state_with_side(
                    exchange_id,
                    Some(symbol),
                    row,
                    Some(side),
                )?);
            }
        }
    }

    Ok(orders)
}

pub fn pair_from_symbol(symbol: &str) -> ExchangeApiResult<Bit2cPair> {
    let normalized = symbol
        .trim()
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Bit2C symbol must not be empty".to_string(),
        });
    }

    for pair in SUPPORTED_PAIRS {
        if normalized == pair.venue.to_ascii_uppercase()
            || normalized == format!("{}/{}", pair.base, pair.quote)
            || normalized == format!("{}{}", pair.base, pair.quote)
        {
            return Ok(*pair);
        }
    }

    Err(ExchangeApiError::Unsupported {
        operation: "bit2c.unsupported_symbol",
    })
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(50).clamp(1, 200)
}

pub fn classify_bit2c_error(message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("apikey") || msg.contains("api key") || msg.contains("signature") {
        ExchangeErrorClass::Authentication
    } else if msg.contains("nonce") {
        ExchangeErrorClass::InvalidRequest
    } else if msg.contains("no order") || msg.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("balance") || msg.contains("fund") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("pair") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("rate") || msg.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn exchange_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: Value,
) -> ExchangeApiError {
    let message = message.into();
    let mut error = ExchangeError::new(
        exchange_id,
        classify_bit2c_error(&message),
        message,
        Utc::now(),
    );
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn pair_to_rules(exchange_id: &ExchangeId, pair: &Bit2cPair) -> ExchangeApiResult<SymbolRules> {
    let canonical_symbol = CanonicalSymbol::new(pair.base, pair.quote).map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, pair.venue)
                .map_err(validation_error)?,
        },
        base_asset: pair.base.to_string(),
        quote_asset: pair.quote.to_string(),
        price_increment: None,
        quantity_increment: None,
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::Exchange(ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            "Bit2C order book missing bids/asks array",
            Utc::now(),
        ))
    })?;
    levels
        .iter()
        .map(|level| {
            let values = level.as_array().ok_or_else(|| {
                ExchangeApiError::Exchange(ExchangeError::new(
                    exchange_id.clone(),
                    ExchangeErrorClass::Decode,
                    "Bit2C order book level must be [price, amount]",
                    Utc::now(),
                ))
            })?;
            let price = values.first().and_then(number_from_value).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bit2C order book price: {level}"),
                }
            })?;
            let quantity = values.get(1).and_then(number_from_value).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bit2C order book quantity: {level}"),
                }
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn parse_bit2c_order_state_with_side(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
    side_hint: Option<OrderSide>,
) -> ExchangeApiResult<OrderState> {
    let row = value
        .get("data")
        .or_else(|| value.get("order"))
        .or_else(|| value.get("Order"))
        .unwrap_or(value);
    let pair = if let Some(pair) = string_field(row, &["pair", "Pair"]) {
        pair_from_symbol(pair)?
    } else if let Some(symbol) = symbol_hint {
        pair_from_symbol(&symbol.exchange_symbol.symbol)?
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Bit2C order row missing pair".to_string(),
        });
    };
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, pair.venue)
            .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint
        .and_then(|symbol| symbol.canonical_symbol.clone())
        .or_else(|| CanonicalSymbol::new(pair.base, pair.quote).ok());
    let initial_amount = value_as_string(
        row.get("initialAmount")
            .or_else(|| row.get("InitialAmount"))
            .or_else(|| row.get("initial_amount")),
    );
    let remaining_amount = value_as_string(
        row.get("amount")
            .or_else(|| row.get("Amount"))
            .or_else(|| row.get("remainingAmount"))
            .or_else(|| row.get("remaining_amount")),
    );
    let filled_quantity = match (&initial_amount, &remaining_amount) {
        (Some(initial), Some(remaining)) => {
            decimal_diff_string(initial, remaining).unwrap_or_else(|| "0".to_string())
        }
        _ => value_as_string(
            row.get("filledAmount")
                .or_else(|| row.get("FilledAmount"))
                .or_else(|| row.get("filled_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
    };
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: None,
        exchange_order_id: row
            .get("id")
            .or_else(|| row.get("Id"))
            .or_else(|| row.get("orderId"))
            .or_else(|| row.get("order_id"))
            .and_then(value_to_string),
        side: parse_bit2c_side(row)
            .or(side_hint)
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_bit2c_order_type(row),
        time_in_force: Some(TimeInForce::GTC),
        status: parse_bit2c_order_status(row).unwrap_or(OrderStatus::Open),
        quantity: initial_amount
            .or(remaining_amount)
            .unwrap_or_else(|| "0".to_string()),
        price: value_as_string(row.get("price").or_else(|| row.get("Price"))),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: row
            .get("created")
            .or_else(|| row.get("Created"))
            .and_then(timestamp_seconds),
        updated_at: now,
    })
}

fn parse_bit2c_side(row: &Value) -> Option<OrderSide> {
    if let Some(is_bid) = row
        .get("isBid")
        .or_else(|| row.get("IsBid"))
        .and_then(Value::as_bool)
    {
        return Some(if is_bid {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        });
    }
    match row.get("type").or_else(|| row.get("Type")) {
        Some(Value::Number(number)) => match number.as_i64()? {
            0 => Some(OrderSide::Buy),
            1 => Some(OrderSide::Sell),
            _ => None,
        },
        Some(Value::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "0" | "buy" | "bid" => Some(OrderSide::Buy),
            "1" | "sell" | "ask" => Some(OrderSide::Sell),
            _ => None,
        },
        _ => None,
    }
}

fn parse_bit2c_order_type(row: &Value) -> OrderType {
    match row
        .get("order_type")
        .or_else(|| row.get("orderType"))
        .or_else(|| row.get("OrderType"))
    {
        Some(Value::Number(number)) => match number.as_i64().unwrap_or_default() {
            1 => OrderType::Market,
            2 => OrderType::StopLimit,
            _ => OrderType::Limit,
        },
        Some(Value::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "market" | "mkt" => OrderType::Market,
            "2" | "stop" | "stl" | "stoplimit" | "stop_limit" => OrderType::StopLimit,
            _ => OrderType::Limit,
        },
        _ => OrderType::Limit,
    }
}

fn parse_bit2c_order_status(row: &Value) -> Option<OrderStatus> {
    match row
        .get("status_type")
        .or_else(|| row.get("statusType"))
        .or_else(|| row.get("StatusType"))
        .or_else(|| row.get("status"))
        .or_else(|| row.get("Status"))
    {
        Some(Value::Number(number)) => match number.as_i64()? {
            0 => Some(OrderStatus::New),
            1 => Some(OrderStatus::Open),
            2 => Some(OrderStatus::Rejected),
            3 => Some(OrderStatus::New),
            4 => Some(OrderStatus::Cancelled),
            5 => Some(OrderStatus::Filled),
            _ => Some(OrderStatus::Unknown),
        },
        Some(Value::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "0" | "new" => Some(OrderStatus::New),
            "1" | "open" | "active" => Some(OrderStatus::Open),
            "2" | "nofunds" | "no_funds" | "rejected" => Some(OrderStatus::Rejected),
            "3" | "waitforinsert" | "wait_for_insert" => Some(OrderStatus::New),
            "4" | "deleted" | "cancelled" | "canceled" => Some(OrderStatus::Cancelled),
            "5" | "completed" | "filled" => Some(OrderStatus::Filled),
            _ => Some(OrderStatus::Unknown),
        },
        _ => None,
    }
}

fn string_field<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(normalize_number_string(number.to_string())),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(normalize_number_string(number.to_string())),
        _ => None,
    }
}

fn normalize_number_string(value: String) -> String {
    if value.contains('.') {
        let trimmed = value
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
        if trimmed.is_empty() {
            "0".to_string()
        } else {
            trimmed
        }
    } else {
        value
    }
}

fn timestamp_seconds(value: &Value) -> Option<chrono::DateTime<Utc>> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .and_then(|timestamp| chrono::DateTime::from_timestamp(timestamp, 0)),
        Value::String(text) => text
            .parse::<i64>()
            .ok()
            .and_then(|timestamp| chrono::DateTime::from_timestamp(timestamp, 0)),
        _ => None,
    }
}

fn decimal_diff_string(initial: &str, remaining: &str) -> Option<String> {
    let initial = initial.replace(',', "").parse::<f64>().ok()?;
    let remaining = remaining.replace(',', "").parse::<f64>().ok()?;
    let diff = (initial - remaining).max(0.0);
    let value = format!("{diff:.8}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string();
    Some(if value.is_empty() {
        "0".to_string()
    } else {
        value
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
