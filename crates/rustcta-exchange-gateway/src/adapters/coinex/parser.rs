use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "market response is not an array",
                value,
            )
        })?;
    markets
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, market_type, value))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "market")
        .or_else(|_| required_str(exchange_id, value, "name"))?
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_compact_symbol(&exchange_symbol)
        .ok_or_else(|| parse_error(exchange_id.clone(), "market missing assets", value))?;
    let base_asset = value
        .get("base_ccy")
        .or_else(|| value.get("base_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quote_ccy")
        .or_else(|| value.get("quote_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let price_precision = integer_from_value(
        value
            .get("price_precision")
            .or_else(|| value.get("pricing_decimal")),
    );
    let quantity_precision = integer_from_value(
        value
            .get("amount_precision")
            .or_else(|| value.get("trading_decimal")),
    );
    let tradable = value
        .get("is_trading_available")
        .or_else(|| value.get("trading_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_precision
            .map(increment_from_precision)
            .or_else(|| string_or_number(value.get("tick_size"))),
        quantity_increment: quantity_precision
            .map(increment_from_precision)
            .or_else(|| string_or_number(value.get("step_size"))),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_amount")),
        max_quantity: string_or_number(value.get("max_amount")),
        min_notional: string_or_number(value.get("min_notional")),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let book = data.get("depth").unwrap_or(data);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex order book request requires canonical_symbol".to_string(),
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
        .get("last")
        .or_else(|| book.get("last"))
        .or_else(|| data.get("sequence"))
        .or_else(|| book.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("updated_at")
        .or_else(|| data.get("updated_at"))
        .or_else(|| data.get("timestamp"))
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_coinex_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        _ => 50,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book level", level)
            })?;
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

pub(super) fn required_str<'a>(
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

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
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

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}
