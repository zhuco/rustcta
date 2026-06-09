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
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .get("data")
        .or_else(|| value.get("result"))
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
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "market")
        .or_else(|_| required_str(exchange_id, value, "name"))
        .or_else(|_| required_str(exchange_id, value, "ticker_id"))?
        .replace(['-', '/'], "_")
        .to_ascii_uppercase();
    let market_type = match value
        .get("type")
        .or_else(|| value.get("product_type"))
        .and_then(Value::as_str)
        .unwrap_or("spot")
        .to_ascii_lowercase()
        .as_str()
    {
        "futures" | "future" | "perpetual" => MarketType::Perpetual,
        _ => MarketType::Spot,
    };
    let (fallback_base, fallback_quote) = split_whitebit_symbol(&exchange_symbol, market_type)
        .ok_or_else(|| parse_error(exchange_id.clone(), "market missing assets", value))?;
    let base_asset = value
        .get("stock")
        .or_else(|| value.get("stock_currency"))
        .or_else(|| value.get("base_ccy"))
        .or_else(|| value.get("base_currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("money")
        .or_else(|| value.get("money_currency"))
        .or_else(|| value.get("quote_ccy"))
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
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            exchange_symbol.clone(),
        )
        .map_err(validation_error)?,
    };
    let price_precision = integer_from_value(
        value
            .get("moneyPrec")
            .or_else(|| value.get("price_precision"))
            .or_else(|| value.get("pricing_decimal")),
    );
    let quantity_precision = integer_from_value(
        value
            .get("stockPrec")
            .or_else(|| value.get("amount_precision"))
            .or_else(|| value.get("trading_decimal")),
    );
    let tradable = value
        .get("tradesEnabled")
        .or_else(|| value.get("is_trading_available"))
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
        min_quantity: string_or_number(value.get("minAmount").or_else(|| value.get("min_amount"))),
        max_quantity: string_or_number(value.get("max_amount")),
        min_notional: string_or_number(value.get("minTotal").or_else(|| value.get("min_notional"))),
        max_notional: string_or_number(value.get("maxTotal")),
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
                message: "whitebit order book request requires canonical_symbol".to_string(),
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
        .get("update_id")
        .or_else(|| book.get("update_id"))
        .or_else(|| data.get("last"))
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
        .and_then(timestamp_to_utc);
    Ok(snapshot)
}

pub fn normalize_whitebit_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
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

fn split_whitebit_symbol(symbol: &str, market_type: MarketType) -> Option<(String, String)> {
    if market_type == MarketType::Perpetual {
        return symbol
            .strip_suffix("_PERP")
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), "USDT".to_string()));
    }
    if let Some((base, quote)) = symbol.split_once('_') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
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
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    if let Some(number) = value.as_f64() {
        return Some(number as i64);
    }
    value
        .as_str()?
        .parse::<f64>()
        .ok()
        .map(|value| value as i64)
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

fn timestamp_to_utc(timestamp: i64) -> Option<DateTime<Utc>> {
    if timestamp > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(timestamp)
    } else {
        DateTime::<Utc>::from_timestamp(timestamp, 0)
    }
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
