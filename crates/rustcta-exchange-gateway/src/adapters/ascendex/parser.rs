use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_spot_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    symbol_items(value, "products response missing data")?
        .iter()
        .map(|item| parse_spot_symbol_rule(exchange_id, item))
        .collect()
}

pub fn parse_futures_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    symbol_items(value, "futures contracts response missing data")?
        .iter()
        .map(|item| parse_futures_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_spot_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_ascendex_symbol(&raw_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("baseAsset")
        .or_else(|| value.get("baseCurrency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteAsset")
        .or_else(|| value.get("quoteCurrency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let price_precision = integer_from_value(value.get("priceScale"));
    let quantity_precision = integer_from_value(value.get("qtyScale"));
    let symbol = symbol_scope(
        exchange_id,
        MarketType::Spot,
        &base_asset,
        &quote_asset,
        raw_symbol,
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize"))
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_or_number(value.get("lotSize"))
            .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minQty")),
        max_quantity: string_or_number(value.get("maxQty")),
        min_notional: string_or_number(value.get("minNotional")),
        max_notional: string_or_number(value.get("maxNotional")),
        price_precision,
        quantity_precision,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_futures_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_futures_symbol(&raw_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("underlying")
        .or_else(|| value.get("baseAsset"))
        .and_then(Value::as_str)
        .map(|value| value.trim_end_matches("-PERP").to_ascii_uppercase())
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("settlementAsset")
        .or_else(|| value.get("quoteAsset"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let price_filter = value.get("priceFilter").unwrap_or(value);
    let lot_filter = value.get("lotSizeFilter").unwrap_or(value);
    let price_increment = string_or_number(price_filter.get("tickSize"));
    let quantity_increment = string_or_number(lot_filter.get("lotSize"));
    let symbol = symbol_scope(
        exchange_id,
        MarketType::Perpetual,
        &base_asset,
        &quote_asset,
        raw_symbol,
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: string_or_number(price_filter.get("minPrice")),
        max_price: string_or_number(price_filter.get("maxPrice")),
        min_quantity: string_or_number(lot_filter.get("minQty")),
        max_quantity: string_or_number(lot_filter.get("maxQty")),
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_step(price_increment.as_deref()),
        quantity_precision: precision_from_step(quantity_increment.as_deref()),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let book = data.get("data").unwrap_or(data);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "AscendEX order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = book.get("seqnum").and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("ts")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['_', '-'], "/").to_ascii_uppercase();
    if normalized.is_empty() || !normalized.contains('/') {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot normalize AscendEX spot symbol {symbol}"),
        });
    }
    Ok(normalized)
}

pub fn normalize_futures_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['_', '/'], "-").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if normalized.ends_with("-PERP") {
        Ok(normalized)
    } else {
        Ok(format!("{normalized}-PERP"))
    }
}

pub fn normalize_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    match market_type {
        MarketType::Spot => normalize_spot_symbol(symbol),
        MarketType::Perpetual => normalize_futures_symbol(symbol),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "ascendex.symbol.market_type",
        }),
    }
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn symbol_items<'a>(value: &'a Value, message: &str) -> ExchangeApiResult<&'a [Value]> {
    let data = data_payload(value);
    data.get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(ExchangeId::unchecked("ascendex"), message, value))
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing bid/ask levels",
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

fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    base_asset: &str,
    quote_asset: &str,
    exchange_symbol: String,
) -> ExchangeApiResult<SymbolScope> {
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(base_asset, quote_asset).map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
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

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

pub(super) fn split_ascendex_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('/') {
        return (!base.is_empty() && !quote.is_empty())
            .then(|| (base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
    }
    split_compact_symbol(&symbol.replace(['-', '_'], ""))
}

pub(super) fn split_futures_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some(base) = symbol.strip_suffix("-PERP") {
        return Some((base.to_ascii_uppercase(), "USDT".to_string()));
    }
    split_compact_symbol(&symbol.replace(['-', '/', '_'], ""))
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 8] = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "TRY", "BNB"];
    let upper = symbol.to_ascii_uppercase();
    QUOTES.iter().find_map(|quote| {
        upper
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

pub(super) fn parse_side(
    exchange_id: &ExchangeId,
    side: &str,
) -> ExchangeApiResult<rustcta_types::OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(rustcta_types::OrderSide::Buy),
        "SELL" => Ok(rustcta_types::OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported order side",
            &Value::String(side.to_string()),
        )),
    }
}

pub(super) fn parse_position_side(value: Option<&str>) -> rustcta_types::PositionSide {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("LONG") => rustcta_types::PositionSide::Long,
        Some("SHORT") => rustcta_types::PositionSide::Short,
        Some("NET") | Some("BOTH") => rustcta_types::PositionSide::Net,
        _ => rustcta_types::PositionSide::None,
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

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?;
    let fractional = step.split('.').nth(1)?.trim_end_matches('0');
    Some(fractional.len() as u32)
}
