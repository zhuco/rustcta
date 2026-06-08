use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderType, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "exchangeInfo missing symbols", value))?;
    symbols
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteAsset")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
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
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let notional_filter =
        find_filter(&filters, "MIN_NOTIONAL").or_else(|| find_filter(&filters, "NOTIONAL"));
    let exchange_order_types = value
        .get("orderTypes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("TRADING");
    let trading = status.eq_ignore_ascii_case("TRADING");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(price_filter.and_then(|filter| filter.get("tickSize"))),
        quantity_increment: string_or_number(lot_filter.and_then(|filter| filter.get("stepSize"))),
        min_price: string_or_number(price_filter.and_then(|filter| filter.get("minPrice"))),
        max_price: string_or_number(price_filter.and_then(|filter| filter.get("maxPrice"))),
        min_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("minQty"))),
        max_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("maxQty"))),
        min_notional: string_or_number(
            notional_filter
                .and_then(|filter| filter.get("minNotional"))
                .or_else(|| notional_filter.and_then(|filter| filter.get("notional"))),
        ),
        max_notional: string_or_number(
            notional_filter.and_then(|filter| filter.get("maxNotional")),
        ),
        price_precision: precision_from_step_text(
            price_filter
                .and_then(|filter| filter.get("tickSize"))
                .and_then(text_from_value),
        )
        .or_else(|| value.get("pricePrecision").and_then(value_as_u32)),
        quantity_precision: precision_from_step_text(
            lot_filter
                .and_then(|filter| filter.get("stepSize"))
                .and_then(text_from_value),
        )
        .or_else(|| {
            value
                .get("baseAssetPrecision")
                .or_else(|| value.get("quantityPrecision"))
                .and_then(value_as_u32)
        }),
        supports_market_orders: trading && has_or_unlisted(&exchange_order_types, "MARKET"),
        supports_limit_orders: trading && has_or_unlisted(&exchange_order_types, "LIMIT"),
        supports_post_only: trading && has_or_unlisted(&exchange_order_types, "LIMIT_MAKER"),
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids").or_else(|| value.get("b")))?;
    let asks = parse_levels(exchange_id, value.get("asks").or_else(|| value.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binanceus order book request requires canonical_symbol".to_string(),
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
        .get("lastUpdateId")
        .or_else(|| value.get("u"))
        .and_then(Value::as_u64);
    snapshot.exchange_timestamp = value
        .get("E")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_binanceus_symbol(symbol: &str) -> ExchangeApiResult<String> {
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
        _ => 20,
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

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
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
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn non_zero_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.trim_matches('0').trim_matches('.').is_empty() {
        None
    } else {
        Some(value)
    }
}

fn text_from_value(value: &Value) -> Option<&str> {
    value.as_str()
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            let text = text_from_value(value)
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            decimal_text_to_f64(&text)
        })
        .transpose()
}

pub(super) fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Binance.US decimal value {value}: {error}"),
        })
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(raw) => Some(raw.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Binance.US side {side}"),
        }),
    }
}

pub(super) fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    match order_type.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "LIMIT_MAKER" => OrderType::PostOnly,
        "LIMIT" => match tif.map(str::to_ascii_uppercase).as_deref() {
            Some("IOC") => OrderType::IOC,
            Some("FOK") => OrderType::FOK,
            Some("GTX") => OrderType::PostOnly,
            _ => OrderType::Limit,
        },
        _ => OrderType::Limit,
    }
}

pub(super) fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field).and_then(value_as_i64))
        .find_map(DateTime::<Utc>::from_timestamp_millis)
}

pub(super) fn average_price_text(value: &Value) -> Option<String> {
    let filled = string_or_number(value.get("executedQty").or_else(|| value.get("z")))?;
    if filled.parse::<f64>().ok()? <= 0.0 {
        return None;
    }
    let quote = string_or_number(value.get("cummulativeQuoteQty").or_else(|| value.get("Z")))?;
    let average = quote.parse::<f64>().ok()? / filled.parse::<f64>().ok()?;
    if average > 0.0 {
        Some(average.to_string())
    } else {
        None
    }
}

fn precision_from_step_text(step: Option<&str>) -> Option<u32> {
    let step = step?;
    if step == "0" || step.starts_with("0E") {
        return Some(0);
    }
    let mantissa = step.split(['e', 'E']).next().unwrap_or(step);
    Some(
        mantissa
            .trim_end_matches('0')
            .split('.')
            .nth(1)
            .map(|fraction| fraction.len() as u32)
            .unwrap_or(0),
    )
}

fn has_or_unlisted(order_types: &[Value], expected: &str) -> bool {
    order_types.is_empty()
        || order_types
            .iter()
            .any(|order_type| order_type.as_str() == Some(expected))
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
