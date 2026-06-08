use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    symbol_items(value)
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols response missing list", value))?
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, value))
        .filter(|result| {
            result
                .as_ref()
                .map(|rule| rule.quote_asset == "USDT")
                .unwrap_or(true)
        })
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_bydfi_symbol(required_str(exchange_id, value, "symbol")?)?;
    let (fallback_base, fallback_quote) = split_bydfi_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("baseAsset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteAsset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let price_precision = integer_from_value(value.get("pricePrecision"));
    let quantity_precision = integer_from_value(
        value
            .get("volumePrecision")
            .or_else(|| value.get("basePrecision")),
    );
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("NORMAL")
        .to_ascii_uppercase();
    let tradable = matches!(status.as_str(), "NORMAL" | "TRADING" | "OPEN");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                exchange_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: quantity_precision.map(increment_from_precision),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("limitMinQty")
                .or_else(|| value.get("marketMinQty")),
        ),
        max_quantity: string_or_number(
            value
                .get("limitMaxQty")
                .or_else(|| value.get("marketMaxQty")),
        ),
        min_notional: None,
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BYDFi order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("lastUpdateId")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()));
    snapshot.exchange_timestamp = data
        .get("E")
        .or_else(|| data.get("time"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bydfi_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if trimmed.contains('-') {
        return Ok(trimmed.replace('/', "-").replace('_', "-"));
    }
    let compact = trimmed.replace(['/', '_'], "");
    if let Some((base, quote)) = split_bydfi_symbol(&compact) {
        return Ok(format!("{base}-{quote}"));
    }
    Ok(compact)
}

pub fn normalize_depth(depth: u32) -> String {
    match depth {
        0..=5 => "5".to_string(),
        6..=10 => "10".to_string(),
        11..=20 => "20".to_string(),
        21..=50 => "50".to_string(),
        51..=100 => "100".to_string(),
        101..=500 => "500".to_string(),
        _ => "1000".to_string(),
    }
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

pub(super) fn symbol_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.get("symbols")
        .or_else(|| data.get("list"))
        .or_else(|| data.get("contracts"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .or_else(|| {
            if data.is_object() && data.get("symbol").is_some() {
                Some(std::slice::from_ref(data))
            } else {
                None
            }
        })
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
        .map(|level| parse_level(exchange_id, level))
        .collect()
}

fn parse_level(exchange_id: &ExchangeId, level: &Value) -> ExchangeApiResult<OrderBookLevel> {
    if let Some(array) = level.as_array() {
        if array.len() == 1 && array[0].is_object() {
            return parse_level(exchange_id, &array[0]);
        }
        let price = array
            .first()
            .and_then(number_from_value)
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
        let quantity = array
            .get(1)
            .and_then(number_from_value)
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
        return OrderBookLevel::new(price, quantity).map_err(validation_error);
    }
    let price = decimal_as_f64(level.get("price"))
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid object level price", level))?;
    let quantity = decimal_as_f64(level.get("amount").or_else(|| level.get("qty")))
        .ok_or_else(|| parse_error(exchange_id.clone(), "invalid object level qty", level))?;
    OrderBookLevel::new(price, quantity).map_err(validation_error)
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

pub(super) fn split_bydfi_symbol(symbol: &str) -> Option<(String, String)> {
    let normalized = symbol.trim().to_ascii_uppercase().replace(['/', '_'], "-");
    if let Some((base, quote)) = normalized.split_once('-') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    const QUOTES: [&str; 8] = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "TRY", "BNB"];
    let compact = normalized.replace('-', "");
    QUOTES.iter().find_map(|quote| {
        compact
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

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
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
        Some("LONG") | Some("BUY") => rustcta_types::PositionSide::Long,
        Some("SHORT") | Some("SELL") => rustcta_types::PositionSide::Short,
        Some("BOTH") | Some("NET") | Some("ONEWAY") | Some("ONE_WAY") => {
            rustcta_types::PositionSide::Net
        }
        _ => rustcta_types::PositionSide::None,
    }
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message,
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn number_from_value(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .and_then(|value| u32::try_from(value).ok())
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}
