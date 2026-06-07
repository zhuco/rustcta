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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    symbol_items(value)
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols response missing list", value))?
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, market_type, value))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_bitunix_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("base")
        .or_else(|| value.get("baseAsset"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quote")
        .or_else(|| value.get("quoteAsset"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let price_precision = integer_from_value(value.get("quotePrecision"));
    let quantity_precision = integer_from_value(
        value
            .get("basePrecision")
            .or_else(|| value.get("quantityPrecision")),
    );
    let status = value
        .get("symbolStatus")
        .or_else(|| value.get("status"))
        .or_else(|| value.get("isOpen"))
        .and_then(value_as_status)
        .unwrap_or_else(|| "OPEN".to_string());
    let tradable = matches!(
        status.to_ascii_uppercase().as_str(),
        "OPEN" | "1" | "TRUE" | "TRADING"
    ) && value
        .get("isApiSupported")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: quantity_precision.map(increment_from_precision),
        min_price: string_or_number(value.get("minPrice")),
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("minTradeVolume")
                .or_else(|| value.get("minVolume")),
        ),
        max_quantity: string_or_number(
            value
                .get("maxLimitOrderVolume")
                .or_else(|| value.get("maxVolume")),
        ),
        min_notional: None,
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: market_type != MarketType::Spot || tradable,
        supports_reduce_only: market_type != MarketType::Spot,
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
                message: "bitunix order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = data
        .get("ts")
        .or_else(|| data.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bitunix_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(trimmed.replace(['/', '_', '-'], ""))
}

pub fn normalize_depth(depth: u32, market_type: MarketType) -> String {
    match market_type {
        MarketType::Spot => depth.clamp(1, 200).to_string(),
        _ => match depth {
            0..=1 => "1".to_string(),
            2..=5 => "5".to_string(),
            6..=15 => "15".to_string(),
            16..=50 => "50".to_string(),
            _ => "max".to_string(),
        },
    }
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn symbol_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.get("list")
        .or_else(|| data.get("symbols"))
        .or_else(|| data.get("coinPairList"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
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
            if let Some(array) = level.as_array() {
                let price = array
                    .first()
                    .and_then(number_from_value)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid price", level))?;
                let quantity = array
                    .get(1)
                    .and_then(number_from_value)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid quantity", level))?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = decimal_as_f64(level.get("price")).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid object level price", level)
            })?;
            let quantity = decimal_as_f64(
                level
                    .get("volume")
                    .or_else(|| level.get("qty"))
                    .or_else(|| level.get("amount")),
            )
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid object level qty", level))?;
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

pub(super) fn split_bitunix_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    let compact = symbol
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '_', '-'], "");
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

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value).filter(|value| !value.trim().is_empty())
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
        "BUY" | "2" => Ok(rustcta_types::OrderSide::Buy),
        "SELL" | "1" => Ok(rustcta_types::OrderSide::Sell),
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
        Some("BOTH") | Some("NET") | Some("ONE_WAY") => rustcta_types::PositionSide::Net,
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

fn value_as_status(value: &Value) -> Option<String> {
    match value {
        Value::Bool(true) => Some("OPEN".to_string()),
        Value::Bool(false) => Some("STOP".to_string()),
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
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
