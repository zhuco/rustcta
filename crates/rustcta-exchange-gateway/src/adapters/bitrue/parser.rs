use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
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
    let (fallback_base, fallback_quote) = split_bitrue_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("baseAsset")
        .or_else(|| value.get("baseCoin"))
        .or_else(|| value.get("multiplierCoin"))
        .or_else(|| value.get("asset"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteAsset")
        .or_else(|| value.get("quoteCoin"))
        .or_else(|| value.get("currency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let price_precision = integer_from_value(value.get("pricePrecision"));
    let quantity_precision = integer_from_value(value.get("quantityPrecision"));
    let status = value
        .get("status")
        .or_else(|| value.get("state"))
        .map(|value| value.to_string().trim_matches('"').to_string())
        .unwrap_or_else(|| "TRADING".to_string());
    let tradable = matches!(status.as_str(), "1")
        || !matches!(
            status.to_ascii_uppercase().as_str(),
            "0" | "HALT" | "SUSPEND" | "OFFLINE" | "CLOSE" | "DISABLED"
        );
    let price_filter = filter_by_type(value, "PRICE_FILTER");
    let lot_size_filter = filter_by_type(value, "LOT_SIZE");

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
        price_increment: string_or_number(value.get("tickSize"))
            .or_else(|| string_or_number(price_filter.and_then(|filter| filter.get("tickSize"))))
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_or_number(value.get("stepSize"))
            .or_else(|| string_or_number(value.get("size")))
            .or_else(|| string_or_number(lot_size_filter.and_then(|filter| filter.get("stepSize"))))
            .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: string_or_number(price_filter.and_then(|filter| filter.get("minPrice"))),
        max_price: string_or_number(price_filter.and_then(|filter| filter.get("maxPrice"))),
        min_quantity: string_or_number(
            value
                .get("minQty")
                .or_else(|| value.get("minOrderVolume"))
                .or_else(|| value.get("tradeMinLimit"))
                .or_else(|| value.get("minTradeQty"))
                .or_else(|| lot_size_filter.and_then(|filter| filter.get("minQty"))),
        ),
        max_quantity: string_or_number(
            value
                .get("maxQty")
                .or_else(|| value.get("maxLimitVolume"))
                .or_else(|| value.get("maxMarketVolume"))
                .or_else(|| value.get("maxTradeQty"))
                .or_else(|| lot_size_filter.and_then(|filter| filter.get("maxQty"))),
        ),
        min_notional: string_or_number(
            value
                .get("minNotional")
                .or_else(|| value.get("minOrderMoney"))
                .or_else(|| value.get("minVal")),
        ),
        max_notional: string_or_number(
            value
                .get("maxLimitMoney")
                .or_else(|| value.get("maxMarketMoney")),
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
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
    let data = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitrue order book request requires canonical_symbol".to_string(),
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
        .or_else(|| data.get("T"))
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bitrue_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if market_type == MarketType::Perpetual {
        if trimmed.starts_with("E-") {
            return Ok(trimmed);
        }
        if trimmed.contains('-') {
            return Ok(format!("E-{trimmed}"));
        }
        let compact = trimmed.replace(['/', '_'], "");
        let (base, quote) =
            split_compact_symbol(&compact).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot normalize Bitrue futures symbol {symbol}"),
            })?;
        return Ok(format!("E-{base}-{quote}"));
    }
    if trimmed.contains('-') {
        return Ok(trimmed.replace('-', ""));
    }
    if trimmed.contains('/') || trimmed.contains('_') {
        return Ok(trimmed.replace(['/', '_'], ""));
    }
    if split_compact_symbol(&trimmed).is_some() {
        return Ok(trimmed);
    }
    let compact = trimmed.replace(['/', '_'], "");
    let (base, quote) =
        split_compact_symbol(&compact).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot normalize Bitrue symbol {symbol}"),
        })?;
    Ok(format!("{base}{quote}"))
}

pub fn normalize_depth(depth: u32, market_type: MarketType) -> u32 {
    match market_type {
        MarketType::Spot => depth.clamp(1, 100),
        _ => match depth {
            0..=5 => 5,
            6..=10 => 10,
            11..=20 => 20,
            21..=50 => 50,
            51..=100 => 100,
            101..=500 => 500,
            _ => 1000,
        },
    }
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn symbol_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.get("symbols")
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

pub(super) fn split_bitrue_symbol(symbol: &str) -> Option<(String, String)> {
    let symbol = symbol.strip_prefix("E-").unwrap_or(symbol);
    if let Some((base, quote)) = symbol.split_once('-') {
        return (!base.is_empty() && !quote.is_empty())
            .then(|| (base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
    }
    split_compact_symbol(symbol)
}

fn filter_by_type<'a>(value: &'a Value, filter_type: &str) -> Option<&'a Value> {
    value.get("filters")?.as_array()?.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
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

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
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
        Some("BOTH") | Some("NET") => rustcta_types::PositionSide::Net,
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
