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
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = result_array(exchange_id, value, "deribit instruments response")?;
    instruments
        .iter()
        .map(|instrument| parse_symbol_rule(exchange_id, instrument))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let instrument_name = required_str(exchange_id, value, "instrument_name")?.to_string();
    let market_type = market_type_from_kind(value.get("kind").and_then(Value::as_str))
        .or_else(|| market_type_from_instrument(&instrument_name))
        .unwrap_or(MarketType::Futures);
    let base_asset = value
        .get("base_currency")
        .or_else(|| value.get("base_currency_long"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| base_from_instrument(&instrument_name));
    let quote_asset = value
        .get("quote_currency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "USD".to_string());
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, &instrument_name)
            .map_err(validation_error)?,
    };
    let tradable = value
        .get("is_active")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let tick_size = string_or_number(value.get("tick_size"));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: tick_size.clone(),
        quantity_increment: string_or_number(value.get("contract_size"))
            .or_else(|| Some("1".to_string())),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_trade_amount")),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_increment(tick_size.as_deref()),
        quantity_precision: Some(0),
        supports_market_orders: tradable && market_type != MarketType::Option,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: matches!(market_type, MarketType::Perpetual | MarketType::Futures),
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let result = value.get("result").unwrap_or(value);
    let bids = parse_levels(exchange_id, result.get("bids"), true)?;
    let asks = parse_levels(exchange_id, result.get("asks"), false)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deribit order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = result
        .get("change_id")
        .or_else(|| result.get("last_change_id"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = result
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_deribit_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let raw = symbol.exchange_symbol.symbol.trim();
    if raw.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "deribit symbol cannot be empty".to_string(),
        });
    }
    Ok(raw.replace('/', "-").to_ascii_uppercase())
}

pub fn currency_for_symbol(symbol: &SymbolScope) -> String {
    symbol
        .canonical_symbol
        .as_ref()
        .map(|symbol| symbol.base_asset().to_ascii_uppercase())
        .unwrap_or_else(|| base_from_instrument(&symbol.exchange_symbol.symbol))
}

pub fn market_type_from_instrument(symbol: &str) -> Option<MarketType> {
    let normalized = symbol.to_ascii_uppercase();
    if normalized.ends_with("-PERPETUAL") {
        Some(MarketType::Perpetual)
    } else if normalized.contains("-C") || normalized.contains("-P") {
        Some(MarketType::Option)
    } else if normalized.split('-').count() >= 2 {
        Some(MarketType::Futures)
    } else {
        None
    }
}

pub fn symbol_scope_from_instrument(
    exchange_id: &ExchangeId,
    instrument_name: &str,
) -> ExchangeApiResult<SymbolScope> {
    let market_type = market_type_from_instrument(instrument_name).unwrap_or(MarketType::Futures);
    let base = base_from_instrument(instrument_name);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, "USD").map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, instrument_name)
            .map_err(validation_error)?,
    })
}

pub fn base_from_instrument(symbol: &str) -> String {
    symbol
        .split('-')
        .next()
        .unwrap_or(symbol)
        .trim()
        .to_ascii_uppercase()
}

fn market_type_from_kind(kind: Option<&str>) -> Option<MarketType> {
    match kind?.to_ascii_lowercase().as_str() {
        "option" => Some(MarketType::Option),
        "future" | "future_combo" => Some(MarketType::Futures),
        _ => None,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    bids: bool,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "deribit order book missing price levels",
            &Value::Null,
        )
    })?;
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let array = level
            .as_array()
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid deribit book level", level))?;
        let price = array.first().and_then(decimal_as_f64).ok_or_else(|| {
            parse_error(exchange_id.clone(), "invalid deribit level price", level)
        })?;
        let quantity = array.get(1).and_then(decimal_as_f64).ok_or_else(|| {
            parse_error(exchange_id.clone(), "invalid deribit level amount", level)
        })?;
        if quantity > 0.0 {
            parsed.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
    }
    if bids {
        parsed.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        parsed.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(parsed)
}

pub(super) fn result_array<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    message: &str,
) -> ExchangeApiResult<&'a [Value]> {
    value
        .get("result")
        .unwrap_or(value)
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| parse_error(exchange_id.clone(), message, value))
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("deribit response missing {field}"),
            value,
        )
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() && text != "null" => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn text(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(decimal_as_f64)
}

pub(super) fn decimal_as_f64(value: &Value) -> Option<f64> {
    string_or_number(Some(value))?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_f64().map(|value| value as i64))
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}

pub(super) fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str()?.parse::<u64>().ok())
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(raw.clone()),
        occurred_at: Utc::now(),
    })
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn precision_from_increment(value: Option<&str>) -> Option<u32> {
    let value = value?;
    let (_, decimals) = value.split_once('.')?;
    Some(decimals.trim_end_matches('0').len() as u32)
}
