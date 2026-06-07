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
    data_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BloFin instruments response missing data",
                value,
            )
        })?
        .iter()
        .filter(|item| {
            item.get("contractType")
                .and_then(Value::as_str)
                .is_none_or(|contract_type| contract_type.eq_ignore_ascii_case("linear"))
        })
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let inst_id = required_str(exchange_id, value, "instId")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_blofin_symbol(&inst_id)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("baseCurrency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteCurrency")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let tradable = value
        .get("state")
        .and_then(Value::as_str)
        .is_none_or(|state| state.eq_ignore_ascii_case("live"));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                inst_id,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("lotSize")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minSize")),
        max_quantity: string_or_number(value.get("maxLimitSize")),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(value.get("tickSize")).and_then(decimal_precision),
        quantity_precision: string_or_number(value.get("lotSize")).and_then(decimal_precision),
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
    let book = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let bids = parse_levels(exchange_id, book.get("bids").or_else(|| book.get("b")))?;
    let asks = parse_levels(exchange_id, book.get("asks").or_else(|| book.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "blofin order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = book
        .get("ts")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_blofin_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BloFin symbol must not be empty".to_string(),
        });
    }
    if trimmed.contains('-') {
        return Ok(trimmed.replace('/', "-").replace('_', "-"));
    }
    split_blofin_symbol(&trimmed)
        .map(|(base, quote)| format!("{base}-{quote}"))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!(
                "BloFin symbol {trimmed} must be BASE-QUOTE or use a known quote suffix"
            ),
        })
}

pub fn normalize_depth(depth: u32) -> String {
    depth.clamp(1, 100).to_string()
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

pub(super) fn data_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.as_array().map(Vec::as_slice)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BloFin order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "BloFin level must be array", level)
            })?;
            let price = array
                .first()
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(decimal_as_f64)
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
            &format!("BloFin payload missing field {field}"),
            value,
        )
    })
}

pub(super) fn split_blofin_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 7] = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "TRY"];
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
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
}

pub(super) fn decimal_as_f64(value: &Value) -> Option<f64> {
    string_or_number(Some(value))?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn parse_side(
    exchange_id: &ExchangeId,
    side: &str,
) -> ExchangeApiResult<rustcta_types::OrderSide> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(rustcta_types::OrderSide::Buy),
        "sell" => Ok(rustcta_types::OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported BloFin order side",
            &Value::String(side.to_string()),
        )),
    }
}

pub(super) fn parse_position_side(side: Option<&str>) -> rustcta_types::PositionSide {
    match side.unwrap_or("net").to_ascii_lowercase().as_str() {
        "long" => rustcta_types::PositionSide::Long,
        "short" => rustcta_types::PositionSide::Short,
        "net" => rustcta_types::PositionSide::Net,
        _ => rustcta_types::PositionSide::None,
    }
}

pub(super) fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim().trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

pub(super) fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
