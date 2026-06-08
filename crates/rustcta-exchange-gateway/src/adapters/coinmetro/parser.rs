use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

const QUOTE_ASSETS: &[&str] = &["USDT", "USDC", "EUR", "USD", "GBP", "AUD", "BTC"];

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let items = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinmetro markets response is not an array",
                value,
            )
        })?;
    items
        .iter()
        .map(|item| parse_symbol_rule(exchange_id, item))
        .collect()
}

pub fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let market = normalize_coinmetro_symbol(required_str(exchange_id, value, "pair")?)?;
    let (base_asset, quote_asset) = symbol_assets(value, &market)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, market)
            .map_err(validation_error)?,
    };
    let price_precision = value
        .get("precision")
        .and_then(value_as_u64)
        .map(|value| value as u32);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_precision.map(step_from_precision),
        quantity_increment: None,
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision,
        quantity_precision: None,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value
        .get("book")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_object_levels(exchange_id, book.get("bid"), true)?;
    let asks = parse_object_levels(exchange_id, book.get("ask"), false)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinmetro order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = book.get("seqNumber").and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_coinmetro_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinmetro symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub(super) fn infer_assets_from_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = normalize_coinmetro_symbol(symbol)?;
    for quote in QUOTE_ASSETS {
        if normalized.len() > quote.len() && normalized.ends_with(quote) {
            let base = normalized[..normalized.len() - quote.len()].to_string();
            return Ok((base, (*quote).to_string()));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot infer coinmetro base/quote assets for symbol {normalized}"),
    })
}

pub(super) fn symbol_scope_assets(symbol: &SymbolScope) -> ExchangeApiResult<(String, String)> {
    if let Some(canonical) = &symbol.canonical_symbol {
        return Ok((
            canonical.base_asset().to_string(),
            canonical.quote_asset().to_string(),
        ));
    }
    infer_assets_from_symbol(&symbol.exchange_symbol.symbol)
}

fn symbol_assets(value: &Value, market: &str) -> ExchangeApiResult<(String, String)> {
    if let (Some(base), Some(quote)) = (
        value.get("base").and_then(Value::as_str),
        value.get("quote").and_then(Value::as_str),
    ) {
        return Ok((base.to_ascii_uppercase(), quote.to_ascii_uppercase()));
    }
    infer_assets_from_symbol(market)
}

fn parse_object_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    descending: bool,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_object).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "coinmetro order book missing object levels",
            &Value::Null,
        )
    })?;
    let mut parsed = Vec::new();
    for (price, quantity) in levels {
        let price = price.parse::<f64>().map_err(validation_error)?;
        let quantity = number_from_value(quantity)
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", quantity))?;
        if price > 0.0 && quantity > 0.0 {
            parsed.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
    }
    if descending {
        parsed.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        parsed.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(parsed)
}

fn step_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("coinmetro response missing field {field}"),
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

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    number_from_value(value)
        .map(Some)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid coinmetro decimal value {value}"),
        })
}

pub(super) fn format_decimal(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
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
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
