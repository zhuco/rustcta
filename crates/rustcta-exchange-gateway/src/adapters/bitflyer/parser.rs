use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_markets(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer markets response missing array",
            value,
        )
    })?;
    markets
        .iter()
        .map(|market| parse_market(exchange_id, market))
        .collect()
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    parse_markets(exchange_id, value)
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let product_code = required_str(exchange_id, value, "product_code")?.to_ascii_uppercase();
    let market_type = market_type_from_product_code(&product_code);
    let (base_asset, quote_asset) = canonical_assets_from_product_code(&product_code)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, product_code)
            .map_err(validation_error)?,
    };
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset: quote_asset.clone(),
        price_increment: Some(default_price_increment(&quote_asset).to_string()),
        quantity_increment: Some("0.00000001".to_string()),
        min_price: None,
        max_price: None,
        min_quantity: Some("0.001".to_string()),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: Some(precision_from_step(default_price_increment(&quote_asset))),
        quantity_precision: Some(8),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitFlyer order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_bitflyer_datetime);
    Ok(snapshot)
}

pub fn normalize_product_code(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitFlyer product_code must not be empty".to_string(),
        });
    }
    Ok(trimmed.replace(['/', '-'], "_").to_ascii_uppercase())
}

pub fn market_type_from_product_code(product_code: &str) -> MarketType {
    if product_code.to_ascii_uppercase().starts_with("FX_") {
        MarketType::Margin
    } else {
        MarketType::Spot
    }
}

pub fn canonical_assets_from_product_code(
    product_code: &str,
) -> ExchangeApiResult<(String, String)> {
    let normalized = normalize_product_code(product_code)?;
    let pair = normalized.strip_prefix("FX_").unwrap_or(&normalized);
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer bitFlyer canonical symbol from {product_code}"),
        })?;
    if base.trim().is_empty() || quote.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid bitFlyer product_code {product_code}"),
        });
    }
    Ok((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
}

pub(super) fn parse_bitflyer_datetime(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .ok()
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = decimal_value_to_f64(level.get("price"))?
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = decimal_value_to_f64(level.get("size"))?
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level size", level))?;
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

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid bitFlyer decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
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

fn default_price_increment(quote_asset: &str) -> &'static str {
    if quote_asset.eq_ignore_ascii_case("JPY") {
        "1"
    } else {
        "0.01"
    }
}

fn precision_from_step(step: &str) -> u32 {
    step.trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or(0)
}
