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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let products = value
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "products response missing array",
                value,
            )
        })?;
    products
        .iter()
        .map(|product| parse_symbol_rule(exchange_id, market_type, product))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    requested_market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let product_id = required_str(exchange_id, value, "product_id")?.to_ascii_uppercase();
    let market_type = if requested_market_type == MarketType::Perpetual {
        MarketType::Perpetual
    } else if value
        .get("product_type")
        .and_then(Value::as_str)
        .is_some_and(|product_type| product_type.eq_ignore_ascii_case("FUTURE"))
    {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    };
    let base_asset = value
        .get("base_currency_id")
        .or_else(|| value.get("base_display_symbol"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_coinbase_symbol(&product_id).map(|(base, _)| base))
        .ok_or_else(|| parse_error(exchange_id.clone(), "product missing base asset", value))?;
    let quote_asset = value
        .get("quote_currency_id")
        .or_else(|| value.get("quote_display_symbol"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| split_coinbase_symbol(&product_id).map(|(_, quote)| quote))
        .unwrap_or_else(|| "USDC".to_string());
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, product_id.clone())
            .map_err(validation_error)?,
    };
    let disabled = bool_field(value, "is_disabled")
        || bool_field(value, "trading_disabled")
        || bool_field(value, "view_only")
        || bool_field(value, "cancel_only");
    let limit_only = bool_field(value, "limit_only");
    let post_only = bool_field(value, "post_only");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("price_increment")),
        quantity_increment: string_or_number(value.get("base_increment")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("base_min_size")),
        max_quantity: string_or_number(value.get("base_max_size")),
        min_notional: string_or_number(value.get("quote_min_size")),
        max_notional: string_or_number(value.get("quote_max_size")),
        price_precision: string_or_number(value.get("price_increment"))
            .as_deref()
            .and_then(precision_from_increment),
        quantity_precision: string_or_number(value.get("base_increment"))
            .as_deref()
            .and_then(precision_from_increment),
        supports_market_orders: !disabled && !limit_only && !post_only,
        supports_limit_orders: !disabled,
        supports_post_only: !disabled,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let pricebook = value.get("pricebook").unwrap_or(value);
    let bids = parse_levels(exchange_id, pricebook.get("bids"))?;
    let asks = parse_levels(exchange_id, pricebook.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value.get("last").and_then(value_as_u64);
    snapshot.exchange_timestamp = pricebook
        .get("time")
        .and_then(Value::as_str)
        .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.with_timezone(&Utc));
    Ok(snapshot)
}

pub fn normalize_coinbase_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace('/', "-").to_ascii_uppercase();
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
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("size")
                .or_else(|| level.get("quantity"))
                .and_then(number_from_value)
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
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid decimal {value}: {error}"),
        })
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn parse_rfc3339(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc))
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

fn split_coinbase_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('-').filter(|part| !part.is_empty());
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts
        .find(|part| *part != "PERP" && *part != "INTX")
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "USDC".to_string());
    Some((base, quote))
}

fn bool_field(value: &Value, field: &str) -> bool {
    value.get(field).and_then(Value::as_bool).unwrap_or(false)
}

fn precision_from_increment(increment: &str) -> Option<u32> {
    let text = increment.trim().trim_end_matches('0');
    text.split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
