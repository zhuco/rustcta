use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .get("perp_markets")
        .or_else(|| value.get("perpMarkets"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Mango Markets group snapshot missing perp_markets",
                value,
            )
        })?;

    markets
        .iter()
        .filter(|market| {
            market
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .map(|market| parse_perp_market(exchange_id, market))
        .collect()
}

fn parse_perp_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "name")?;
    let base_asset = value
        .get("base")
        .or_else(|| value.get("base_symbol"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .or_else(|| base_from_perp_symbol(symbol_text))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Mango Markets perp market missing base asset",
                value,
            )
        })?;
    let quote_asset = value
        .get("quote")
        .or_else(|| value.get("quote_symbol"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| "USDC".to_string());
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            symbol_text,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["price_tick"])
        .or_else(|| decimal_path(value, &["tick_size"]))
        .or_else(|| decimal_path(value, &["quote_lot_size"]));
    let quantity_increment = decimal_path(value, &["base_lot_size"])
        .or_else(|| decimal_path(value, &["size_tick"]))
        .or_else(|| decimal_path(value, &["min_order_size"]));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(value, &["min_order_size"]),
        max_quantity: None,
        min_notional: decimal_path(value, &["min_notional"]),
        max_notional: None,
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn base_from_perp_symbol(symbol: &str) -> Option<String> {
    symbol
        .trim()
        .strip_suffix("-PERP")
        .or_else(|| symbol.trim().strip_suffix("_PERP"))
        .map(|base| base.to_ascii_uppercase())
}

fn decimal_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn precision_hint(value: &str) -> Option<u32> {
    if let Ok(number) = value.parse::<u32>() {
        return Some(number);
    }
    value
        .trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("Mango Markets response missing {field}"),
            value,
        )
    })
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
