use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
};
use serde_json::Value;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PositionSourceBoundaryAudit {
    pub boundary: String,
    pub required_sources: Vec<String>,
    pub position_fields: Vec<String>,
    pub reconciliation_required: Vec<String>,
}

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

#[cfg(test)]
pub(super) fn parse_position_source_boundary(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<PositionSourceBoundaryAudit> {
    require_text(exchange_id, value, "exchange", "mango_markets")?;
    require_text(exchange_id, value, "operation", "get_positions")?;
    require_bool(exchange_id, value, "runtime_enabled", false)?;

    let fixture_policy = value.get("fixture_policy").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Mango Markets position source boundary missing fixture_policy",
            value,
        )
    })?;
    require_bool(exchange_id, fixture_policy, "live_call_allowed", false)?;

    Ok(PositionSourceBoundaryAudit {
        boundary: required_str(exchange_id, value, "boundary")?.to_string(),
        required_sources: required_string_array(exchange_id, value, &["source", "requires"])?,
        position_fields: required_string_array(exchange_id, value, &["position_fields"])?,
        reconciliation_required: required_string_array(
            exchange_id,
            value,
            &["reconciliation_required"],
        )?,
    })
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

#[cfg(test)]
fn require_text(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
    expected: &str,
) -> ExchangeApiResult<()> {
    let actual = required_str(exchange_id, value, field)?;
    if actual == expected {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        format!("Mango Markets source boundary {field} expected {expected}, got {actual}"),
        value,
    ))
}

#[cfg(test)]
fn require_bool(
    exchange_id: &ExchangeId,
    value: &Value,
    field: &str,
    expected: bool,
) -> ExchangeApiResult<()> {
    match value.get(field).and_then(Value::as_bool) {
        Some(actual) if actual == expected => Ok(()),
        _ => Err(parse_error(
            exchange_id.clone(),
            format!("Mango Markets source boundary {field} expected {expected}"),
            value,
        )),
    }
}

#[cfg(test)]
fn required_string_array(
    exchange_id: &ExchangeId,
    value: &Value,
    path: &[&str],
) -> ExchangeApiResult<Vec<String>> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!("Mango Markets source boundary missing {}", path.join(".")),
                value,
            )
        })?;
    }
    let values = cursor.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!(
                "Mango Markets source boundary {} must be an array",
                path.join(".")
            ),
            cursor,
        )
    })?;
    let parsed = values
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!(
                    "Mango Markets source boundary {} must contain strings",
                    path.join(".")
                ),
                cursor,
            )
        })?;
    if parsed.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            format!(
                "Mango Markets source boundary {} must not be empty",
                path.join(".")
            ),
            cursor,
        ));
    }
    Ok(parsed)
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
