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
    let symbols = value.as_object().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "HitBTC symbol response missing object",
            value,
        )
    })?;
    symbols
        .iter()
        .filter(|(_, data)| {
            data.get("type")
                .and_then(Value::as_str)
                .is_none_or(|market_type| market_type.eq_ignore_ascii_case("spot"))
        })
        .map(|(symbol, data)| parse_symbol_rule(exchange_id, symbol, data))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let base_asset = required_str(exchange_id, value, "base_currency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quote_currency")?.to_ascii_uppercase();
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("working");
    let tradable = status.eq_ignore_ascii_case("working");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                normalize_hitbtc_symbol_text(symbol)?,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tick_size")),
        quantity_increment: string_or_number(value.get("quantity_increment")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("quantity_increment")),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: value
            .get("tick_size")
            .and_then(|value| string_or_number(Some(value)))
            .map(|value| precision_from_decimal(&value)),
        quantity_precision: value
            .get("quantity_increment")
            .and_then(|value| string_or_number(Some(value)))
            .map(|value| precision_from_decimal(&value)),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut asks = parse_levels(exchange_id, value.get("ask").or_else(|| value.get("asks")))?;
    let mut bids = parse_levels(exchange_id, value.get("bid").or_else(|| value.get("bids")))?;
    let max_depth = depth as usize;
    asks.truncate(max_depth);
    bids.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hitbtc order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value.get("timestamp").and_then(parse_datetime_value);
    Ok(snapshot)
}

pub fn normalize_hitbtc_symbol(scope: &SymbolScope) -> ExchangeApiResult<String> {
    if scope.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "hitbtc.non_spot_market_type",
        });
    }
    if let Some(canonical_symbol) = &scope.canonical_symbol {
        return Ok(format!(
            "{}{}",
            canonical_symbol.base_asset(),
            canonical_symbol.quote_asset()
        )
        .to_ascii_uppercase());
    }
    normalize_hitbtc_symbol_text(&scope.exchange_symbol.symbol)
}

pub(super) fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    exchange_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = normalize_hitbtc_symbol_text(exchange_symbol)?;
    let (base, quote) =
        split_symbol_guess(&exchange_symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer canonical HitBTC symbol from {exchange_symbol}"),
        })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    })
}

pub(super) fn normalize_hitbtc_symbol_text(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "HitBTC symbol must not be empty".to_string(),
        });
    }
    if normalized.ends_with("PERP") {
        return Err(ExchangeApiError::Unsupported {
            operation: "hitbtc.derivatives_profile_not_enabled",
        });
    }
    Ok(normalized)
}

pub(super) fn split_symbol_guess(symbol: &str) -> Option<(String, String)> {
    for quote in [
        "USDT", "USDC", "BTC", "ETH", "EUR", "USD", "DAI", "BNB", "BCH", "TRY",
    ] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_string(), quote.to_string()));
            }
        }
    }
    if symbol.len() > 3 {
        let split_at = symbol.len() - 3;
        return Some((
            symbol[..split_at].to_string(),
            symbol[split_at..].to_string(),
        ));
    }
    None
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "HitBTC order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "HitBTC order book level is not an array",
                    level,
                )
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

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub(super) fn parse_datetime_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(timestamp) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        let millis = if timestamp > 9_999_999_999 {
            timestamp
        } else {
            timestamp * 1_000
        };
        return DateTime::<Utc>::from_timestamp_millis(millis);
    }
    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
}

fn precision_from_decimal(value: &str) -> u32 {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
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

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
