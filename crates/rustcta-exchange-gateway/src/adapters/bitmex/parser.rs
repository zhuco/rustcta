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
    let instruments = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX instrument response is not an array",
            value,
        )
    })?;
    instruments
        .iter()
        .filter(|instrument| is_tradeable_instrument(instrument))
        .map(|instrument| parse_symbol_rule(exchange_id, instrument))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let market_type = market_type_from_instrument(value);
    let (base_asset, quote_asset) = instrument_assets(&exchange_symbol, value);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let state = value.get("state").and_then(Value::as_str).unwrap_or("Open");
    let trading = matches!(
        state.to_ascii_lowercase().as_str(),
        "open" | "unlisted" | "settled"
    ) && state.eq_ignore_ascii_case("Open");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("lotSize"))
            .or_else(|| Some("1".to_string())),
        min_price: None,
        max_price: string_or_number(value.get("limit")),
        min_quantity: string_or_number(value.get("lotSize")),
        max_quantity: string_or_number(value.get("maxOrderQty")),
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_step_value(value.get("tickSize")),
        quantity_precision: precision_from_step_value(value.get("lotSize")),
        supports_market_orders: trading,
        supports_limit_orders: trading,
        supports_post_only: trading,
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let levels = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX orderBook/L2 response is not an array",
            value,
        )
    })?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut exchange_timestamp = None;
    for level in levels {
        if exchange_timestamp.is_none() {
            exchange_timestamp = level
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(parse_rfc3339);
        }
        let price = number_from_value(level.get("price")).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BitMEX order book level missing price",
                level,
            )
        })?;
        let quantity = number_from_value(level.get("size")).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BitMEX order book level missing size",
                level,
            )
        })?;
        let parsed = OrderBookLevel::new(price, quantity).map_err(validation_error)?;
        match level.get("side").and_then(Value::as_str) {
            Some(side) if side.eq_ignore_ascii_case("Buy") => bids.push(parsed),
            Some(side) if side.eq_ignore_ascii_case("Sell") => asks.push(parsed),
            _ => {
                return Err(parse_error(
                    exchange_id.clone(),
                    "BitMEX order book level missing side",
                    level,
                ));
            }
        }
    }
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitmex order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = exchange_timestamp;
    Ok(snapshot)
}

pub fn parse_orderbook10_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX orderBook10 response is not an array",
            value,
        )
    })?;
    let row = rows.first().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX orderBook10 response is empty",
            value,
        )
    })?;
    let mut bids = parse_orderbook10_levels(exchange_id, row.get("bids"), "bids")?;
    let mut asks = parse_orderbook10_levels(exchange_id, row.get("asks"), "asks")?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));

    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitmex order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339);
    Ok(snapshot)
}

pub fn normalize_bitmex_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn bitmex_symbol_key(symbol: &str) -> ExchangeApiResult<String> {
    Ok(normalize_bitmex_symbol(symbol)?.replace('_', ""))
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
}

pub fn market_type_from_instrument(value: &Value) -> MarketType {
    let typ = value
        .get("typ")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if typ == "IFXXXP" {
        return MarketType::Spot;
    }
    if typ.starts_with("FFW") || typ.contains("PERP") {
        return MarketType::Perpetual;
    }
    MarketType::Futures
}

fn is_tradeable_instrument(value: &Value) -> bool {
    let typ = value
        .get("typ")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    typ == "IFXXXP" || typ.starts_with("FFW")
}

fn parse_orderbook10_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("BitMEX orderBook10 missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let pair = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "BitMEX orderBook10 level is not [price, size]",
                    level,
                )
            })?;
            let price = number_from_value(pair.first()).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "BitMEX orderBook10 level missing price",
                    level,
                )
            })?;
            let quantity = number_from_value(pair.get(1)).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "BitMEX orderBook10 level missing size",
                    level,
                )
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn instrument_assets(symbol: &str, value: &Value) -> (String, String) {
    let quote = value
        .get("quoteCurrency")
        .or_else(|| value.get("quote"))
        .and_then(Value::as_str)
        .filter(|asset| !asset.trim().is_empty())
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).1);
    let base = value
        .get("underlying")
        .or_else(|| value.get("rootSymbol"))
        .or_else(|| value.get("baseCurrency"))
        .and_then(Value::as_str)
        .filter(|asset| !asset.trim().is_empty())
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).0);
    (base, quote)
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    for quote in ["USDT", "USDC", "USD", "XBT", "BTC", "ETH"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return (normalize_asset(base), normalize_asset(quote));
            }
        }
    }
    (normalize_asset(symbol), "USD".to_string())
}

fn normalize_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "XBT" => "BTC".to_string(),
        other => other.to_string(),
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

fn precision_from_step_value(value: Option<&Value>) -> Option<u32> {
    let text = string_or_number(value)?;
    precision_from_step_text(Some(&text))
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn precision_from_step_text(value: Option<&str>) -> Option<u32> {
    let value = value?.trim();
    let fractional = value.split_once('.')?.1.trim_end_matches('0');
    Some(fractional.len() as u32)
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
