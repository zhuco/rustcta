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
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bullish markets response is not an array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for market in markets {
        let market_type = market
            .get("marketType")
            .and_then(Value::as_str)
            .and_then(bullish_market_type)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Bullish market payload missing marketType",
                    market,
                )
            })?;
        if !matches!(
            market_type,
            MarketType::Spot | MarketType::Perpetual | MarketType::Futures
        ) {
            continue;
        }
        let symbol = required_str(exchange_id, market, "symbol")?;
        if !requested.is_empty()
            && !requested.iter().any(|scope| {
                scope.market_type == market_type
                    && scope
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(&bullish_symbol(symbol))
            })
        {
            continue;
        }
        rules.push(parse_symbol_rule(exchange_id, market_type, market)?);
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"), "bids")?;
    let asks = parse_levels(exchange_id, value.get("asks"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bullish order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value
        .get("sequenceNumber")
        .and_then(value_as_u64)
        .or_else(|| sequence_number_range_upper(value));
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("datetime"))
        .and_then(value_as_datetime);
    Ok(snapshot)
}

pub fn bullish_market_type(value: &str) -> Option<MarketType> {
    match value.to_ascii_uppercase().as_str() {
        "SPOT" => Some(MarketType::Spot),
        "PERPETUAL" => Some(MarketType::Perpetual),
        "DATED_FUTURE" => Some(MarketType::Futures),
        _ => None,
    }
}

pub fn bullish_symbol(symbol: &str) -> String {
    symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = bullish_symbol(required_str(exchange_id, value, "symbol")?);
    let (fallback_base, fallback_quote) = split_symbol_assets(&exchange_symbol);
    let base_asset = value
        .get("baseSymbol")
        .or_else(|| value.get("underlyingBaseSymbol"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteSymbol")
        .or_else(|| value.get("underlyingQuoteSymbol"))
        .or_else(|| value.get("settlementAssetSymbol"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("quantityTickSize")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minQuantityLimit")),
        max_quantity: string_or_number(value.get("maxQuantityLimit")),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(value.get("tickSize"))
            .and_then(|value| precision(&value)),
        quantity_precision: value
            .get("quantityPrecision")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        supports_market_orders: order_types(value).iter().any(|item| item.contains("MKT")),
        supports_limit_orders: order_types(value)
            .iter()
            .any(|item| item.contains("LMT") || item.contains("LIMIT")),
        supports_post_only: order_types(value)
            .iter()
            .any(|item| item.contains("POST_ONLY")),
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Bullish order book missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    let mut levels = Vec::with_capacity(rows.len());
    if rows.first().is_some_and(Value::is_array) {
        for row in rows {
            let values = row.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Bullish order book level is not an array",
                    row,
                )
            })?;
            let price = number_from_value(values.first()).ok_or_else(|| {
                parse_error(exchange_id.clone(), "Bullish level missing price", row)
            })?;
            let quantity = number_from_value(values.get(1)).ok_or_else(|| {
                parse_error(exchange_id.clone(), "Bullish level missing quantity", row)
            })?;
            levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
    } else {
        for row in rows.chunks(2) {
            let price = number_from_value(row.first()).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Bullish flattened level missing price",
                    &Value::Array(row.to_vec()),
                )
            })?;
            let quantity = number_from_value(row.get(1)).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Bullish flattened level missing quantity",
                    &Value::Array(row.to_vec()),
                )
            })?;
            levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

fn order_types(value: &Value) -> Vec<String> {
    value
        .get("orderTypes")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|item| item.to_ascii_uppercase())
                .collect()
        })
        .unwrap_or_default()
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    let clean = symbol.replace('-', "");
    for quote in ["USDC", "USDT", "USD", "BTC", "ETH"] {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                normalize_asset(&clean[..clean.len() - quote.len()]),
                quote.to_string(),
            );
        }
    }
    (clean, "USDC".to_string())
}

fn normalize_asset(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Bullish payload missing {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn sequence_number_range_upper(value: &Value) -> Option<u64> {
    value
        .get("sequenceNumberRange")
        .and_then(Value::as_array)?
        .last()
        .and_then(value_as_u64)
}

fn value_as_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(time) = DateTime::parse_from_rfc3339(text) {
            return Some(time.with_timezone(&Utc));
        }
        if let Ok(timestamp_ms) = text.parse::<i64>() {
            return DateTime::<Utc>::from_timestamp_millis(timestamp_ms);
        }
    }
    value
        .as_i64()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn precision(value: &str) -> Option<u32> {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
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
