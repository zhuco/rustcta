use chrono::{TimeZone, Utc};
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
            "Backpack markets response is not an array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for market in markets {
        let market_type = parse_market_type(market.get("marketType").and_then(Value::as_str))?;
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            continue;
        }
        if market.get("visible").and_then(Value::as_bool) == Some(false) {
            continue;
        }
        let symbol = required_str(exchange_id, market, "symbol")?;
        if !requested.is_empty()
            && !requested.iter().any(|requested| {
                requested.market_type == market_type
                    && requested
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(symbol)
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
                message: "backpack order book request requires canonical_symbol".to_string(),
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
        .get("lastUpdateId")
        .and_then(|value| value.as_str().and_then(|text| text.parse::<u64>().ok()));
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(number_i64)
        .and_then(|micros| Utc.timestamp_micros(micros).single());
    Ok(snapshot)
}

pub fn normalize_backpack_symbol(symbol: &str) -> String {
    symbol.trim().replace(['-', '/'], "_").to_ascii_uppercase()
}

pub fn parse_market_type(value: Option<&str>) -> ExchangeApiResult<MarketType> {
    match value.unwrap_or("SPOT").to_ascii_uppercase().as_str() {
        "SPOT" => Ok(MarketType::Spot),
        "PERP" | "PERPETUAL" | "FUTURES" => Ok(MarketType::Perpetual),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "backpack.unsupported_market_type",
        }),
    }
}

pub fn backpack_market_type(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("SPOT"),
        MarketType::Perpetual => Ok("PERP"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "backpack.unsupported_market_type",
        }),
    }
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_backpack_symbol(required_str(exchange_id, value, "symbol")?);
    let base_asset = value
        .get("baseSymbol")
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(&exchange_symbol).0);
    let quote_asset = value
        .get("quoteSymbol")
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(&exchange_symbol).1);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            exchange_symbol.clone(),
        )
        .map_err(validation_error)?,
    };
    let filters = value.get("filters").unwrap_or(&Value::Null);
    let price = filters.get("price").unwrap_or(&Value::Null);
    let quantity = filters.get("quantity").unwrap_or(&Value::Null);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(price.get("tickSize")),
        quantity_increment: string_or_number(quantity.get("stepSize")),
        min_price: string_or_number(price.get("minPrice")),
        max_price: string_or_number(price.get("maxPrice")),
        min_quantity: string_or_number(quantity.get("minQuantity")),
        max_quantity: string_or_number(quantity.get("maxQuantity")),
        min_notional: string_or_number(quantity.get("minNotional")),
        max_notional: None,
        price_precision: string_or_number(price.get("tickSize"))
            .and_then(|value| precision(&value)),
        quantity_precision: string_or_number(quantity.get("stepSize"))
            .and_then(|value| precision(&value)),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: market_type == MarketType::Perpetual,
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
            &format!("Backpack order book missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Backpack depth level is not an array",
                row,
            )
        })?;
        let price = number_from_value(values.first()).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Backpack depth level missing price",
                row,
            )
        })?;
        let quantity = number_from_value(values.get(1)).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Backpack depth level missing quantity",
                row,
            )
        })?;
        levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

pub fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Backpack payload missing {field}"),
            value,
        )
    })
}

pub fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub fn number_i64(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

pub fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub fn normalize_asset(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

pub fn split_symbol_assets(symbol: &str) -> (String, String) {
    let mut parts = symbol.split('_').filter(|part| !part.is_empty());
    let base = parts.next().unwrap_or(symbol).to_ascii_uppercase();
    let quote = parts.next().unwrap_or("USDC").to_ascii_uppercase();
    (base, quote)
}

fn precision(value: &str) -> Option<u32> {
    let trimmed = value.trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .or(Some(0))
}
