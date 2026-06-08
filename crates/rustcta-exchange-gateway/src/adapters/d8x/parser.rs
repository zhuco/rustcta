use chrono::{DateTime, TimeZone, Utc};
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
    ensure_no_error(exchange_id, value)?;
    let contracts = value
        .get("contracts")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "D8X contracts response missing contracts",
                value,
            )
        })?;
    contracts
        .iter()
        .filter(|contract| {
            contract
                .get("product_type")
                .and_then(Value::as_str)
                .is_some_and(|product| product.eq_ignore_ascii_case("perpetual"))
        })
        .map(|contract| parse_contract(exchange_id, contract))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_no_error(exchange_id, value)?;
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            value
                .get("ticker_id")
                .and_then(Value::as_str)
                .and_then(|ticker| canonical_from_ticker(ticker).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "D8X order book requires canonical_symbol or response ticker_id".to_string(),
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
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

fn parse_contract(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let ticker_id = required_str(exchange_id, value, "ticker_id")?;
    let canonical_symbol = canonical_from_ticker(ticker_id)?;
    let base_asset = value
        .get("base_currency")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("target_currency")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, ticker_id)
            .map_err(validation_error)?,
    };
    let contract_specs = value.get("contract_specs").unwrap_or(value);
    let price_increment = decimal_path(contract_specs, &["price_tick"])
        .or_else(|| decimal_path(value, &["price_tick"]))
        .or_else(|| decimal_path(value, &["tick_size"]));
    let quantity_increment = decimal_path(contract_specs, &["lot_size"])
        .or_else(|| decimal_path(value, &["lot_size"]))
        .or_else(|| decimal_path(value, &["quantity_increment"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(contract_specs, &["min_quantity"])
            .or_else(|| decimal_path(value, &["min_quantity"])),
        max_quantity: decimal_path(contract_specs, &["max_quantity"])
            .or_else(|| decimal_path(value, &["max_quantity"])),
        min_notional: decimal_path(contract_specs, &["min_notional"])
            .or_else(|| decimal_path(value, &["min_notional"])),
        max_notional: decimal_path(contract_specs, &["max_notional"])
            .or_else(|| decimal_path(value, &["max_notional"])),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: false,
        supports_limit_orders: false,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn canonical_from_ticker(ticker_id: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let parts = ticker_id.trim().split('-').collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer D8X canonical symbol from {ticker_id}"),
        });
    }
    CanonicalSymbol::new(parts[0], parts[1]).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "D8X order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(values) => {
                let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(object) => {
                let price = object
                    .get("price")
                    .or_else(|| object.get("p"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level price", level)
                    })?;
                let quantity = object
                    .get("quantity")
                    .or_else(|| object.get("size"))
                    .or_else(|| object.get("q"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "D8X order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_no_error(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value.get("error").is_none() {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "D8X response contains error",
        value,
    ))
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

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn timestamp_millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
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
            &format!("D8X response missing field {field}"),
            value,
        )
    })
}

fn parse_error(exchange_id: ExchangeId, message: &str, payload: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(payload.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
