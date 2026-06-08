use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn normalize_bittrade_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = value
        .trim()
        .replace(['/', '-', '_', ' '], "")
        .to_ascii_lowercase();
    if symbol.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bittrade symbol must not be empty".to_string(),
        });
    }
    Ok(symbol)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| parse_error(exchange_id, "symbols data array", value))?;
    rows.iter()
        .map(|row| {
            let symbol = normalize_bittrade_symbol(required_str(exchange_id, row, "symbol")?)?;
            let base_asset = required_str(exchange_id, row, "base-currency")?.to_ascii_uppercase();
            let quote_asset =
                required_str(exchange_id, row, "quote-currency")?.to_ascii_uppercase();
            let canonical_symbol =
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &symbol)
                    .map_err(validation_error)?;
            let api_trading = row
                .get("api-trading")
                .and_then(Value::as_str)
                .unwrap_or("enabled")
                .eq_ignore_ascii_case("enabled");
            let online = row
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("online")
                .eq_ignore_ascii_case("online");
            let tradable = api_trading && online;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical_symbol),
                    exchange_symbol,
                },
                base_asset,
                quote_asset,
                price_increment: decimal_step(row.get("price-precision")),
                quantity_increment: decimal_step(row.get("amount-precision")),
                min_price: None,
                max_price: None,
                min_quantity: string_or_number(row.get("min-order-amt"))
                    .or_else(|| string_or_number(row.get("limit-order-min-order-amt"))),
                max_quantity: string_or_number(row.get("max-order-amt"))
                    .or_else(|| string_or_number(row.get("limit-order-max-order-amt"))),
                min_notional: string_or_number(row.get("min-order-value")),
                max_notional: None,
                price_precision: row.get("price-precision").and_then(value_u32),
                quantity_precision: row.get("amount-precision").and_then(value_u32),
                supports_market_orders: tradable,
                supports_limit_orders: tradable,
                supports_post_only: false,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_response(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("tick")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let mut bids = parse_levels(exchange_id, data.get("bids"))?;
    let mut asks = parse_levels(exchange_id, data.get("asks"))?;
    if let Some(depth) = depth {
        bids.truncate(depth as usize);
        asks.truncate(depth as usize);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bittrade order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = data
        .get("version")
        .or_else(|| data.get("seqId"))
        .and_then(value_u64);
    snapshot.exchange_timestamp = data
        .get("ts")
        .or_else(|| value.get("ts"))
        .and_then(value_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "order book levels", &Value::Null))?;
    rows.iter()
        .map(|row| {
            let level = row
                .as_array()
                .ok_or_else(|| parse_error(exchange_id, "price/amount level", row))?;
            let price = level
                .first()
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level price", row))?;
            let quantity = level
                .get(1)
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "level quantity", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    key: &str,
) -> ExchangeApiResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id, key, value))
}

fn decimal_step(value: Option<&Value>) -> Option<String> {
    let digits = value.and_then(value_u32)? as usize;
    if digits == 0 {
        Some("1".to_string())
    } else {
        Some(format!("0.{}1", "0".repeat(digits.saturating_sub(1))))
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("bittrade parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
