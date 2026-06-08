use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn normalize_bitbank_pair(value: &str) -> ExchangeApiResult<String> {
    let pair = value.trim().to_ascii_lowercase();
    if pair.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitbank pair must not be empty".to_string(),
        });
    }
    Ok(pair)
}

pub fn canonical_from_pair(pair: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("bitbank pair {pair} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .and_then(|data| data.get("pairs"))
        .or_else(|| value.get("pairs"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "pairs array", value))?;
    rows.iter()
        .map(|row| {
            let name = row
                .get("name")
                .or_else(|| row.get("pair"))
                .and_then(Value::as_str)
                .ok_or_else(|| parse_error(exchange_id, "pair name", row))?;
            let pair = normalize_bitbank_pair(name)?;
            let canonical = canonical_from_pair(&pair)?;
            let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
                .map_err(validation_error)?;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical.clone()),
                    exchange_symbol,
                },
                base_asset: canonical.base_asset().to_string(),
                quote_asset: canonical.quote_asset().to_string(),
                price_increment: string_field(row, "price_digits")
                    .and_then(|digits| decimal_step(&digits)),
                quantity_increment: string_field(row, "amount_digits")
                    .and_then(|digits| decimal_step(&digits)),
                min_price: None,
                max_price: None,
                min_quantity: string_field(row, "unit_amount"),
                max_quantity: None,
                min_notional: None,
                max_notional: None,
                price_precision: string_field(row, "price_digits")
                    .and_then(|digits| digits.parse::<u32>().ok()),
                quantity_precision: string_field(row, "amount_digits")
                    .and_then(|digits| digits.parse::<u32>().ok()),
                supports_market_orders: true,
                supports_limit_orders: true,
                supports_post_only: true,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitbank order book request requires canonical_symbol".to_string(),
            })?;
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data.get("sequenceId").and_then(value_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
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
                .ok_or_else(|| parse_error(exchange_id, "price level value", row))?;
            let quantity = level
                .get(1)
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "quantity level value", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn decimal_step(digits: &str) -> Option<String> {
    let digits = digits.parse::<usize>().ok()?;
    if digits == 0 {
        Some("1".to_string())
    } else {
        Some(format!("0.{}1", "0".repeat(digits.saturating_sub(1))))
    }
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
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

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("bitbank parser expected {message}"),
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
