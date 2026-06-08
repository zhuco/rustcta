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
    let rows = value
        .get("data")
        .and_then(|data| data.get("list"))
        .and_then(Value::as_array)
        .or_else(|| value.get("symbols").and_then(Value::as_array))
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols list", value))?;
    rows.iter()
        .filter(|row| row.get("type").and_then(Value::as_i64).unwrap_or(1) == 1)
        .map(|row| parse_symbol_rule(exchange_id, row))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_tokocrypto_symbol(required_str(exchange_id, value, "symbol")?)?;
    let base_asset = required_str(exchange_id, value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteAsset")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    };
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let notional_filter =
        find_filter(&filters, "MIN_NOTIONAL").or_else(|| find_filter(&filters, "NOTIONAL"));
    let order_types = value
        .get("orderTypes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let trading = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("TRADING")
        .eq_ignore_ascii_case("TRADING");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(price_filter.and_then(|filter| filter.get("tickSize"))),
        quantity_increment: string_or_number(lot_filter.and_then(|filter| filter.get("stepSize"))),
        min_price: string_or_number(price_filter.and_then(|filter| filter.get("minPrice"))),
        max_price: string_or_number(price_filter.and_then(|filter| filter.get("maxPrice"))),
        min_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("minQty"))),
        max_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("maxQty"))),
        min_notional: string_or_number(
            notional_filter
                .and_then(|filter| filter.get("minNotional"))
                .or_else(|| notional_filter.and_then(|filter| filter.get("notional"))),
        ),
        max_notional: string_or_number(
            notional_filter.and_then(|filter| filter.get("maxNotional")),
        ),
        price_precision: precision_from_step(
            price_filter
                .and_then(|filter| filter.get("tickSize"))
                .and_then(Value::as_str),
        )
        .or_else(|| value.get("pricePrecision").and_then(value_u32)),
        quantity_precision: precision_from_step(
            lot_filter
                .and_then(|filter| filter.get("stepSize"))
                .and_then(Value::as_str),
        )
        .or_else(|| {
            value
                .get("basePrecision")
                .or_else(|| value.get("baseAssetPrecision"))
                .and_then(value_u32)
        }),
        supports_market_orders: trading && has_or_unlisted(&order_types, "MARKET"),
        supports_limit_orders: trading && has_or_unlisted(&order_types, "LIMIT"),
        supports_post_only: trading && has_or_unlisted(&order_types, "LIMIT_MAKER"),
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "tokocrypto order book request requires canonical_symbol".to_string(),
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
        .get("lastUpdateId")
        .or_else(|| data.get("u"))
        .and_then(Value::as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| data.get("E"))
        .and_then(value_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_tokocrypto_symbol(value: &str) -> ExchangeApiResult<String> {
    let normalized = value.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tokocrypto symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') {
        Ok(normalized)
    } else {
        Ok(normalized)
    }
}

pub fn market_data_symbol(value: &str) -> ExchangeApiResult<String> {
    Ok(normalize_tokocrypto_symbol(value)?.replace('_', ""))
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        _ => 500,
    }
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
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book level", level)
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

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
    })
}

fn required_str<'a>(
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

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?;
    let decimal = step.split('.').nth(1)?;
    Some(decimal.trim_end_matches('0').len() as u32)
}

fn has_or_unlisted(order_types: &[Value], expected: &str) -> bool {
    order_types.is_empty()
        || order_types.iter().any(|value| {
            value
                .as_str()
                .is_some_and(|text| text.eq_ignore_ascii_case(expected))
        })
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        format!("tokocrypto parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
