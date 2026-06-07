use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let key = match market_type {
        MarketType::Spot => "symbols",
        MarketType::Perpetual => "contracts",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "toobit.symbol_rules_unsupported_market",
            })
        }
    };
    let symbols = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("exchangeInfo missing {key}"),
            value,
        )
    })?;
    symbols
        .iter()
        .filter(|item| {
            market_type == MarketType::Spot
                || !item
                    .get("inverse")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
        .map(|item| parse_symbol_rule(exchange_id, market_type, item))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_toobit_symbol(&exchange_symbol, market_type)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("underlying")
        .or_else(|| value.get("baseAsset"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .map(|value| value.replace("-SWAP-USDT", ""))
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteAsset")
        .or_else(|| value.get("marginToken"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let price_filter = filter_by_type(filters, "PRICE_FILTER");
    let lot_filter = filter_by_type(filters, "LOT_SIZE");
    let notional_filter = filter_by_type(filters, "MIN_NOTIONAL");
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("TRADING");
    let tradable = matches!(status.to_ascii_uppercase().as_str(), "TRADING");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
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
                .or_else(|| value.get("minNotional")),
        ),
        max_notional: None,
        price_precision: value
            .get("pricePrecision")
            .and_then(value_as_u32)
            .or_else(|| {
                string_or_number(price_filter.and_then(|filter| filter.get("tickSize")))
                    .map(|value| precision_from_decimal(&value))
            }),
        quantity_precision: value
            .get("quantityPrecision")
            .and_then(value_as_u32)
            .or_else(|| {
                string_or_number(lot_filter.and_then(|filter| filter.get("stepSize")))
                    .map(|value| precision_from_decimal(&value))
            }),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
    stale_book_ms: u64,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let data = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "toobit order book request requires canonical_symbol".to_string(),
            })?;
    let received_at = Utc::now();
    let exchange_timestamp = data
        .get("t")
        .or_else(|| data.get("timestamp"))
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        received_at,
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = exchange_timestamp;
    snapshot.sequence = data
        .get("lastUpdateId")
        .or_else(|| data.get("u"))
        .or_else(|| data.get("v"))
        .and_then(value_as_u64);
    snapshot.is_stale = exchange_timestamp
        .map(|ts| received_at.signed_duration_since(ts).num_milliseconds() > stale_book_ms as i64)
        .unwrap_or(false);
    Ok(snapshot)
}

pub fn normalize_toobit_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    match market_type {
        MarketType::Spot => normalize_spot_symbol(symbol),
        MarketType::Perpetual => normalize_perp_symbol(symbol),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "toobit.unsupported_market_type",
        }),
    }
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=20 => 20,
        21..=100 => 100,
        _ => 200,
    }
}

pub(super) fn symbol_scope_from_payload(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = value
        .get("symbol")
        .or_else(|| value.get("symbolName"))
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "missing field symbol", value))?
        .to_ascii_uppercase();
    symbol_scope_from_exchange_symbol(exchange_id, market_type, &exchange_symbol)
}

pub(super) fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    exchange_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = normalize_toobit_symbol(exchange_symbol, market_type)?;
    let (base, quote) = split_toobit_symbol(&exchange_symbol, market_type)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
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

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
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
            let price = decimal_as_f64(array.first())
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = decimal_as_f64(array.get(1))
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn filter_by_type<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
    })
}

fn normalize_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

fn normalize_perp_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace('_', "-")
        .replace('/', "-")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if normalized.contains("-SWAP-") {
        return Ok(normalized);
    }
    if let Some(base) = normalized.strip_suffix("USDT") {
        if !base.is_empty() {
            return Ok(format!("{base}-SWAP-USDT"));
        }
    }
    Ok(normalized)
}

fn split_toobit_symbol(symbol: &str, market_type: MarketType) -> Option<(String, String)> {
    let symbol = symbol.to_ascii_uppercase();
    if market_type == MarketType::Perpetual {
        if let Some(base) = symbol.strip_suffix("-SWAP-USDT") {
            return Some((base.to_string(), "USDT".to_string()));
        }
    }
    for quote in ["USDT", "USDC", "BTC", "ETH", "FDUSD", "TRY", "EUR"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_string(), quote.to_string()));
            }
        }
    }
    None
}

fn precision_from_decimal(value: &str) -> u32 {
    let value = value.trim();
    value
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

#[allow(dead_code)]
pub(super) fn schema_version() -> SchemaVersion {
    SchemaVersion::current()
}
