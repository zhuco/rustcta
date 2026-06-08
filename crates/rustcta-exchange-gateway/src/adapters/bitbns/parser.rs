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
    let markets = value.get("data").unwrap_or(value);
    match markets {
        Value::Array(items) => items
            .iter()
            .filter(|item| !is_inactive(item))
            .map(|item| parse_symbol_rule(exchange_id, None, item))
            .collect(),
        Value::Object(map) => map
            .iter()
            .filter(|(_, item)| !is_inactive(item))
            .map(|(key, item)| parse_symbol_rule(exchange_id, Some(key.as_str()), item))
            .collect(),
        _ => Err(parse_error(
            exchange_id.clone(),
            "Bitbns markets response must be an array or object",
            value,
        )),
    }
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    key: Option<&str>,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let (base_asset, quote_asset, exchange_symbol) = parse_market_identity(key, value)?;
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

    let price_precision = integer_from_fields(
        value,
        if quote_asset == "USDT" {
            &[
                "usdtDecimals",
                "usdt_decimals",
                "price_precision",
                "quote_precision",
            ]
        } else {
            &[
                "inrDecimals",
                "inr_decimals",
                "price_precision",
                "quote_precision",
            ]
        },
    );
    let quantity_precision = integer_from_fields(
        value,
        &[
            "floatPlaces",
            "float_places",
            "quantity_precision",
            "base_precision",
        ],
    );

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset: quote_asset.clone(),
        price_increment: price_precision.map(increment_from_precision),
        quantity_increment: quantity_precision.map(increment_from_precision),
        min_price: None,
        max_price: None,
        min_quantity: string_from_fields(value, &["min_quantity", "minQty", "min_qty"]),
        max_quantity: string_from_fields(value, &["max_quantity", "maxQty", "max_qty"]),
        min_notional: string_from_fields(
            value,
            &[
                "min_notional",
                "minOrderValue",
                "min_order_value",
                "minVolume",
                "min_volume",
            ],
        )
        .or_else(|| default_min_notional(&quote_asset).map(str::to_string)),
        max_notional: string_from_fields(
            value,
            &["max_notional", "maxOrderValue", "max_order_value"],
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: usize,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value.get("data").unwrap_or(value);
    let mut bids = parse_levels(exchange_id, book.get("bids").or_else(|| book.get("buy")))?;
    let mut asks = parse_levels(exchange_id, book.get("asks").or_else(|| book.get("sell")))?;
    bids.truncate(depth);
    asks.truncate(depth);
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| canonical_from_exchange_symbol(&symbol.exchange_symbol.symbol).ok())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbns order book request requires canonical_symbol or split exchange symbol"
                .to_string(),
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
    snapshot.exchange_timestamp = book
        .get("timestamp")
        .or_else(|| book.get("time"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_bitbns_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    split_symbol(symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("invalid Bitbns spot symbol {symbol}; expected BASE_INR or BASE_USDT"),
    })
}

pub fn normalize_depth(depth: Option<u32>) -> ExchangeApiResult<usize> {
    match depth {
        Some(0) => Err(ExchangeApiError::InvalidRequest {
            message: "bitbns order book depth must be greater than zero".to_string(),
        }),
        Some(depth) => Ok(depth.min(50) as usize),
        None => Ok(20),
    }
}

fn parse_market_identity(
    key: Option<&str>,
    value: &Value,
) -> ExchangeApiResult<(String, String, String)> {
    let raw_symbol = string_from_fields(
        value,
        &[
            "symbol",
            "pair",
            "market_symbol",
            "ticker_id",
            "coinMarket",
            "coin_market",
        ],
    )
    .or_else(|| key.map(str::to_string));
    let raw_base = string_from_fields(
        value,
        &["coin", "coinName", "coin_name", "base", "base_currency"],
    );
    let raw_quote = string_from_fields(value, &["market", "quote", "quote_currency", "currency"]);

    let (base, quote) = match (raw_base, raw_quote, raw_symbol) {
        (Some(base), Some(quote), _) => (normalize_asset(&base), normalize_asset(&quote)),
        (_, _, Some(symbol)) => {
            split_symbol(&symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot split Bitbns market symbol {symbol}"),
            })?
        }
        _ => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "Bitbns market missing symbol/base/quote".to_string(),
            })
        }
    };
    let exchange_symbol = format!("{base}_{quote}");
    Ok((base, quote, exchange_symbol))
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('_') {
        if !base.is_empty() && matches!(quote, "INR" | "USDT") {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    ["USDT", "INR"].iter().find_map(|quote| {
        normalized
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.trim_end_matches('_').to_string(), (*quote).to_string()))
    })
}

fn canonical_from_exchange_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) = normalize_bitbns_symbol(symbol)?;
    CanonicalSymbol::new(base, quote).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitbns order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(items) => {
                let price = items.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Bitbns level price", level)
                })?;
                let quantity = items.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid Bitbns level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(map) => {
                let price = map
                    .get("rate")
                    .or_else(|| map.get("price"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid Bitbns level rate", level)
                    })?;
                let quantity = map
                    .get("btc")
                    .or_else(|| map.get("quantity"))
                    .or_else(|| map.get("qty"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid Bitbns level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "invalid Bitbns order book level",
                level,
            )),
        })
        .collect()
}

fn is_inactive(value: &Value) -> bool {
    value
        .get("status")
        .or_else(|| value.get("isActive"))
        .or_else(|| value.get("is_active"))
        .is_some_and(|status| match status {
            Value::Bool(status) => !*status,
            Value::Number(number) => number.as_i64() == Some(0),
            Value::String(text) => matches!(text.as_str(), "0" | "false" | "False" | "inactive"),
            _ => false,
        })
}

fn normalize_asset(value: &str) -> String {
    value.trim().trim_start_matches('_').to_ascii_uppercase()
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(string_or_number)
}

fn integer_from_fields(value: &Value, fields: &[&str]) -> Option<u32> {
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(|value| match value {
            Value::String(text) => text.parse().ok(),
            Value::Number(number) => number.as_u64().map(|number| number as u32),
            _ => None,
        })
}

fn string_or_number(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn default_min_notional(quote: &str) -> Option<&'static str> {
    match quote {
        "INR" => Some("10"),
        "USDT" => Some("0.1"),
        _ => None,
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
