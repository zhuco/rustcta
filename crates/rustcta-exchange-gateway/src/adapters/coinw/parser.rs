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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let data = value.get("data").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinW symbol rules response missing data",
            value,
        )
    })?;
    let instruments = data.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinW symbol rules data is not an array",
            data,
        )
    })?;
    instruments
        .iter()
        .filter(|instrument| coinw_instrument_is_online(market_type, instrument))
        .map(|instrument| parse_symbol_rule(exchange_id, market_type, instrument))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let (exchange_symbol, base_asset, quote_asset) = match market_type {
        MarketType::Spot => {
            let symbol = required_str(exchange_id, value, "currencyPair")?.to_ascii_uppercase();
            let base = value
                .get("currencyBase")
                .and_then(Value::as_str)
                .map(normalize_asset)
                .unwrap_or_else(|| split_symbol_assets(&symbol).0);
            let quote = value
                .get("currencyQuote")
                .and_then(Value::as_str)
                .map(normalize_asset)
                .unwrap_or_else(|| split_symbol_assets(&symbol).1);
            (symbol, base, quote)
        }
        MarketType::Perpetual => {
            let base = value
                .get("base")
                .or_else(|| value.get("name"))
                .and_then(Value::as_str)
                .map(normalize_asset)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "CoinW futures missing base", value)
                })?;
            let quote = value
                .get("quote")
                .and_then(Value::as_str)
                .map(normalize_asset)
                .unwrap_or_else(|| "USDT".to_string());
            (format!("{base}_{quote}"), base, quote)
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.unsupported_market_type",
            });
        }
    };

    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let price_precision = integer_u32(value.get("pricePrecision"));
    let quantity_precision = match market_type {
        MarketType::Spot => integer_u32(value.get("countPrecision")),
        MarketType::Perpetual => None,
        _ => None,
    };
    let price_increment = price_precision.map(decimal_step_from_precision);
    let quantity_increment = quantity_precision
        .map(decimal_step_from_precision)
        .or_else(|| string_or_number(value.get("oneLotSize")))
        .or_else(|| string_or_number(value.get("minSize")));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment,
        quantity_increment,
        min_price: string_or_number(value.get("minBuyPrice")),
        max_price: string_or_number(value.get("maxBuyPrice")),
        min_quantity: string_or_number(value.get("minBuyCount"))
            .or_else(|| string_or_number(value.get("minSize"))),
        max_quantity: string_or_number(value.get("maxBuyCount"))
            .or_else(|| string_or_number(value.get("maxPosition"))),
        min_notional: string_or_number(value.get("minBuyAmount")),
        max_notional: string_or_number(value.get("maxBuyAmount")),
        price_precision,
        quantity_precision,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: market_type == MarketType::Perpetual,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinW order book response missing data",
            value,
        )
    })?;
    let (book, exchange_timestamp) = match symbol.market_type {
        MarketType::Spot => {
            let books = data.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "CoinW spot order book data is not an array",
                    data,
                )
            })?;
            let requested = normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)?;
            let book = books
                .iter()
                .find(|book| {
                    book.get("pair")
                        .and_then(Value::as_str)
                        .map(|pair| pair.eq_ignore_ascii_case(&requested))
                        .unwrap_or(false)
                })
                .or_else(|| books.first())
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "CoinW spot order book is empty", data)
                })?;
            let ts = first_timestamp_millis(book, &["time", "ts", "timestamp"]);
            (book, ts)
        }
        MarketType::Perpetual => {
            let ts = data
                .get("ts")
                .or_else(|| data.get("time"))
                .and_then(number_i64)
                .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
            (data, ts)
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.order_book_unsupported_market_type",
            });
        }
    };

    let bids = parse_levels(exchange_id, symbol.market_type, book.get("bids"), "bids")?;
    let asks = parse_levels(exchange_id, symbol.market_type, book.get("asks"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinw order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = first_u64(book, &["seq", "endSeq", "sequence", "u"]);
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("CoinW order book missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let (price, quantity) = match market_type {
            MarketType::Spot => {
                let parts = level.as_array().ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "CoinW spot level is not an array",
                        level,
                    )
                })?;
                (
                    number_from_value(parts.first()).ok_or_else(|| {
                        parse_error(exchange_id.clone(), "CoinW spot level missing price", level)
                    })?,
                    number_from_value(parts.get(1)).ok_or_else(|| {
                        parse_error(
                            exchange_id.clone(),
                            "CoinW spot level missing quantity",
                            level,
                        )
                    })?,
                )
            }
            MarketType::Perpetual => (
                number_from_value(level.get("p")).ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "CoinW futures level missing price",
                        level,
                    )
                })?,
                number_from_value(level.get("m")).ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "CoinW futures level missing quantity",
                        level,
                    )
                })?,
            ),
            _ => unreachable!("checked by caller"),
        };
        if quantity <= 0.0 {
            continue;
        }
        parsed.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    if side == "bids" {
        parsed.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        parsed.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(parsed)
}

pub fn normalize_coinw_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_coinw_perp_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_coinw_spot_symbol(symbol)?;
    if normalized.contains('_') {
        return Ok(normalized);
    }
    for quote in ["USDT", "USDC"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok(format!("{base}_{quote}"));
            }
        }
    }
    Ok(format!("{normalized}_USDT"))
}

pub fn normalize_coinw_perp_base(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_coinw_perp_symbol(symbol)?;
    let (base, quote) = normalized
        .split_once('_')
        .unwrap_or((normalized.as_str(), "USDT"));
    if quote.eq_ignore_ascii_case("USDT") {
        Ok(base.to_ascii_uppercase())
    } else {
        Ok(normalized)
    }
}

pub fn normalize_spot_depth(depth: u32) -> u32 {
    if depth <= 5 {
        5
    } else {
        20
    }
}

fn coinw_instrument_is_online(market_type: MarketType, value: &Value) -> bool {
    match market_type {
        MarketType::Spot => value.get("state").and_then(Value::as_i64).unwrap_or(1) == 1,
        MarketType::Perpetual => value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("online")
            .eq_ignore_ascii_case("online"),
        _ => false,
    }
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    let normalized = symbol.replace(['/', '-'], "_").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('_') {
        return (normalize_asset(base), normalize_asset(quote));
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return (normalize_asset(base), normalize_asset(quote));
            }
        }
    }
    (normalize_asset(&normalized), "USDT".to_string())
}

fn normalize_asset(asset: &str) -> String {
    asset.trim().to_ascii_uppercase()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("CoinW response missing field {field}"),
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

fn integer_u32(value: Option<&Value>) -> Option<u32> {
    match value? {
        Value::Number(number) => number.as_u64().map(|value| value as u32),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn first_u64(value: &Value, fields: &[&str]) -> Option<u64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_u64))
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(number_i64))
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
}

fn decimal_step_from_precision(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

fn number_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn value_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64().or_else(|| {
            number
                .as_f64()
                .filter(|value| *value >= 0.0)
                .map(|value| value as u64)
        }),
        Value::String(text) => text.parse().ok().or_else(|| {
            text.split_once('.')
                .and_then(|(whole, _)| whole.parse().ok())
        }),
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
