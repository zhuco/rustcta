use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRateSnapshot, SymbolRules,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let pairs = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "currency_pairs response is not an array",
            value,
        )
    })?;
    parse_rules_skipping_invalid_symbols(pairs, |value| parse_symbol_rule(exchange_id, value))
}

pub fn parse_perpetual_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let contracts = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "contracts response is not an array",
            value,
        )
    })?;
    parse_rules_skipping_invalid_symbols(contracts, |value| {
        parse_perpetual_symbol_rule(exchange_id, value)
    })
}

fn parse_rules_skipping_invalid_symbols(
    values: &[Value],
    mut parse: impl FnMut(&Value) -> ExchangeApiResult<SymbolRules>,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let mut rules = Vec::new();
    for value in values {
        match parse(value) {
            Ok(rule) => rules.push(rule),
            Err(error) if should_skip_symbol_rule_error(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(rules)
}

fn should_skip_symbol_rule_error(error: &ExchangeApiError) -> bool {
    match error {
        ExchangeApiError::InvalidRequest { message } => {
            message.contains("canonical_symbol") || message.contains("exchange_symbol")
        }
        _ => false,
    }
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "id")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "base")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quote")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
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
    let price_precision = integer_from_value(value.get("precision")).unwrap_or(0);
    let quantity_precision = integer_from_value(value.get("amount_precision")).unwrap_or(0);
    let tradable = value
        .get("trade_status")
        .and_then(Value::as_str)
        .is_none_or(|status| status.eq_ignore_ascii_case("tradable"));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: Some(increment_from_precision(price_precision)),
        quantity_increment: Some(increment_from_precision(quantity_precision)),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("min_base_amount")),
        max_quantity: None,
        min_notional: string_or_number(value.get("min_quote_amount")),
        max_notional: None,
        price_precision: Some(price_precision),
        quantity_precision: Some(quantity_precision),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_perpetual_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "name")
        .or_else(|_| required_str(exchange_id, value, "contract"))?
        .to_ascii_uppercase();
    let (base_asset, quote_asset) = split_gateio_pair(&exchange_symbol)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    };
    let price_increment = string_or_number(
        value
            .get("order_price_round")
            .or_else(|| value.get("mark_price_round")),
    );
    let quantity_increment =
        string_or_number(value.get("order_size_round")).or_else(|| Some("1".to_string()));
    let price_precision = price_increment
        .as_deref()
        .map(decimal_precision)
        .or_else(|| integer_from_value(value.get("price_precision")));
    let quantity_precision = quantity_increment
        .as_deref()
        .map(decimal_precision)
        .or_else(|| integer_from_value(value.get("quantity_precision")));
    let tradable = !value
        .get("in_delisting")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && value
            .get("trade_status")
            .and_then(Value::as_str)
            .is_none_or(|status| status.eq_ignore_ascii_case("tradable"));

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment,
        quantity_increment,
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("order_size_min")),
        max_quantity: string_or_number(value.get("order_size_max")),
        min_notional: None,
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: tradable,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio order book request requires canonical_symbol".to_string(),
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
        .get("id")
        .or_else(|| value.get("u"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("current")
        .or_else(|| value.get("t"))
        .and_then(gateio_timestamp);
    Ok(snapshot)
}

pub fn parse_funding_rate_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FundingRateSnapshot> {
    let data = value
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(value);
    let funding_rate = string_or_number(
        data.get("funding_rate")
            .or_else(|| data.get("funding_rate_indicative"))
            .or_else(|| data.get("fundingRate")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "missing funding_rate", data))?;
    Ok(FundingRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        funding_rate,
        predicted_funding_rate: string_or_number(
            data.get("funding_rate_indicative")
                .or_else(|| data.get("predicted_funding_rate"))
                .or_else(|| data.get("predictedFundingRate")),
        ),
        funding_time: data
            .get("funding_next_apply")
            .or_else(|| data.get("funding_time"))
            .or_else(|| data.get("fundingTime"))
            .and_then(gateio_timestamp),
        next_funding_time: data
            .get("funding_next_apply")
            .or_else(|| data.get("next_funding_time"))
            .or_else(|| data.get("nextFundingTime"))
            .and_then(gateio_timestamp),
        mark_price: string_or_number(data.get("mark_price").or_else(|| data.get("markPrice"))),
        index_price: string_or_number(data.get("index_price").or_else(|| data.get("indexPrice"))),
        open_interest: string_or_number(
            data.get("total_size")
                .or_else(|| data.get("open_interest"))
                .or_else(|| data.get("openInterest")),
        ),
        turnover_24h: string_or_number(
            data.get("volume_24h_quote")
                .or_else(|| data.get("turnover24h")),
        ),
        volume_24h: string_or_number(
            data.get("volume_24h_base")
                .or_else(|| data.get("volume24h")),
        ),
        source: Some("gateio_public_futures_tickers".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn normalize_gateio_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') {
        return Ok(normalized);
    }
    split_compact_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("cannot infer Gate.io currency_pair from {symbol}"),
    })
}

pub fn split_gateio_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = normalize_gateio_symbol(symbol)?;
    let (base, quote) =
        normalized
            .split_once('_')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot infer Gate.io base/quote from {symbol}"),
            })?;
    Ok((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        _ => 100,
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
            let (price_value, quantity_value) = if let Some(array) = level.as_array() {
                (array.first(), array.get(1))
            } else {
                (
                    level.get("p").or_else(|| level.get("price")),
                    level
                        .get("s")
                        .or_else(|| level.get("amount"))
                        .or_else(|| level.get("quantity")),
                )
            };
            let price = price_value
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = quantity_value
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
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

fn split_compact_symbol(symbol: &str) -> Option<String> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| format!("{base}_{quote}"))
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
        _ => None,
    })
}

fn decimal_precision(value: &str) -> u32 {
    value
        .trim()
        .split_once('.')
        .map(|(_, decimals)| decimals.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn gateio_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => return None,
    };
    if let Ok(milliseconds) = text.parse::<i64>() {
        if milliseconds > 10_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(milliseconds);
        }
        return DateTime::<Utc>::from_timestamp(milliseconds, 0);
    }
    let seconds = text.parse::<f64>().ok()?;
    let whole_seconds = seconds.trunc() as i64;
    let nanos = ((seconds.fract()) * 1_000_000_000.0).round() as u32;
    DateTime::<Utc>::from_timestamp(whole_seconds, nanos)
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        return "1".to_string();
    }
    format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
