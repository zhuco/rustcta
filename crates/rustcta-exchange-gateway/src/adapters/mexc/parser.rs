use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
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
    if market_type == MarketType::Perpetual {
        return parse_contract_symbol_rules(exchange_id, value);
    }
    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "exchangeInfo missing symbols", value))?;
    symbols
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_contract_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let symbols = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| parse_error(exchange_id.clone(), "contract detail missing data", value))?;
    symbols
        .iter()
        .map(|value| parse_contract_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteAsset")?.to_ascii_uppercase();
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
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let notional_filter = find_filter(&filters, "MIN_NOTIONAL");

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
            notional_filter.and_then(|filter| filter.get("minNotional")),
        ),
        max_notional: None,
        price_precision: precision_from_step(
            price_filter
                .and_then(|filter| filter.get("tickSize"))
                .and_then(number_from_value),
        ),
        quantity_precision: precision_from_step(
            lot_filter
                .and_then(|filter| filter.get("stepSize"))
                .and_then(number_from_value),
        ),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_contract_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) =
        split_contract_symbol(&exchange_symbol).unwrap_or_default();
    let base_asset = value
        .get("baseCoin")
        .or_else(|| value.get("base_coin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteCoin")
        .or_else(|| value.get("quote_coin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_quote);
    if base_asset.is_empty() || quote_asset.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("MEXC contract symbol missing base/quote: {value}"),
        });
    }
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
    let tradable = value
        .get("state")
        .and_then(value_as_i64)
        .is_none_or(|state| state == 0);
    let price_increment = string_or_number(
        value
            .get("priceUnit")
            .or_else(|| value.get("price_unit"))
            .or_else(|| value.get("tickSize")),
    );
    let quantity_increment = string_or_number(
        value
            .get("volUnit")
            .or_else(|| value.get("vol_unit"))
            .or_else(|| value.get("stepSize")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minVol").or_else(|| value.get("min_vol"))),
        max_quantity: string_or_number(value.get("maxVol").or_else(|| value.get("max_vol"))),
        min_notional: None,
        max_notional: None,
        price_precision: integer_from_value(
            value.get("priceScale").or_else(|| value.get("price_scale")),
        )
        .or_else(|| {
            precision_from_step(
                price_increment
                    .as_deref()
                    .and_then(|step| step.parse().ok()),
            )
        }),
        quantity_precision: integer_from_value(
            value.get("volScale").or_else(|| value.get("vol_scale")),
        )
        .or_else(|| {
            precision_from_step(
                quantity_increment
                    .as_deref()
                    .and_then(|step| step.parse().ok()),
            )
        }),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("data").unwrap_or(value);
    let bids = parse_levels(
        exchange_id,
        payload.get("bids").or_else(|| payload.get("b")),
    )?;
    let asks = parse_levels(
        exchange_id,
        payload.get("asks").or_else(|| payload.get("a")),
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = payload
        .get("lastUpdateId")
        .or_else(|| payload.get("version"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("E"))
        .or_else(|| payload.get("timestamp"))
        .or_else(|| payload.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_funding_rate_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FundingRateSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let funding_rate = string_or_number(data.get("fundingRate")).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "MEXC contract funding response missing fundingRate",
            data,
        )
    })?;
    Ok(FundingRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        funding_rate,
        predicted_funding_rate: None,
        funding_time: data
            .get("timestamp")
            .or_else(|| value.get("timestamp"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        next_funding_time: data
            .get("nextSettleTime")
            .or_else(|| data.get("nextFundingTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        mark_price: None,
        source: Some("mexc.contract.funding_rate".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn normalize_mexc_symbol(symbol: &str) -> ExchangeApiResult<String> {
    normalize_mexc_symbol_for_market(symbol, MarketType::Spot)
}

pub fn normalize_mexc_symbol_for_market(
    symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if market_type == MarketType::Perpetual {
        split_compact_symbol(&normalized)
            .map(|(base, quote)| format!("{base}_{quote}"))
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot infer MEXC contract symbol from {symbol}"),
            })
    } else {
        Ok(normalized)
    }
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        _ => 50,
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
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn non_zero_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.trim_matches('0').trim_matches('.').is_empty() {
        None
    } else {
        Some(value)
    }
}

fn text_from_value(value: &Value) -> Option<&str> {
    value.as_str()
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

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            let text = text_from_value(value)
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            decimal_text_to_f64(&text)
        })
        .transpose()
}

pub(super) fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid MEXC decimal value {value}: {error}"),
        })
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(raw) => Some(raw.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn parse_side(side: &str) -> ExchangeApiResult<rustcta_types::OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(rustcta_types::OrderSide::Buy),
        "SELL" => Ok(rustcta_types::OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported MEXC side {side}"),
        }),
    }
}

pub(super) fn parse_order_type(order_type: &str, tif: Option<&str>) -> rustcta_types::OrderType {
    match order_type.to_ascii_uppercase().as_str() {
        "MARKET" => rustcta_types::OrderType::Market,
        "LIMIT_MAKER" => rustcta_types::OrderType::PostOnly,
        "LIMIT" => match tif.map(str::to_ascii_uppercase).as_deref() {
            Some("IOC") => rustcta_types::OrderType::IOC,
            Some("FOK") => rustcta_types::OrderType::FOK,
            Some("GTX") => rustcta_types::OrderType::PostOnly,
            _ => rustcta_types::OrderType::Limit,
        },
        _ => rustcta_types::OrderType::Limit,
    }
}

pub(super) fn parse_time_in_force(
    value: Option<&str>,
) -> Option<rustcta_exchange_api::TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(rustcta_exchange_api::TimeInForce::GTC),
        "IOC" => Some(rustcta_exchange_api::TimeInForce::IOC),
        "FOK" => Some(rustcta_exchange_api::TimeInForce::FOK),
        "GTX" => Some(rustcta_exchange_api::TimeInForce::GTX),
        _ => None,
    }
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .filter_map(|field| value.get(*field).and_then(value_as_i64))
        .find_map(DateTime::<Utc>::from_timestamp_millis)
}

pub(super) fn average_price_text(value: &Value) -> Option<String> {
    let filled = string_or_number(value.get("executedQty").or_else(|| value.get("z")))?;
    if filled.parse::<f64>().ok()? <= 0.0 {
        return None;
    }
    let quote = string_or_number(value.get("cummulativeQuoteQty").or_else(|| value.get("Z")))?;
    let average = quote.parse::<f64>().ok()? / filled.parse::<f64>().ok()?;
    if average > 0.0 {
        Some(average.to_string())
    } else {
        None
    }
}

fn precision_from_step(step: Option<f64>) -> Option<u32> {
    let step = step?;
    if step <= 0.0 {
        return Some(0);
    }
    Some(
        format!("{step:.16}")
            .trim_end_matches('0')
            .split('.')
            .nth(1)
            .map(|fraction| fraction.len() as u32)
            .unwrap_or(0),
    )
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| {
        value
            .as_u64()
            .map(|value| value as u32)
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn split_contract_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('_');
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    (!base.is_empty() && !quote.is_empty()).then_some((base, quote))
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 6] = ["USDT", "USDC", "BTC", "ETH", "USD", "EUR"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), quote.to_string()))
    })
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
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
