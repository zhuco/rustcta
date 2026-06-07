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
    fallback_market_type: Option<MarketType>,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = rows(value);
    let mut rules = Vec::new();
    for item in rows {
        let market_type = fallback_market_type
            .or_else(|| parse_market_type_from_item(item))
            .unwrap_or(MarketType::Spot);
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            continue;
        }
        if is_inactive(item) {
            continue;
        }
        let symbol = item
            .get("coindcx_name")
            .or_else(|| item.get("symbol"))
            .or_else(|| item.get("pair"))
            .or_else(|| item.get("market"))
            .or_else(|| item.get("instrument"))
            .and_then(Value::as_str)
            .map(normalize_coindcx_symbol)
            .ok_or_else(|| parse_error(exchange_id.clone(), "CoinDCX symbol missing", item))?;
        if !requested.is_empty()
            && !requested.iter().any(|requested| {
                requested.market_type == market_type
                    && normalize_coindcx_symbol(&requested.exchange_symbol.symbol)
                        .eq_ignore_ascii_case(&symbol)
            })
        {
            continue;
        }
        rules.push(parse_symbol_rule(exchange_id, market_type, &symbol, item)?);
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"), "bids")?;
    let asks = parse_levels(exchange_id, data.get("asks"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coindcx order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = string_or_number(
        data.get("last_update_id")
            .or_else(|| data.get("lastUpdateId"))
            .or_else(|| data.get("sequence")),
    )
    .and_then(|value| value.parse::<u64>().ok());
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("ts"))
        .and_then(number_i64)
        .and_then(timestamp_from_millis_or_seconds);
    Ok(snapshot)
}

pub fn normalize_coindcx_symbol(symbol: &str) -> String {
    symbol.trim().replace(['-', '/'], "_").to_ascii_uppercase()
}

pub fn coindcx_market_symbol(symbol: &str) -> String {
    normalize_coindcx_symbol(symbol).replace('_', "")
}

pub fn coindcx_futures_symbol(symbol: &str) -> String {
    let normalized = normalize_coindcx_symbol(symbol);
    if normalized.ends_with("-FUTURES") {
        normalized
    } else {
        format!("{}-FUTURES", normalized.replace('_', ""))
    }
}

pub fn split_symbol_assets(symbol: &str) -> (String, String) {
    let clean = normalize_coindcx_symbol(symbol).replace("-FUTURES", "");
    if let Some((base, quote)) = clean.split_once('_') {
        return (base.to_string(), quote.to_string());
    }
    for quote in ["USDT", "INR", "BTC", "ETH", "USDC", "BUSD"] {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                clean[..clean.len() - quote.len()].to_string(),
                quote.to_string(),
            );
        }
    }
    (clean, "USDT".to_string())
}

pub fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let normalized = normalize_coindcx_symbol(symbol);
    let (base, quote) = split_symbol_assets(&normalized);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
            .map_err(validation_error)?,
    })
}

pub fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub fn number_i64(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

pub fn timestamp_from_millis_or_seconds(value: i64) -> Option<chrono::DateTime<Utc>> {
    if value > 10_000_000_000 {
        Utc.timestamp_millis_opt(value).single()
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}

pub fn rows(value: &Value) -> Vec<&Value> {
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.get("markets").and_then(Value::as_array))
        .or_else(|| value.get("result").and_then(Value::as_array))
        .or_else(|| value.as_array())
        .map(|rows| rows.iter().collect())
        .unwrap_or_else(|| vec![value])
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

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let base_asset = value
        .get("base_currency_short_name")
        .or_else(|| value.get("base_currency"))
        .or_else(|| value.get("base_asset"))
        .or_else(|| value.get("base"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).0);
    let quote_asset = value
        .get("target_currency_short_name")
        .or_else(|| value.get("quote_currency"))
        .or_else(|| value.get("quote_asset"))
        .or_else(|| value.get("quote"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).1);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let scope = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol.to_string())
            .map_err(validation_error)?,
    };
    let tick = string_or_number(
        value
            .get("min_price_increment")
            .or_else(|| value.get("price_increment"))
            .or_else(|| value.get("tick_size")),
    );
    let step = string_or_number(
        value
            .get("min_quantity_increment")
            .or_else(|| value.get("quantity_increment"))
            .or_else(|| value.get("step_size")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: scope,
        base_asset,
        quote_asset,
        price_increment: tick.clone(),
        quantity_increment: step.clone(),
        min_price: string_or_number(value.get("min_price")),
        max_price: string_or_number(value.get("max_price")),
        min_quantity: string_or_number(
            value
                .get("min_quantity")
                .or_else(|| value.get("min_order_size"))
                .or_else(|| value.get("min_size")),
        ),
        max_quantity: string_or_number(value.get("max_quantity").or_else(|| value.get("max_size"))),
        min_notional: string_or_number(
            value
                .get("min_notional")
                .or_else(|| value.get("min_order_value"))
                .or_else(|| value.get("min_notional_value")),
        ),
        max_notional: None,
        price_precision: tick.as_deref().and_then(precision),
        quantity_precision: step.as_deref().and_then(precision),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: market_type == MarketType::Spot,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let value = value.ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("CoinDCX order book missing {side}"),
            &Value::Null,
        )
    })?;
    let mut levels = Vec::new();
    match value {
        Value::Array(rows) => {
            for row in rows {
                if let Some(values) = row.as_array() {
                    let price = decimal_as_f64(values.first()).ok_or_else(|| {
                        parse_error(exchange_id.clone(), "CoinDCX level missing price", row)
                    })?;
                    let quantity = decimal_as_f64(values.get(1)).ok_or_else(|| {
                        parse_error(exchange_id.clone(), "CoinDCX level missing quantity", row)
                    })?;
                    levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
                } else if let Some(object) = row.as_object() {
                    let price = decimal_as_f64(object.get("price")).ok_or_else(|| {
                        parse_error(exchange_id.clone(), "CoinDCX level missing price", row)
                    })?;
                    let quantity = decimal_as_f64(
                        object
                            .get("quantity")
                            .or_else(|| object.get("qty"))
                            .or_else(|| object.get("volume")),
                    )
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "CoinDCX level missing quantity", row)
                    })?;
                    levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
                }
            }
        }
        Value::Object(map) => {
            for (price, quantity) in map {
                let price = price.parse::<f64>().map_err(validation_error)?;
                let quantity = decimal_as_f64(Some(quantity)).ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "CoinDCX object depth level missing quantity",
                        quantity,
                    )
                })?;
                levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
            }
        }
        _ => {
            return Err(parse_error(
                exchange_id.clone(),
                "CoinDCX order book side has unsupported shape",
                value,
            ));
        }
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

fn parse_market_type_from_item(value: &Value) -> Option<MarketType> {
    let text = value
        .get("market_type")
        .or_else(|| value.get("kind"))
        .or_else(|| value.get("product_type"))
        .or_else(|| value.get("instrument_type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if text.contains("future") || text.contains("perp") || text.contains("derivative") {
        Some(MarketType::Perpetual)
    } else if text.contains("spot") {
        Some(MarketType::Spot)
    } else {
        None
    }
}

fn is_inactive(value: &Value) -> bool {
    value
        .get("status")
        .or_else(|| value.get("ecode"))
        .or_else(|| value.get("state"))
        .and_then(Value::as_str)
        .map(|status| {
            let status = status.to_ascii_lowercase();
            status.contains("inactive")
                || status.contains("disabled")
                || status.contains("delisted")
                || status == "false"
        })
        .unwrap_or(false)
}

fn normalize_asset(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn precision(value: &str) -> Option<u32> {
    let trimmed = value.trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .or(Some(0))
}
