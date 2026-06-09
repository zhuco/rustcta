use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    pairs: &Value,
    currencies: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let currency_tags = currency_id_to_tag(currencies)?;
    let pairs = pairs.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "LATOKEN pair response must be an array",
            pairs,
        )
    })?;
    pairs
        .iter()
        .filter(|pair| pair_is_active(pair))
        .map(|pair| parse_symbol_rule(exchange_id, pair, &currency_tags))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
    currency_tags: &HashMap<String, String>,
) -> ExchangeApiResult<SymbolRules> {
    let base_asset = asset_tag(value, "baseCurrency", "baseCurrencyTag", currency_tags)
        .ok_or_else(|| parse_error(exchange_id.clone(), "LATOKEN pair missing base", value))?;
    let quote_asset = asset_tag(value, "quoteCurrency", "quoteCurrencyTag", currency_tags)
        .ok_or_else(|| parse_error(exchange_id.clone(), "LATOKEN pair missing quote", value))?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol_text = format!("{base_asset}_{quote_asset}");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                exchange_symbol_text,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_from_fields(value, &["priceTick", "price_tick"]),
        quantity_increment: string_from_fields(value, &["quantityTick", "quantity_tick"]),
        min_price: None,
        max_price: None,
        min_quantity: string_from_fields(value, &["minOrderQuantity", "min_order_quantity"]),
        max_quantity: None,
        min_notional: string_from_fields(value, &["minOrderCostUsd", "min_order_cost_usd"]),
        max_notional: string_from_fields(value, &["maxOrderCostUsd", "max_order_cost_usd"]),
        price_precision: integer_from_fields(value, &["priceDecimals", "price_decimals"]),
        quantity_precision: integer_from_fields(value, &["quantityDecimals", "quantity_decimals"]),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(
        exchange_id,
        value.get("bid").or_else(|| value.get("bids")),
        true,
    )?;
    let mut asks = parse_levels(
        exchange_id,
        value.get("ask").or_else(|| value.get("asks")),
        false,
    )?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = first_timestamp(value, &["timestamp", "time", "updatedAt"]);
    snapshot.sequence = value_as_u64(value.get("nonce").or_else(|| value.get("sequence")));
    Ok(snapshot)
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(100).clamp(1, 1000)
}

pub fn split_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = normalize_exchange_symbol(symbol)?;
    let (base, quote) =
        normalized
            .split_once('_')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("LATOKEN symbol {symbol} must use BASE_QUOTE form"),
            })?;
    Ok((base.to_string(), quote.to_string()))
}

pub fn normalize_exchange_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "LATOKEN symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed
        .replace(['/', '-'], "_")
        .replace("__", "_")
        .to_ascii_uppercase();
    if upper.contains('_') {
        return Ok(upper);
    }
    let (base, quote) =
        split_compact_symbol(&upper).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer LATOKEN quote asset from symbol {symbol}"),
        })?;
    Ok(format!("{base}_{quote}"))
}

fn currency_id_to_tag(value: &Value) -> ExchangeApiResult<HashMap<String, String>> {
    let currencies = value.as_array().ok_or_else(|| {
        parse_error(
            ExchangeId::new("latoken").expect("latoken"),
            "LATOKEN currency response must be an array",
            value,
        )
    })?;
    Ok(currencies
        .iter()
        .filter_map(|currency| {
            let id = currency.get("id").and_then(Value::as_str)?;
            let tag = currency.get("tag").and_then(Value::as_str)?;
            Some((id.to_string(), tag.to_ascii_uppercase()))
        })
        .collect())
}

fn asset_tag(
    value: &Value,
    id_field: &str,
    tag_field: &str,
    currency_tags: &HashMap<String, String>,
) -> Option<String> {
    value
        .get(tag_field)
        .and_then(Value::as_str)
        .filter(|tag| !tag.trim().is_empty())
        .map(|tag| tag.to_ascii_uppercase())
        .or_else(|| {
            value
                .get(id_field)
                .and_then(Value::as_str)
                .and_then(|id| currency_tags.get(id).cloned())
        })
}

fn pair_is_active(value: &Value) -> bool {
    value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status.eq_ignore_ascii_case("PAIR_STATUS_ACTIVE"))
        .unwrap_or(true)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    bids: bool,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "LATOKEN order book response missing levels",
            &Value::Null,
        )
    })?;
    let mut parsed = levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid LATOKEN level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid LATOKEN level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid LATOKEN level price", level)
                })?;
            let quantity = level
                .get("quantity")
                .or_else(|| level.get("amount"))
                .and_then(number_from_value)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid LATOKEN level quantity", level)
                })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    if bids {
        parsed.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        parsed.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(parsed)
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    for quote in [
        "USDT", "USDC", "USD", "EUR", "BTC", "ETH", "BNB", "LA", "TRY", "BRL",
    ] {
        if let Some(base) = symbol.strip_suffix(quote).filter(|base| !base.is_empty()) {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    None
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn integer_from_fields(value: &Value, fields: &[&str]) -> Option<u32> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
            .and_then(|value| u32::try_from(value).ok())
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn value_as_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str()?.trim().parse().ok())
    })
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        let value = value.get(*field)?;
        if let Some(millis) = value.as_i64().filter(|millis| *millis > 10_000_000_000) {
            return DateTime::<Utc>::from_timestamp_millis(millis);
        }
        let seconds = value.as_i64().or_else(|| value.as_str()?.parse().ok())?;
        Utc.timestamp_opt(seconds, 0).single()
    })
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
