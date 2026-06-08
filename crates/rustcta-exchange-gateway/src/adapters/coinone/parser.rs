use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub const COINONE_QUOTE: &str = "KRW";

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .get("markets")
        .or_else(|| value.get("data"))
        .or_else(|| value.get("currencies"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "markets response missing rows", value))?;
    markets
        .iter()
        .filter(|market| quote_currency(market).is_none_or(|quote| quote == COINONE_QUOTE))
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let quote_asset = quote_currency(value).unwrap_or_else(|| COINONE_QUOTE.to_string());
    if quote_asset != COINONE_QUOTE {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinone.non_krw_market",
        });
    }
    let base_asset = value
        .get("target_currency")
        .or_else(|| value.get("targetCurrency"))
        .or_else(|| value.get("currency"))
        .or_else(|| value.get("base_currency"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "market missing target_currency", value))?
        .to_ascii_uppercase();
    let exchange_symbol = format!("{base_asset}-{quote_asset}");
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
    let tradable = value
        .get("trade_status")
        .or_else(|| value.get("tradeStatus"))
        .and_then(Value::as_str)
        .map(|status| matches!(status.to_ascii_lowercase().as_str(), "normal" | "tradable"))
        .or_else(|| value.get("trading").and_then(Value::as_bool))
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("price_unit")
                .or_else(|| value.get("priceUnit"))
                .or_else(|| value.get("tick_size")),
        )
        .or_else(|| Some("1".to_string())),
        quantity_increment: string_or_number(
            value
                .get("qty_unit")
                .or_else(|| value.get("quantity_unit"))
                .or_else(|| value.get("target_currency_unit")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("min_qty")
                .or_else(|| value.get("minQuantity"))
                .or_else(|| value.get("min_order_amount")),
        ),
        max_quantity: string_or_number(value.get("max_qty").or_else(|| value.get("maxQuantity"))),
        min_notional: string_or_number(
            value
                .get("min_order_amount")
                .or_else(|| value.get("min_quote_amount"))
                .or_else(|| value.get("minNotional")),
        ),
        max_notional: None,
        price_precision: precision_from_step(
            string_or_number(
                value
                    .get("price_unit")
                    .or_else(|| value.get("priceUnit"))
                    .or_else(|| value.get("tick_size")),
            )
            .as_deref(),
        ),
        quantity_precision: precision_from_step(
            string_or_number(
                value
                    .get("qty_unit")
                    .or_else(|| value.get("quantity_unit"))
                    .or_else(|| value.get("target_currency_unit")),
            )
            .as_deref(),
        ),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("orderbook")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_levels(
        exchange_id,
        data.get("bids").or_else(|| data.get("bid")),
        true,
    )?;
    let asks = parse_levels(
        exchange_id,
        data.get("asks").or_else(|| data.get("ask")),
        false,
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinone order book request requires canonical_symbol".to_string(),
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
        .get("sequence")
        .or_else(|| data.get("id"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = first_timestamp_millis(data, &["timestamp", "time"]);
    Ok(snapshot)
}

pub fn normalize_coinone_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let normalized = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let (base, quote) = normalized
        .split_once('-')
        .map(|(base, quote)| (base.to_string(), quote.to_string()))
        .or_else(|| {
            normalized
                .strip_suffix(COINONE_QUOTE)
                .filter(|base| !base.is_empty())
                .map(|base| (base.to_string(), COINONE_QUOTE.to_string()))
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Coinone KRW symbol from {symbol}"),
        })?;
    if quote != COINONE_QUOTE {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinone.non_krw_market",
        });
    }
    Ok((base, quote))
}

pub fn normalize_coinone_exchange_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let (base, quote) = normalize_coinone_symbol(symbol)?;
    Ok(format!("{base}-{quote}"))
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=15 => 15,
        _ => 30,
    }
}

pub(super) fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    bids: bool,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing levels",
            &Value::Null,
        )
    })?;
    let mut parsed = levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("qty")
                .or_else(|| level.get("quantity"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
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

pub(super) fn quote_currency(value: &Value) -> Option<String> {
    value
        .get("quote_currency")
        .or_else(|| value.get("quoteCurrency"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
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

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid Coinone decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
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

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?.trim();
    if step.is_empty() {
        return None;
    }
    let trimmed = step.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .or(Some(0))
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

#[allow(dead_code)]
fn _schema_version_marker(_: SchemaVersion) {}
