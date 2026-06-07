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
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .get("rows")
        .or_else(|| value.get("rows"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "woo instruments response is not an array",
                value,
            )
        })?;
    rows.iter()
        .map(|value| parse_symbol_rule(exchange_id, value))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (market_type, fallback_base, fallback_quote) = parse_woo_market_symbol(&exchange_symbol)?;
    let base_asset = value
        .get("baseAsset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quoteAsset")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, &exchange_symbol)
            .map_err(validation_error)?,
    };
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            matches!(
                status.to_ascii_uppercase().as_str(),
                "TRADING" | "ONLINE" | "OPEN"
            )
        })
        .unwrap_or(true);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("quoteTick")),
        quantity_increment: string_or_number(value.get("baseTick")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("baseMin")),
        max_quantity: string_or_number(value.get("baseMax")),
        min_notional: string_or_number(value.get("minNotional")).or_else(|| {
            decimal_product(
                string_or_number(value.get("quoteMin")).as_deref(),
                Some("1"),
            )
        }),
        max_notional: string_or_number(value.get("quoteMax")),
        price_precision: precision_from_increment(
            string_or_number(value.get("quoteTick")).as_deref(),
        ),
        quantity_precision: precision_from_increment(
            string_or_number(value.get("baseTick")).as_deref(),
        ),
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
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = data
        .get("seq")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_woo_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let raw = symbol.symbol_text();
    let normalized = raw
        .trim()
        .replace('/', "_")
        .replace('-', "_")
        .to_ascii_uppercase();
    if normalized.starts_with("SPOT_") || normalized.starts_with("PERP_") {
        return Ok(normalized);
    }
    let (base, quote) = symbol
        .canonical_symbol
        .as_ref()
        .map(|canonical| {
            (
                canonical.base_asset().to_string(),
                canonical.quote_asset().to_string(),
            )
        })
        .or_else(|| split_compact_symbol(&normalized))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("woo cannot infer base/quote assets from symbol {raw}"),
        })?;
    let prefix = match symbol.market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual | MarketType::Futures => "PERP",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.unsupported_market_type",
            });
        }
    };
    Ok(format!("{prefix}_{base}_{quote}"))
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
}

pub fn parse_woo_market_symbol(symbol: &str) -> ExchangeApiResult<(MarketType, String, String)> {
    let normalized = symbol.trim().to_ascii_uppercase();
    let parts = normalized.split('_').collect::<Vec<_>>();
    match parts.as_slice() {
        ["SPOT", base, quote] if !base.is_empty() && !quote.is_empty() => {
            Ok((MarketType::Spot, (*base).to_string(), (*quote).to_string()))
        }
        ["PERP", base, quote] | ["FUTURES", base, quote]
            if !base.is_empty() && !quote.is_empty() =>
        {
            Ok((
                MarketType::Perpetual,
                (*base).to_string(),
                (*quote).to_string(),
            ))
        }
        _ => split_compact_symbol(&normalized)
            .map(|(base, quote)| (MarketType::Spot, base, quote))
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("invalid woo symbol {symbol}"),
            }),
    }
}

pub trait SymbolScopeExt {
    fn symbol_text(&self) -> &str;
}

impl SymbolScopeExt for SymbolScope {
    fn symbol_text(&self) -> &str {
        &self.exchange_symbol.symbol
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "woo order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(array) => {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(map) => {
                let price = map
                    .get("price")
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level price", level)
                    })?;
                let quantity = map
                    .get("quantity")
                    .or_else(|| map.get("size"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "invalid woo order book level",
                level,
            )),
        })
        .collect()
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
        Value::String(text) if text != "null" && !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn precision_from_increment(increment: Option<&str>) -> Option<u32> {
    let increment = increment?;
    let decimals = increment.split_once('.')?.1.trim_end_matches('0');
    Some(decimals.len() as u32)
}

fn decimal_product(left: Option<&str>, right: Option<&str>) -> Option<String> {
    let value = left?.parse::<f64>().ok()? * right?.parse::<f64>().ok()?;
    Some(value.to_string())
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
