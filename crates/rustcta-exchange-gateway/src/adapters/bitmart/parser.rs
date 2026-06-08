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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    symbol_items(value)
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols response missing list", value))?
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, market_type, value))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (fallback_base, fallback_quote) = split_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = value
        .get("base_currency")
        .or_else(|| value.get("base"))
        .or_else(|| value.get("baseCoin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_base);
    let quote_asset = value
        .get("quote_currency")
        .or_else(|| value.get("quote"))
        .or_else(|| value.get("quoteCoin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or(fallback_quote);
    let status = value
        .get("trade_status")
        .or_else(|| value.get("status"))
        .and_then(|value| value_as_string(Some(value)))
        .unwrap_or_else(|| "trading".to_string());
    let tradable = matches!(
        status.to_ascii_lowercase().as_str(),
        "trading" | "active" | "open" | "1"
    );
    let price_precision = integer_from_value(
        value
            .get("price_precision")
            .or_else(|| value.get("pricePrecision")),
    );
    let quantity_precision = integer_from_value(
        value
            .get("base_min_size")
            .and_then(decimal_places)
            .map(Value::from)
            .as_ref()
            .or_else(|| value.get("vol_precision"))
            .or_else(|| value.get("quantityPrecision")),
    );
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: value_as_string(value.get("price_min_precision"))
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: value_as_string(value.get("base_min_size"))
            .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: None,
        max_price: None,
        min_quantity: value_as_string(value.get("min_buy_amount"))
            .or_else(|| value_as_string(value.get("base_min_size"))),
        max_quantity: None,
        min_notional: value_as_string(value.get("min_buy_amount")),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: market_type != MarketType::Spot,
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let bids = parse_levels(exchange_id, data.get("buys").or_else(|| data.get("bids")))?;
    let asks = parse_levels(exchange_id, data.get("sells").or_else(|| data.get("asks")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitmart order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let compact = symbol.trim().to_ascii_uppercase().replace(['/', '-'], "_");
    if compact.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(if market_type == MarketType::Spot {
        compact
    } else {
        compact.replace('_', "")
    })
}

pub fn normalize_depth(depth: Option<u32>) -> String {
    depth.unwrap_or(50).clamp(1, 200).to_string()
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn symbol_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.get("symbols")
        .or_else(|| data.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
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
            if let Some(array) = level.as_array() {
                let price = array
                    .first()
                    .and_then(number_from_value)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid price", level))?;
                let quantity = array
                    .get(1)
                    .and_then(number_from_value)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid quantity", level))?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = decimal_as_f64(level.get("price")).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid object level price", level)
            })?;
            let quantity = decimal_as_f64(
                level
                    .get("amount")
                    .or_else(|| level.get("volume"))
                    .or_else(|| level.get("qty")),
            )
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid object level qty", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
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

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    value_as_string(value)?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
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

fn number_from_value(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| {
        value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}

fn decimal_places(value: &Value) -> Option<u32> {
    let text = value.as_str()?;
    text.split_once('.')
        .map(|(_, fractional)| fractional.trim_end_matches('0').len() as u32)
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        return Some((base.to_string(), quote.to_string()));
    }
    const QUOTES: [&str; 6] = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}
