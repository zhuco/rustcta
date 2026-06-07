use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let instruments = data_payload(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Deepcoin instruments response is not an array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for instrument in instruments {
        if let Some(rule) = parse_symbol_rule(exchange_id, market_type, instrument)? {
            rules.push(rule);
        }
    }
    Ok(rules)
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<SymbolRules>> {
    let exchange_symbol = required_str(exchange_id, value, "instId")?.to_ascii_uppercase();
    let (base_asset, quote_asset) = match market_type {
        MarketType::Spot => {
            let base = value
                .get("baseCcy")
                .and_then(Value::as_str)
                .map(str::to_ascii_uppercase)
                .or_else(|| {
                    split_deepcoin_symbol(&exchange_symbol, MarketType::Spot).map(|parts| parts.0)
                })
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "spot symbol missing base", value)
                })?;
            let quote = value
                .get("quoteCcy")
                .and_then(Value::as_str)
                .map(str::to_ascii_uppercase)
                .or_else(|| {
                    split_deepcoin_symbol(&exchange_symbol, MarketType::Spot).map(|parts| parts.1)
                })
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "spot symbol missing quote", value)
                })?;
            (base, quote)
        }
        MarketType::Perpetual => {
            let Some((base, quote)) =
                split_deepcoin_symbol(&exchange_symbol, MarketType::Perpetual)
            else {
                return Ok(None);
            };
            if quote != "USDT" {
                return Ok(None);
            }
            (base, quote)
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "deepcoin.unsupported_market_type",
            })
        }
    };
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    };
    let tradable = value
        .get("state")
        .or_else(|| value.get("status"))
        .and_then(Value::as_str)
        .is_none_or(|state| {
            let state = state.to_ascii_lowercase();
            matches!(state.as_str(), "live" | "trading" | "online" | "1")
        });
    let price_increment = string_or_number(value.get("tickSz").or_else(|| value.get("tickSize")));
    let quantity_increment = string_or_number(value.get("lotSz").or_else(|| value.get("stepSize")));
    Ok(Some(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minSz").or_else(|| value.get("minQty"))),
        max_quantity: string_or_number(
            value
                .get("maxLmtSz")
                .or_else(|| value.get("maxMktSz"))
                .or_else(|| value.get("maxQty")),
        ),
        min_notional: string_or_number(value.get("minNotional").or_else(|| value.get("minAmt"))),
        max_notional: None,
        price_precision: precision_from_step(price_increment.as_deref()),
        quantity_precision: precision_from_step(quantity_increment.as_deref()),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    }))
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = data_payload(value);
    let bids = parse_levels(exchange_id, payload.get("bids"))?;
    let asks = parse_levels(exchange_id, payload.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deepcoin order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = payload
        .get("ts")
        .or_else(|| payload.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_deepcoin_symbol(
    symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let mut normalized = trimmed.replace(['/', '_'], "-").to_ascii_uppercase();
    if !normalized.contains('-') {
        normalized =
            split_compact_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot infer Deepcoin symbol from {symbol}"),
            })?;
    }
    if market_type == MarketType::Perpetual && !normalized.ends_with("-SWAP") {
        normalized.push_str("-SWAP");
    }
    if market_type == MarketType::Spot && normalized.ends_with("-SWAP") {
        normalized.truncate(normalized.len() - "-SWAP".len());
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 400)
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

pub(super) fn split_deepcoin_symbol(
    symbol: &str,
    market_type: MarketType,
) -> Option<(String, String)> {
    let mut symbol = symbol.trim().to_ascii_uppercase();
    if market_type == MarketType::Perpetual {
        symbol = symbol.trim_end_matches("-SWAP").to_string();
    }
    if let Some((base, quote)) = symbol.split_once('-') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    split_compact_symbol(&symbol).and_then(|value| {
        let (base, quote) = value.split_once('-')?;
        Some((base.to_string(), quote.to_string()))
    })
}

pub(super) fn product_inst_type(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual => "SWAP",
        _ => "UNSUPPORTED",
    }
}

pub(super) fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" | "s" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

pub(super) fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        "net" | "merge" => PositionSide::Net,
        _ => PositionSide::None,
    }
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
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value).filter(|value| !value.is_empty())
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Deepcoin order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Deepcoin order book level is not an array",
                    level,
                )
            })?;
            let price = decimal_as_f64(array.first()).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book price", level)
            })?;
            let quantity = decimal_as_f64(array.get(1)).ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book quantity", level)
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn split_compact_symbol(symbol: &str) -> Option<String> {
    const QUOTES: [&str; 7] = ["USDT", "USDC", "BTC", "ETH", "USD", "EUR", "TRY"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| format!("{base}-{quote}"))
    })
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?.trim();
    if step.is_empty() {
        return None;
    }
    let normalized = step.trim_end_matches('0').trim_end_matches('.');
    Some(
        normalized
            .split('.')
            .nth(1)
            .map(|fraction| fraction.len() as u32)
            .unwrap_or(0),
    )
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
