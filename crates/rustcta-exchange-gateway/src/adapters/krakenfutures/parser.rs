use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_futures_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let rows = value
        .get("instruments")
        .or_else(|| {
            value
                .get("result")
                .and_then(|result| result.get("instruments"))
        })
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Kraken Futures instruments response missing instruments",
                value,
            )
        })?;
    rows.iter()
        .filter_map(|row| parse_futures_symbol_rule(exchange_id, row).transpose())
        .collect()
}

fn parse_futures_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Option<SymbolRules>> {
    let symbol_text = value
        .get("symbol")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "missing futures symbol", value))?;
    let Some(canonical) =
        canonical_from_futures_symbol(symbol_text).or_else(|| canonical_from_futures_assets(value))
    else {
        return Ok(None);
    };
    let market_type = MarketType::Perpetual;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
        .map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical.clone()),
        exchange_symbol,
    };
    let price_increment =
        string_or_number(value.get("tickSize").or_else(|| value.get("tick_size")));
    let quantity_increment =
        string_or_number(value.get("lotSize").or_else(|| value.get("contractSize")))
            .or_else(|| Some("1".to_string()));
    Ok(Some(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset: canonical.base_asset().to_string(),
        quote_asset: canonical.quote_asset().to_string(),
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("minTradeSize")
                .or_else(|| value.get("minimumOrderSize")),
        ),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_step_text(price_increment.as_deref()),
        quantity_precision: precision_from_step_text(quantity_increment.as_deref()),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    }))
}

pub fn parse_futures_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let book = value
        .get("orderBook")
        .or_else(|| value.get("orderbook"))
        .or_else(|| value.get("book"))
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    parse_orderbook(
        exchange_id,
        symbol,
        book,
        book.get("bids").or_else(|| book.get("bid")),
        book.get("asks").or_else(|| book.get("ask")),
        book.get("sequence")
            .or_else(|| book.get("seq"))
            .and_then(u64_from_value),
    )
}

fn parse_orderbook(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
    bids_value: Option<&Value>,
    asks_value: Option<&Value>,
    sequence: Option<u64>,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Kraken order book request requires canonical_symbol".to_string(),
            })?;
    let bids = parse_levels(exchange_id, bids_value)?;
    let asks = parse_levels(exchange_id, asks_value)?;
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
    snapshot.sequence = sequence;
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .or_else(|| value.get("time"))
        .and_then(timestamp_from_value)
        .or_else(|| {
            value
                .get("bids")
                .and_then(Value::as_array)
                .and_then(|levels| levels.first())
                .and_then(Value::as_array)
                .and_then(|level| level.get(2))
                .and_then(timestamp_from_value)
        });
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Kraken order book missing bid/ask levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(f64_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(f64_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .or_else(|| level.get("p"))
                .and_then(f64_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("qty")
                .or_else(|| level.get("quantity"))
                .or_else(|| level.get("size"))
                .and_then(f64_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub fn normalize_futures_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    if symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.non_perpetual_symbol",
        });
    }
    let raw = symbol.exchange_symbol.symbol.trim().to_ascii_uppercase();
    if raw.starts_with("PF_") || raw.starts_with("PI_") {
        return Ok(raw);
    }
    let canonical =
        symbol
            .canonical_symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Kraken futures symbol requires canonical_symbol".to_string(),
            })?;
    let base = krakenfutures_asset(canonical.base_asset());
    Ok(format!("PF_{}{}", base, canonical.quote_asset()))
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 500)
}

pub fn canonical_from_pair(pair: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let normalized = pair.trim().to_ascii_uppercase().replace('-', "/");
    if let Some((base, quote)) = normalized.split_once('/') {
        return CanonicalSymbol::new(normalize_asset(base), normalize_asset(quote))
            .map_err(validation_error);
    }
    for quote in ["USDT", "USDC", "USD", "EUR", "BTC", "ETH"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return CanonicalSymbol::new(normalize_asset(base), normalize_asset(quote))
                    .map_err(validation_error);
            }
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("unable to parse Kraken pair {pair}"),
    })
}

pub fn canonical_from_futures_symbol(symbol: &str) -> Option<CanonicalSymbol> {
    let normalized = symbol.trim().to_ascii_uppercase().replace(['-', '/'], "_");
    let raw = normalized
        .strip_prefix("PF_")
        .or_else(|| normalized.strip_prefix("PI_"))
        .unwrap_or(&normalized);
    for quote in ["USDT", "USDC", "USD", "EUR"] {
        if let Some(base) = raw.strip_suffix(quote) {
            if !base.is_empty() {
                return CanonicalSymbol::new(normalize_asset(base), normalize_asset(quote)).ok();
            }
        }
    }
    None
}

fn canonical_from_futures_assets(value: &Value) -> Option<CanonicalSymbol> {
    let base = value
        .get("underlying")
        .or_else(|| value.get("base"))
        .or_else(|| value.get("baseCurrency"))
        .and_then(Value::as_str)
        .map(normalize_asset)?;
    let quote = value
        .get("quoteCurrency")
        .or_else(|| value.get("quote"))
        .and_then(Value::as_str)
        .map(normalize_asset)?;
    CanonicalSymbol::new(base, quote).ok()
}

pub fn normalize_asset(asset: &str) -> String {
    let raw = asset.trim().to_ascii_uppercase();
    let candidate = if raw.len() == 4 && (raw.starts_with('X') || raw.starts_with('Z')) {
        raw[1..].to_string()
    } else {
        raw
    };
    match candidate.as_str() {
        "XBT" => "BTC".to_string(),
        "XDG" => "DOGE".to_string(),
        value => value.to_string(),
    }
}

pub fn krakenfutures_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "BTC" => "XBT".to_string(),
        "DOGE" => "XDG".to_string(),
        value => value.to_string(),
    }
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn f64_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn u64_from_value(value: &Value) -> Option<u64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64(),
        _ => None,
    }
}

pub(super) fn timestamp_from_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(ts) = DateTime::parse_from_rfc3339(text) {
            return Some(ts.with_timezone(&Utc));
        }
        if let Ok(seconds) = text.parse::<f64>() {
            return timestamp_from_seconds(seconds);
        }
    }
    f64_from_value(value).and_then(timestamp_from_seconds)
}

pub(super) fn timestamp_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }
    let seconds = value.trunc() as i64;
    let nanos = (value.fract().abs() * 1_000_000_000.0).round() as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
}

pub(super) fn precision_from_step_text(value: Option<&str>) -> Option<u32> {
    let text = value?;
    text.split_once('.')
        .map(|(_, decimals)| decimals.trim_end_matches('0').len() as u32)
        .or(Some(0))
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn reject_krakenfutures_error_response(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<()> {
    let spot_errors = value
        .get("error")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .collect::<Vec<_>>();
    if !spot_errors.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            spot_errors.join("; "),
            value,
        ));
    }
    let futures_result = value.get("result").and_then(Value::as_str);
    if futures_result.is_some_and(|result| !result.eq_ignore_ascii_case("success")) {
        let message = value
            .get("error")
            .or_else(|| value.get("errors"))
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| futures_result.unwrap_or("Kraken Futures error"));
        return Err(parse_error(exchange_id.clone(), message, value));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rustcta_types::ExchangeId;
    use serde_json::json;

    use super::*;

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("krakenfutures").expect("exchange")
    }

    #[test]
    fn krakenfutures_futures_symbol_rules_should_parse_linear_perp() {
        let value = json!({
            "instruments": [{
                "symbol": "PF_XBTUSDT",
                "type": "perpetual",
                "tickSize": "0.1",
                "contractSize": "1",
                "minTradeSize": "0.001"
            }]
        });

        let rules = parse_futures_symbol_rules(&exchange_id(), &value).expect("rules");

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
        assert_eq!(
            rules[0].symbol.canonical_symbol.as_ref().unwrap().as_str(),
            "BTC/USDT"
        );
    }
}
