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
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let quote_asset = value
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or("EUR")
        .to_ascii_uppercase();
    let canonical_symbol = CanonicalSymbol::new("BTC", &quote_asset).map_err(validation_error)?;
    Ok(vec![SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                format!("BTC/{quote_asset}"),
            )
            .map_err(validation_error)?,
        },
        base_asset: "BTC".to_string(),
        quote_asset,
        price_increment: None,
        quantity_increment: Some("0.00000001".to_string()),
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: Some(8),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    }])
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    if let Some(depth) = depth {
        let depth = depth.clamp(1, 100) as usize;
        bids.truncate(depth);
        asks.truncate(depth);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "paymium order book requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("at")
        .and_then(timestamp_value_to_datetime)
        .or_else(|| value.get("timestamp").and_then(timestamp_value_to_datetime));
    Ok(snapshot)
}

pub fn normalize_paymium_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "paymium symbol must not be empty".to_string(),
        });
    }
    let normalized = trimmed
        .replace(':', "/")
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    let (base, quote) = if let Some((base, quote)) = normalized.split_once('/') {
        (base.to_string(), quote.to_string())
    } else if let Some(quote) = normalized.strip_prefix("BTC") {
        ("BTC".to_string(), quote.to_string())
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Paymium base/quote from symbol {symbol}"),
        });
    };
    if base != "BTC" || quote != "EUR" {
        return Err(ExchangeApiError::Unsupported {
            operation: "paymium.only_btc_eur_spot_public_rest",
        });
    }
    Ok((base, quote))
}

pub fn paymium_currency_path(symbol: &str) -> ExchangeApiResult<String> {
    let (_base, quote) = normalize_paymium_symbol(symbol)?;
    Ok(quote.to_ascii_lowercase())
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Paymium order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = level
                .get("price")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("amount")
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level amount", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn timestamp_value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        if number > 1_000_000_000_000 {
            return DateTime::<Utc>::from_timestamp_millis(number);
        }
        return Utc.timestamp_opt(number, 0).single();
    }
    DateTime::parse_from_rfc3339(value.as_str()?)
        .map(|time| time.with_timezone(&Utc))
        .ok()
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message,
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
