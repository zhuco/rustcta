use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookResponse, SymbolRules, SymbolRulesResponse,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

use crate::adapters::response_metadata;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRulesResponse> {
    let rows = rows(value);
    let rules = rows
        .iter()
        .filter_map(|row| parse_symbol_rule(exchange_id, market_type, row).transpose())
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), None),
        rules,
    })
}

pub fn parse_order_book(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let data = data_payload(value);
    let bids = parse_levels(data.get("bids").or_else(|| data.get("bid")).unwrap_or(data))?;
    let asks = parse_levels(data.get("asks").or_else(|| data.get("ask")).unwrap_or(data))?;
    let mut order_book = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        symbol
            .canonical_symbol
            .clone()
            .unwrap_or_else(|| canonical_from_exchange_symbol(&symbol.exchange_symbol.symbol)),
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    order_book.exchange_symbol = Some(symbol.exchange_symbol);
    order_book.sequence = data
        .get("sequence")
        .or_else(|| data.get("seq"))
        .and_then(value_as_u64);
    order_book.exchange_timestamp = timestamp_from_value(
        data.get("time")
            .or_else(|| data.get("timestamp"))
            .or_else(|| data.get("created_at")),
    );
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), None),
        order_book,
    })
}

pub fn normalize_bigone_symbol(value: &str, market_type: MarketType) -> String {
    let trimmed = value.trim();
    if trimmed.contains('-') {
        trimmed.to_ascii_uppercase()
    } else if market_type == MarketType::Perpetual && trimmed.to_ascii_uppercase().ends_with("USDT")
    {
        let base = trimmed.trim_end_matches("USDT").trim_end_matches("USD");
        format!("{}-USDT", base.to_ascii_uppercase())
    } else {
        let upper = trimmed.to_ascii_uppercase();
        for quote in ["USDT", "USDC", "BTC", "ETH", "USD"] {
            if upper.ends_with(quote) && upper.len() > quote.len() {
                return format!("{}-{quote}", &upper[..upper.len() - quote.len()]);
            }
        }
        upper
    }
}

pub fn data_payload(value: &Value) -> &Value {
    value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("asset_pair"))
        .or_else(|| value.get("contract"))
        .unwrap_or(value)
}

pub fn rows(value: &Value) -> Vec<Value> {
    let data = data_payload(value);
    if let Some(array) = data.as_array() {
        return array.clone();
    }
    for key in [
        "asset_pairs",
        "contracts",
        "orders",
        "assets",
        "positions",
        "fills",
        "trades",
        "items",
        "list",
    ] {
        if let Some(array) = data.get(key).and_then(Value::as_array) {
            return array.clone();
        }
    }
    vec![data.clone()]
}

pub fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = normalize_bigone_symbol(symbol, market_type);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_from_exchange_symbol(&symbol)),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

pub fn canonical_from_exchange_symbol(symbol: &str) -> CanonicalSymbol {
    let normalized = normalize_bigone_symbol(symbol, MarketType::Spot);
    if let Some((base, quote)) = normalized.split_once('-') {
        CanonicalSymbol::new(base, quote)
            .unwrap_or_else(|_| CanonicalSymbol::new("BTC", "USDT").unwrap())
    } else {
        CanonicalSymbol::new("BTC", "USDT").unwrap()
    }
}

pub fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

pub fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

pub fn timestamp_from_value(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let value = value?;
    if let Some(text) = value.as_str() {
        return DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .or_else(|| text.parse::<i64>().ok().and_then(timestamp_from_i64));
    }
    value.as_i64().and_then(timestamp_from_i64)
}

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

#[allow(dead_code)]
pub fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
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

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<SymbolRules>> {
    let symbol = value_as_string(
        value
            .get("name")
            .or_else(|| value.get("symbol"))
            .or_else(|| value.get("asset_pair_name"))
            .or_else(|| value.get("instrument_id")),
    )
    .or_else(|| {
        let base = value_as_string(value.get("base_asset").or_else(|| value.get("base")));
        let quote = value_as_string(value.get("quote_asset").or_else(|| value.get("quote")));
        Some(format!("{}-{}", base?, quote?))
    });
    let Some(symbol) = symbol else {
        return Ok(None);
    };
    let scope = symbol_scope(exchange_id, market_type, &symbol)?;
    let base = value_as_string(
        value
            .get("base_asset")
            .or_else(|| value.get("base"))
            .or_else(|| value.get("base_currency")),
    )
    .unwrap_or_else(|| {
        scope
            .canonical_symbol
            .as_ref()
            .map(|symbol| symbol.base_asset().to_string())
            .unwrap_or_else(|| "BTC".to_string())
    });
    let quote = value_as_string(
        value
            .get("quote_asset")
            .or_else(|| value.get("quote"))
            .or_else(|| value.get("quote_currency")),
    )
    .unwrap_or_else(|| {
        scope
            .canonical_symbol
            .as_ref()
            .map(|symbol| symbol.quote_asset().to_string())
            .unwrap_or_else(|| "USDT".to_string())
    });
    let status = value_as_string(value.get("status").or_else(|| value.get("state")))
        .unwrap_or_else(|| "active".to_string())
        .to_ascii_lowercase();
    let rule = SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: scope,
        base_asset: base.to_ascii_uppercase(),
        quote_asset: quote.to_ascii_uppercase(),
        price_increment: value_as_string(
            value
                .get("quote_scale")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.get("tick_size")),
        ),
        quantity_increment: value_as_string(
            value
                .get("base_scale")
                .or_else(|| value.get("amount_increment"))
                .or_else(|| value.get("quantity_increment"))
                .or_else(|| value.get("step_size")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: value_as_string(
            value
                .get("min_quote_value")
                .or_else(|| value.get("min_amount"))
                .or_else(|| value.get("min_order_size")),
        ),
        max_quantity: None,
        min_notional: value_as_string(
            value
                .get("min_quote_value")
                .or_else(|| value.get("min_notional")),
        ),
        max_notional: None,
        price_precision: value
            .get("quote_scale")
            .and_then(value_as_u64)
            .map(|value| value as u32),
        quantity_precision: value
            .get("base_scale")
            .and_then(value_as_u64)
            .map(|value| value as u32),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    };
    if matches!(status.as_str(), "offline" | "delisted" | "disabled") {
        Ok(None)
    } else {
        Ok(Some(rule))
    }
}

fn parse_levels(value: &Value) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let Some(array) = value.as_array() else {
        return Ok(Vec::new());
    };
    array
        .iter()
        .filter_map(|level| {
            let (price, quantity) = if let Some(items) = level.as_array() {
                (value_as_f64(items.first()), value_as_f64(items.get(1)))
            } else {
                (
                    value_as_f64(level.get("price").or_else(|| level.get("p"))),
                    value_as_f64(
                        level
                            .get("amount")
                            .or_else(|| level.get("quantity").or_else(|| level.get("q"))),
                    ),
                )
            };
            Some(OrderBookLevel::new(price?, quantity?).map_err(validation_error))
        })
        .collect()
}

fn timestamp_from_i64(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        Utc.timestamp_millis_opt(value).single()
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}
