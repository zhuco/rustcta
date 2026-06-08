#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn luno_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn parse_luno_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let tickers = value
        .get("tickers")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("luno tickers response missing tickers"))?;
    let mut rules = Vec::new();
    for ticker in tickers {
        let pair = string_field(ticker, "pair")?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| luno_symbol(symbol) == pair.to_ascii_uppercase())
        {
            continue;
        }
        let (base, quote) = split_luno_pair(&pair)?;
        let scope = symbol_scope(exchange_id.clone(), &base, &quote, &pair)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base,
            quote_asset: quote,
            price_increment: None,
            quantity_increment: None,
            min_price: None,
            max_price: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision: None,
            quantity_precision: None,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_luno_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(value.get("bids"))?;
    let asks = parse_levels(value.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("luno orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid luno orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    Ok(snapshot)
}

fn split_luno_pair(pair: &str) -> ExchangeApiResult<(String, String)> {
    let pair = pair.trim().to_ascii_uppercase();
    for base in ["XBT", "BTC", "ETH", "LTC", "XRP", "BCH", "USDC"] {
        if pair.starts_with(base) && pair.len() > base.len() {
            let quote = &pair[base.len()..];
            let base = if base == "XBT" { "BTC" } else { base };
            return Ok((base.to_string(), quote.to_string()));
        }
    }
    Err(invalid(format!("unsupported luno pair format {pair}")))
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base, quote)
        .map_err(|error| invalid(format!("invalid luno canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, raw_symbol)
        .map_err(|error| invalid(format!("invalid luno exchange symbol: {error}")))?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("luno orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let price = field_f64(level, "price")?;
            let quantity = field_f64(level, "volume")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid luno level: {error}")))
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("luno missing string field {field}")))
}

fn field_f64(value: &Value, field: &str) -> ExchangeApiResult<f64> {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .or_else(|| value.as_f64())
        })
        .ok_or_else(|| invalid(format!("luno invalid numeric {field}")))
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
