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
    pair_list(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitfinex symbol list missing array",
                value,
            )
        })?
        .iter()
        .filter_map(Value::as_str)
        .map(|pair| parse_symbol_rule(exchange_id, market_type, pair))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    pair: &str,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = bitfinex_symbol_from_pair(pair, market_type)?;
    let (base_asset, quote_asset) = split_bitfinex_pair(pair).unwrap_or_else(|| {
        if market_type == MarketType::Perpetual {
            ("UNKNOWN".to_string(), "USTF0".to_string())
        } else {
            ("UNKNOWN".to_string(), "USD".to_string())
        }
    });
    let canonical_symbol =
        CanonicalSymbol::new(normalize_asset(&base_asset), normalize_asset(&quote_asset))
            .map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
        base_asset: normalize_asset(&base_asset),
        quote_asset: normalize_asset(&quote_asset),
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
        supports_post_only: true,
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let levels = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex order book response missing array",
            value,
        )
    })?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for level in levels {
        let array = level.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitfinex order book level missing array",
                level,
            )
        })?;
        let price = array.first().and_then(value_as_f64).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitfinex order book level missing price",
                level,
            )
        })?;
        let amount = array.get(2).and_then(value_as_f64).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitfinex order book level missing amount",
                level,
            )
        })?;
        let level = OrderBookLevel::new(price, amount.abs()).map_err(validation_error)?;
        if amount >= 0.0 {
            bids.push(level);
        } else {
            asks.push(level);
        }
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitfinex order book request requires canonical_symbol".to_string(),
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
    Ok(snapshot)
}

pub fn normalize_bitfinex_symbol(
    symbol: &str,
    market_type: MarketType,
) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed.to_ascii_uppercase().replace('/', "");
    if upper.starts_with('T') {
        return Ok(format!("t{}", &upper[1..]));
    }
    if market_type == MarketType::Perpetual && upper.contains(':') {
        return Ok(format!("t{upper}"));
    }
    Ok(format!("t{upper}"))
}

pub fn canonical_from_bitfinex_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let pair = symbol.trim_start_matches(['t', 'T']);
    let (base, quote) = split_bitfinex_pair(pair).unwrap_or_else(|| {
        if pair.contains(':') {
            ("UNKNOWN".to_string(), "USTF0".to_string())
        } else {
            ("UNKNOWN".to_string(), "USD".to_string())
        }
    });
    CanonicalSymbol::new(normalize_asset(&base), normalize_asset(&quote)).map_err(validation_error)
}

pub fn market_type_from_symbol(symbol: &str, fallback: MarketType) -> MarketType {
    if symbol.contains("F0:") || symbol.ends_with(":USTF0") {
        MarketType::Perpetual
    } else {
        fallback
    }
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
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

pub(super) fn millis(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn pair_list(value: &Value) -> Option<&[Value]> {
    let array = value.as_array()?;
    if array.first().is_some_and(Value::is_array) {
        array.first()?.as_array().map(Vec::as_slice)
    } else {
        Some(array.as_slice())
    }
}

fn bitfinex_symbol_from_pair(pair: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let pair = pair.trim().to_ascii_uppercase();
    if pair.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Bitfinex pair must not be empty".to_string(),
        });
    }
    if pair.starts_with('T') {
        Ok(format!("t{}", &pair[1..]))
    } else if market_type == MarketType::Perpetual && pair.contains(':') {
        Ok(format!("t{pair}"))
    } else {
        Ok(format!("t{pair}"))
    }
}

fn split_bitfinex_pair(pair: &str) -> Option<(String, String)> {
    let pair = pair
        .trim()
        .trim_start_matches(['t', 'T'])
        .to_ascii_uppercase();
    if let Some((base, quote)) = pair.split_once(':') {
        return Some((base.to_string(), quote.to_string()));
    }
    const QUOTES: [&str; 12] = [
        "USTF0", "TESTUSD", "USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP", "JPY", "TRY", "UST",
    ];
    QUOTES.iter().find_map(|quote| {
        pair.strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn normalize_asset(asset: &str) -> String {
    match asset.to_ascii_uppercase().as_str() {
        "UST" | "USTF0" => "USDT".to_string(),
        "BTCF0" => "BTC".to_string(),
        "ETHF0" => "ETH".to_string(),
        other => other.trim_end_matches("F0").to_string(),
    }
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
