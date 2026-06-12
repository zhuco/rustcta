use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bybit instruments missing result.list",
                value,
            )
        })?;
    rows.iter()
        .map(|row| parse_symbol_rule(exchange_id, market_type, row))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    row: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, row, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, row, "baseCoin")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, row, "quoteCoin")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let price_filter = row.get("priceFilter").unwrap_or(&Value::Null);
    let lot_filter = row.get("lotSizeFilter").unwrap_or(&Value::Null);
    let status = row
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("Trading");
    let trading = status.eq_ignore_ascii_case("Trading");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(price_filter.get("tickSize")),
        quantity_increment: string_or_number(
            lot_filter
                .get("qtyStep")
                .or_else(|| lot_filter.get("basePrecision")),
        ),
        min_price: string_or_number(price_filter.get("minPrice")),
        max_price: string_or_number(price_filter.get("maxPrice")),
        min_quantity: string_or_number(lot_filter.get("minOrderQty")),
        max_quantity: string_or_number(lot_filter.get("maxOrderQty")),
        min_notional: string_or_number(lot_filter.get("minOrderAmt")),
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: trading,
        supports_limit_orders: trading,
        supports_post_only: trading,
        supports_reduce_only: market_type != MarketType::Spot,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let result = value.get("result").unwrap_or(value);
    let bids = parse_levels(exchange_id, result.get("b"))?;
    let asks = parse_levels(exchange_id, result.get("a"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit order book request requires canonical_symbol".to_string(),
            })?;
    let snapshot = OrderBookSnapshot {
        schema_version: SchemaVersion::current(),
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        bids,
        asks,
        sequence: result
            .get("seq")
            .or_else(|| result.get("u"))
            .and_then(Value::as_u64),
        exchange_timestamp: result
            .get("ts")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        received_at: Utc::now(),
        is_stale: false,
    };
    snapshot.validate().map_err(validation_error)?;
    Ok(snapshot)
}

pub fn parse_funding_rate_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    ticker_value: &Value,
    history_value: Option<&Value>,
) -> ExchangeApiResult<FundingRateSnapshot> {
    let ticker = ticker_value
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bybit ticker funding response missing result.list",
                ticker_value,
            )
        })?;
    let funding_rate = string_or_number(ticker.get("fundingRate")).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bybit ticker funding response missing fundingRate",
            ticker,
        )
    })?;
    let history_row = history_value
        .and_then(|value| value.get("result"))
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first());
    Ok(FundingRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        funding_rate,
        predicted_funding_rate: None,
        funding_time: history_row
            .and_then(|row| row.get("fundingRateTimestamp"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        next_funding_time: ticker
            .get("nextFundingTime")
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        mark_price: string_or_number(ticker.get("markPrice")),
        index_price: string_or_number(ticker.get("indexPrice")),
        open_interest: string_or_number(ticker.get("openInterest")),
        turnover_24h: string_or_number(ticker.get("turnover24h")),
        volume_24h: string_or_number(ticker.get("volume24h")),
        source: Some("bybit.v5.market.tickers".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn normalize_bybit_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
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
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            let text = string_or_number(Some(value)).unwrap_or_else(|| value.to_string());
            text.parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bybit decimal value {text}: {error}"),
                })
        })
        .transpose()
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
            "Bybit order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid Bybit order book level", level)
            })?;
            let price = decimal_value_to_f64(array.first())?.ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid Bybit level price", level)
            })?;
            let quantity = decimal_value_to_f64(array.get(1))?.ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid Bybit level quantity", level)
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
