#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, SchemaVersion,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexServerTime {
    pub server_time_ms: i64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexTicker24h {
    pub symbol: SymbolScope,
    pub last_price: String,
    pub bid_price: Option<String>,
    pub ask_price: Option<String>,
    pub open_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub base_volume: Option<String>,
    pub quote_volume: Option<String>,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
    pub funding_rate: Option<String>,
    pub predicted_funding_rate: Option<String>,
    pub open_interest: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexPublicTrade {
    pub symbol: SymbolScope,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexFundingRate {
    pub symbol: SymbolScope,
    pub funding_rate: String,
    pub funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexCandle {
    pub symbol: SymbolScope,
    pub interval: String,
    pub opened_at: DateTime<Utc>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: Option<String>,
    pub turnover: Option<String>,
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let data = response_data(value);
    let mut rules = Vec::new();
    if let Some(products) = data.get("products").and_then(Value::as_array) {
        for product in products {
            if product
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.eq_ignore_ascii_case("Spot"))
            {
                rules.push(parse_spot_symbol_rule(exchange_id, product)?);
            }
        }
    }
    if let Some(products) = data.get("perpProductsV2").and_then(Value::as_array) {
        for product in products {
            let settle = product
                .get("settleCurrency")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if matches!(settle, "USDT" | "USDC") {
                rules.push(parse_perp_symbol_rule(exchange_id, product)?);
            }
        }
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let result = value.get("result").unwrap_or(value);
    let book = result.get("book").unwrap_or(result);
    let bids = parse_levels(
        exchange_id,
        book.get("bids")
            .or_else(|| result.get("orderbook_p").and_then(|book| book.get("bids"))),
        symbol.market_type,
    )?;
    let asks = parse_levels(
        exchange_id,
        book.get("asks")
            .or_else(|| result.get("orderbook_p").and_then(|book| book.get("asks"))),
        symbol.market_type,
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "phemex order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = result.get("sequence").and_then(value_as_u64);
    snapshot.exchange_timestamp = result.get("timestamp").and_then(timestamp_from_ns_or_ms);
    Ok(snapshot)
}

pub fn parse_server_time(value: &Value) -> ExchangeApiResult<PhemexServerTime> {
    let data = response_data(value);
    let server_time_ms = data
        .get("serverTime")
        .and_then(value_as_i64)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "phemex server time response missing serverTime".to_string(),
        })?;
    Ok(PhemexServerTime {
        server_time_ms,
        observed_at: Utc::now(),
    })
}

pub fn parse_ticker_24h(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PhemexTicker24h> {
    let result = value.get("result").unwrap_or(value);
    let market_type = symbol.market_type;
    let price = |ep: &str, rp: &str| {
        if market_type == MarketType::Spot {
            result.get(ep).and_then(|value| scaled_text(value, 8))
        } else {
            string_or_number(result.get(rp))
        }
    };
    let quantity = |ev: &str, rq: &str| {
        if market_type == MarketType::Spot {
            result.get(ev).and_then(|value| scaled_text(value, 8))
        } else {
            string_or_number(result.get(rq))
        }
    };
    let last_price = price("lastEp", "closeRp").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ticker response missing last/close price",
            result,
        )
    })?;
    Ok(PhemexTicker24h {
        symbol,
        last_price,
        bid_price: price("bidEp", "bidRp"),
        ask_price: price("askEp", "askRp"),
        open_price: price("openEp", "openRp"),
        high_price: price("highEp", "highRp"),
        low_price: price("lowEp", "lowRp"),
        base_volume: quantity("volumeEv", "volumeRq"),
        quote_volume: quantity("turnoverEv", "turnoverRv"),
        mark_price: string_or_number(result.get("markPriceRp")),
        index_price: string_or_number(result.get("indexPriceRp")),
        funding_rate: string_or_number(result.get("fundingRateRr")),
        predicted_funding_rate: string_or_number(result.get("predFundingRateRr")),
        open_interest: string_or_number(result.get("openInterestRv")),
        timestamp: result.get("timestamp").and_then(timestamp_from_ns_or_ms),
    })
}

pub fn parse_tickers_24h(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<PhemexTicker24h>> {
    let rows = value
        .get("result")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "ticker response missing array", value))?;
    rows.iter()
        .map(|row| {
            let symbol = required_str(exchange_id, row, "symbol")?;
            let scope = canonical_from_phemex_symbol(exchange_id, market_type, symbol, None, None)?;
            parse_ticker_24h(exchange_id, scope, row)
        })
        .collect()
}

pub fn parse_public_trades(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<PhemexPublicTrade>> {
    let result = value.get("result").unwrap_or(value);
    let trades = result
        .get("trades")
        .or_else(|| result.get("trades_p"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "trades response missing rows", value))?;
    trades
        .iter()
        .map(|trade| parse_public_trade(exchange_id, &symbol, trade))
        .collect()
}

pub fn parse_funding_history(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<PhemexFundingRate>> {
    let rows = response_data(value)
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "funding history missing rows", value))?;
    rows.iter()
        .map(|row| {
            Ok(PhemexFundingRate {
                symbol: symbol.clone(),
                funding_rate: string_or_number(
                    row.get("fundingRate")
                        .or_else(|| row.get("fundingRateRr"))
                        .or_else(|| row.get("rate")),
                )
                .ok_or_else(|| parse_error(exchange_id.clone(), "funding row missing rate", row))?,
                funding_time: first_timestamp_millis(
                    row,
                    &["fundingTime", "fundingTimeNs", "timestamp", "createdAt"],
                ),
            })
        })
        .collect()
}

pub fn parse_perp_candles(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    interval: &str,
    value: &Value,
) -> ExchangeApiResult<Vec<PhemexCandle>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("result"))
        .and_then(|data| data.get("rows").or_else(|| data.get("kline")))
        .and_then(Value::as_array)
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.as_array())
        .ok_or_else(|| parse_error(exchange_id.clone(), "kline response missing rows", value))?;
    rows.iter()
        .map(|row| parse_perp_candle(exchange_id, &symbol, interval, row))
        .collect()
}

pub fn normalize_phemex_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    let raw = symbol.trim();
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if market_type == MarketType::Spot {
        if raw.starts_with('s') {
            Ok(format!(
                "s{}",
                normalized.strip_prefix('S').unwrap_or(&normalized)
            ))
        } else {
            Ok(format!("s{normalized}"))
        }
    } else {
        Ok(normalized)
    }
}

pub fn canonical_from_phemex_symbol(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
    base: Option<&str>,
    quote: Option<&str>,
) -> ExchangeApiResult<SymbolScope> {
    let base_asset = base
        .map(ToOwned::to_owned)
        .or_else(|| infer_base_quote(symbol).map(|pair| pair.0))
        .ok_or_else(|| parse_error(exchange_id.clone(), "missing base asset", &Value::Null))?
        .to_ascii_uppercase();
    let quote_asset = quote
        .map(ToOwned::to_owned)
        .or_else(|| infer_base_quote(symbol).map(|pair| pair.1))
        .ok_or_else(|| parse_error(exchange_id.clone(), "missing quote asset", &Value::Null))?
        .to_ascii_uppercase();
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

pub fn required_str<'a>(
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

pub fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value).filter(|value| !value.trim().is_empty())
}

pub fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    string_or_number(value)
        .map(|text| decimal_text_to_f64(&text).map(Some))
        .unwrap_or(Ok(None))
}

pub fn decimal_text_to_f64(text: &str) -> ExchangeApiResult<f64> {
    text.trim()
        .replace(',', "")
        .parse::<f64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("invalid decimal {text}"),
        })
}

pub fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(timestamp_from_ns_or_ms))
}

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

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_spot_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_string();
    let base_asset = required_str(exchange_id, value, "baseCurrency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteCurrency")?.to_ascii_uppercase();
    let price_precision = integer_from_value(value.get("pricePrecision"));
    let quantity_precision = integer_from_value(value.get("baseQtyPrecision"));
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .is_none_or(|status| status.eq_ignore_ascii_case("Listed"));
    let symbol = canonical_from_phemex_symbol(
        exchange_id,
        MarketType::Spot,
        &exchange_symbol,
        Some(&base_asset),
        Some(&quote_asset),
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: first_token(value.get("quoteTickSize"))
            .or_else(|| decimal_from_ev(value.get("quoteTickSizeEv"), value.get("priceScale"))),
        quantity_increment: first_token(value.get("baseTickSize")).or_else(|| {
            decimal_from_ev(value.get("baseTickSizeEv"), value.get("baseQtyPrecision"))
        }),
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: first_token(value.get("maxBaseOrderSize")),
        min_notional: first_token(value.get("minOrderValue")),
        max_notional: first_token(value.get("maxOrderValue")),
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_perp_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_string();
    let base_asset = required_str(exchange_id, value, "baseCurrency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteCurrency")?.to_ascii_uppercase();
    let price_precision = integer_from_value(value.get("pricePrecision"));
    let quantity_precision = integer_from_value(value.get("qtyPrecision"));
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .is_none_or(|status| status.eq_ignore_ascii_case("Listed"));
    let symbol = canonical_from_phemex_symbol(
        exchange_id,
        MarketType::Perpetual,
        &exchange_symbol,
        Some(&base_asset),
        Some(&quote_asset),
    )?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("qtyStepSize")),
        min_price: string_or_number(value.get("minPriceRp")),
        max_price: string_or_number(value.get("maxPriceRp")),
        min_quantity: None,
        max_quantity: string_or_number(value.get("maxOrderQtyRq")),
        min_notional: string_or_number(value.get("minOrderValueRv")),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: tradable,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
    market_type: MarketType,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing levels",
            &Value::Null,
        )
    })?;
    Ok(levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "book level is not array", level)
            })?;
            let price = if market_type == MarketType::Spot {
                scaled_ep(array.first(), 8)?
            } else {
                decimal_value_to_f64(array.first())?
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?
            };
            let quantity = if market_type == MarketType::Spot {
                scaled_ep(array.get(1), 8)?
            } else {
                decimal_value_to_f64(array.get(1))?.ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?
            };
            if quantity <= 0.0 {
                return Ok(None);
            }
            OrderBookLevel::new(price, quantity)
                .map(Some)
                .map_err(validation_error)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect())
}

fn response_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn first_token(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .and_then(|text| text.split_whitespace().next())
        .map(|text| text.replace(',', ""))
        .or_else(|| string_or_number(value))
}

fn decimal_from_ev(value: Option<&Value>, scale: Option<&Value>) -> Option<String> {
    let raw = value_as_i128(value)?;
    let scale = integer_from_value(scale)?;
    Some(decimal_from_scaled(raw, scale))
}

fn scaled_ep(value: Option<&Value>, scale: u32) -> ExchangeApiResult<f64> {
    let raw = value_as_i128(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "invalid scaled decimal".to_string(),
    })?;
    Ok(raw as f64 / 10_f64.powi(scale as i32))
}

fn scaled_text(value: &Value, scale: u32) -> Option<String> {
    Some(decimal_from_scaled(value_as_i128(Some(value))?, scale))
}

fn parse_public_trade(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<PhemexPublicTrade> {
    let row = value
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "trade row is not array", value))?;
    let timestamp = row
        .first()
        .and_then(timestamp_from_ns_or_ms)
        .ok_or_else(|| parse_error(exchange_id.clone(), "trade missing timestamp", value))?;
    let side = match row.get(1).and_then(Value::as_str).unwrap_or_default() {
        "Buy" | "buy" => OrderSide::Buy,
        "Sell" | "sell" => OrderSide::Sell,
        _ => {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unsupported Phemex public trade side: {value}"),
            });
        }
    };
    let price = if symbol.market_type == MarketType::Spot {
        row.get(2)
            .and_then(|value| scaled_text(value, 8))
            .ok_or_else(|| parse_error(exchange_id.clone(), "trade missing price", value))?
    } else {
        string_or_number(row.get(2))
            .ok_or_else(|| parse_error(exchange_id.clone(), "trade missing price", value))?
    };
    let quantity = if symbol.market_type == MarketType::Spot {
        row.get(3)
            .and_then(|value| scaled_text(value, 8))
            .ok_or_else(|| parse_error(exchange_id.clone(), "trade missing quantity", value))?
    } else {
        string_or_number(row.get(3))
            .ok_or_else(|| parse_error(exchange_id.clone(), "trade missing quantity", value))?
    };
    Ok(PhemexPublicTrade {
        symbol: symbol.clone(),
        side,
        price,
        quantity,
        traded_at: timestamp,
    })
}

fn parse_perp_candle(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    interval: &str,
    value: &Value,
) -> ExchangeApiResult<PhemexCandle> {
    if let Some(row) = value.as_array() {
        let opened_at = row
            .first()
            .and_then(timestamp_from_ns_or_ms)
            .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing timestamp", value))?;
        return Ok(PhemexCandle {
            symbol: symbol.clone(),
            interval: interval.to_string(),
            opened_at,
            open: string_or_number(row.get(3).or_else(|| row.get(1)))
                .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing open", value))?,
            high: string_or_number(row.get(4).or_else(|| row.get(2)))
                .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing high", value))?,
            low: string_or_number(row.get(5).or_else(|| row.get(3)))
                .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing low", value))?,
            close: string_or_number(row.get(6).or_else(|| row.get(4)))
                .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing close", value))?,
            volume: string_or_number(row.get(7).or_else(|| row.get(5))),
            turnover: string_or_number(row.get(8).or_else(|| row.get(6))),
        });
    }
    let opened_at = first_timestamp_millis(value, &["timestamp", "time", "openTime"])
        .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing timestamp", value))?;
    Ok(PhemexCandle {
        symbol: symbol.clone(),
        interval: interval.to_string(),
        opened_at,
        open: string_or_number(value.get("open").or_else(|| value.get("openRp")))
            .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing open", value))?,
        high: string_or_number(value.get("high").or_else(|| value.get("highRp")))
            .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing high", value))?,
        low: string_or_number(value.get("low").or_else(|| value.get("lowRp")))
            .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing low", value))?,
        close: string_or_number(value.get("close").or_else(|| value.get("closeRp")))
            .ok_or_else(|| parse_error(exchange_id.clone(), "kline missing close", value))?,
        volume: string_or_number(value.get("volume").or_else(|| value.get("volumeRq"))),
        turnover: string_or_number(value.get("turnover").or_else(|| value.get("turnoverRv"))),
    })
}

fn decimal_from_scaled(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value < 0 { "-" } else { "" };
    let digits = value.abs().to_string();
    if digits.len() <= scale as usize {
        let padded = format!(
            "{}{}",
            "0".repeat(scale as usize + 1 - digits.len()),
            digits
        );
        let split = padded.len() - scale as usize;
        return format!("{sign}{}.{}", &padded[..split], &padded[split..])
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
    }
    let split = digits.len() - scale as usize;
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn infer_base_quote(symbol: &str) -> Option<(String, String)> {
    let symbol = symbol
        .trim()
        .trim_start_matches('s')
        .trim_start_matches('.')
        .trim_start_matches('M')
        .to_ascii_uppercase();
    const QUOTES: [&str; 5] = ["USDT", "USDC", "USD", "BTC", "ETH"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
        _ => None,
    })
}

fn value_as_i128(value: Option<&Value>) -> Option<i128> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_i64().map(i128::from),
        _ => None,
    })
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn timestamp_from_ns_or_ms(value: &Value) -> Option<DateTime<Utc>> {
    let timestamp = value_as_u64(value)? as i64;
    if timestamp > 10_000_000_000_000 {
        let seconds = timestamp / 1_000_000_000;
        let nanos = (timestamp % 1_000_000_000) as u32;
        DateTime::<Utc>::from_timestamp(seconds, nanos)
    } else if timestamp > 10_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(timestamp)
    } else {
        DateTime::<Utc>::from_timestamp(timestamp, 0)
    }
}
