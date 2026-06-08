use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, EXCHANGE_API_SCHEMA_VERSION,
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
    let markets = value_array(value, &["data", "markets", "market", "pairs", "result"])
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ZebPay market response missing array data",
                value,
            )
        })?;
    markets
        .iter()
        .filter(|market| market_is_tradable(market))
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol = first_string(
        value,
        &[
            "trade_pair",
            "tradePair",
            "symbol",
            "pair",
            "pairName",
            "name",
            "market",
        ],
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ZebPay market missing trade pair",
            value,
        )
    })?;
    let normalized_symbol = normalize_zebpay_symbol(&symbol)?;
    let (fallback_base, fallback_quote) = split_pair_symbol(&normalized_symbol)
        .ok_or_else(|| parse_error(exchange_id.clone(), "ZebPay pair missing assets", value))?;
    let base_asset = first_string(
        value,
        &[
            "base",
            "baseCurrency",
            "base_currency",
            "baseAsset",
            "base_asset",
            "baseCoin",
        ],
    )
    .map(|text| text.to_ascii_uppercase())
    .unwrap_or(fallback_base);
    let quote_asset = first_string(
        value,
        &[
            "quote",
            "quoteCurrency",
            "quote_currency",
            "quoteAsset",
            "quote_asset",
            "quoteCoin",
        ],
    )
    .map(|text| text.to_ascii_uppercase())
    .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let price_precision = integer_from_fields(
        value,
        &[
            "pricePrecision",
            "price_precision",
            "priceScale",
            "priceDecimal",
        ],
    );
    let quantity_precision = integer_from_fields(
        value,
        &[
            "quantityPrecision",
            "quantity_precision",
            "quantityScale",
            "amountPrecision",
            "sizePrecision",
        ],
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                normalized_symbol,
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_from_fields(
            value,
            &["tickSize", "tick_size", "priceStep", "minPriceIncrement"],
        )
        .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_from_fields(
            value,
            &[
                "stepSize",
                "step_size",
                "quantityStep",
                "sizeStep",
                "minSizeIncrement",
            ],
        )
        .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: string_from_fields(value, &["minPrice", "min_price"]),
        max_price: string_from_fields(value, &["maxPrice", "max_price"]),
        min_quantity: string_from_fields(value, &["minQuantity", "min_quantity", "minSize"]),
        max_quantity: string_from_fields(value, &["maxQuantity", "max_quantity", "maxSize"]),
        min_notional: string_from_fields(
            value,
            &["minNotional", "min_notional", "minOrderValue", "minAmount"],
        ),
        max_notional: string_from_fields(
            value,
            &["maxNotional", "max_notional", "maxOrderValue", "maxAmount"],
        ),
        price_precision,
        quantity_precision,
        supports_market_orders: bool_from_fields(value, &["marketOrderEnabled"]).unwrap_or(true),
        supports_limit_orders: bool_from_fields(value, &["limitOrderEnabled"]).unwrap_or(true),
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let mut bids = parse_levels(
        exchange_id,
        data.get("bids")
            .or_else(|| data.get("bid"))
            .or_else(|| data.get("buy")),
    )?;
    let mut asks = parse_levels(
        exchange_id,
        data.get("asks")
            .or_else(|| data.get("ask"))
            .or_else(|| data.get("sell")),
    )?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ZebPay order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value_as_u64(
        data.get("sequence")
            .or_else(|| data.get("lastUpdateId"))
            .or_else(|| data.get("last")),
    );
    snapshot.exchange_timestamp = first_timestamp(data, &["timestamp", "time", "updatedAt"])
        .or_else(|| first_timestamp(value, &["timestamp", "time", "updatedAt"]));
    Ok(snapshot)
}

pub fn normalize_zebpay_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "ZebPay symbol must not be empty".to_string(),
        });
    }
    let upper = trimmed
        .replace(['/', '_'], "-")
        .replace("--", "-")
        .to_ascii_uppercase();
    if upper.contains('-') {
        return Ok(upper);
    }
    let (base, quote) =
        split_compact_symbol(&upper).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer ZebPay quote asset from symbol {symbol}"),
        })?;
    Ok(format!("{base}-{quote}"))
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(15).clamp(1, 50)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ZebPay order book response missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid ZebPay level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid ZebPay level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .or_else(|| level.get("rate"))
                .and_then(number_from_value)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid ZebPay level price", level)
                })?;
            let quantity = level
                .get("quantity")
                .or_else(|| level.get("size"))
                .or_else(|| level.get("amount"))
                .or_else(|| level.get("volume"))
                .and_then(number_from_value)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid ZebPay level quantity", level)
                })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn market_is_tradable(value: &Value) -> bool {
    bool_from_fields(value, &["active", "enabled", "tradingEnabled"]).unwrap_or_else(|| {
        value
            .get("status")
            .and_then(Value::as_str)
            .map(|status| {
                matches!(
                    status.to_ascii_lowercase().as_str(),
                    "active" | "enabled" | "online" | "trading"
                )
            })
            .unwrap_or(true)
    })
}

fn value_array<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a Vec<Value>> {
    if let Some(array) = value.as_array() {
        return Some(array);
    }
    fields
        .iter()
        .filter_map(|field| value.get(*field))
        .find_map(Value::as_array)
}

fn split_pair_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('-') {
        if !base.is_empty() && !quote.is_empty() {
            return Some((base.to_string(), quote.to_string()));
        }
    }
    split_compact_symbol(symbol)
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 12] = [
        "USDT", "USDC", "BUSD", "INR", "AUD", "SGD", "USD", "EUR", "BTC", "ETH", "BNB", "TRY",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn first_string(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_string)
            .filter(|text| !text.trim().is_empty())
    })
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_string))
}

fn bool_from_fields(value: &Value, fields: &[&str]) -> Option<bool> {
    fields.iter().find_map(|field| match value.get(*field)? {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) => match text.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "active" | "enabled" => Some(true),
            "0" | "false" | "no" | "inactive" | "disabled" => Some(false),
            _ => None,
        },
        Value::Number(number) => number.as_i64().map(|value| value != 0),
        _ => None,
    })
}

fn integer_from_fields(value: &Value, fields: &[&str]) -> Option<u32> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| match value {
            Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
            Value::String(text) => text.parse::<u32>().ok(),
            _ => None,
        })
    })
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat((precision - 1) as usize))
    }
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn value_as_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        let value = value.get(*field)?;
        if let Some(number) = value.as_i64() {
            return if number > 10_000_000_000 {
                Utc.timestamp_millis_opt(number).single()
            } else {
                Utc.timestamp_opt(number, 0).single()
            };
        }
        let text = value.as_str()?;
        DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .or_else(|| {
                text.parse::<i64>().ok().and_then(|number| {
                    if number > 10_000_000_000 {
                        Utc.timestamp_millis_opt(number).single()
                    } else {
                        Utc.timestamp_opt(number, 0).single()
                    }
                })
            })
    })
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
