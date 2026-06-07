use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
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
) -> Result<Vec<SymbolRules>, ExchangeApiError> {
    let rows = array_payload(value, &["data", "markets", "instruments", "symbols"])?;
    let mut rules = Vec::new();
    for row in rows {
        let symbol_text = text_from_fields(
            row,
            &[
                "symbol",
                "market",
                "instrument_id",
                "contract_code",
                "contractCode",
            ],
        )
        .unwrap_or_default();
        if symbol_text.trim().is_empty() {
            continue;
        }
        let normalized = normalize_symbol(&symbol_text, market_type)?;
        let (base, quote) = split_symbol(&normalized)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(
                CanonicalSymbol::new(base.clone(), quote.clone()).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                market_type,
                normalized.clone(),
            )
            .map_err(validation_error)?,
        };
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset: base,
            quote_asset: quote,
            price_increment: text_from_fields(
                row,
                &["price_precision", "priceStep", "tick_size", "tickSize"],
            )
            .and_then(precision_to_increment)
            .or_else(|| text_from_fields(row, &["price_increment", "min_price_increment"])),
            quantity_increment: text_from_fields(
                row,
                &[
                    "amount_precision",
                    "volume_precision",
                    "quantity_precision",
                    "size_precision",
                ],
            )
            .and_then(precision_to_increment)
            .or_else(|| {
                text_from_fields(
                    row,
                    &[
                        "quantity_increment",
                        "amount_increment",
                        "lot_size",
                        "sizeStep",
                    ],
                )
            }),
            min_price: text_from_fields(row, &["min_price"]),
            max_price: text_from_fields(row, &["max_price"]),
            min_quantity: text_from_fields(
                row,
                &[
                    "min_amount",
                    "min_quantity",
                    "min_order_amount",
                    "minVolume",
                    "minSize",
                ],
            ),
            max_quantity: text_from_fields(
                row,
                &["max_amount", "max_quantity", "maxVolume", "maxSize"],
            ),
            min_notional: text_from_fields(row, &["min_notional", "min_order_value", "minValue"]),
            max_notional: text_from_fields(row, &["max_notional", "max_order_value"]),
            price_precision: text_from_fields(row, &["price_precision", "pricePrecision"])
                .and_then(|value| value.parse().ok()),
            quantity_precision: text_from_fields(
                row,
                &[
                    "amount_precision",
                    "volume_precision",
                    "quantity_precision",
                    "size_precision",
                ],
            )
            .and_then(|value| value.parse().ok()),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: market_type == MarketType::Perpetual,
            supports_reduce_only: market_type == MarketType::Perpetual,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> Result<OrderBookSnapshot, ExchangeApiError> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(data.get("bids").or_else(|| data.get("bid")))?;
    let asks = parse_levels(data.get("asks").or_else(|| data.get("ask")))?;
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "DigiFinex order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("version")
        .or_else(|| data.get("sequence"))
        .and_then(number_u64);
    snapshot.exchange_timestamp = data
        .get("date")
        .or_else(|| data.get("timestamp"))
        .and_then(timestamp_from_value);
    Ok(snapshot)
}

pub fn normalize_symbol(symbol: &str, market_type: MarketType) -> Result<String, ExchangeApiError> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "DigiFinex symbol cannot be empty".to_string(),
        });
    }
    let normalized = trimmed
        .replace('-', "_")
        .replace('/', "_")
        .to_ascii_lowercase();
    if market_type == MarketType::Perpetual
        && !normalized.contains('_')
        && normalized.ends_with("usdt")
    {
        let base = normalized.trim_end_matches("usdt");
        return Ok(format!("{base}_usdt"));
    }
    Ok(normalized)
}

pub fn split_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('_');
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    Some((base, quote))
}

pub fn text_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value_as_string(value.get(*field)))
}

pub fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn decimal(value: Option<&Value>) -> Option<f64> {
    value_as_string(value)?.parse().ok()
}

pub fn timestamp_from_value(value: &Value) -> Option<DateTime<Utc>> {
    let timestamp = value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())?;
    if timestamp < 10_000_000_000 {
        DateTime::<Utc>::from_timestamp(timestamp, 0)
    } else {
        DateTime::<Utc>::from_timestamp_millis(timestamp)
    }
}

pub fn number_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<OrderBookLevel>, ExchangeApiError> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            ExchangeId::unchecked("digifinex"),
            "DigiFinex order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .filter_map(|level| {
            let (price, quantity) = if let Some(row) = level.as_array() {
                (decimal(row.first()), decimal(row.get(1)))
            } else {
                (
                    decimal(level.get("price").or_else(|| level.get("p"))),
                    decimal(
                        level
                            .get("amount")
                            .or_else(|| level.get("quantity"))
                            .or_else(|| level.get("q")),
                    ),
                )
            };
            match (price, quantity) {
                (Some(price), Some(quantity)) if price > 0.0 && quantity > 0.0 => {
                    Some(OrderBookLevel::new(price, quantity).map_err(validation_error))
                }
                _ => None,
            }
        })
        .collect()
}

fn array_payload<'a>(value: &'a Value, fields: &[&str]) -> Result<&'a [Value], ExchangeApiError> {
    if let Some(array) = value.as_array() {
        return Ok(array);
    }
    for field in fields {
        if let Some(array) = value.get(*field).and_then(Value::as_array) {
            return Ok(array);
        }
    }
    Err(parse_error(
        ExchangeId::unchecked("digifinex"),
        "DigiFinex response missing array payload",
        value,
    ))
}

fn precision_to_increment(value: String) -> Option<String> {
    if value.contains('.') {
        return Some(value);
    }
    let precision = value.parse::<usize>().ok()?;
    if precision == 0 {
        Some("1".to_string())
    } else {
        Some(format!("0.{}1", "0".repeat(precision.saturating_sub(1))))
    }
}

pub fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: Some("digifinex_parse".to_string()),
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
