use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
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
    symbol_items(value, market_type)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "XT symbols response missing list",
                value,
            )
        })?
        .iter()
        .map(|value| parse_symbol_rule(exchange_id, market_type, value))
        .collect()
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_lowercase();
    let (fallback_base, fallback_quote) = split_xt_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("unknown".to_string(), "usdt".to_string()));
    let base_asset = value
        .get("baseCurrency")
        .or_else(|| value.get("baseCoin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| fallback_base.to_ascii_uppercase());
    let quote_asset = value
        .get("quoteCurrency")
        .or_else(|| value.get("quoteCoin"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| fallback_quote.to_ascii_uppercase());
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let status = value
        .get("state")
        .map(|value| value.to_string())
        .unwrap_or_else(|| "ONLINE".to_string())
        .to_ascii_uppercase();
    let open_api = value
        .get("openapiEnabled")
        .or_else(|| value.get("isOpenApi"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let trading_enabled = value
        .get("tradingEnabled")
        .or_else(|| value.get("tradeSwitch"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let tradable = open_api
        && trading_enabled
        && !matches!(
            status.as_str(),
            "OFFLINE" | "DELISTED" | "0" | "CLOSED" | "SUSPEND"
        );
    let price_precision = integer_from_value(value.get("pricePrecision"));
    let quantity_precision = integer_from_value(
        value
            .get("quantityPrecision")
            .or_else(|| value.get("baseCoinPrecision")),
    );
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("minStepPrice"))
            .or_else(|| filter_value(filters, "PRICE", "tickSize"))
            .or_else(|| price_precision.map(increment_from_precision)),
        quantity_increment: string_or_number(value.get("minQty"))
            .or_else(|| filter_value(filters, "QUANTITY", "tickSize"))
            .or_else(|| quantity_precision.map(increment_from_precision)),
        min_price: string_or_number(value.get("minPrice"))
            .or_else(|| filter_value(filters, "PRICE", "min")),
        max_price: string_or_number(value.get("maxPrice"))
            .or_else(|| filter_value(filters, "PRICE", "max")),
        min_quantity: string_or_number(value.get("minQty"))
            .or_else(|| filter_value(filters, "QUANTITY", "min")),
        max_quantity: filter_value(filters, "QUANTITY", "max"),
        min_notional: string_or_number(value.get("minNotional"))
            .or_else(|| filter_value(filters, "QUOTE_QTY", "min")),
        max_notional: string_or_number(value.get("maxNotional")),
        price_precision,
        quantity_precision,
        supports_market_orders: tradable && supports_order_type(value, "MARKET"),
        supports_limit_orders: tradable && supports_order_type(value, "LIMIT"),
        supports_post_only: tradable && supports_tif(value, "GTX"),
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "XT order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("t"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_xt_symbol(symbol: &str, _market_type: MarketType) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "symbol must not be empty".to_string(),
        });
    }
    if trimmed.contains('_') {
        return Ok(trimmed);
    }
    let compact = trimmed.replace(['/', '-'], "");
    let (base, quote) =
        split_compact_symbol(&compact).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot normalize XT symbol {symbol}"),
        })?;
    Ok(format!("{base}_{quote}"))
}

pub fn normalize_depth(depth: u32, market_type: MarketType) -> u32 {
    match market_type {
        MarketType::Spot => depth.clamp(1, 500),
        MarketType::Perpetual => depth.clamp(1, 50),
        _ => depth,
    }
}

pub(super) fn data_payload(value: &Value) -> &Value {
    value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn symbol_items(value: &Value, market_type: MarketType) -> Option<&[Value]> {
    let data = data_payload(value);
    if market_type == MarketType::Spot {
        data.get("symbols")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
    } else {
        data.as_array().map(Vec::as_slice)
    }
}

fn filter_value(filters: &[Value], filter_name: &str, field: &str) -> Option<String> {
    filters
        .iter()
        .find(|filter| {
            filter
                .get("filter")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case(filter_name))
        })
        .and_then(|filter| string_or_number(filter.get(field)))
}

fn supports_order_type(value: &Value, expected: &str) -> bool {
    value
        .get("orderTypes")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .any(|item| item.eq_ignore_ascii_case(expected))
        })
        .or_else(|| {
            value
                .get("supportOrderType")
                .and_then(Value::as_str)
                .map(|text| {
                    text.split([',', ';', '|'])
                        .any(|item| item.trim().eq_ignore_ascii_case(expected))
                })
        })
        .unwrap_or(true)
}

fn supports_tif(value: &Value, expected: &str) -> bool {
    value
        .get("timeInForces")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .any(|item| item.eq_ignore_ascii_case(expected))
        })
        .or_else(|| {
            value
                .get("supportTimeInForce")
                .and_then(Value::as_str)
                .map(|text| {
                    text.split([',', ';', '|'])
                        .any(|item| item.trim().eq_ignore_ascii_case(expected))
                })
        })
        .unwrap_or(false)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "XT order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid XT order book level", level)
            })?;
            let price = array
                .first()
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
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

pub(super) fn split_xt_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        return (!base.is_empty() && !quote.is_empty())
            .then(|| (base.to_ascii_lowercase(), quote.to_ascii_lowercase()));
    }
    split_compact_symbol(symbol)
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "usdt", "usdc", "busd", "usd", "btc", "eth", "eur", "try", "bnb",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value).filter(|value| !value.trim().is_empty())
}

pub(super) fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

pub(super) fn parse_side(
    exchange_id: &ExchangeId,
    side: &str,
) -> ExchangeApiResult<rustcta_types::OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" => Ok(rustcta_types::OrderSide::Buy),
        "SELL" => Ok(rustcta_types::OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported order side",
            &Value::String(side.to_string()),
        )),
    }
}

pub(super) fn parse_position_side(value: Option<&str>) -> rustcta_types::PositionSide {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("LONG") => rustcta_types::PositionSide::Long,
        Some("SHORT") => rustcta_types::PositionSide::Short,
        Some("BOTH") | Some("NET") => rustcta_types::PositionSide::Net,
        _ => rustcta_types::PositionSide::None,
    }
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

fn integer_from_value(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(|number| number as u32),
        _ => None,
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn increment_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    }
}
