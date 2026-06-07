use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, ValidationError,
};
use serde_json::Value;

pub fn parse_spot_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    data_array(exchange_id, value, "coinstore spot symbols missing data")?
        .iter()
        .map(|row| parse_spot_symbol_rule(exchange_id, row))
        .collect()
}

pub fn parse_futures_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .get("data")
        .and_then(|data| data.get("contracts").or(Some(data)))
        .or_else(|| value.get("contracts"))
        .unwrap_or(value);
    rows.as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinstore futures contracts missing",
                value,
            )
        })?
        .iter()
        .map(|row| parse_futures_symbol_rule(exchange_id, row))
        .collect()
}

pub fn parse_spot_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    parse_orderbook_snapshot(exchange_id, MarketType::Spot, symbol, value)
}

pub fn parse_futures_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    parse_orderbook_snapshot(exchange_id, MarketType::Perpetual, symbol, value)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    spot_value: Option<&Value>,
    futures_value: Option<&Value>,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let spot_rows = spot_value
        .map(|value| data_array(exchange_id, value, "coinstore spot fees missing data"))
        .transpose()?
        .unwrap_or_default();
    let futures_rows = futures_value
        .map(|value| futures_contract_rows(exchange_id, value))
        .transpose()?
        .unwrap_or_default();
    let mut snapshots = Vec::new();
    for requested in requested_symbols {
        let requested_symbol = if requested.market_type == MarketType::Perpetual {
            normalize_futures_symbol_lossy(&requested.exchange_symbol.symbol)
        } else {
            normalize_spot_symbol_lossy(&requested.exchange_symbol.symbol)
        };
        let (rows, source, maker_keys, taker_keys): (&[Value], &str, &[&str], &[&str]) =
            if requested.market_type == MarketType::Perpetual {
                (
                    futures_rows,
                    "coinstore.futures_configs_public",
                    &["makerFeeRate", "makerFee", "maker_fee", "makerFeeRatio"],
                    &["takerFeeRate", "takerFee", "taker_fee", "takerFeeRatio"],
                )
            } else {
                (
                    spot_rows,
                    "coinstore.spot_symbols_config",
                    &["makerFee", "maker_fee", "makerFeeRate", "makerFeeRatio"],
                    &["takerFee", "taker_fee", "takerFeeRate", "takerFeeRatio"],
                )
            };
        let Some(row) = rows.iter().find(|row| {
            row_symbol(row, requested.market_type)
                .is_some_and(|symbol| symbol.eq_ignore_ascii_case(&requested_symbol))
        }) else {
            continue;
        };
        let maker_rate = first_string_or_number(row, maker_keys).unwrap_or_else(|| "0".to_string());
        let taker_rate = first_string_or_number(row, taker_keys).unwrap_or_else(|| "0".to_string());
        snapshots.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: requested.clone(),
            maker_rate,
            taker_rate,
            source: Some(source.to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(snapshots)
}

fn parse_spot_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol = value_as_string(
        value
            .get("name")
            .or_else(|| value.get("symbol"))
            .or_else(|| value.get("symbolName")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "coinstore spot symbol missing", value))?;
    let base_asset = value_as_string(
        value
            .get("baseCurrency")
            .or_else(|| value.get("base"))
            .or_else(|| value.get("baseAsset")),
    )
    .unwrap_or_else(|| split_spot_symbol(&raw_symbol).0)
    .to_ascii_uppercase();
    let quote_asset = value_as_string(
        value
            .get("quoteCurrency")
            .or_else(|| value.get("quote"))
            .or_else(|| value.get("quoteAsset")),
    )
    .unwrap_or_else(|| split_spot_symbol(&raw_symbol).1)
    .to_ascii_uppercase();
    let price_precision = integer_precision(
        value
            .get("pricePrecision")
            .or_else(|| value.get("price_precision")),
    );
    let quantity_precision = integer_precision(
        value
            .get("quantityPrecision")
            .or_else(|| value.get("quantity_precision")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol_scope(
            exchange_id,
            MarketType::Spot,
            &base_asset,
            &quote_asset,
            normalize_spot_symbol_lossy(&raw_symbol),
        )?,
        base_asset,
        quote_asset,
        price_increment: price_precision.map(step_from_precision),
        quantity_increment: quantity_precision.map(step_from_precision),
        min_price: string_or_number(value.get("minPrice").or_else(|| value.get("min_price"))),
        max_price: string_or_number(value.get("maxPrice").or_else(|| value.get("max_price"))),
        min_quantity: string_or_number(
            value
                .get("minTradeQuantity")
                .or_else(|| value.get("minQuantity"))
                .or_else(|| value.get("minQty")),
        ),
        max_quantity: None,
        min_notional: string_or_number(
            value
                .get("minTradeAmount")
                .or_else(|| value.get("minNotional")),
        ),
        max_notional: None,
        price_precision,
        quantity_precision,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_futures_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let raw_symbol_name = value_as_string(
        value
            .get("name")
            .or_else(|| value.get("symbol"))
            .or_else(|| value.get("contractName")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "coinstore futures symbol missing",
            value,
        )
    })?;
    let exchange_symbol = value_as_string(value.get("contractId"))
        .unwrap_or_else(|| normalize_futures_symbol_lossy(&raw_symbol_name));
    let base_asset = value_as_string(
        value
            .get("baseAsset")
            .or_else(|| value.get("baseCurrency"))
            .or_else(|| value.get("base")),
    )
    .unwrap_or_else(|| split_futures_symbol(&raw_symbol_name).0)
    .to_ascii_uppercase();
    let quote_asset = value_as_string(
        value
            .get("quoteAsset")
            .or_else(|| value.get("quoteCurrency"))
            .or_else(|| value.get("marginAsset")),
    )
    .unwrap_or_else(|| "USDT".to_string())
    .to_ascii_uppercase();
    let price_increment =
        string_or_number(value.get("tickSize").or_else(|| value.get("priceTick")));
    let quantity_increment = string_or_number(
        value
            .get("contractSize")
            .or_else(|| value.get("stepSize"))
            .or_else(|| value.get("minOrderSize")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol_scope(
            exchange_id,
            MarketType::Perpetual,
            &base_asset,
            &quote_asset,
            exchange_symbol,
        )?,
        base_asset,
        quote_asset,
        price_increment,
        quantity_increment,
        min_price: string_or_number(value.get("minPrice").or_else(|| value.get("min_price"))),
        max_price: string_or_number(value.get("maxPrice").or_else(|| value.get("max_price"))),
        min_quantity: string_or_number(value.get("minOrderSize").or_else(|| value.get("minQty"))),
        max_quantity: string_or_number(value.get("maxOrderSize").or_else(|| value.get("maxQty"))),
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let book = data
        .as_array()
        .and_then(|rows| rows.first())
        .unwrap_or(data);
    let bids = parse_levels(exchange_id, book.get("bids").or_else(|| book.get("bid")))?;
    let asks = parse_levels(exchange_id, book.get("asks").or_else(|| book.get("ask")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinstore order book requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = book
        .get("seq")
        .or_else(|| book.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = book
        .get("ts")
        .or_else(|| book.get("timestamp"))
        .or_else(|| book.get("time"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    levels
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinstore book missing levels",
                &Value::Null,
            )
        })?
        .iter()
        .map(|level| {
            if let Some(values) = level.as_array() {
                let price = values
                    .first()
                    .and_then(number_from_value)
                    .ok_or_else(|| parse_error(exchange_id.clone(), "invalid book price", level))?;
                let quantity = values.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid book quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .or_else(|| level.get("p"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid book price", level))?;
            let quantity = level
                .get("quantity")
                .or_else(|| level.get("qty"))
                .or_else(|| level.get("q"))
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid book quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub fn normalize_spot_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_spot_symbol_lossy(symbol);
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore spot symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_spot_symbol_lossy(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn normalize_futures_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = normalize_futures_symbol_lossy(symbol);
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore futures symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_futures_symbol_lossy(symbol: &str) -> String {
    symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase()
}

fn futures_contract_rows<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<&'a [Value]> {
    let rows = value
        .get("data")
        .and_then(|data| data.get("contracts").or(Some(data)))
        .or_else(|| value.get("contracts"))
        .unwrap_or(value);
    rows.as_array().map(Vec::as_slice).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "coinstore futures fees missing data",
            value,
        )
    })
}

fn row_symbol(row: &Value, market_type: MarketType) -> Option<String> {
    let raw_symbol = if market_type == MarketType::Perpetual {
        value_as_string(
            row.get("contractId")
                .or_else(|| row.get("name"))
                .or_else(|| row.get("symbol"))
                .or_else(|| row.get("contractName")),
        )?
    } else {
        value_as_string(
            row.get("name")
                .or_else(|| row.get("symbol"))
                .or_else(|| row.get("symbolName")),
        )?
    };
    Some(if market_type == MarketType::Perpetual {
        normalize_futures_symbol_lossy(&raw_symbol)
    } else {
        normalize_spot_symbol_lossy(&raw_symbol)
    })
}

fn first_string_or_number(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| string_or_number(value.get(*key)))
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    base_asset: &str,
    quote_asset: &str,
    raw_symbol: String,
) -> ExchangeApiResult<SymbolScope> {
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(
                base_asset.to_ascii_uppercase(),
                quote_asset.to_ascii_uppercase(),
            )
            .map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
            .map_err(validation_error)?,
    })
}

fn split_spot_symbol(symbol: &str) -> (String, String) {
    let normalized = normalize_spot_symbol_lossy(symbol);
    ["USDT", "USDC", "BTC", "ETH"]
        .iter()
        .find_map(|quote| {
            normalized
                .strip_suffix(quote)
                .filter(|base| !base.is_empty())
                .map(|base| (base.to_string(), (*quote).to_string()))
        })
        .unwrap_or((normalized, "USDT".to_string()))
}

fn split_futures_symbol(symbol: &str) -> (String, String) {
    let normalized = normalize_futures_symbol_lossy(symbol);
    if let Some((base, quote)) = normalized.split_once('-') {
        return (base.to_string(), quote.to_string());
    }
    split_spot_symbol(&normalized)
}

pub fn data_array<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    message: &str,
) -> ExchangeApiResult<&'a [Value]> {
    let data = value
        .get("data")
        .and_then(|data| {
            data.as_array()
                .map(Vec::as_slice)
                .or_else(|| {
                    data.get("list")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                })
                .or_else(|| {
                    data.get("items")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                })
        })
        .or_else(|| value.as_array().map(Vec::as_slice));
    data.ok_or_else(|| parse_error(exchange_id.clone(), message, value))
}

pub fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Coinstore decimal {value}: {error}"),
        })
}

pub fn decimal_field(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| string_or_number(value.get(*key)))
        .and_then(|value| value.parse().ok())
}

fn number_from_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

fn integer_precision(value: Option<&Value>) -> Option<u32> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str()?.parse().ok())
            .and_then(|value| u32::try_from(value).ok())
    })
}

fn step_from_precision(precision: u32) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat((precision - 1) as usize))
    }
}

pub fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: rustcta_types::SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: Some("COINSTORE_PARSE".to_string()),
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(raw.clone()),
        occurred_at: Utc::now(),
    })
}

pub fn validation_error(error: ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
