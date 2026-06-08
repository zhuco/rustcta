#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::Value;

pub fn parse_hibachi_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let contracts = value
        .get("futureContracts")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi exchange-info missing futureContracts",
                value,
            )
        })?;
    contracts
        .iter()
        .map(|contract| parse_contract(exchange_id, contract))
        .collect()
}

pub fn parse_hibachi_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bid"))?;
    let asks = parse_levels(exchange_id, value.get("ask"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Hibachi order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = value
        .get("sequence")
        .or_else(|| value.get("seqNum"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("time"))
        .and_then(value_as_i64)
        .and_then(timestamp_auto);
    Ok(snapshot)
}

pub fn parse_hibachi_fee_snapshots(symbols: &[SymbolScope], value: &Value) -> Vec<FeeRateSnapshot> {
    let fee_config = value.get("feeConfig");
    let maker_rate =
        decimal_path(fee_config, &["tradeMakerFeeRate"]).unwrap_or_else(|| "0".to_string());
    let taker_rate =
        decimal_path(fee_config, &["tradeTakerFeeRate"]).unwrap_or_else(|| "0".to_string());
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker_rate.clone(),
            taker_rate: taker_rate.clone(),
            source: Some("hibachi.market.exchange_info.feeConfig".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_hibachi_order(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        symbol_text.to_string(),
    )
    .map_err(validation_error)?;
    let canonical_symbol = canonical_from_perp_symbol(symbol_text).ok();
    let order_type_text = value
        .get("orderType")
        .and_then(|order_type| {
            order_type
                .get("type")
                .or_else(|| order_type.get("orderType"))
                .or(Some(order_type))
        })
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let side = match required_str(exchange_id, value, "side")?
        .to_ascii_uppercase()
        .as_str()
    {
        "BID" | "BUY" => OrderSide::Buy,
        "ASK" | "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    };
    let quantity = decimal_path(Some(value), &["totalQuantity"]).unwrap_or_else(|| "0".to_string());
    let available =
        decimal_path(Some(value), &["availableQuantity"]).unwrap_or_else(|| quantity.clone());
    let filled_quantity =
        decimal_subtract(&quantity, &available).unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value
            .get("clientOrderId")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        exchange_order_id: value
            .get("orderId")
            .and_then(value_as_u64)
            .map(|value| value.to_string()),
        side,
        position_side: Some(PositionSide::Net),
        order_type: parse_order_type(order_type_text),
        time_in_force: None,
        status: parse_order_status(value.get("status").and_then(Value::as_str)),
        quantity,
        price: decimal_path(Some(value), &["price"]),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: order_type_text.eq_ignore_ascii_case("POST_ONLY"),
        created_at: value
            .get("creationTime")
            .and_then(value_as_i64)
            .and_then(timestamp_auto),
        updated_at: value
            .get("finishTime")
            .and_then(value_as_i64)
            .and_then(timestamp_auto)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_hibachi_orders(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("orders")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi orders response must be an array",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_hibachi_order(exchange_id, order))
        .collect()
}

pub fn hibachi_order_book_channel(symbol: &str) -> String {
    format!("orderbook/{symbol}")
}

pub fn hibachi_trade_channel(symbol: &str) -> String {
    format!("trades/{symbol}")
}

fn parse_contract(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?.to_string();
    let base_asset = required_str(exchange_id, value, "underlyingSymbol")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "settlementSymbol")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
            .map_err(validation_error)?;
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "live" | "active" | "trading" | "open"
            )
        })
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol,
        },
        base_asset,
        quote_asset,
        price_increment: decimal_path(Some(value), &["tickSize"]),
        quantity_increment: decimal_path(Some(value), &["stepSize"]),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(Some(value), &["minOrderSize"]),
        max_quantity: None,
        min_notional: decimal_path(Some(value), &["minNotional"]),
        max_notional: None,
        price_precision: decimal_path(Some(value), &["tickSize"]).and_then(decimal_precision),
        quantity_precision: decimal_path(Some(value), &["stepSize"]).and_then(decimal_precision),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    side: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = side
        .and_then(|side| side.get("levels"))
        .or(side)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi order book missing levels",
                &Value::Null,
            )
        })?;
    levels
        .iter()
        .filter_map(|level| {
            let price = level.get("price").and_then(value_as_f64)?;
            let quantity = level.get("quantity").and_then(value_as_f64)?;
            (price > 0.0 && quantity > 0.0).then_some((price, quantity))
        })
        .map(|(price, quantity)| OrderBookLevel::new(price, quantity).map_err(validation_error))
        .collect()
}

fn canonical_from_perp_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) =
        symbol
            .trim()
            .split_once('/')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot infer Hibachi canonical symbol from {symbol}"),
            })?;
    let quote = quote.trim_end_matches("-P").trim_end_matches("-PERP");
    CanonicalSymbol::new(base, quote).map_err(validation_error)
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "POST_ONLY" => OrderType::PostOnly,
        "IOC" => OrderType::IOC,
        "FOK" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "OPEN" | "NEW" | "PLACED" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "FINISHED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("Hibachi payload missing {field}"),
            value,
        )
    })
}

fn decimal_path(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut cursor = value?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

fn decimal_subtract(total: &str, available: &str) -> Option<String> {
    let total = total.parse::<f64>().ok()?;
    let available = available.parse::<f64>().ok()?;
    Some(
        format!("{:.12}", (total - available).max(0.0))
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
    )
}

fn timestamp_auto(value: i64) -> Option<chrono::DateTime<Utc>> {
    if value > 10_000_000_000 {
        Utc.timestamp_millis_opt(value).single()
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        message.into(),
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
