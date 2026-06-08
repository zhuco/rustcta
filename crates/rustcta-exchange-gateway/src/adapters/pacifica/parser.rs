#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ResponseMetadata, SymbolRules, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, MarketType, OrderBookLevel, OrderBookSnapshot};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct PacificaBook {
    pub symbol: String,
    pub sequence: Option<u64>,
    pub timestamp_ms: Option<i64>,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PacificaPosition {
    pub symbol: String,
    pub amount: String,
    pub entry_price: Option<String>,
    pub mark_price: Option<String>,
    pub unrealized_pnl: Option<String>,
    pub liquidation_price: Option<String>,
    pub leverage: Option<String>,
}

pub fn parse_symbol_rules_response(
    exchange_id: ExchangeId,
    payload: &str,
) -> ExchangeApiResult<SymbolRulesResponse> {
    let value: Value = parse_json(payload, "market info")?;
    let rows = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::Serialization {
            message: "Pacifica market info missing data array".to_string(),
        }
    })?;
    let rules = rows
        .iter()
        .map(|row| {
            let symbol = string_field(row, "symbol")?;
            let scope = rustcta_exchange_api::SymbolScope {
                exchange: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: Some(CanonicalSymbol::new(&symbol, "USDC").map_err(to_api)?),
                exchange_symbol: rustcta_types::ExchangeSymbol::new(
                    exchange_id.clone(),
                    MarketType::Perpetual,
                    &symbol,
                )
                .map_err(to_api)?,
            };
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: scope,
                base_asset: symbol,
                quote_asset: "USDC".to_string(),
                price_increment: optional_string(row, "tick_size"),
                quantity_increment: optional_string(row, "lot_size"),
                min_price: optional_string(row, "min_tick"),
                max_price: optional_string(row, "max_tick"),
                min_quantity: optional_string(row, "min_order_size"),
                max_quantity: optional_string(row, "max_order_size"),
                min_notional: None,
                max_notional: None,
                price_precision: None,
                quantity_precision: None,
                supports_market_orders: true,
                supports_limit_orders: true,
                supports_post_only: true,
                supports_reduce_only: true,
                updated_at: Utc::now(),
            })
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id, Utc::now()),
        rules,
    })
}

pub fn parse_book_payload(payload: &str) -> ExchangeApiResult<PacificaBook> {
    let value: Value = parse_json(payload, "order book")?;
    let data = value
        .get("data")
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "Pacifica order book missing data".to_string(),
        })?;
    let levels =
        data.get("l")
            .and_then(Value::as_array)
            .ok_or_else(|| ExchangeApiError::Serialization {
                message: "Pacifica order book missing level array".to_string(),
            })?;
    Ok(PacificaBook {
        symbol: string_field(data, "s")?,
        sequence: value
            .get("last_order_id")
            .and_then(Value::as_u64)
            .or_else(|| data.get("li").and_then(Value::as_u64)),
        timestamp_ms: data.get("t").and_then(value_as_i64),
        bids: parse_levels(levels.first())?,
        asks: parse_levels(levels.get(1))?,
    })
}

pub fn parse_order_book_snapshot(
    exchange_id: ExchangeId,
    market_type: MarketType,
    payload: &str,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = parse_book_payload(payload)?;
    let canonical_symbol = CanonicalSymbol::new(&book.symbol, "USDC").map_err(to_api)?;
    let bids = book
        .bids
        .iter()
        .map(|(price, amount)| {
            OrderBookLevel::new(
                parse_f64(price, "bid price")?,
                parse_f64(amount, "bid amount")?,
            )
            .map_err(to_api)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let asks = book
        .asks
        .iter()
        .map(|(price, amount)| {
            OrderBookLevel::new(
                parse_f64(price, "ask price")?,
                parse_f64(amount, "ask amount")?,
            )
            .map_err(to_api)
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id,
        market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(to_api)?;
    snapshot.sequence = book.sequence;
    snapshot.exchange_timestamp = book
        .timestamp_ms
        .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single());
    Ok(snapshot)
}

pub fn parse_positions_payload(payload: &str) -> ExchangeApiResult<Vec<PacificaPosition>> {
    let value: Value = parse_json(payload, "positions")?;
    let rows = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::Serialization {
            message: "Pacifica positions payload missing data array".to_string(),
        }
    })?;
    rows.iter()
        .map(|row| {
            Ok(PacificaPosition {
                symbol: string_field(row, "symbol")?,
                amount: string_field(row, "amount")?,
                entry_price: optional_string(row, "entry_price"),
                mark_price: optional_string(row, "mark_price"),
                unrealized_pnl: optional_string(row, "unrealized_pnl"),
                liquidation_price: optional_string(row, "liquidation_price"),
                leverage: optional_string(row, "leverage"),
            })
        })
        .collect()
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<(String, String)>> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let price = row.get("p").and_then(value_as_string).ok_or_else(|| {
                ExchangeApiError::Serialization {
                    message: "Pacifica level missing price".to_string(),
                }
            })?;
            let amount = row.get("a").and_then(value_as_string).ok_or_else(|| {
                ExchangeApiError::Serialization {
                    message: "Pacifica level missing amount".to_string(),
                }
            })?;
            Ok((price, amount))
        })
        .collect()
}

fn parse_json(payload: &str, label: &str) -> ExchangeApiResult<Value> {
    serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
        message: format!("invalid Pacifica {label} payload: {error}"),
    })
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    optional_string(value, field).ok_or_else(|| ExchangeApiError::Serialization {
        message: format!("Pacifica payload missing {field}"),
    })
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(value_as_string)
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
}

fn parse_f64(value: &str, label: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid Pacifica {label}: {error}"),
        })
}

fn to_api(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
