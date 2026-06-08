use crate::smart_money::BinanceDepthUpdate;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinanceTradeTick {
    pub symbol: String,
    pub event_time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub buyer_is_maker: bool,
}

impl BinanceTradeTick {
    pub fn taker_side(&self) -> &'static str {
        if self.buyer_is_maker {
            "sell"
        } else {
            "buy"
        }
    }

    pub fn notional_usdt(&self) -> Decimal {
        self.price * self.quantity
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinanceMarkPriceTick {
    pub symbol: String,
    pub event_time: DateTime<Utc>,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinanceMarketStreamEvent {
    Depth(BinanceDepthUpdate),
    Trade(BinanceTradeTick),
    MarkPrice(BinanceMarkPriceTick),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BinanceMarketParseError {
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("unsupported event type {0}")]
    UnsupportedEvent(String),
    #[error("invalid decimal field {field}: {value}")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("invalid timestamp field {0}")]
    InvalidTimestamp(&'static str),
    #[error("json parse error: {0}")]
    Json(String),
}

pub fn build_combined_stream_path(symbols: &[String], depth_ms: u16) -> String {
    let streams = symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.to_ascii_lowercase();
            [
                format!("{symbol}@depth@{depth_ms}ms"),
                format!("{symbol}@trade"),
                format!("{symbol}@markPrice"),
            ]
        })
        .collect::<Vec<_>>()
        .join("/");
    format!("/stream?streams={streams}")
}

pub fn parse_binance_market_stream(
    raw: &str,
) -> Result<BinanceMarketStreamEvent, BinanceMarketParseError> {
    let value: Value =
        serde_json::from_str(raw).map_err(|err| BinanceMarketParseError::Json(err.to_string()))?;
    let data = value.get("data").unwrap_or(&value);
    let event_type = data
        .get("e")
        .and_then(Value::as_str)
        .ok_or(BinanceMarketParseError::MissingField("e"))?;
    match event_type {
        "depthUpdate" => serde_json::from_value::<BinanceDepthUpdate>(data.clone())
            .map(BinanceMarketStreamEvent::Depth)
            .map_err(|err| BinanceMarketParseError::Json(err.to_string())),
        "trade" => parse_trade(data).map(BinanceMarketStreamEvent::Trade),
        "markPriceUpdate" => parse_mark_price(data).map(BinanceMarketStreamEvent::MarkPrice),
        other => Err(BinanceMarketParseError::UnsupportedEvent(other.to_string())),
    }
}

fn parse_trade(value: &Value) -> Result<BinanceTradeTick, BinanceMarketParseError> {
    Ok(BinanceTradeTick {
        symbol: string_field(value, "s")?.to_string(),
        event_time: millis_field(value, "E")?,
        trade_id: u64_field(value, "t")?,
        price: decimal_field(value, "p")?,
        quantity: decimal_field(value, "q")?,
        buyer_is_maker: bool_field(value, "m")?,
    })
}

fn parse_mark_price(value: &Value) -> Result<BinanceMarkPriceTick, BinanceMarketParseError> {
    Ok(BinanceMarkPriceTick {
        symbol: string_field(value, "s")?.to_string(),
        event_time: millis_field(value, "E")?,
        mark_price: decimal_field(value, "p")?,
        index_price: decimal_field(value, "i")?,
        funding_rate: decimal_field(value, "r")?,
        next_funding_time: value
            .get("T")
            .and_then(Value::as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    })
}

fn string_field<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a str, BinanceMarketParseError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BinanceMarketParseError::MissingField(field))
}

fn bool_field(value: &Value, field: &'static str) -> Result<bool, BinanceMarketParseError> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or(BinanceMarketParseError::MissingField(field))
}

fn u64_field(value: &Value, field: &'static str) -> Result<u64, BinanceMarketParseError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(BinanceMarketParseError::MissingField(field))
}

fn millis_field(
    value: &Value,
    field: &'static str,
) -> Result<DateTime<Utc>, BinanceMarketParseError> {
    let millis = value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or(BinanceMarketParseError::MissingField(field))?;
    DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or(BinanceMarketParseError::InvalidTimestamp(field))
}

fn decimal_field(value: &Value, field: &'static str) -> Result<Decimal, BinanceMarketParseError> {
    let raw = string_field(value, field)?;
    Decimal::from_str(raw).map_err(|_| BinanceMarketParseError::InvalidDecimal {
        field,
        value: raw.to_string(),
    })
}
