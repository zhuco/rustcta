use std::sync::Arc;

use crate::Exchange;
use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;

use crate::core::types::{Interval, Kline, MarketType};

#[derive(Debug, Clone)]
pub struct IncomingKline {
    pub symbol: String,
    pub interval: String,
    pub kline: Kline,
}

#[derive(Debug, Clone)]
pub struct CombinedStreamEnvelope<T> {
    pub stream: Option<String>,
    pub data: T,
}

#[derive(Debug, Deserialize)]
struct WsKlinePayload {
    #[serde(rename = "e")]
    event_type: Option<String>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "k")]
    inner: Option<WsKlineData>,
}

#[derive(Debug, Deserialize)]
struct WsKlineData {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "q")]
    quote_volume: Option<String>,
    #[serde(rename = "n")]
    trade_count: Option<u64>,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "x")]
    is_closed: bool,
}

pub fn parse_kline_message(raw: &str) -> Result<Option<IncomingKline>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;

    if value.get("result").is_some() {
        return Ok(None);
    }

    if let Some(data) = value.get("data") {
        let payload: WsKlinePayload = serde_json::from_value(data.clone())?;
        return convert_payload(payload, data.get("s").and_then(|s| s.as_str()));
    }

    let payload: WsKlinePayload = serde_json::from_value(value.clone())?;
    convert_payload(payload, value.get("s").and_then(|s| s.as_str()))
}

fn convert_payload(
    payload: WsKlinePayload,
    stream_symbol: Option<&str>,
) -> Result<Option<IncomingKline>> {
    if payload.event_type.as_deref() != Some("kline") {
        return Ok(None);
    }

    let Some(inner) = payload.inner else {
        return Ok(None);
    };

    if !inner.is_closed {
        return Ok(None);
    }

    let symbol = payload
        .symbol
        .or_else(|| stream_symbol.map(|s| s.to_string()))
        .unwrap_or_default();

    if symbol.is_empty() {
        return Err(anyhow!("Missing symbol in kline payload"));
    }

    let open_time = Utc
        .timestamp_opt(inner.open_time / 1000, 0)
        .single()
        .ok_or_else(|| anyhow!("Invalid open time {}", inner.open_time))?;
    let close_time = Utc
        .timestamp_opt(inner.close_time / 1000, 0)
        .single()
        .ok_or_else(|| anyhow!("Invalid close time {}", inner.close_time))?;

    let open = inner.open.parse::<f64>()?;
    let high = inner.high.parse::<f64>()?;
    let low = inner.low.parse::<f64>()?;
    let close = inner.close.parse::<f64>()?;
    let volume = inner.volume.parse::<f64>()?;
    let quote_volume = inner
        .quote_volume
        .as_deref()
        .unwrap_or("0")
        .parse::<f64>()?;

    Ok(Some(IncomingKline {
        symbol: symbol.clone(),
        interval: inner.interval.clone(),
        kline: Kline {
            symbol,
            interval: inner.interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count: inner.trade_count.unwrap_or(0),
        },
    }))
}

pub async fn load_historical_klines(
    exchange: Arc<Box<dyn Exchange>>,
    symbol: &str,
    intervals: &[Interval],
    market_type: MarketType,
    limit: usize,
) -> Result<Vec<(Interval, Vec<Kline>)>> {
    let mut results = Vec::new();
    for interval in intervals {
        let klines = exchange
            .get_klines(symbol, *interval, market_type, Some(limit as u32))
            .await
            .map_err(|e| anyhow!("failed to fetch {:?} for {}: {}", interval, symbol, e))?;
        results.push((*interval, klines));
    }
    Ok(results)
}

pub fn ensure_chronological(klines: &mut Vec<Kline>) {
    klines.sort_by_key(|k| k.close_time);
    klines.dedup_by_key(|k| k.close_time);
}

pub fn merge_initial_series(
    existing: &mut VecDequeWrapper,
    mut new_bars: Vec<Kline>,
    max_len: usize,
) {
    ensure_chronological(&mut new_bars);
    for bar in new_bars {
        existing.push(bar, max_len);
    }
}

pub struct VecDequeWrapper<'a> {
    inner: &'a mut VecDeque<Kline>,
}

use std::collections::VecDeque;

impl<'a> VecDequeWrapper<'a> {
    pub fn new(inner: &'a mut VecDeque<Kline>) -> Self {
        Self { inner }
    }

    pub fn push(&mut self, bar: Kline, max_len: usize) {
        self.inner.push_back(bar);
        while self.inner.len() > max_len {
            self.inner.pop_front();
        }
    }
}

pub fn verify_sequence(
    last_close: Option<DateTime<Utc>>,
    new_close: DateTime<Utc>,
    expected: i64,
) -> bool {
    if let Some(last) = last_close {
        let delta = new_close - last;
        if delta.num_minutes() as i64 == expected {
            return true;
        }
        if delta.num_seconds() == 0 {
            return true;
        }
        return false;
    }
    true
}
