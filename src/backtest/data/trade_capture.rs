use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::data::trade_dataset::{TradeDatasetOutput, TradeDatasetWriter};
use crate::core::types::OrderSide;
use crate::core::types::Trade;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTradeRecord {
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub trade_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeCaptureSession {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub stream: String,
    pub trades: Vec<RawTradeRecord>,
}

#[derive(Debug, Clone)]
pub struct TradeCaptureOutput {
    pub raw_path: PathBuf,
    pub normalized: TradeDatasetOutput,
}

#[derive(Debug, Clone)]
pub struct TradeCaptureWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TradeCaptureReader {
    root: PathBuf,
}

impl TradeCaptureWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_binance_futures_capture(
        &self,
        symbol: &str,
        session: &TradeCaptureSession,
    ) -> Result<PathBuf> {
        let capture_dir = capture_dir(&self.root, "binance", "futures", symbol);
        fs::create_dir_all(&capture_dir)?;
        let raw_path = capture_dir.join("capture.json");
        fs::write(&raw_path, serde_json::to_vec_pretty(session)?)?;
        Ok(raw_path)
    }
}

impl TradeCaptureReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_binance_futures_capture(&self, symbol: &str) -> Result<TradeCaptureSession> {
        let raw_path = capture_dir(&self.root, "binance", "futures", symbol).join("capture.json");
        Ok(serde_json::from_slice(&fs::read(raw_path)?)?)
    }
}

pub fn import_binance_futures_trade_capture(
    root: impl AsRef<Path>,
    symbol: &str,
) -> Result<TradeDatasetOutput> {
    let root = root.as_ref();
    let session = TradeCaptureReader::new(root).read_binance_futures_capture(symbol)?;
    let normalized = normalize_capture_session(&session);
    let writer = TradeDatasetWriter::new(root);
    writer.write_binance_futures_trades(symbol, &normalized)
}

pub fn normalize_capture_session(session: &TradeCaptureSession) -> Vec<Trade> {
    let mut trades = session
        .trades
        .iter()
        .map(|record| Trade {
            id: record.trade_id.clone(),
            symbol: session.symbol.clone(),
            side: record.side,
            amount: record.quantity,
            price: record.price,
            timestamp: record.logical_ts,
            order_id: None,
            fee: None,
        })
        .collect::<Vec<_>>();

    trades.sort_by(|lhs, rhs| lhs.timestamp.cmp(&rhs.timestamp).then(lhs.id.cmp(&rhs.id)));
    trades.dedup_by(|lhs, rhs| lhs.id == rhs.id);
    trades
}

fn capture_dir(root: &Path, exchange: &str, market: &str, symbol: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("raw")
        .join(format!("exchange={exchange}"))
        .join(format!("market={market}"))
        .join(format!("symbol={symbol_path}"))
        .join("stream=trade")
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
