use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::data::depth_dataset::{DepthDatasetOutput, DepthDatasetWriter};
use crate::backtest::schema::DepthDeltaEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDepthSnapshot {
    pub last_update_id: u64,
    pub timestamp: DateTime<Utc>,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDepthDeltaRecord {
    pub exchange_ts: DateTime<Utc>,
    pub logical_ts: DateTime<Utc>,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub previous_final_update_id: Option<u64>,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthCaptureSession {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub stream: String,
    pub snapshot: RawDepthSnapshot,
    pub deltas: Vec<RawDepthDeltaRecord>,
}

#[derive(Debug, Clone)]
pub struct DepthCaptureOutput {
    pub raw_path: PathBuf,
    pub normalized: DepthDatasetOutput,
}

#[derive(Debug, Clone)]
pub struct DepthCaptureWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DepthCaptureReader {
    root: PathBuf,
}

impl DepthCaptureWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_binance_futures_capture(
        &self,
        symbol: &str,
        session: &DepthCaptureSession,
    ) -> Result<PathBuf> {
        let capture_dir = capture_dir(&self.root, "binance", "futures", symbol);
        fs::create_dir_all(&capture_dir)?;
        let raw_path = capture_dir.join("capture.json");
        fs::write(&raw_path, serde_json::to_vec_pretty(session)?)?;
        Ok(raw_path)
    }
}

impl DepthCaptureReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_binance_futures_capture(&self, symbol: &str) -> Result<DepthCaptureSession> {
        let raw_path = capture_dir(&self.root, "binance", "futures", symbol).join("capture.json");
        Ok(serde_json::from_slice(&fs::read(raw_path)?)?)
    }
}

pub fn import_binance_futures_depth_capture(
    root: impl AsRef<Path>,
    symbol: &str,
) -> Result<DepthDatasetOutput> {
    let root = root.as_ref();
    let session = DepthCaptureReader::new(root).read_binance_futures_capture(symbol)?;
    let normalized = normalize_capture_session(&session)?;
    let writer = DepthDatasetWriter::new(root);
    writer.write_binance_futures_depth_deltas(symbol, &normalized)
}

pub fn normalize_capture_session(session: &DepthCaptureSession) -> Result<Vec<DepthDeltaEvent>> {
    let mut normalized = Vec::new();
    let symbol = session.symbol.clone();
    let exchange = session.exchange.clone();
    let market = session.market.clone();

    normalized.push(DepthDeltaEvent {
        exchange: exchange.clone(),
        market: market.clone(),
        symbol: symbol.clone(),
        exchange_ts: session.snapshot.timestamp,
        logical_ts: session.snapshot.timestamp,
        first_update_id: session.snapshot.last_update_id,
        final_update_id: session.snapshot.last_update_id,
        bids: session.snapshot.bids.clone(),
        asks: session.snapshot.asks.clone(),
    });

    let mut deltas = session.deltas.clone();
    deltas.sort_by_key(|delta| (delta.logical_ts, delta.final_update_id));

    let snapshot_update_id = session.snapshot.last_update_id;
    let start_index = deltas
        .iter()
        .position(|delta| {
            delta.first_update_id <= snapshot_update_id
                && delta.final_update_id >= snapshot_update_id
        })
        .or_else(|| {
            deltas.iter().position(|delta| {
                delta.first_update_id <= snapshot_update_id + 1
                    && delta.final_update_id > snapshot_update_id
            })
        })
        .ok_or_else(|| {
            anyhow!("capture does not contain a delta aligned with snapshot update id")
        })?;

    let mut previous_final_update_id = snapshot_update_id;
    for (offset, delta) in deltas.into_iter().skip(start_index).enumerate() {
        if delta.final_update_id <= previous_final_update_id {
            continue;
        }

        let is_first_delta = offset == 0;
        if let Some(previous) = delta.previous_final_update_id {
            if !is_first_delta && previous != previous_final_update_id {
                return Err(anyhow!(
                    "depth delta sequence gap detected: expected previous {}, got {}",
                    previous_final_update_id,
                    previous
                ));
            }
        } else if !is_first_delta && delta.first_update_id > previous_final_update_id + 1 {
            return Err(anyhow!(
                "depth delta sequence gap detected: expected <= {}, got {}",
                previous_final_update_id + 1,
                delta.first_update_id
            ));
        }

        previous_final_update_id = delta.final_update_id;
        normalized.push(DepthDeltaEvent {
            exchange: exchange.clone(),
            market: market.clone(),
            symbol: symbol.clone(),
            exchange_ts: delta.exchange_ts,
            logical_ts: delta.logical_ts,
            first_update_id: delta.first_update_id,
            final_update_id: delta.final_update_id,
            bids: delta.bids,
            asks: delta.asks,
        });
    }

    Ok(normalized)
}

fn capture_dir(root: &Path, exchange: &str, market: &str, symbol: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("raw")
        .join(format!("exchange={exchange}"))
        .join(format!("market={market}"))
        .join(format!("symbol={symbol_path}"))
        .join("stream=depth_delta")
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
