use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::types::Trade;

#[derive(Debug, Clone)]
pub struct TradeDatasetWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TradeDatasetReader {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TradeDatasetOutput {
    pub data_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeDatasetManifest {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub row_count: usize,
    pub first_trade_ts: Option<DateTime<Utc>>,
    pub last_trade_ts: Option<DateTime<Utc>>,
}

impl TradeDatasetWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_binance_futures_trades(
        &self,
        symbol: &str,
        trades: &[Trade],
    ) -> Result<TradeDatasetOutput> {
        let mut normalized = trades.to_vec();
        sort_and_dedupe_trades(&mut normalized);
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        fs::create_dir_all(&dataset_dir)?;

        let data_path = dataset_dir.join("trades.json");
        let manifest_path = dataset_dir.join("manifest.json");

        fs::write(&data_path, serde_json::to_vec_pretty(&normalized)?)?;

        let manifest = TradeDatasetManifest {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            row_count: normalized.len(),
            first_trade_ts: normalized.first().map(|item| item.timestamp),
            last_trade_ts: normalized.last().map(|item| item.timestamp),
        };
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        Ok(TradeDatasetOutput {
            data_path,
            manifest_path,
        })
    }
}

impl TradeDatasetReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_binance_futures_trades(
        &self,
        symbol: &str,
    ) -> Result<(TradeDatasetManifest, Vec<Trade>)> {
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        let manifest_path = dataset_dir.join("manifest.json");
        let data_path = dataset_dir.join("trades.json");

        let manifest: TradeDatasetManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
        let trades: Vec<Trade> = serde_json::from_slice(&fs::read(data_path)?)?;

        Ok((manifest, trades))
    }

    pub fn has_binance_futures_trades(&self, symbol: &str) -> bool {
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        dataset_dir.join("manifest.json").exists() && dataset_dir.join("trades.json").exists()
    }
}

fn sort_and_dedupe_trades(trades: &mut Vec<Trade>) {
    trades.sort_by(|lhs, rhs| lhs.timestamp.cmp(&rhs.timestamp).then(lhs.id.cmp(&rhs.id)));
    trades.dedup_by(|lhs, rhs| lhs.id == rhs.id);
}

fn dataset_dir(root: &Path, exchange: &str, market: &str, symbol: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("normalized")
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
