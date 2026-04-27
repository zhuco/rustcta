use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::schema::DepthDeltaEvent;

#[derive(Debug, Clone)]
pub struct DepthDatasetWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DepthDatasetReader {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DepthDatasetOutput {
    pub data_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthDatasetManifest {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub row_count: usize,
    pub first_logical_ts: Option<DateTime<Utc>>,
    pub last_logical_ts: Option<DateTime<Utc>>,
}

impl DepthDatasetWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_binance_futures_depth_deltas(
        &self,
        symbol: &str,
        deltas: &[DepthDeltaEvent],
    ) -> Result<DepthDatasetOutput> {
        let mut normalized = deltas.to_vec();
        sort_and_dedupe_depth_deltas(&mut normalized);
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        fs::create_dir_all(&dataset_dir)?;

        let data_path = dataset_dir.join("depth_deltas.json");
        let manifest_path = dataset_dir.join("manifest.json");

        fs::write(&data_path, serde_json::to_vec_pretty(&normalized)?)?;

        let manifest = DepthDatasetManifest {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            row_count: normalized.len(),
            first_logical_ts: normalized.first().map(|item| item.logical_ts),
            last_logical_ts: normalized.last().map(|item| item.logical_ts),
        };
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        Ok(DepthDatasetOutput {
            data_path,
            manifest_path,
        })
    }
}

impl DepthDatasetReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_binance_futures_depth_deltas(
        &self,
        symbol: &str,
    ) -> Result<(DepthDatasetManifest, Vec<DepthDeltaEvent>)> {
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        let manifest_path = dataset_dir.join("manifest.json");
        let data_path = dataset_dir.join("depth_deltas.json");

        let manifest: DepthDatasetManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
        let deltas: Vec<DepthDeltaEvent> = serde_json::from_slice(&fs::read(data_path)?)?;

        Ok((manifest, deltas))
    }

    pub fn has_binance_futures_depth_deltas(&self, symbol: &str) -> bool {
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol);
        dataset_dir.join("manifest.json").exists() && dataset_dir.join("depth_deltas.json").exists()
    }
}

fn sort_and_dedupe_depth_deltas(deltas: &mut Vec<DepthDeltaEvent>) {
    deltas.sort_by_key(|delta| (delta.logical_ts, delta.final_update_id));
    deltas.dedup_by_key(|delta| delta.final_update_id);
}

fn dataset_dir(root: &Path, exchange: &str, market: &str, symbol: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("normalized")
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
