use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::types::Kline;

#[derive(Debug, Clone)]
pub struct KlineDatasetWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct KlineDatasetReader {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct KlineDatasetOutput {
    pub data_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineDatasetManifest {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub interval: String,
    pub row_count: usize,
    pub first_close_time: Option<DateTime<Utc>>,
    pub last_close_time: Option<DateTime<Utc>>,
}

impl KlineDatasetWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_binance_futures_klines(
        &self,
        symbol: &str,
        interval: &str,
        klines: &[Kline],
    ) -> Result<KlineDatasetOutput> {
        let mut normalized = klines.to_vec();
        sort_and_dedupe_klines(&mut normalized);
        let symbol_path = sanitize_path_component(symbol);

        let dataset_dir = self
            .root
            .join("normalized")
            .join("exchange=binance")
            .join("market=futures")
            .join(format!("symbol={symbol_path}"))
            .join(format!("interval={interval}"));
        fs::create_dir_all(&dataset_dir)?;

        let data_path = dataset_dir.join("klines.json");
        let manifest_path = dataset_dir.join("manifest.json");

        fs::write(&data_path, serde_json::to_vec_pretty(&normalized)?)?;

        let manifest = KlineDatasetManifest {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            row_count: normalized.len(),
            first_close_time: normalized.first().map(|item| item.close_time),
            last_close_time: normalized.last().map(|item| item.close_time),
        };
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        Ok(KlineDatasetOutput {
            data_path,
            manifest_path,
        })
    }
}

impl KlineDatasetReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_binance_futures_klines(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<(KlineDatasetManifest, Vec<Kline>)> {
        let dataset_dir = dataset_dir(&self.root, "binance", "futures", symbol, interval);
        let manifest_path = dataset_dir.join("manifest.json");
        let data_path = dataset_dir.join("klines.json");

        let manifest: KlineDatasetManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
        let klines: Vec<Kline> = serde_json::from_slice(&fs::read(data_path)?)?;

        Ok((manifest, klines))
    }
}

fn sort_and_dedupe_klines(klines: &mut Vec<Kline>) {
    klines.sort_by_key(|kline| kline.close_time);
    klines.dedup_by_key(|kline| kline.close_time);
}

fn dataset_dir(root: &Path, exchange: &str, market: &str, symbol: &str, interval: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("normalized")
        .join(format!("exchange={exchange}"))
        .join(format!("market={market}"))
        .join(format!("symbol={symbol_path}"))
        .join(format!("interval={interval}"))
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
