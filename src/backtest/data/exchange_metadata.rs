use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::core::types::TradingPair;

#[derive(Debug, Clone)]
pub struct ExchangeMetadataWriter {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ExchangeMetadataReader {
    root: PathBuf,
}

impl ExchangeMetadataWriter {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write_trading_pair(
        &self,
        exchange: &str,
        market: &str,
        trading_pair: &TradingPair,
    ) -> Result<PathBuf> {
        let metadata_dir = metadata_dir(&self.root, exchange, market, &trading_pair.symbol);
        fs::create_dir_all(&metadata_dir)?;
        let path = metadata_dir.join("trading_pair.json");
        fs::write(&path, serde_json::to_vec_pretty(trading_pair)?)?;
        Ok(path)
    }
}

impl ExchangeMetadataReader {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn read_trading_pair(
        &self,
        exchange: &str,
        market: &str,
        symbol: &str,
    ) -> Result<TradingPair> {
        let path = metadata_dir(&self.root, exchange, market, symbol).join("trading_pair.json");
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn has_trading_pair(&self, exchange: &str, market: &str, symbol: &str) -> bool {
        metadata_dir(&self.root, exchange, market, symbol)
            .join("trading_pair.json")
            .exists()
    }
}

fn metadata_dir(root: &Path, exchange: &str, market: &str, symbol: &str) -> PathBuf {
    let symbol_path = sanitize_path_component(symbol);
    root.join("metadata")
        .join(format!("exchange={exchange}"))
        .join(format!("market={market}"))
        .join(format!("symbol={symbol_path}"))
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
