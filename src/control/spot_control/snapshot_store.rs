use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::control::spot_control::lifecycle::normalize_symbol;

use super::{SpotControlRuntimePublisherConfig, SpotControlRuntimeSnapshot};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSpotControlSnapshotRecord {
    pub schema_version: u32,
    pub snapshot: SpotControlRuntimeSnapshot,
}

#[async_trait]
pub trait SpotControlSnapshotStore: Send + Sync {
    async fn persist_snapshot(&self, snapshot: &SpotControlRuntimeSnapshot) -> Result<(), String>;
    async fn get_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<SpotControlRuntimeSnapshot>, String>;
    async fn get_latest_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<SpotControlRuntimeSnapshot>, String>;
    async fn list_snapshots(
        &self,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<SpotControlRuntimeSnapshot>, String>;
    async fn replay_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<SpotControlRuntimeSnapshot>, String> {
        self.get_snapshot(snapshot_id).await
    }
}

#[derive(Debug, Clone)]
pub struct JsonlSpotControlSnapshotStore {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl JsonlSpotControlSnapshotStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl SpotControlSnapshotStore for JsonlSpotControlSnapshotStore {
    async fn persist_snapshot(&self, snapshot: &SpotControlRuntimeSnapshot) -> Result<(), String> {
        if snapshot_contains_secret(snapshot) {
            return Err("snapshot contains a secret-like key and was not persisted".to_string());
        }
        let _guard = self.write_lock.lock().await;
        if read_all(&self.path)?
            .iter()
            .any(|existing| existing.snapshot_id == snapshot.snapshot_id)
        {
            return Err(format!(
                "snapshot {} already exists and is immutable",
                snapshot.snapshot_id
            ));
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create snapshot dir {}: {error}", parent.display()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| format!("open snapshot store {}: {error}", self.path.display()))?;
        let record = StoredSpotControlSnapshotRecord {
            schema_version: snapshot.schema_version,
            snapshot: snapshot.clone(),
        };
        serde_json::to_writer(&mut file, &record)
            .map_err(|error| format!("serialize snapshot {}: {error}", snapshot.snapshot_id))?;
        file.write_all(b"\n")
            .map_err(|error| format!("write snapshot newline: {error}"))?;
        Ok(())
    }

    async fn get_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<SpotControlRuntimeSnapshot>, String> {
        Ok(read_all(&self.path)?
            .into_iter()
            .find(|snapshot| snapshot.snapshot_id == snapshot_id))
    }

    async fn get_latest_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<SpotControlRuntimeSnapshot>, String> {
        let symbol = normalize_symbol(symbol);
        Ok(read_all(&self.path)?
            .into_iter()
            .rev()
            .find(|snapshot| snapshot.symbol == symbol))
    }

    async fn list_snapshots(
        &self,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<SpotControlRuntimeSnapshot>, String> {
        let symbol = normalize_symbol(symbol);
        let mut values = read_all(&self.path)?
            .into_iter()
            .filter(|snapshot| snapshot.symbol == symbol)
            .collect::<Vec<_>>();
        if values.len() > limit {
            values.drain(0..values.len() - limit);
        }
        Ok(values)
    }
}

pub fn snapshot_store_from_config(
    config: &SpotControlRuntimePublisherConfig,
) -> Option<Arc<dyn SpotControlSnapshotStore>> {
    if !config.snapshot_store.enabled {
        return None;
    }
    if config.snapshot_store.backend.eq_ignore_ascii_case("jsonl") {
        Some(Arc::new(JsonlSpotControlSnapshotStore::new(
            &config.snapshot_store.path,
        )))
    } else {
        None
    }
}

fn read_all(path: &Path) -> Result<Vec<SpotControlRuntimeSnapshot>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path)
        .map_err(|error| format!("open snapshot store {}: {error}", path.display()))?;
    let reader = BufReader::new(file);
    let mut by_id: HashMap<String, SpotControlRuntimeSnapshot> = HashMap::new();
    let mut output = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|error| format!("read snapshot store: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<StoredSpotControlSnapshotRecord>(&line)
            .map_err(|error| format!("parse snapshot record: {error}"))?;
        if by_id.contains_key(&record.snapshot.snapshot_id) {
            return Err(format!(
                "immutable snapshot id {} appears more than once",
                record.snapshot.snapshot_id
            ));
        }
        by_id.insert(record.snapshot.snapshot_id.clone(), record.snapshot.clone());
        output.push(record.snapshot);
    }
    Ok(output)
}

fn snapshot_contains_secret(snapshot: &SpotControlRuntimeSnapshot) -> bool {
    let Ok(value) = serde_json::to_value(snapshot) else {
        return true;
    };
    contains_secret_key(&value)
}

fn contains_secret_key(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            matches!(
                key.as_str(),
                "api_key" | "secret" | "api_secret" | "passphrase" | "authorization"
            ) || contains_secret_key(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(contains_secret_key),
        _ => false,
    }
}
