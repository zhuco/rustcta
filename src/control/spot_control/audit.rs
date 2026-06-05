use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::SpotSymbolLifecycleState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    #[serde(default)]
    pub command_id_optional: Option<String>,
    #[serde(default)]
    pub snapshot_id_optional: Option<String>,
    pub user: String,
    pub action: String,
    pub symbol: String,
    #[serde(default)]
    pub previous_state: Option<SpotSymbolLifecycleState>,
    #[serde(default)]
    pub new_state: Option<SpotSymbolLifecycleState>,
    pub result: String,
    pub reason: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub safe_details: Value,
}

impl AuditEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_id: Option<String>,
        user: impl Into<String>,
        action: impl Into<String>,
        symbol: impl Into<String>,
        previous_state: Option<SpotSymbolLifecycleState>,
        new_state: Option<SpotSymbolLifecycleState>,
        result: impl Into<String>,
        reason: impl Into<String>,
        safe_details: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            command_id_optional: command_id,
            snapshot_id_optional: None,
            user: user.into(),
            action: action.into(),
            symbol: symbol.into(),
            previous_state,
            new_state,
            result: result.into(),
            reason: reason.into(),
            timestamp: Utc::now(),
            safe_details,
        }
    }

    pub fn with_snapshot_id(mut self, snapshot_id: Option<String>) -> Self {
        self.snapshot_id_optional = snapshot_id;
        self
    }
}

pub fn append_jsonl<T: Serialize>(path: impl AsRef<Path>, item: &T) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, item)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub fn read_jsonl<T: DeserializeOwned>(path: impl AsRef<Path>) -> std::io::Result<Vec<T>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    let mut items = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let item = serde_json::from_str(&line).map_err(std::io::Error::other)?;
        items.push(item);
    }
    Ok(items)
}
