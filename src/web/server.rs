use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use super::models::MonitoringConfig;
use super::MonitoringState;

pub fn spawn_monitoring_snapshot_writer(
    config: MonitoringConfig,
    state: MonitoringState,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        return None;
    }
    let interval_ms = config.refresh_interval_ms.max(100);
    Some(tokio::spawn(async move {
        loop {
            if let Err(error) = write_snapshot_once(&config.snapshot_path, &state).await {
                log::warn!(
                    "failed to write control-api monitoring snapshot path={}: {error}",
                    config.snapshot_path
                );
            }
            sleep(Duration::from_millis(interval_ms)).await;
        }
    }))
}

async fn write_snapshot_once(path: &str, state: &MonitoringState) -> Result<()> {
    let snapshot = state.snapshot().await;
    let value = serde_json::to_value(&snapshot).context("serialize monitoring snapshot")?;
    if contains_secret_key(&value) {
        return Err(anyhow!(
            "refusing to persist monitoring snapshot because it contains a secret-like key"
        ));
    }
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create snapshot directory {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(&snapshot).context("encode monitoring snapshot")?;
    tokio::fs::write(&tmp, bytes)
        .await
        .with_context(|| format!("write temporary snapshot {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("rename snapshot {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn contains_secret_key(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            matches!(
                key.as_str(),
                "api_key" | "secret" | "api_secret" | "passphrase" | "authorization"
            ) || contains_secret_key(value)
        }),
        Value::Array(values) => values.iter().any(contains_secret_key),
        _ => false,
    }
}
