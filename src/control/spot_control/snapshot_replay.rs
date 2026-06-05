use std::sync::Arc;

use serde_json::json;

use super::{
    enable_validation_from_runtime_snapshot, SnapshotDecisionRecord, SnapshotReplayDecisionKind,
    SnapshotReplayReport, SpotControlSnapshotStore,
};

#[derive(Clone)]
pub struct SpotControlSnapshotReplay {
    store: Arc<dyn SpotControlSnapshotStore>,
    model_version: String,
}

impl SpotControlSnapshotReplay {
    pub fn new(store: Arc<dyn SpotControlSnapshotStore>) -> Self {
        Self {
            store,
            model_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    pub async fn replay(&self, snapshot_id: &str) -> Result<Option<SnapshotReplayReport>, String> {
        let Some(snapshot) = self.store.replay_snapshot(snapshot_id).await? else {
            return Ok(None);
        };
        let original_decisions = snapshot_decisions(&snapshot);
        let replayed_decisions = vec![
            SnapshotDecisionRecord {
                kind: SnapshotReplayDecisionKind::EnableValidation,
                value: serde_json::to_value(enable_validation_from_runtime_snapshot(&snapshot))
                    .map_err(|error| format!("serialize replayed enable validation: {error}"))?,
            },
            SnapshotDecisionRecord {
                kind: SnapshotReplayDecisionKind::DirectionReadiness,
                value: serde_json::to_value(&snapshot.direction_readiness)
                    .map_err(|error| format!("serialize replayed direction readiness: {error}"))?,
            },
            SnapshotDecisionRecord {
                kind: SnapshotReplayDecisionKind::LiquidationPreview,
                value: serde_json::to_value(&snapshot.liquidation_preview)
                    .map_err(|error| format!("serialize replayed liquidation preview: {error}"))?,
            },
        ];
        let differences = decision_differences(&original_decisions, &replayed_decisions);
        Ok(Some(SnapshotReplayReport {
            snapshot_id: snapshot.snapshot_id,
            original_decisions,
            replayed_decisions,
            matching: differences.is_empty(),
            differences,
            model_version: self.model_version.clone(),
        }))
    }
}

pub fn snapshot_decisions(
    snapshot: &super::SpotControlRuntimeSnapshot,
) -> Vec<SnapshotDecisionRecord> {
    vec![
        SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::EnableValidation,
            value: serde_json::to_value(enable_validation_from_runtime_snapshot(snapshot))
                .unwrap_or_else(|error| json!({ "error": error.to_string() })),
        },
        SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::DirectionReadiness,
            value: serde_json::to_value(&snapshot.direction_readiness)
                .unwrap_or_else(|error| json!({ "error": error.to_string() })),
        },
        SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::LiquidationPreview,
            value: serde_json::to_value(&snapshot.liquidation_preview)
                .unwrap_or_else(|error| json!({ "error": error.to_string() })),
        },
    ]
}

fn decision_differences(
    original: &[SnapshotDecisionRecord],
    replayed: &[SnapshotDecisionRecord],
) -> Vec<String> {
    let mut differences = Vec::new();
    for original in original {
        match replayed.iter().find(|item| item.kind == original.kind) {
            Some(replayed) if replayed.value == original.value => {}
            Some(replayed) => differences.push(format!(
                "{:?} changed original={} replayed={}",
                original.kind, original.value, replayed.value
            )),
            None => differences.push(format!("{:?} missing from replay", original.kind)),
        }
    }
    differences
}
