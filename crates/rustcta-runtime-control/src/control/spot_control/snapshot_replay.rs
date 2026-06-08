use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReplayDecisionKind {
    EnableValidation,
    DirectionReadiness,
    LiquidationPreview,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDecisionRecord {
    pub kind: SnapshotReplayDecisionKind,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotReplayReport {
    pub snapshot_id: String,
    pub original_decisions: Vec<SnapshotDecisionRecord>,
    pub replayed_decisions: Vec<SnapshotDecisionRecord>,
    pub matching: bool,
    pub differences: Vec<String>,
    pub model_version: String,
}

impl SnapshotReplayReport {
    pub fn from_decisions(
        snapshot_id: impl Into<String>,
        original_decisions: Vec<SnapshotDecisionRecord>,
        replayed_decisions: Vec<SnapshotDecisionRecord>,
        model_version: impl Into<String>,
    ) -> Self {
        let differences = decision_differences(&original_decisions, &replayed_decisions);
        Self {
            snapshot_id: snapshot_id.into(),
            original_decisions,
            replayed_decisions,
            matching: differences.is_empty(),
            differences,
            model_version: model_version.into(),
        }
    }
}

pub fn decision_differences(
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn snapshot_replay_report_should_mark_matching_decisions() {
        let decisions = vec![SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::EnableValidation,
            value: json!({ "allowed": true }),
        }];

        let report = SnapshotReplayReport::from_decisions(
            "snapshot-1",
            decisions.clone(),
            decisions,
            "test-model",
        );

        assert!(report.matching);
        assert!(report.differences.is_empty());
    }

    #[test]
    fn decision_differences_should_report_changed_values() {
        let original = vec![SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::LiquidationPreview,
            value: json!({ "rejected": false }),
        }];
        let replayed = vec![SnapshotDecisionRecord {
            kind: SnapshotReplayDecisionKind::LiquidationPreview,
            value: json!({ "rejected": true }),
        }];

        let differences = decision_differences(&original, &replayed);

        assert_eq!(differences.len(), 1);
        assert!(differences[0].contains("LiquidationPreview changed"));
    }
}
