use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeComponentFreshness {
    Fresh,
    Stale,
    Missing,
    Error,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotComponentStatus {
    pub component: String,
    pub status: RuntimeComponentFreshness,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub age_ms: Option<i64>,
    pub source: String,
    #[serde(default)]
    pub warning_optional: Option<String>,
}

impl SnapshotComponentStatus {
    pub fn fresh(
        component: impl Into<String>,
        updated_at: DateTime<Utc>,
        source: impl Into<String>,
    ) -> Self {
        let age_ms = Utc::now()
            .signed_duration_since(updated_at)
            .num_milliseconds()
            .max(0);
        Self {
            component: component.into(),
            status: RuntimeComponentFreshness::Fresh,
            updated_at: Some(updated_at),
            age_ms: Some(age_ms),
            source: source.into(),
            warning_optional: None,
        }
    }

    pub fn stale(
        component: impl Into<String>,
        updated_at: DateTime<Utc>,
        source: impl Into<String>,
        warning: impl Into<String>,
    ) -> Self {
        let age_ms = Utc::now()
            .signed_duration_since(updated_at)
            .num_milliseconds()
            .max(0);
        Self {
            component: component.into(),
            status: RuntimeComponentFreshness::Stale,
            updated_at: Some(updated_at),
            age_ms: Some(age_ms),
            source: source.into(),
            warning_optional: Some(warning.into()),
        }
    }

    pub fn missing(component: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: RuntimeComponentFreshness::Missing,
            updated_at: None,
            age_ms: None,
            source: source.into(),
            warning_optional: Some(
                "component missing from authoritative runtime state".to_string(),
            ),
        }
    }

    pub fn unsupported(component: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: RuntimeComponentFreshness::Unsupported,
            updated_at: None,
            age_ms: None,
            source: source.into(),
            warning_optional: Some("component unsupported by current runtime path".to_string()),
        }
    }

    pub fn error(
        component: impl Into<String>,
        source: impl Into<String>,
        warning: impl Into<String>,
    ) -> Self {
        Self {
            component: component.into(),
            status: RuntimeComponentFreshness::Error,
            updated_at: None,
            age_ms: None,
            source: source.into(),
            warning_optional: Some(warning.into()),
        }
    }

    pub fn is_critical_failure(&self) -> bool {
        matches!(
            self.status,
            RuntimeComponentFreshness::Stale
                | RuntimeComponentFreshness::Missing
                | RuntimeComponentFreshness::Error
        )
    }
}
