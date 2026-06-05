use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{LiquidityRole, MarketType, OrderSide};

use super::{RuntimeComponentFreshness, SpotControlRuntimeSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDataAuthority {
    ExchangeRest,
    ExchangeWebSocket,
    LocalRegistry,
    LocalReservationManager,
    ReconciliationService,
    ConfigFallback,
    Unsupported,
}

#[derive(Debug, Clone)]
pub struct RuntimeComponentValue<T> {
    pub value_optional: Option<T>,
    pub authority: RuntimeDataAuthority,
    pub fetched_at: DateTime<Utc>,
    pub exchange_timestamp_optional: Option<DateTime<Utc>>,
    pub age_ms: i64,
    pub freshness_status: RuntimeComponentFreshness,
    pub error_optional: Option<String>,
}

impl<T> RuntimeComponentValue<T> {
    pub fn fresh(
        value: T,
        authority: RuntimeDataAuthority,
        fetched_at: DateTime<Utc>,
        exchange_timestamp_optional: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            value_optional: Some(value),
            authority,
            fetched_at,
            exchange_timestamp_optional,
            age_ms: age_ms(fetched_at),
            freshness_status: RuntimeComponentFreshness::Fresh,
            error_optional: None,
        }
    }

    pub fn stale_from_last_valid(mut self, error: impl Into<String>) -> Self {
        self.age_ms = age_ms(self.fetched_at);
        self.freshness_status = RuntimeComponentFreshness::Stale;
        self.error_optional = Some(error.into());
        self
    }

    pub fn missing(authority: RuntimeDataAuthority, error: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            value_optional: None,
            authority,
            fetched_at: now,
            exchange_timestamp_optional: None,
            age_ms: 0,
            freshness_status: RuntimeComponentFreshness::Missing,
            error_optional: Some(error.into()),
        }
    }

    pub fn unsupported(error: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            value_optional: None,
            authority: RuntimeDataAuthority::Unsupported,
            fetched_at: now,
            exchange_timestamp_optional: None,
            age_ms: 0,
            freshness_status: RuntimeComponentFreshness::Unsupported,
            error_optional: Some(error.into()),
        }
    }

    pub fn refresh_age(mut self) -> Self {
        self.age_ms = age_ms(self.fetched_at);
        self
    }
}

fn age_ms(at: DateTime<Utc>) -> i64 {
    Utc::now()
        .signed_duration_since(at)
        .num_milliseconds()
        .max(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeComponentMetadata {
    pub component: String,
    pub authority: RuntimeDataAuthority,
    pub fetched_at: DateTime<Utc>,
    #[serde(default)]
    pub exchange_timestamp_optional: Option<DateTime<Utc>>,
    pub age_ms: i64,
    pub freshness_status: RuntimeComponentFreshness,
    #[serde(default)]
    pub error_optional: Option<String>,
}

impl<T> RuntimeComponentValue<T> {
    pub fn metadata(&self, component: impl Into<String>) -> RuntimeComponentMetadata {
        RuntimeComponentMetadata {
            component: component.into(),
            authority: self.authority,
            fetched_at: self.fetched_at,
            exchange_timestamp_optional: self.exchange_timestamp_optional,
            age_ms: age_ms(self.fetched_at),
            freshness_status: self.freshness_status,
            error_optional: self.error_optional.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderOwnershipClass {
    SpotControl,
    ArbitrageStrategy,
    OtherKnownStrategy,
    ManualOperator,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillOwnershipRecord {
    pub exchange: String,
    pub symbol: String,
    pub fill_id: String,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_order_id_optional: Option<String>,
    pub ownership_class: OrderOwnershipClass,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
    #[serde(default)]
    pub fee: Option<f64>,
    #[serde(default)]
    pub fee_asset: Option<String>,
    pub liquidity_role: LiquidityRole,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotConsistency {
    StrongEnoughForControl,
    SuitableForObservation,
    Stale,
    Inconsistent,
    MissingCriticalData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConsistencyReport {
    pub snapshot_id: String,
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub oldest_required_component_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub newest_component_at: Option<DateTime<Utc>>,
    pub maximum_component_age_ms: i64,
    pub component_time_skew_ms: i64,
    pub status: SnapshotConsistency,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

impl SnapshotConsistencyReport {
    pub fn from_snapshot(
        snapshot: &SpotControlRuntimeSnapshot,
        max_component_time_skew_ms: u64,
    ) -> Self {
        let component_times = snapshot
            .component_statuses
            .iter()
            .filter_map(|status| status.updated_at)
            .collect::<Vec<_>>();
        let oldest = component_times.iter().min().copied();
        let newest = component_times.iter().max().copied();
        let maximum_component_age_ms = component_times
            .iter()
            .map(|at| {
                snapshot
                    .generated_at
                    .signed_duration_since(*at)
                    .num_milliseconds()
                    .max(0)
            })
            .max()
            .unwrap_or_default();
        let component_time_skew_ms = match (oldest, newest) {
            (Some(oldest), Some(newest)) => newest.signed_duration_since(oldest).num_milliseconds(),
            _ => 0,
        };
        let mut warnings = snapshot.warnings.clone();
        let mut critical_errors = snapshot.critical_errors.clone();
        let missing_critical = snapshot
            .component_statuses
            .iter()
            .any(|item| item.freshness_status_blocks_control());
        if component_time_skew_ms > max_component_time_skew_ms as i64 {
            critical_errors.push(format!(
                "component time skew {component_time_skew_ms}ms exceeds policy"
            ));
        }
        let status = if missing_critical {
            SnapshotConsistency::MissingCriticalData
        } else if component_time_skew_ms > max_component_time_skew_ms as i64 {
            SnapshotConsistency::Inconsistent
        } else if !critical_errors.is_empty() {
            SnapshotConsistency::Stale
        } else if !warnings.is_empty() {
            SnapshotConsistency::SuitableForObservation
        } else {
            SnapshotConsistency::StrongEnoughForControl
        };
        if matches!(status, SnapshotConsistency::SuitableForObservation) {
            warnings.push("snapshot is observable but contains non-critical warnings".to_string());
        }
        Self {
            snapshot_id: snapshot.snapshot_id.clone(),
            generated_at: snapshot.generated_at,
            oldest_required_component_at: oldest,
            newest_component_at: newest,
            maximum_component_age_ms,
            component_time_skew_ms,
            status,
            warnings,
            critical_errors,
        }
    }

    pub fn allows_control(&self) -> bool {
        self.status == SnapshotConsistency::StrongEnoughForControl
    }
}

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

trait SnapshotComponentStatusExt {
    fn freshness_status_blocks_control(&self) -> bool;
}

impl SnapshotComponentStatusExt for super::SnapshotComponentStatus {
    fn freshness_status_blocks_control(&self) -> bool {
        matches!(
            self.status,
            RuntimeComponentFreshness::Missing | RuntimeComponentFreshness::Error
        )
    }
}

pub fn runtime_normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

pub fn runtime_normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

pub fn fill_dedupe_key(exchange: &str, fill_id: &str) -> String {
    format!(
        "{}:{}",
        runtime_normalize_exchange(exchange),
        fill_id.trim()
    )
}

pub fn market_type_authority(_market_type: MarketType) -> RuntimeDataAuthority {
    RuntimeDataAuthority::ExchangeRest
}
