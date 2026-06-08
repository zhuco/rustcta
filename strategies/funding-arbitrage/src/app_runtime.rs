use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::{FundingCoreConfig, FundingScanReport};
use crate::live_plan::{build_live_plan_at, FundingLivePlan};
use crate::runtime_contract::{
    build_runtime_contract, FundingDashboardSnapshot, FundingRuntimeContract,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingStorageEvent {
    pub event_kind: &'static str,
    pub count: usize,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingNotification {
    pub notification_kind: &'static str,
    pub message: String,
    pub emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingExecutionIntentSummary {
    pub planned_entries: usize,
    pub submitted_intents: usize,
    pub live_orders_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingRuntimeCycle {
    pub contract: FundingRuntimeContract,
    pub live_plan: FundingLivePlan,
    pub dashboard_snapshot: FundingDashboardSnapshot,
    pub storage_events: Vec<FundingStorageEvent>,
    pub notifications: Vec<FundingNotification>,
    pub execution: FundingExecutionIntentSummary,
}

#[derive(Debug, Clone)]
pub struct FundingAppRuntime {
    config: FundingCoreConfig,
    live_orders_enabled: bool,
    storage_events: Vec<FundingStorageEvent>,
    notifications: Vec<FundingNotification>,
}

impl FundingAppRuntime {
    pub fn new(config: FundingCoreConfig) -> Self {
        Self {
            config,
            live_orders_enabled: false,
            storage_events: Vec::new(),
            notifications: Vec::new(),
        }
    }

    pub fn with_live_orders_enabled(mut self, live_orders_enabled: bool) -> Self {
        self.live_orders_enabled = live_orders_enabled;
        self
    }

    pub fn run_cycle(
        &mut self,
        report: &FundingScanReport,
        captured_at: DateTime<Utc>,
    ) -> FundingRuntimeCycle {
        let live_plan = build_live_plan_at(&self.config, report, captured_at);
        let mut contract = build_runtime_contract(&self.config, captured_at);
        let submitted_intents = if self.live_orders_enabled {
            live_plan.entries.len()
        } else {
            0
        };
        contract.dashboard_snapshot.live_orders_enabled = self.live_orders_enabled;
        contract.dashboard_snapshot.planned_entries = live_plan.entries.len();
        contract.dashboard_snapshot.skipped_entries = live_plan.skipped.len();

        self.record(
            "funding_scan_selections",
            report.selections.len(),
            captured_at,
        );
        self.record("funding_scan_errors", report.errors.len(), captured_at);
        self.record(
            "funding_live_plan_entries",
            live_plan.entries.len(),
            captured_at,
        );

        if !live_plan.entries.is_empty() {
            self.notifications.push(FundingNotification {
                notification_kind: "live_plan_summary",
                message: format!(
                    "{} funding live entries planned through provider contract",
                    live_plan.entries.len()
                ),
                emitted_at: captured_at,
            });
        }

        let planned_entries = contract.dashboard_snapshot.planned_entries;

        FundingRuntimeCycle {
            dashboard_snapshot: contract.dashboard_snapshot.clone(),
            contract,
            live_plan,
            storage_events: self.storage_events.clone(),
            notifications: self.notifications.clone(),
            execution: FundingExecutionIntentSummary {
                planned_entries,
                submitted_intents,
                live_orders_enabled: self.live_orders_enabled,
            },
        }
    }

    pub fn storage_events(&self) -> &[FundingStorageEvent] {
        &self.storage_events
    }

    pub fn notifications(&self) -> &[FundingNotification] {
        &self.notifications
    }

    fn record(&mut self, event_kind: &'static str, count: usize, recorded_at: DateTime<Utc>) {
        self.storage_events.push(FundingStorageEvent {
            event_kind,
            count,
            recorded_at,
        });
    }
}

impl Default for FundingAppRuntime {
    fn default() -> Self {
        Self::new(FundingCoreConfig::default())
    }
}
