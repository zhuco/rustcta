use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::{CloseEvaluation, FundingSpreadExpansionMakerConfig, OpenEvaluation};
use crate::runtime_contract::{
    build_runtime_contract, FundingSpreadDashboardSnapshot, FundingSpreadRuntimeContract,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct FundingSpreadRuntimeInput {
    pub open_evaluations: Vec<OpenEvaluation>,
    pub close_evaluations: Vec<CloseEvaluation>,
    pub open_orders_planned: usize,
    pub close_orders_planned: usize,
    pub repair_actions_planned: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingSpreadStorageEvent {
    pub event_kind: &'static str,
    pub count: usize,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingSpreadNotification {
    pub notification_kind: &'static str,
    pub message: String,
    pub emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingSpreadExecutionIntentSummary {
    pub planned_orders: usize,
    pub submitted_orders: usize,
    pub live_orders_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingSpreadRuntimeCycle {
    pub contract: FundingSpreadRuntimeContract,
    pub dashboard_snapshot: FundingSpreadDashboardSnapshot,
    pub storage_events: Vec<FundingSpreadStorageEvent>,
    pub notifications: Vec<FundingSpreadNotification>,
    pub execution: FundingSpreadExecutionIntentSummary,
}

#[derive(Debug, Clone)]
pub struct FundingSpreadAppRuntime {
    config: FundingSpreadExpansionMakerConfig,
    live_orders_enabled: bool,
    storage_events: Vec<FundingSpreadStorageEvent>,
    notifications: Vec<FundingSpreadNotification>,
}

impl FundingSpreadAppRuntime {
    pub fn new(config: FundingSpreadExpansionMakerConfig) -> Self {
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
        mut input: FundingSpreadRuntimeInput,
        captured_at: DateTime<Utc>,
    ) -> FundingSpreadRuntimeCycle {
        let mut contract = build_runtime_contract(&self.config, captured_at);
        let accepted_open = input
            .open_evaluations
            .iter()
            .filter(|evaluation| evaluation.reject_reason.is_none())
            .count();
        let target_closes = input
            .close_evaluations
            .iter()
            .filter(|evaluation| evaluation.should_close)
            .count();
        let planned_orders = input.open_orders_planned + input.close_orders_planned;
        let submitted_orders = if self.live_orders_enabled {
            planned_orders
        } else {
            0
        };
        contract.dashboard_snapshot.live_orders_enabled = self.live_orders_enabled;

        self.record(
            "funding_spread_open_evaluations",
            input.open_evaluations.len(),
            captured_at,
        );
        self.record("funding_spread_open_accepted", accepted_open, captured_at);
        self.record("funding_spread_close_triggers", target_closes, captured_at);
        self.record(
            "funding_spread_repair_actions",
            input.repair_actions_planned,
            captured_at,
        );

        if accepted_open > 0 || target_closes > 0 {
            self.notifications.push(FundingSpreadNotification {
                notification_kind: "funding_spread_cycle_summary",
                message: format!(
                    "accepted_open={}, close_triggers={}, planned_orders={}",
                    accepted_open, target_closes, planned_orders
                ),
                emitted_at: captured_at,
            });
        }

        input.open_evaluations.clear();
        input.close_evaluations.clear();

        FundingSpreadRuntimeCycle {
            dashboard_snapshot: contract.dashboard_snapshot.clone(),
            contract,
            storage_events: self.storage_events.clone(),
            notifications: self.notifications.clone(),
            execution: FundingSpreadExecutionIntentSummary {
                planned_orders,
                submitted_orders,
                live_orders_enabled: self.live_orders_enabled,
            },
        }
    }

    fn record(&mut self, event_kind: &'static str, count: usize, recorded_at: DateTime<Utc>) {
        self.storage_events.push(FundingSpreadStorageEvent {
            event_kind,
            count,
            recorded_at,
        });
    }
}

impl Default for FundingSpreadAppRuntime {
    fn default() -> Self {
        Self::new(FundingSpreadExpansionMakerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_should_not_submit_orders_when_live_disabled() {
        let mut runtime = FundingSpreadAppRuntime::default();
        let cycle = runtime.run_cycle(
            FundingSpreadRuntimeInput {
                open_orders_planned: 2,
                close_orders_planned: 2,
                ..FundingSpreadRuntimeInput::default()
            },
            Utc::now(),
        );
        assert_eq!(cycle.execution.planned_orders, 4);
        assert_eq!(cycle.execution.submitted_orders, 0);
        assert!(!cycle.dashboard_snapshot.live_orders_enabled);
    }
}
