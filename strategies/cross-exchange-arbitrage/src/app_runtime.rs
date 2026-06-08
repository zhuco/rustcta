use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::runtime_contract::{
    build_runtime_contract, CrossArbDashboardSnapshot, CrossArbMarketSnapshotRow,
    CrossArbOpportunityRow, CrossArbRouteHealthRow, CrossArbRuntimeContract,
};
use crate::CrossExchangeArbitrageConfig;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CrossArbRuntimeInput {
    pub market_snapshots: usize,
    pub opportunities: usize,
    pub execution_intents: usize,
    pub open_bundles: usize,
    pub pending_orders: usize,
    pub market_snapshot_rows: Vec<CrossArbMarketSnapshotRow>,
    pub opportunity_rows: Vec<CrossArbOpportunityRow>,
    pub route_health_rows: Vec<CrossArbRouteHealthRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbStorageEvent {
    pub event_kind: &'static str,
    pub count: usize,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbNotification {
    pub notification_kind: &'static str,
    pub message: String,
    pub emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbExecutionIntentSummary {
    pub requested_intents: usize,
    pub submitted_intents: usize,
    pub live_orders_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbRuntimeCycle {
    pub contract: CrossArbRuntimeContract,
    pub dashboard_snapshot: CrossArbDashboardSnapshot,
    pub storage_events: Vec<CrossArbStorageEvent>,
    pub notifications: Vec<CrossArbNotification>,
    pub execution: CrossArbExecutionIntentSummary,
}

#[derive(Debug, Clone)]
pub struct CrossArbAppRuntime {
    config: CrossExchangeArbitrageConfig,
    live_orders_enabled: bool,
    storage_events: Vec<CrossArbStorageEvent>,
    notifications: Vec<CrossArbNotification>,
}

impl CrossArbAppRuntime {
    pub fn new(config: CrossExchangeArbitrageConfig) -> Self {
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
        input: CrossArbRuntimeInput,
        captured_at: DateTime<Utc>,
    ) -> CrossArbRuntimeCycle {
        let mut contract = build_runtime_contract(&self.config, captured_at);
        let submitted_intents = if self.live_orders_enabled {
            input.execution_intents
        } else {
            0
        };
        contract.dashboard_snapshot.live_orders_enabled = self.live_orders_enabled;
        contract.dashboard_snapshot.open_bundles = input.open_bundles;
        contract.dashboard_snapshot.pending_orders = input.pending_orders;
        contract.dashboard_snapshot.market_snapshots = input.market_snapshot_rows;
        contract.dashboard_snapshot.opportunities = input.opportunity_rows;
        contract.dashboard_snapshot.route_health = input.route_health_rows;

        self.record("market_snapshots", input.market_snapshots, captured_at);
        self.record("opportunities", input.opportunities, captured_at);
        self.record("execution_intents", submitted_intents, captured_at);

        if input.opportunities > 0 {
            self.notifications.push(CrossArbNotification {
                notification_kind: "opportunity_summary",
                message: format!(
                    "{} cross-exchange opportunities observed without direct adapter access",
                    input.opportunities
                ),
                emitted_at: captured_at,
            });
        }

        CrossArbRuntimeCycle {
            dashboard_snapshot: contract.dashboard_snapshot.clone(),
            contract,
            storage_events: self.storage_events.clone(),
            notifications: self.notifications.clone(),
            execution: CrossArbExecutionIntentSummary {
                requested_intents: input.execution_intents,
                submitted_intents,
                live_orders_enabled: self.live_orders_enabled,
            },
        }
    }

    pub fn storage_events(&self) -> &[CrossArbStorageEvent] {
        &self.storage_events
    }

    pub fn notifications(&self) -> &[CrossArbNotification] {
        &self.notifications
    }

    fn record(&mut self, event_kind: &'static str, count: usize, recorded_at: DateTime<Utc>) {
        self.storage_events.push(CrossArbStorageEvent {
            event_kind,
            count,
            recorded_at,
        });
    }
}

impl Default for CrossArbAppRuntime {
    fn default() -> Self {
        Self::new(CrossExchangeArbitrageConfig::default())
    }
}
