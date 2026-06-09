use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::VecDeque;

use crate::runtime_contract::{
    async_db_queue_state, async_db_writer_contract, build_runtime_contract,
    position_takeover_contract, singleton_process_lock_contract, CrossArbAsyncDbQueueState,
    CrossArbDashboardSnapshot, CrossArbExchangeReadinessRow, CrossArbMarketSnapshotRow,
    CrossArbOpportunityRow, CrossArbRouteHealthRow, CrossArbRuntimeContract,
    DEFAULT_ASYNC_DB_QUEUE_EVENTS,
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
    pub exchange_readiness_rows: Vec<CrossArbExchangeReadinessRow>,
    pub db_writer_ready: bool,
    pub position_takeover_complete: bool,
    pub startup_single_leg_positions_detected: usize,
    pub startup_single_leg_positions_resolved: usize,
    pub singleton_acquired: bool,
    pub price_audit_events: Vec<CrossArbDbPriceAuditEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbStorageEvent {
    pub event_kind: &'static str,
    pub count: usize,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbDbPriceAuditLeg {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub position_side: String,
    pub planned_execution_price: String,
    pub actual_fill_price: Option<String>,
    pub planned_base_quantity: String,
    pub actual_base_quantity: Option<String>,
    pub planned_notional_usdt: String,
    pub actual_notional_usdt: Option<String>,
    pub fee_usdt: Option<String>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbDbPriceAuditEvent {
    pub event_kind: &'static str,
    pub bundle_id: String,
    pub lifecycle: &'static str,
    pub correlation_id: String,
    pub planned_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub non_blocking_enqueue_required: bool,
    pub legs: Vec<CrossArbDbPriceAuditLeg>,
    pub expected_net_pnl_usdt: Option<String>,
    pub actual_pnl_usdt: Option<String>,
    pub failure_reason: Option<String>,
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
    pub startup_readiness_satisfied: bool,
    pub new_opens_allowed: bool,
    pub blocked_by_startup_gate: bool,
    pub blocked_by_loss_guard: bool,
    pub consecutive_loss_closes: u32,
    pub max_consecutive_losses: u32,
    pub stopped_by_loss_guard: bool,
    pub open_legs_concurrent_required: bool,
    pub close_legs_concurrent_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbRuntimeCycle {
    pub contract: CrossArbRuntimeContract,
    pub dashboard_snapshot: CrossArbDashboardSnapshot,
    pub storage_events: Vec<CrossArbStorageEvent>,
    pub notifications: Vec<CrossArbNotification>,
    pub execution: CrossArbExecutionIntentSummary,
    pub queued_price_audit_events: Vec<CrossArbDbPriceAuditEvent>,
    pub async_db_queue: CrossArbAsyncDbQueueState,
}

#[derive(Debug, Clone)]
pub struct CrossArbAppRuntime {
    config: CrossExchangeArbitrageConfig,
    live_orders_enabled: bool,
    storage_events: Vec<CrossArbStorageEvent>,
    notifications: Vec<CrossArbNotification>,
    async_db_queue: VecDeque<CrossArbDbPriceAuditEvent>,
    async_db_queue_capacity: usize,
    dropped_db_events: usize,
    consecutive_loss_closes: u32,
    stopped_by_loss_guard: bool,
}

impl CrossArbAppRuntime {
    pub fn new(config: CrossExchangeArbitrageConfig) -> Self {
        Self {
            config,
            live_orders_enabled: false,
            storage_events: Vec::new(),
            notifications: Vec::new(),
            async_db_queue: VecDeque::new(),
            async_db_queue_capacity: DEFAULT_ASYNC_DB_QUEUE_EVENTS,
            dropped_db_events: 0,
            consecutive_loss_closes: 0,
            stopped_by_loss_guard: false,
        }
    }

    pub fn with_live_orders_enabled(mut self, live_orders_enabled: bool) -> Self {
        self.live_orders_enabled = live_orders_enabled;
        self
    }

    pub fn with_async_db_queue_capacity(mut self, async_db_queue_capacity: usize) -> Self {
        self.async_db_queue_capacity = async_db_queue_capacity.max(1);
        self
    }

    pub fn run_cycle(
        &mut self,
        mut input: CrossArbRuntimeInput,
        captured_at: DateTime<Utc>,
    ) -> CrossArbRuntimeCycle {
        let mut contract = build_runtime_contract(&self.config, captured_at);
        let price_audit_events = std::mem::take(&mut input.price_audit_events);
        self.apply_consecutive_loss_guard(&price_audit_events, captured_at);
        let async_db_queue =
            self.enqueue_price_audit_events(price_audit_events, input.db_writer_ready, captured_at);
        self.apply_runtime_state(&mut contract, &input, async_db_queue.clone());

        let startup_readiness_satisfied = contract.readiness_gate.all_gates_ready();
        let new_opens_allowed = startup_readiness_satisfied && !self.stopped_by_loss_guard;
        let submitted_intents = if self.live_orders_enabled && new_opens_allowed {
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

        if self.live_orders_enabled && !startup_readiness_satisfied && input.execution_intents > 0 {
            self.notifications.push(CrossArbNotification {
                notification_kind: "startup_gate_blocked_execution",
                message:
                    "cross-exchange execution intents blocked until DB writer, position takeover, singleton lock, and exchange readiness gates are complete"
                        .to_string(),
                emitted_at: captured_at,
            });
        }

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
                startup_readiness_satisfied,
                new_opens_allowed,
                blocked_by_startup_gate: !startup_readiness_satisfied,
                blocked_by_loss_guard: self.stopped_by_loss_guard,
                consecutive_loss_closes: self.consecutive_loss_closes,
                max_consecutive_losses: self.config.max_consecutive_losses.max(1),
                stopped_by_loss_guard: self.stopped_by_loss_guard,
                open_legs_concurrent_required: true,
                close_legs_concurrent_required: true,
            },
            queued_price_audit_events: self.async_db_queue.iter().cloned().collect(),
            async_db_queue,
        }
    }

    pub fn storage_events(&self) -> &[CrossArbStorageEvent] {
        &self.storage_events
    }

    pub fn notifications(&self) -> &[CrossArbNotification] {
        &self.notifications
    }

    pub fn async_db_queue_len(&self) -> usize {
        self.async_db_queue.len()
    }

    pub fn dropped_db_events(&self) -> usize {
        self.dropped_db_events
    }

    pub fn consecutive_loss_closes(&self) -> u32 {
        self.consecutive_loss_closes
    }

    pub fn stopped_by_loss_guard(&self) -> bool {
        self.stopped_by_loss_guard
    }

    pub fn drain_async_db_queue_batch(
        &mut self,
        max_events: usize,
    ) -> Vec<CrossArbDbPriceAuditEvent> {
        let mut drained = Vec::new();
        for _ in 0..max_events {
            let Some(event) = self.async_db_queue.pop_front() else {
                break;
            };
            drained.push(event);
        }
        drained
    }

    fn record(&mut self, event_kind: &'static str, count: usize, recorded_at: DateTime<Utc>) {
        self.storage_events.push(CrossArbStorageEvent {
            event_kind,
            count,
            recorded_at,
        });
    }

    fn apply_consecutive_loss_guard(
        &mut self,
        events: &[CrossArbDbPriceAuditEvent],
        recorded_at: DateTime<Utc>,
    ) {
        let max_consecutive_losses = self.config.max_consecutive_losses.max(1);
        for event in events {
            if !event.lifecycle.eq_ignore_ascii_case("close") {
                continue;
            }
            let Some(actual_pnl_usdt) = event.actual_pnl_usdt.as_deref().and_then(parse_pnl) else {
                continue;
            };
            if actual_pnl_usdt < 0.0 {
                self.consecutive_loss_closes = self.consecutive_loss_closes.saturating_add(1);
            } else {
                self.consecutive_loss_closes = 0;
            }

            if self.consecutive_loss_closes >= max_consecutive_losses && !self.stopped_by_loss_guard
            {
                self.stopped_by_loss_guard = true;
                self.record("risk_auto_stop", 1, recorded_at);
                self.notifications.push(CrossArbNotification {
                    notification_kind: "risk_auto_stop",
                    message: format!(
                        "cross-exchange arbitrage stopped after {} consecutive losing closed arbitrages",
                        self.consecutive_loss_closes
                    ),
                    emitted_at: recorded_at,
                });
            }
        }
    }

    fn enqueue_price_audit_events(
        &mut self,
        events: Vec<CrossArbDbPriceAuditEvent>,
        db_writer_ready: bool,
        recorded_at: DateTime<Utc>,
    ) -> CrossArbAsyncDbQueueState {
        let mut enqueued = 0;
        let mut dropped_this_cycle = 0;
        let mut last_enqueue_at = None;

        for mut event in events {
            event.non_blocking_enqueue_required = true;
            if self.async_db_queue.len() < self.async_db_queue_capacity {
                self.async_db_queue.push_back(event);
                enqueued += 1;
                last_enqueue_at = Some(recorded_at);
            } else {
                self.dropped_db_events += 1;
                dropped_this_cycle += 1;
            }
        }

        if enqueued > 0 {
            self.record("async_price_audit_events_enqueued", enqueued, recorded_at);
        }

        if dropped_this_cycle > 0 {
            self.notifications.push(CrossArbNotification {
                notification_kind: "async_db_queue_overflow",
                message: format!(
                    "{dropped_this_cycle} cross-exchange price audit events dropped because the async DB queue is full"
                ),
                emitted_at: recorded_at,
            });
        }

        async_db_queue_state(
            db_writer_ready,
            self.async_db_queue.len(),
            self.async_db_queue_capacity,
            self.dropped_db_events,
            last_enqueue_at,
        )
    }

    fn apply_runtime_state(
        &self,
        contract: &mut CrossArbRuntimeContract,
        input: &CrossArbRuntimeInput,
        async_db_queue: CrossArbAsyncDbQueueState,
    ) {
        if !input.exchange_readiness_rows.is_empty() {
            contract.readiness_gate.exchanges = input.exchange_readiness_rows.clone();
        }

        let startup_single_leg_positions_resolved = input.startup_single_leg_positions_detected
            <= input.startup_single_leg_positions_resolved;

        contract.readiness_gate.db_writer_ready = input.db_writer_ready;
        contract.readiness_gate.position_takeover_complete = input.position_takeover_complete;
        contract
            .readiness_gate
            .startup_single_leg_positions_resolved = startup_single_leg_positions_resolved;
        contract.readiness_gate.singleton_acquired = input.singleton_acquired;
        let ready = contract.readiness_gate.all_gates_ready();
        contract.readiness_gate.ready_to_evaluate_opportunities = ready;
        contract.readiness_gate.ready_to_open_new_positions = ready;

        contract.async_db_writer =
            async_db_writer_contract(input.db_writer_ready, self.async_db_queue_capacity);
        contract.position_takeover = position_takeover_contract(
            input.position_takeover_complete,
            input.startup_single_leg_positions_detected,
            input.startup_single_leg_positions_resolved,
        );
        contract.singleton_process_lock = singleton_process_lock_contract(input.singleton_acquired);

        contract.dashboard_snapshot.readiness_gate = contract.readiness_gate.clone();
        contract.dashboard_snapshot.async_db_writer = contract.async_db_writer.clone();
        contract.dashboard_snapshot.async_db_queue = async_db_queue;
        contract.dashboard_snapshot.position_takeover = contract.position_takeover.clone();
        contract.dashboard_snapshot.singleton_process_lock =
            contract.singleton_process_lock.clone();
        contract.dashboard_snapshot.execution_provider_contract =
            contract.execution_provider_contract.clone();
    }
}

impl Default for CrossArbAppRuntime {
    fn default() -> Self {
        Self::new(CrossExchangeArbitrageConfig::default())
    }
}

fn parse_pnl(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}
