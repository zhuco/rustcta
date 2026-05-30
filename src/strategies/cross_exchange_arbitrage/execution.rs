//! Live execution bridge for cross-exchange arbitrage signals and private fills.

use super::{
    ArbSignal, ArbSignalAction, CrossArbRuntime, CrossArbRuntimeState,
    MakerExecutionStatsReadModel, MarketSnapshot, OpenExecutionStyle, Opportunity, PositionError,
    SimulatedBundleState, SimulatedBundleStatus, StorageSink,
};
use crate::execution::{
    deterministic_client_order_id, normalize_client_order_id, ArbitrageBundle, BundleLeg,
    BundleStatus, CancelAck, CancelCommand, EngineDecision, ExecutionAction, ExecutionEngine,
    ExecutionLedger, ExecutionRequest, FillEvent, FillLiquidity, MakerFill, OrderAck, OrderCommand,
    OrderCommandStatus, OrderIntent, OrderQuery, OrderSide, OrderState, OrderType, PositionSide,
    PrivateEvent, PrivateEventKind, TimeInForce,
};
use crate::market::{
    exchange_symbol_for, round_to_step, CanonicalSymbol, ExchangeId, ExchangeSymbol,
    InstrumentMeta, RoundingMode,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackedCrossArbOrder {
    pub bundle_id: String,
    pub leg: BundleLeg,
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub maker_side: OrderSide,
    pub taker_exchange: ExchangeId,
    pub taker_exchange_symbol: ExchangeSymbol,
    pub max_slippage_pct: Option<f64>,
    #[serde(default)]
    pub trigger_hedge_on_fill: bool,
    #[serde(default)]
    pub maker_entry_price: Option<f64>,
    #[serde(default)]
    pub planned_taker_price: Option<f64>,
    pub mode: crate::market::RuntimeMode,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbOrderIndex {
    by_client_order_id: HashMap<String, TrackedCrossArbOrder>,
    by_exchange_order_id: HashMap<(ExchangeId, String), TrackedCrossArbOrder>,
}

impl CrossArbOrderIndex {
    pub fn record_command(&mut self, command: &OrderCommand, tracked: TrackedCrossArbOrder) {
        self.by_client_order_id
            .insert(normalize_client_order_id(&command.client_order_id), tracked);
    }

    pub fn record_exchange_order_id(
        &mut self,
        exchange: ExchangeId,
        exchange_order_id: impl Into<String>,
        tracked: TrackedCrossArbOrder,
    ) {
        self.by_exchange_order_id
            .insert((exchange, exchange_order_id.into()), tracked);
    }

    pub fn resolve_fill(&self, fill: &FillEvent) -> Option<&TrackedCrossArbOrder> {
        fill.client_order_id
            .as_deref()
            .and_then(|client_order_id| {
                self.by_client_order_id
                    .get(&normalize_client_order_id(client_order_id))
            })
            .or_else(|| {
                fill.exchange_order_id
                    .as_ref()
                    .and_then(|exchange_order_id| {
                        self.by_exchange_order_id
                            .get(&(fill.exchange.clone(), exchange_order_id.clone()))
                    })
            })
    }

    pub fn resolve_order(&self, order: &OrderState) -> Option<&TrackedCrossArbOrder> {
        order
            .client_order_id
            .as_deref()
            .and_then(|client_order_id| {
                self.by_client_order_id
                    .get(&normalize_client_order_id(client_order_id))
            })
            .or_else(|| {
                order
                    .exchange_order_id
                    .as_ref()
                    .and_then(|exchange_order_id| {
                        self.by_exchange_order_id
                            .get(&(order.exchange.clone(), exchange_order_id.clone()))
                    })
            })
    }
}

pub struct CrossArbExecutionCoordinator {
    engine: ExecutionEngine,
    ledger: ExecutionLedger,
    order_index: CrossArbOrderIndex,
    pending_maker_orders: HashMap<String, PendingMakerOrder>,
    maker_stats: HashMap<MakerStatsKey, MakerExecutionStats>,
    private_fill_cumulative_qty: HashMap<String, f64>,
    synthetic_order_fill_credit: HashMap<String, f64>,
    seen_private_trade_ids: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingMakerOrder {
    command: OrderCommand,
    exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MakerStatsKey {
    canonical_symbol: CanonicalSymbol,
    exchange: ExchangeId,
    side: OrderSide,
}

#[derive(Debug, Default, Clone, PartialEq)]
struct MakerExecutionStats {
    consecutive_ttl_cancels: u32,
    total_ttl_cancels: u64,
    total_fills: u64,
    last_event_at: Option<DateTime<Utc>>,
    cooldown_until: Option<DateTime<Utc>>,
}

impl CrossArbExecutionCoordinator {
    pub fn new(engine: ExecutionEngine) -> Self {
        Self {
            engine,
            ledger: ExecutionLedger::default(),
            order_index: CrossArbOrderIndex::default(),
            pending_maker_orders: HashMap::new(),
            maker_stats: HashMap::new(),
            private_fill_cumulative_qty: HashMap::new(),
            synthetic_order_fill_credit: HashMap::new(),
            seen_private_trade_ids: HashSet::new(),
        }
    }

    pub fn ledger(&self) -> &ExecutionLedger {
        &self.ledger
    }

    pub fn order_index(&self) -> &CrossArbOrderIndex {
        &self.order_index
    }

    pub fn tracked_order_for_fill(&self, fill: &FillEvent) -> Option<TrackedCrossArbOrder> {
        self.order_index.resolve_fill(fill).cloned()
    }

    pub fn pending_maker_count(&self) -> usize {
        self.pending_maker_orders.len()
    }

    pub fn maker_execution_stats(
        &self,
        config: &super::CrossExchangeArbitrageConfig,
    ) -> Vec<MakerExecutionStatsReadModel> {
        let mut stats = self
            .maker_stats
            .iter()
            .map(|(key, stats)| MakerExecutionStatsReadModel {
                canonical_symbol: key.canonical_symbol.clone(),
                exchange: key.exchange.clone(),
                side: to_strategy_order_side(key.side),
                consecutive_ttl_cancels: stats.consecutive_ttl_cancels,
                total_ttl_cancels: stats.total_ttl_cancels,
                total_fills: stats.total_fills,
                current_aggressive_ticks: aggressive_ticks_for_cancels(
                    config,
                    stats.consecutive_ttl_cancels,
                ),
                cooldown_until: stats.cooldown_until,
                last_event_at: stats.last_event_at,
            })
            .collect::<Vec<_>>();
        stats.sort_by(|left, right| {
            right
                .consecutive_ttl_cancels
                .cmp(&left.consecutive_ttl_cancels)
                .then_with(|| right.last_event_at.cmp(&left.last_event_at))
        });
        stats
    }

    pub fn prepare_open_request(
        &mut self,
        runtime: &mut CrossArbRuntime,
        signal: &ArbSignal,
    ) -> Result<Option<ExecutionRequest>> {
        if signal.action != ArbSignalAction::Open {
            return Ok(None);
        }
        let decision = runtime.state.risk_state.decision();
        if !decision.allow_new_entries {
            anyhow::bail!(
                "new entries blocked by risk state: mode={:?} paused_new_entries={} close_only={} kill_switch={} needs_private_resync={}",
                decision.mode,
                runtime.state.paused_new_entries,
                runtime.state.close_only,
                runtime.state.kill_switch,
                decision.needs_private_resync
            );
        }
        let opportunity_id = signal
            .opportunity_id
            .as_ref()
            .ok_or_else(|| anyhow!("open signal {} has no opportunity_id", signal.signal_id))?;
        let opportunity = runtime
            .state
            .opportunities
            .iter()
            .find(|item| item.opportunity_id == *opportunity_id)
            .cloned()
            .ok_or_else(|| anyhow!("opportunity {opportunity_id} not found for open signal"))?;
        if runtime.state.config.execution.open_execution_style == OpenExecutionStyle::MakerTaker
            && !runtime
                .state
                .is_private_stream_ready(&opportunity.maker_exchange)
        {
            anyhow::bail!(
                "maker-taker open blocked because maker exchange {} private stream is not ready",
                opportunity.maker_exchange
            );
        }
        let mut request = execution_request_from_signal(&runtime.state, signal, &opportunity)?;
        apply_maker_aggression_to_request(
            &runtime.state.config,
            &opportunity,
            self.aggressive_ticks_for_opportunity(&runtime.state.config, &opportunity),
            &mut request,
        )?;
        register_open_bundle(runtime, signal, &opportunity, &request)?;
        Ok(Some(request))
    }

    pub async fn execute_open_signal(
        &mut self,
        runtime: &mut CrossArbRuntime,
        signal: &ArbSignal,
    ) -> Result<Option<EngineDecision>> {
        let Some(request) = self.prepare_open_request(runtime, signal)? else {
            return Ok(None);
        };
        let decision = self.engine.execute_request(request.clone()).await;
        self.record_decision_with(&decision, |command| {
            tracked_order_for_request(command, &request)
        });
        update_runtime_after_open_decision(runtime, &decision);
        Ok(Some(decision))
    }

    pub fn hedge_candidate_from_private_event(
        &mut self,
        runtime: &mut CrossArbRuntime,
        event: &PrivateEvent,
    ) -> Option<MakerFill> {
        match &event.kind {
            PrivateEventKind::Fill(fill) => {
                let tracked = self.order_index.resolve_fill(fill)?.clone();
                let fill = self.unseen_private_fill_event(
                    fill,
                    &tracked,
                    runtime.state.config.reconciliation.quantity_tolerance,
                )?;
                let event = PrivateEvent::fill(fill, event.received_at);
                maker_fill_from_tracked_private_event(runtime, &tracked, &event)
            }
            PrivateEventKind::Order(order) => {
                let tracked = self.order_index.resolve_order(order)?.clone();
                if !order_state_has_fill(
                    order,
                    runtime.state.config.reconciliation.quantity_tolerance,
                ) {
                    return None;
                }
                let fill = self.unseen_private_order_state_fill(
                    order,
                    &tracked,
                    runtime.state.config.reconciliation.quantity_tolerance,
                    event.received_at,
                )?;
                let event = PrivateEvent::fill(fill, event.received_at);
                maker_fill_from_tracked_private_event(runtime, &tracked, &event)
            }
            _ => None,
        }
    }

    pub fn apply_tracked_private_order_state_event(
        &mut self,
        runtime: &mut CrossArbRuntime,
        event: &PrivateEvent,
    ) {
        let PrivateEventKind::Order(order) = &event.kind else {
            return;
        };
        let Some(tracked) = self.order_index.resolve_order(order).cloned() else {
            return;
        };
        if tracked.trigger_hedge_on_fill && is_hedge_trigger_leg(tracked.leg) {
            return;
        }
        if !order_state_has_fill(
            order,
            runtime.state.config.reconciliation.quantity_tolerance,
        ) {
            return;
        }
        let Some(fill) = self.unseen_private_order_state_fill(
            order,
            &tracked,
            runtime.state.config.reconciliation.quantity_tolerance,
            event.received_at,
        ) else {
            return;
        };
        apply_tracked_private_fill_to_runtime(runtime, &tracked, &fill, event.received_at);
    }

    pub fn apply_tracked_private_fill_event(
        &mut self,
        runtime: &mut CrossArbRuntime,
        event: &PrivateEvent,
    ) {
        let PrivateEventKind::Fill(fill) = &event.kind else {
            return;
        };
        let Some(tracked) = self.order_index.resolve_fill(fill).cloned() else {
            return;
        };
        if tracked.trigger_hedge_on_fill && is_hedge_trigger_leg(tracked.leg) {
            return;
        }
        let Some(fill) = self.unseen_private_fill_event(
            fill,
            &tracked,
            runtime.state.config.reconciliation.quantity_tolerance,
        ) else {
            return;
        };
        apply_tracked_private_fill_to_runtime(runtime, &tracked, &fill, event.received_at);
    }

    pub async fn execute_hedge_for_maker_fill(&mut self, fill: MakerFill) -> EngineDecision {
        self.reduce_pending_maker_after_fill(&fill);
        let decision = self.engine.execute_hedge_for_maker_fill(fill).await;
        self.record_decision_with(&decision, tracked_order);
        decision
    }

    pub async fn verify_submitted_hedge_order(
        &mut self,
        runtime: &mut CrossArbRuntime,
        decision: &EngineDecision,
        now: DateTime<Utc>,
    ) {
        let Some(command) = decision.plan.commands.first() else {
            return;
        };
        if command.reduce_only
            || decision.blocked_reason.is_some()
            || decision.submitted_orders.is_empty()
        {
            return;
        }
        let Some(ack) = decision
            .submitted_orders
            .iter()
            .find(|ack| ack.client_order_id == command.client_order_id)
        else {
            return;
        };
        let query = OrderQuery {
            exchange: command.exchange.clone(),
            exchange_symbol: command.exchange_symbol.clone(),
            client_order_id: Some(command.client_order_id.clone()),
            exchange_order_id: ack.exchange_order_id.clone(),
        };
        let order = match self.engine.router().route_get_order(query).await {
            Ok(order) => order,
            Err(error) => {
                runtime.state.risk_events.push(super::RiskEventReadModel {
                    event_id: format!("hedge-readback-failed-{}", now.timestamp_millis()),
                    canonical_symbol: Some(command.canonical_symbol.clone()),
                    exchange: Some(command.exchange.clone()),
                    reason: super::RejectReason::RouteUnhealthy,
                    message: format!(
                        "hedge order {} submitted but REST readback failed: {error}",
                        command.client_order_id
                    ),
                    created_at: now,
                });
                runtime.state.record_hedge_repair_required(
                    command,
                    "hedge order submitted but REST readback failed",
                    now,
                );
                runtime.persist_hedge_repair_task(
                    &hedge_repair_task_id(&command.bundle_id, command.reduce_only),
                    now,
                );
                runtime.persist_hedge_record(&command.bundle_id, now);
                return;
            }
        };
        runtime.state.ingest_order_state(order.clone());
        if order_state_has_fill(
            &order,
            runtime.state.config.reconciliation.quantity_tolerance,
        ) {
            let hedge_fill_quantity = order.filled_quantity.min(command.quantity).max(0.0);
            let event = PrivateEvent::order(order, now);
            self.apply_tracked_private_order_state_event(runtime, &event);
            if command.quantity - hedge_fill_quantity
                > runtime.state.config.reconciliation.quantity_tolerance
            {
                runtime.state.record_hedge_repair_required(
                    command,
                    format!(
                        "hedge order {} partially filled on readback: filled={} qty={}",
                        command.client_order_id, hedge_fill_quantity, command.quantity
                    ),
                    now,
                );
                runtime.persist_hedge_repair_task(
                    &hedge_repair_task_id(&command.bundle_id, command.reduce_only),
                    now,
                );
                runtime.persist_hedge_record(&command.bundle_id, now);
            }
        } else {
            runtime.state.record_hedge_repair_required(
                command,
                format!(
                    "hedge order {} was accepted but not filled on readback: status={:?} filled={} qty={}",
                    command.client_order_id, order.status, order.filled_quantity, order.quantity
                ),
                now,
            );
            runtime.persist_hedge_repair_task(
                &hedge_repair_task_id(&command.bundle_id, command.reduce_only),
                now,
            );
            runtime.persist_hedge_record(&command.bundle_id, now);
        }
    }

    pub async fn execute_repair_command(
        &mut self,
        mode: crate::market::RuntimeMode,
        command: OrderCommand,
        now: DateTime<Utc>,
    ) -> EngineDecision {
        let plan = crate::execution::ExecutionPlan {
            request_id: format!("repair-{}", command.bundle_id),
            mode,
            commands: vec![command],
            blocked_reason: None,
            requires_reconcile: false,
            created_at: now,
        };
        let decision = self.engine.submit_commands(plan).await;
        self.record_decision_with(&decision, tracked_order);
        decision
    }

    pub async fn cancel_expired_maker_orders(
        &mut self,
        runtime: &mut CrossArbRuntime,
        now: DateTime<Utc>,
    ) -> Vec<CancelAck> {
        let ttl_ms = runtime.state.config.execution.maker_order_ttl_ms.max(1) as i64;
        let expired = self
            .pending_maker_orders
            .iter()
            .filter_map(|(bundle_id, pending)| {
                let command = &pending.command;
                let age_ms = now
                    .signed_duration_since(command.created_at)
                    .num_milliseconds();
                let still_pending = runtime
                    .state
                    .open_bundles
                    .get(bundle_id)
                    .map(|bundle| {
                        matches!(
                            bundle.status,
                            SimulatedBundleStatus::MakerPending
                                | SimulatedBundleStatus::ClosingSimulated
                        )
                    })
                    .unwrap_or(false);
                (still_pending && age_ms >= ttl_ms).then(|| (bundle_id.clone(), pending.clone()))
            })
            .collect::<Vec<_>>();

        let mut acks = Vec::new();
        for (bundle_id, pending) in expired {
            let PendingMakerOrder {
                command,
                exchange_order_id,
            } = pending;
            let cancel = CancelCommand {
                exchange: command.exchange.clone(),
                canonical_symbol: command.canonical_symbol.clone(),
                exchange_symbol: command.exchange_symbol.clone(),
                client_order_id: Some(command.client_order_id.clone()),
                exchange_order_id: exchange_order_id.clone(),
                reason: Some("maker TTL expired".to_string()),
                requested_at: now,
            };
            match self.engine.router().route_cancel(cancel).await {
                Ok(ack) => {
                    runtime
                        .state
                        .ingest_order_state(cancelled_order_state_from_ack(&command, &ack, now));
                    self.pending_maker_orders.remove(&bundle_id);
                    if command.reduce_only {
                        let release_result = match command.position_side {
                            PositionSide::Long | PositionSide::Short => {
                                runtime.state.position_manager.release_bundle_close_leg_qty(
                                    &bundle_id,
                                    command.position_side,
                                    command.quantity,
                                    now,
                                )
                            }
                            PositionSide::Net => runtime
                                .state
                                .position_manager
                                .release_bundle_close_qty(&bundle_id, command.quantity, now),
                        };
                        let _ = release_result;
                        if let Some(bundle) = runtime.state.open_bundles.get_mut(&bundle_id) {
                            bundle.status = SimulatedBundleStatus::OpenSimulated;
                            bundle.updated_at = now;
                        }
                        let _ = runtime.state.position_manager.mark_bundle_status(
                            &bundle_id,
                            BundleStatus::OpenSimulated,
                            now,
                        );
                    } else if should_drop_unfilled_open_bundle(runtime, &bundle_id) {
                        runtime.state.open_bundles.remove(&bundle_id);
                        runtime.state.position_manager.remove_bundle(&bundle_id);
                    } else {
                        apply_open_maker_cancel_status(runtime, &bundle_id, now);
                    }
                    self.record_maker_ttl_cancel(&runtime.state.config, &command, now);
                    if let Some(stats) = self
                        .maker_stats
                        .get(&maker_stats_key_from_command(&command))
                    {
                        log::info!(
                            "cross_arb maker TTL cancelled exchange={} symbol={} side={:?} client_order_id={} exchange_order_id={} qty={} ttl_ms={} consecutive_cancels={} total_cancels={} aggressive_ticks={} cooldown_until={}",
                            command.exchange,
                            command.canonical_symbol,
                            command.side,
                            command.client_order_id,
                            exchange_order_id.as_deref().unwrap_or("-"),
                            command.quantity,
                            ttl_ms,
                            stats.consecutive_ttl_cancels,
                            stats.total_ttl_cancels,
                            aggressive_ticks_for_cancels(
                                &runtime.state.config,
                                stats.consecutive_ttl_cancels,
                            ),
                            stats
                                .cooldown_until
                                .map(|time| time.to_rfc3339())
                                .unwrap_or_else(|| "-".to_string())
                        );
                    }
                    acks.push(ack);
                }
                Err(error) => {
                    let error_message = error.to_string();
                    if is_unknown_order_cancel_error(&error_message) {
                        if let Some(fill) = self
                            .recover_terminal_maker_fill_after_cancel_error(
                                runtime,
                                &command,
                                exchange_order_id.clone(),
                                &error_message,
                                now,
                            )
                            .await
                        {
                            let hedge_decision = self.execute_hedge_for_maker_fill(fill).await;
                            self.pending_maker_orders.remove(&bundle_id);
                            update_runtime_after_hedge_decision(runtime, &hedge_decision);
                            continue;
                        } else {
                            self.pending_maker_orders.remove(&bundle_id);
                            if command.reduce_only {
                                let release_result = match command.position_side {
                                    PositionSide::Long | PositionSide::Short => {
                                        runtime.state.position_manager.release_bundle_close_leg_qty(
                                            &bundle_id,
                                            command.position_side,
                                            command.quantity,
                                            now,
                                        )
                                    }
                                    PositionSide::Net => {
                                        runtime.state.position_manager.release_bundle_close_qty(
                                            &bundle_id,
                                            command.quantity,
                                            now,
                                        )
                                    }
                                };
                                let _ = release_result;
                                if let Some(bundle) = runtime.state.open_bundles.get_mut(&bundle_id)
                                {
                                    bundle.status = SimulatedBundleStatus::OpenSimulated;
                                    bundle.updated_at = now;
                                }
                                let _ = runtime.state.position_manager.mark_bundle_status(
                                    &bundle_id,
                                    BundleStatus::OpenSimulated,
                                    now,
                                );
                            } else if should_drop_unfilled_open_bundle(runtime, &bundle_id) {
                                runtime.state.open_bundles.remove(&bundle_id);
                                runtime.state.position_manager.remove_bundle(&bundle_id);
                            } else {
                                apply_open_maker_cancel_status(runtime, &bundle_id, now);
                            }
                            let ack = CancelAck {
                                exchange: command.exchange.clone(),
                                client_order_id: Some(command.client_order_id.clone()),
                                exchange_order_id: None,
                                accepted: true,
                                status: OrderCommandStatus::Cancelled,
                                message: Some(format!(
                                    "maker TTL cancel treated as terminal after REST readback found no fill: {error_message}"
                                )),
                                acknowledged_at: now,
                            };
                            runtime
                                .state
                                .ingest_order_state(cancelled_order_state_from_ack(
                                    &command, &ack, now,
                                ));
                            self.record_maker_ttl_cancel(&runtime.state.config, &command, now);
                            if let Some(stats) = self
                                .maker_stats
                                .get(&maker_stats_key_from_command(&command))
                            {
                                log::info!(
                                    "cross_arb maker TTL cancelled exchange={} symbol={} side={:?} client_order_id={} exchange_order_id={} qty={} ttl_ms={} consecutive_cancels={} total_cancels={} aggressive_ticks={} cooldown_until={} terminal_readback=true",
                                    command.exchange,
                                    command.canonical_symbol,
                                    command.side,
                                    command.client_order_id,
                                    exchange_order_id.as_deref().unwrap_or("-"),
                                    command.quantity,
                                    ttl_ms,
                                    stats.consecutive_ttl_cancels,
                                    stats.total_ttl_cancels,
                                    aggressive_ticks_for_cancels(
                                        &runtime.state.config,
                                        stats.consecutive_ttl_cancels,
                                    ),
                                    stats
                                        .cooldown_until
                                        .map(|time| time.to_rfc3339())
                                        .unwrap_or_else(|| "-".to_string())
                                );
                            }
                            acks.push(ack);
                        }
                    } else {
                        runtime.state.risk_events.push(super::RiskEventReadModel {
                            event_id: format!("maker-ttl-cancel-failed-{}", now.timestamp_millis()),
                            canonical_symbol: Some(command.canonical_symbol.clone()),
                            exchange: Some(command.exchange.clone()),
                            reason: super::RejectReason::RouteUnhealthy,
                            message: format!(
                                "maker order {} TTL cancel failed: {error_message}",
                                command.client_order_id
                            ),
                            created_at: now,
                        });
                    }
                }
            }
        }
        acks
    }

    fn reduce_pending_maker_after_fill(&mut self, fill: &MakerFill) {
        let Some(pending) = self.pending_maker_orders.get_mut(&fill.bundle_id) else {
            return;
        };
        let stats = self
            .maker_stats
            .entry(maker_stats_key_from_command(&pending.command))
            .or_default();
        stats.total_fills = stats.total_fills.saturating_add(1);
        stats.consecutive_ttl_cancels = 0;
        stats.cooldown_until = None;
        stats.last_event_at = Some(fill.filled_at);
        let command = &mut pending.command;
        command.quantity = (command.quantity - fill.filled_quantity).max(0.0);
        if command.quantity <= 1e-12 {
            self.pending_maker_orders.remove(&fill.bundle_id);
        }
    }

    fn unseen_private_fill_event(
        &mut self,
        fill: &FillEvent,
        tracked: &TrackedCrossArbOrder,
        tolerance: f64,
    ) -> Option<FillEvent> {
        let trade_key = private_trade_key(fill);
        if self.seen_private_trade_ids.contains(&trade_key) {
            return None;
        }
        self.seen_private_trade_ids.insert(trade_key);

        let fill_key = tracked_order_fill_key(tracked);
        let credit = self
            .synthetic_order_fill_credit
            .get(&fill_key)
            .copied()
            .unwrap_or_default();
        if credit + tolerance >= fill.quantity {
            self.synthetic_order_fill_credit
                .insert(fill_key.clone(), (credit - fill.quantity).max(0.0));
            return None;
        }

        let quantity = fill.quantity - credit;
        if quantity <= tolerance {
            return None;
        }
        self.synthetic_order_fill_credit
            .insert(fill_key.clone(), 0.0);
        let previous_cumulative = self
            .private_fill_cumulative_qty
            .get(&fill_key)
            .copied()
            .unwrap_or_default();
        self.private_fill_cumulative_qty
            .insert(fill_key, previous_cumulative + quantity);

        let mut delta_fill = fill.clone();
        delta_fill.quantity = quantity;
        delta_fill.quote_quantity = if fill.price > 0.0 {
            fill.price * quantity
        } else if fill.quantity > 0.0 {
            fill.quote_quantity * quantity / fill.quantity
        } else {
            0.0
        };
        delta_fill.fee = fill
            .fee
            .map(|fee| fee * quantity / fill.quantity.max(quantity));
        delta_fill.realized_pnl = fill
            .realized_pnl
            .map(|pnl| pnl * quantity / fill.quantity.max(quantity));
        Some(delta_fill)
    }

    fn unseen_private_order_state_fill(
        &mut self,
        order: &OrderState,
        tracked: &TrackedCrossArbOrder,
        tolerance: f64,
        received_at: DateTime<Utc>,
    ) -> Option<FillEvent> {
        let filled_quantity = if order.quantity > 0.0 {
            order.filled_quantity.min(order.quantity).max(0.0)
        } else {
            order.filled_quantity.max(0.0)
        };
        if filled_quantity <= tolerance {
            return None;
        }

        let fill_key = tracked_order_fill_key(tracked);
        let previous = self
            .private_fill_cumulative_qty
            .get(&fill_key)
            .copied()
            .unwrap_or_default();
        let delta = (filled_quantity - previous).min(order.quantity).max(0.0);
        if delta <= tolerance {
            return None;
        }

        self.private_fill_cumulative_qty
            .insert(fill_key.clone(), filled_quantity);
        let credit = self
            .synthetic_order_fill_credit
            .get(&fill_key)
            .copied()
            .unwrap_or_default();
        self.synthetic_order_fill_credit
            .insert(fill_key, credit + delta);
        let fill = fill_from_tracked_order_state_delta(
            tracked,
            order,
            delta,
            filled_quantity,
            received_at,
        );
        log::info!(
            "cross_arb private order-state fill applied exchange={} symbol={} bundle={} qty={} cumulative={} status={:?} trigger_hedge={}",
            order.exchange,
            order.canonical_symbol,
            tracked.bundle_id,
            delta,
            filled_quantity,
            order.status,
            tracked.trigger_hedge_on_fill && is_hedge_trigger_leg(tracked.leg)
        );
        Some(fill)
    }

    fn record_maker_ttl_cancel(
        &mut self,
        config: &super::CrossExchangeArbitrageConfig,
        command: &OrderCommand,
        now: DateTime<Utc>,
    ) {
        if !is_pending_maker_command(command) {
            return;
        }
        let stats = self
            .maker_stats
            .entry(maker_stats_key_from_command(command))
            .or_default();
        stats.total_ttl_cancels = stats.total_ttl_cancels.saturating_add(1);
        stats.consecutive_ttl_cancels = stats.consecutive_ttl_cancels.saturating_add(1);
        let cooldown_after = config.execution.maker_cooldown_after_cancels.max(1);
        if stats.consecutive_ttl_cancels >= cooldown_after
            && stats.consecutive_ttl_cancels % cooldown_after == 0
        {
            stats.cooldown_until = Some(
                now + chrono::Duration::milliseconds(config.execution.maker_cooldown_ms as i64),
            );
        }
        stats.last_event_at = Some(now);
    }

    pub fn maker_cooldown_until_for_opportunity(
        &self,
        opportunity: &Opportunity,
    ) -> Option<DateTime<Utc>> {
        self.maker_stats
            .get(&maker_stats_key_from_opportunity(opportunity))?
            .cooldown_until
    }

    fn aggressive_ticks_for_opportunity(
        &self,
        config: &super::CrossExchangeArbitrageConfig,
        opportunity: &Opportunity,
    ) -> u32 {
        let key = maker_stats_key_from_opportunity(opportunity);
        let cancels = self
            .maker_stats
            .get(&key)
            .map(|stats| stats.consecutive_ttl_cancels)
            .unwrap_or(0);
        aggressive_ticks_for_cancels(config, cancels)
    }

    async fn recover_terminal_maker_fill_after_cancel_error(
        &mut self,
        runtime: &mut CrossArbRuntime,
        command: &OrderCommand,
        exchange_order_id: Option<String>,
        cancel_error: &str,
        now: DateTime<Utc>,
    ) -> Option<MakerFill> {
        let query = OrderQuery {
            exchange: command.exchange.clone(),
            exchange_symbol: command.exchange_symbol.clone(),
            client_order_id: Some(command.client_order_id.clone()),
            exchange_order_id,
        };
        let order = match self.engine.router().route_get_order(query).await {
            Ok(order) => order,
            Err(error) => {
                runtime.state.risk_events.push(super::RiskEventReadModel {
                    event_id: format!("maker-ttl-readback-failed-{}", now.timestamp_millis()),
                    canonical_symbol: Some(command.canonical_symbol.clone()),
                    exchange: Some(command.exchange.clone()),
                    reason: super::RejectReason::RouteUnhealthy,
                    message: format!(
                        "maker order {} TTL cancel returned terminal error, REST readback failed: cancel_error={cancel_error}; readback_error={error}",
                        command.client_order_id
                    ),
                    created_at: now,
                });
                return None;
            }
        };
        runtime.state.ingest_order_state(order.clone());
        if order.filled_quantity <= runtime.state.config.reconciliation.quantity_tolerance
            || !matches!(
                order.status,
                OrderCommandStatus::Filled | OrderCommandStatus::PartiallyFilled
            )
        {
            return None;
        }

        let fill = fill_from_order_state(command, &order, now);
        let tracked = self
            .order_index
            .resolve_fill(&fill)
            .cloned()
            .unwrap_or_else(|| tracked_order(command));
        let event = PrivateEvent::fill(fill, now);
        let hedge = maker_fill_from_tracked_private_event(runtime, &tracked, &event);
        if hedge.is_some() {
            runtime.state.risk_events.push(super::RiskEventReadModel {
                event_id: format!("maker-ttl-readback-filled-{}", now.timestamp_millis()),
                canonical_symbol: Some(command.canonical_symbol.clone()),
                exchange: Some(command.exchange.clone()),
                reason: super::RejectReason::RouteUnhealthy,
                message: format!(
                    "maker order {} was filled before TTL cancel completed; recovered by REST order readback and submitting hedge",
                    command.client_order_id
                ),
                created_at: now,
            });
        }
        hedge
    }

    pub async fn execute_close_bundle(
        &mut self,
        runtime: &mut CrossArbRuntime,
        candidate: &LiveCloseCandidate,
    ) -> Result<EngineDecision> {
        let previous_bundle_status = runtime
            .state
            .open_bundles
            .get(&candidate.bundle_id)
            .map(|bundle| bundle.status);
        let previous_position_status = runtime
            .state
            .position_manager
            .bundle(&candidate.bundle_id)
            .map(|bundle| bundle.status);
        if runtime.state.config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
            runtime.state.position_manager.reserve_bundle_close_qty(
                &candidate.bundle_id,
                candidate.quantity,
                candidate.generated_at,
            )?;
        } else {
            runtime
                .state
                .position_manager
                .reserve_bundle_close_leg_qty(
                    &candidate.bundle_id,
                    candidate.maker_close_position_side,
                    candidate.quantity,
                    candidate.generated_at,
                )?;
        }
        if let Some(bundle) = runtime.state.open_bundles.get_mut(&candidate.bundle_id) {
            bundle.status = SimulatedBundleStatus::ClosingSimulated;
            bundle.updated_at = candidate.generated_at;
        }

        let plan = crate::execution::ExecutionPlan {
            request_id: format!("close-{}", candidate.bundle_id),
            mode: runtime.state.config.mode,
            commands: close_commands_from_candidate(runtime, candidate),
            blocked_reason: None,
            requires_reconcile: false,
            created_at: candidate.generated_at,
        };
        let decision = self.engine.submit_commands(plan).await;
        self.record_decision_with(&decision, |command| {
            tracked_order_for_close_candidate(runtime.state.config.mode, command, candidate)
        });
        if let Some(reason) = &decision.blocked_reason {
            runtime.state.risk_events.push(super::RiskEventReadModel {
                event_id: format!(
                    "close-decision-{}",
                    decision.plan.created_at.timestamp_millis()
                ),
                canonical_symbol: Some(candidate.canonical_symbol.clone()),
                exchange: None,
                reason: super::RejectReason::RouteUnhealthy,
                message: reason.clone(),
                created_at: decision.plan.created_at,
            });
            if decision.submitted_orders.is_empty() {
                if runtime.state.config.execution.open_execution_style
                    == OpenExecutionStyle::DualTaker
                {
                    let _ = runtime.state.position_manager.release_bundle_close_qty(
                        &candidate.bundle_id,
                        candidate.quantity,
                        decision.plan.created_at,
                    );
                } else {
                    let _ = runtime.state.position_manager.release_bundle_close_leg_qty(
                        &candidate.bundle_id,
                        candidate.maker_close_position_side,
                        candidate.quantity,
                        decision.plan.created_at,
                    );
                }
                if let Some(bundle) = runtime.state.open_bundles.get_mut(&candidate.bundle_id) {
                    bundle.status =
                        previous_bundle_status.unwrap_or(SimulatedBundleStatus::OrphanLeg);
                    bundle.updated_at = decision.plan.created_at;
                }
                if let Some(status) = previous_position_status {
                    let _ = runtime.state.position_manager.mark_bundle_status(
                        &candidate.bundle_id,
                        status,
                        decision.plan.created_at,
                    );
                }
            } else {
                if let Some(bundle) = runtime.state.open_bundles.get_mut(&candidate.bundle_id) {
                    bundle.status = SimulatedBundleStatus::OrphanLeg;
                    bundle.updated_at = decision.plan.created_at;
                }
                let _ = runtime.state.position_manager.mark_bundle_status(
                    &candidate.bundle_id,
                    BundleStatus::ReconcileRequired,
                    decision.plan.created_at,
                );
            }
        }
        Ok(decision)
    }

    fn record_decision_with(
        &mut self,
        decision: &EngineDecision,
        tracker: impl Fn(&OrderCommand) -> TrackedCrossArbOrder,
    ) {
        for command in &decision.plan.commands {
            self.ledger
                .record_order(command.clone(), decision.plan.created_at);
            let command_was_submitted = decision
                .submitted_orders
                .iter()
                .any(|ack| ack.client_order_id == command.client_order_id);
            let should_track = !decision.plan.mode.allows_live_orders()
                || decision.blocked_reason.is_none()
                || command_was_submitted;
            if !should_track {
                continue;
            }
            self.order_index.record_command(command, tracker(command));
            if is_pending_maker_command(command) {
                self.pending_maker_orders.insert(
                    command.bundle_id.clone(),
                    PendingMakerOrder {
                        command: command.clone(),
                        exchange_order_id: None,
                    },
                );
            }
        }
        for ack in &decision.submitted_orders {
            if let Some(exchange_order_id) = &ack.exchange_order_id {
                if let Some(command) = decision
                    .plan
                    .commands
                    .iter()
                    .find(|command| command.client_order_id == ack.client_order_id)
                {
                    self.order_index.record_exchange_order_id(
                        ack.exchange.clone(),
                        exchange_order_id.clone(),
                        tracker(command),
                    );
                    if let Some(pending) = self.pending_maker_orders.get_mut(&command.bundle_id) {
                        if pending.command.client_order_id == ack.client_order_id {
                            pending.exchange_order_id = Some(exchange_order_id.clone());
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveCloseCandidate {
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub long_exchange_symbol: ExchangeSymbol,
    pub short_exchange_symbol: ExchangeSymbol,
    pub quantity: f64,
    pub target_notional_usdt: f64,
    pub gross_spread_pnl_usdt: f64,
    pub realized_funding_pnl_usdt: f64,
    pub open_fee_paid_usdt: f64,
    pub close_fee_est_usdt: f64,
    pub close_spread_pct: f64,
    pub close_profit_pct: f64,
    pub maker_close_exchange: ExchangeId,
    pub maker_close_exchange_symbol: ExchangeSymbol,
    pub maker_close_side: OrderSide,
    pub maker_close_position_side: PositionSide,
    pub maker_close_price: f64,
    pub maker_close_book_spread_pct: f64,
    pub taker_close_exchange: ExchangeId,
    pub taker_close_exchange_symbol: ExchangeSymbol,
    pub taker_close_side: OrderSide,
    pub taker_close_position_side: PositionSide,
    pub generated_at: DateTime<Utc>,
}

pub fn live_close_metrics_for_bundle(
    runtime: &CrossArbRuntime,
    bundle_id: &str,
    snapshots: &[MarketSnapshot],
    now: DateTime<Utc>,
) -> Option<LiveCloseCandidate> {
    let simulated = runtime.state.open_bundles.get(bundle_id)?;
    if !matches!(
        simulated.status,
        SimulatedBundleStatus::OpenSimulated | SimulatedBundleStatus::OrphanLeg
    ) {
        return None;
    }
    let position = runtime.state.position_manager.bundle(bundle_id)?;
    let quantity_tolerance = runtime.state.config.reconciliation.quantity_tolerance;
    let quantity = position.closeable_qty(quantity_tolerance);
    if quantity <= 0.0 {
        return None;
    }

    let long_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.book.exchange == position.long_leg.exchange)?;
    let short_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.book.exchange == position.short_leg.exchange)?;
    let long_bid = long_snapshot.book.best_bid()?.price;
    let long_ask = long_snapshot.book.best_ask()?.price;
    let short_bid = short_snapshot.book.best_bid()?.price;
    let short_ask = short_snapshot.book.best_ask()?.price;
    let long_entry = position.long_leg.avg_entry_price?;
    let short_entry = position.short_leg.avg_entry_price?;
    if long_entry <= 0.0 || short_entry <= 0.0 {
        return None;
    }
    let gross_spread_pnl_usdt = position.gross_spread_pnl_at(long_bid, short_ask);
    let close_spread_pct = if short_ask > 0.0 {
        short_ask / long_bid.max(f64::EPSILON) - 1.0
    } else {
        0.0
    };
    let fee_model = super::FeeModel::from_config(&runtime.state.config.fees);
    let target_notional_usdt = (position.target_notional_usdt * quantity
        / position.long_leg.filled_qty.max(quantity))
    .max(1.0);
    let close_fee_est = fee_model.fee_amount(
        &position.long_leg.exchange,
        close_fee_role_for_leg(
            &runtime.state.config,
            book_spread_pct(&long_snapshot.book),
            book_spread_pct(&short_snapshot.book),
            PositionSide::Long,
        ),
        target_notional_usdt,
    ) + fee_model.fee_amount(
        &position.short_leg.exchange,
        close_fee_role_for_leg(
            &runtime.state.config,
            book_spread_pct(&long_snapshot.book),
            book_spread_pct(&short_snapshot.book),
            PositionSide::Short,
        ),
        target_notional_usdt,
    );
    let maker_close = close_maker_selection(
        &runtime.state.config,
        long_snapshot,
        short_snapshot,
        long_ask,
        short_bid,
    );
    let denominator = position.target_notional_usdt.max(1.0);
    let close_profit_pct = (gross_spread_pnl_usdt + position.realized_funding_pnl()
        - position.open_fee_paid()
        - close_fee_est
        - runtime.state.config.risk.taker_slippage_buffer * target_notional_usdt
        - runtime.state.config.risk.safety_buffer * target_notional_usdt)
        / denominator;
    Some(LiveCloseCandidate {
        bundle_id: bundle_id.to_string(),
        canonical_symbol: position.canonical_symbol.clone(),
        long_exchange: position.long_leg.exchange.clone(),
        short_exchange: position.short_leg.exchange.clone(),
        long_exchange_symbol: position.long_leg.exchange_symbol.clone(),
        short_exchange_symbol: position.short_leg.exchange_symbol.clone(),
        quantity,
        target_notional_usdt,
        gross_spread_pnl_usdt,
        realized_funding_pnl_usdt: position.realized_funding_pnl(),
        open_fee_paid_usdt: position.open_fee_paid(),
        close_fee_est_usdt: close_fee_est,
        close_spread_pct,
        close_profit_pct,
        maker_close_exchange: maker_close.maker_exchange,
        maker_close_exchange_symbol: maker_close.maker_exchange_symbol,
        maker_close_side: maker_close.maker_side,
        maker_close_position_side: maker_close.maker_position_side,
        maker_close_price: maker_close.maker_price,
        maker_close_book_spread_pct: maker_close.maker_book_spread_pct,
        taker_close_exchange: maker_close.taker_exchange,
        taker_close_exchange_symbol: maker_close.taker_exchange_symbol,
        taker_close_side: maker_close.taker_side,
        taker_close_position_side: maker_close.taker_position_side,
        generated_at: now,
    })
}

pub fn live_close_candidate_for_bundle(
    runtime: &CrossArbRuntime,
    bundle_id: &str,
    snapshots: &[MarketSnapshot],
    now: DateTime<Utc>,
) -> Option<LiveCloseCandidate> {
    let candidate = live_close_metrics_for_bundle(runtime, bundle_id, snapshots, now)?;
    (candidate.close_profit_pct >= runtime.state.config.thresholds.lock_profit_dual_taker_pct)
        .then_some(candidate)
}

#[derive(Debug, Clone)]
struct CloseMakerSelection {
    maker_exchange: ExchangeId,
    maker_exchange_symbol: ExchangeSymbol,
    maker_side: OrderSide,
    maker_position_side: PositionSide,
    maker_price: f64,
    maker_book_spread_pct: f64,
    taker_exchange: ExchangeId,
    taker_exchange_symbol: ExchangeSymbol,
    taker_side: OrderSide,
    taker_position_side: PositionSide,
}

fn close_maker_selection(
    config: &super::CrossExchangeArbitrageConfig,
    long_snapshot: &MarketSnapshot,
    short_snapshot: &MarketSnapshot,
    long_ask: f64,
    short_bid: f64,
) -> CloseMakerSelection {
    let long_spread = book_spread_pct(&long_snapshot.book);
    let short_spread = book_spread_pct(&short_snapshot.book);
    let prefer_close_long = long_spread >= short_spread;
    if prefer_close_long {
        CloseMakerSelection {
            maker_exchange: long_snapshot.book.exchange.clone(),
            maker_exchange_symbol: long_snapshot.book.exchange_symbol.clone(),
            maker_side: OrderSide::Sell,
            maker_position_side: PositionSide::Long,
            maker_price: close_maker_price(
                long_ask,
                long_snapshot.instrument.as_ref(),
                OrderSide::Sell,
                config.execution.maker_price_offset_ticks,
            ),
            maker_book_spread_pct: long_spread,
            taker_exchange: short_snapshot.book.exchange.clone(),
            taker_exchange_symbol: short_snapshot.book.exchange_symbol.clone(),
            taker_side: OrderSide::Buy,
            taker_position_side: PositionSide::Short,
        }
    } else {
        CloseMakerSelection {
            maker_exchange: short_snapshot.book.exchange.clone(),
            maker_exchange_symbol: short_snapshot.book.exchange_symbol.clone(),
            maker_side: OrderSide::Buy,
            maker_position_side: PositionSide::Short,
            maker_price: close_maker_price(
                short_bid,
                short_snapshot.instrument.as_ref(),
                OrderSide::Buy,
                config.execution.maker_price_offset_ticks,
            ),
            maker_book_spread_pct: short_spread,
            taker_exchange: long_snapshot.book.exchange.clone(),
            taker_exchange_symbol: long_snapshot.book.exchange_symbol.clone(),
            taker_side: OrderSide::Sell,
            taker_position_side: PositionSide::Long,
        }
    }
}

fn close_fee_role_for_leg(
    config: &super::CrossExchangeArbitrageConfig,
    long_book_spread_pct: f64,
    short_book_spread_pct: f64,
    leg: PositionSide,
) -> super::FeeRole {
    if config.execution.open_execution_style == OpenExecutionStyle::DualTaker {
        return super::FeeRole::Taker;
    }
    let maker_leg = if long_book_spread_pct >= short_book_spread_pct {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    if leg == maker_leg {
        super::FeeRole::Maker
    } else {
        super::FeeRole::Taker
    }
}

fn close_maker_price(
    best_price: f64,
    instrument: Option<&InstrumentMeta>,
    side: OrderSide,
    offset_ticks: u32,
) -> f64 {
    if offset_ticks == 0 {
        return best_price;
    }
    let Some(instrument) = instrument else {
        return best_price;
    };
    let tick = instrument.price_tick;
    if !tick.is_finite() || tick <= 0.0 {
        return best_price;
    }
    let base_price = match side {
        OrderSide::Sell => instrument.quantize_price(best_price, RoundingMode::Ceil),
        OrderSide::Buy => instrument.quantize_price(best_price, RoundingMode::Floor),
    };
    let offset = tick * f64::from(offset_ticks);
    let adjusted = match side {
        OrderSide::Sell => base_price + offset,
        OrderSide::Buy => base_price - offset,
    };
    if adjusted <= 0.0 || !adjusted.is_finite() {
        return best_price;
    }
    instrument
        .quantize_price(adjusted, RoundingMode::Nearest)
        .max(tick)
}

fn book_spread_pct(book: &crate::market::OrderBook5) -> f64 {
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) if bid.price > 0.0 && ask.price >= bid.price => {
            ask.price / bid.price - 1.0
        }
        _ => 0.0,
    }
}

fn close_commands_from_candidate(
    runtime: &CrossArbRuntime,
    candidate: &LiveCloseCandidate,
) -> Vec<OrderCommand> {
    if runtime.state.config.execution.open_execution_style != OpenExecutionStyle::DualTaker {
        return vec![close_maker_command_from_candidate(runtime, candidate)];
    }

    let mode = runtime.state.config.mode;
    vec![
        OrderCommand::new(
            mode,
            candidate.bundle_id.clone(),
            BundleLeg::CloseLong,
            1,
            candidate.long_exchange.clone(),
            candidate.canonical_symbol.clone(),
            candidate.long_exchange_symbol.clone(),
            OrderIntent::CloseLongTaker,
            OrderSide::Sell,
            PositionSide::Long,
            OrderType::Market,
            candidate.quantity,
            None,
            TimeInForce::Ioc,
            false,
            true,
            Some(runtime.state.config.execution.taker_ioc_slippage_limit_pct),
            candidate.generated_at,
        ),
        OrderCommand::new(
            mode,
            candidate.bundle_id.clone(),
            BundleLeg::CloseShort,
            1,
            candidate.short_exchange.clone(),
            candidate.canonical_symbol.clone(),
            candidate.short_exchange_symbol.clone(),
            OrderIntent::CloseShortTaker,
            OrderSide::Buy,
            PositionSide::Short,
            OrderType::Market,
            candidate.quantity,
            None,
            TimeInForce::Ioc,
            false,
            true,
            Some(runtime.state.config.execution.taker_ioc_slippage_limit_pct),
            candidate.generated_at,
        ),
    ]
}

fn close_maker_command_from_candidate(
    runtime: &CrossArbRuntime,
    candidate: &LiveCloseCandidate,
) -> OrderCommand {
    let (leg, intent) = match candidate.maker_close_position_side {
        PositionSide::Long => (BundleLeg::CloseLong, OrderIntent::CloseLongMaker),
        PositionSide::Short => (BundleLeg::CloseShort, OrderIntent::CloseShortMaker),
        PositionSide::Net => (BundleLeg::Maker, OrderIntent::CloseLongMaker),
    };
    OrderCommand::new(
        runtime.state.config.mode,
        candidate.bundle_id.clone(),
        leg,
        1,
        candidate.maker_close_exchange.clone(),
        candidate.canonical_symbol.clone(),
        candidate.maker_close_exchange_symbol.clone(),
        intent,
        candidate.maker_close_side,
        candidate.maker_close_position_side,
        OrderType::Limit,
        candidate.quantity,
        Some(candidate.maker_close_price),
        TimeInForce::PostOnly,
        true,
        true,
        Some(runtime.state.config.execution.taker_ioc_slippage_limit_pct),
        candidate.generated_at,
    )
}

pub fn maker_fill_from_tracked_private_event(
    runtime: &mut CrossArbRuntime,
    tracked: &TrackedCrossArbOrder,
    event: &PrivateEvent,
) -> Option<MakerFill> {
    let PrivateEventKind::Fill(fill) = &event.kind else {
        return None;
    };
    let normalized_fill = fill_for_tracked_leg(fill, tracked.leg);
    if !normalized_fill.reduce_only.unwrap_or(false) && matches!(tracked.leg, BundleLeg::Maker) {
        let _ = runtime.state.update_bundle_open_leg_route(
            &tracked.bundle_id,
            normalized_fill.position_side,
            normalized_fill.exchange.clone(),
            normalized_fill.exchange_symbol.clone(),
            normalized_fill.quantity,
            normalized_fill.filled_at,
        );
    }
    if let Err(error) = runtime
        .state
        .apply_fill_event(&tracked.bundle_id, &normalized_fill)
    {
        if should_ignore_restored_bundle_fill_mismatch(
            runtime,
            &tracked.bundle_id,
            &normalized_fill,
            &error,
        ) {
            return None;
        }
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!("fill-apply-{}", event.received_at.timestamp_millis()),
            canonical_symbol: Some(normalized_fill.canonical_symbol.clone()),
            exchange: Some(normalized_fill.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: error.to_string(),
            created_at: event.received_at,
        });
        return None;
    }

    if !tracked.trigger_hedge_on_fill || !is_hedge_trigger_leg(tracked.leg) {
        runtime.persist_hedge_record(&tracked.bundle_id, normalized_fill.filled_at);
        return None;
    }
    let reduce_only = normalized_fill.reduce_only.unwrap_or(false);
    if reduce_only {
        runtime
            .state
            .mark_hedge_record_closing(&tracked.bundle_id, normalized_fill.filled_at);
        if let Some(bundle) = runtime.state.open_bundles.get_mut(&tracked.bundle_id) {
            bundle.status = SimulatedBundleStatus::ClosingSimulated;
            bundle.updated_at = normalized_fill.filled_at;
        }
        let _ = runtime.state.position_manager.mark_bundle_status(
            &tracked.bundle_id,
            BundleStatus::ClosingSimulated,
            normalized_fill.filled_at,
        );
    } else {
        runtime
            .state
            .record_hedge_started(&tracked.bundle_id, normalized_fill.filled_at);
        if let Some(bundle) = runtime.state.open_bundles.get_mut(&tracked.bundle_id) {
            bundle.status = SimulatedBundleStatus::Hedging;
            bundle.updated_at = normalized_fill.filled_at;
        }
        let _ = runtime.state.position_manager.mark_bundle_status(
            &tracked.bundle_id,
            BundleStatus::Hedging,
            normalized_fill.filled_at,
        );
    }
    runtime.persist_hedge_record(&tracked.bundle_id, normalized_fill.filled_at);

    let mut taker_exchange = tracked.taker_exchange.clone();
    let mut taker_exchange_symbol = tracked.taker_exchange_symbol.clone();
    let mut hedge_price = tracked.planned_taker_price;
    match select_best_live_taker_for_fill(runtime, tracked, &normalized_fill, reduce_only) {
        Some(selection) => {
            taker_exchange = selection.exchange;
            taker_exchange_symbol = selection.exchange_symbol;
            hedge_price = selection.price;
        }
        None if !reduce_only => {
            runtime.state.set_close_only();
            runtime.state.risk_events.push(super::RiskEventReadModel {
                event_id: format!("maker-fill-no-safe-taker-{}", event.received_at.timestamp_millis()),
                canonical_symbol: Some(normalized_fill.canonical_symbol.clone()),
                exchange: Some(tracked.taker_exchange.clone()),
                reason: super::RejectReason::RouteUnhealthy,
                message: format!(
                    "maker fill {} has no current taker route that preserves configured net edge; close-only enabled",
                    normalized_fill.trade_id
                ),
                created_at: event.received_at,
            });
        }
        None => {}
    }

    Some(MakerFill {
        bundle_id: tracked.bundle_id.clone(),
        mode: tracked.mode,
        canonical_symbol: normalized_fill.canonical_symbol.clone(),
        taker_exchange,
        taker_exchange_symbol,
        maker_side: tracked.maker_side,
        filled_quantity: normalized_fill.quantity,
        hedge_price,
        max_slippage_pct: tracked.max_slippage_pct,
        reduce_only,
        filled_at: normalized_fill.filled_at,
    })
}

fn apply_tracked_private_fill_to_runtime(
    runtime: &mut CrossArbRuntime,
    tracked: &TrackedCrossArbOrder,
    fill: &FillEvent,
    received_at: DateTime<Utc>,
) {
    let normalized_fill = fill_for_tracked_leg(fill, tracked.leg);
    if !normalized_fill.reduce_only.unwrap_or(false)
        && matches!(
            tracked.leg,
            BundleLeg::Hedge | BundleLeg::Long | BundleLeg::Short
        )
    {
        let _ = runtime.state.update_bundle_open_leg_route(
            &tracked.bundle_id,
            normalized_fill.position_side,
            normalized_fill.exchange.clone(),
            normalized_fill.exchange_symbol.clone(),
            normalized_fill.quantity,
            normalized_fill.filled_at,
        );
    }
    if let Err(error) = runtime
        .state
        .apply_fill_event(&tracked.bundle_id, &normalized_fill)
    {
        if should_ignore_restored_bundle_fill_mismatch(
            runtime,
            &tracked.bundle_id,
            &normalized_fill,
            &error,
        ) {
            return;
        }
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!("fill-apply-{}", received_at.timestamp_millis()),
            canonical_symbol: Some(normalized_fill.canonical_symbol.clone()),
            exchange: Some(normalized_fill.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: error.to_string(),
            created_at: received_at,
        });
        return;
    }
    runtime.persist_hedge_record(&tracked.bundle_id, normalized_fill.filled_at);
}

fn should_ignore_restored_bundle_fill_mismatch(
    runtime: &CrossArbRuntime,
    bundle_id: &str,
    fill: &FillEvent,
    error: &PositionError,
) -> bool {
    matches!(error, PositionError::FillDoesNotMatchBundle { .. })
        && bundle_id.starts_with("restored-")
        && runtime
            .state
            .position_manager
            .bundle(bundle_id)
            .map(|bundle| {
                bundle.canonical_symbol == fill.canonical_symbol
                    && bundle.status == BundleStatus::OpenSimulated
                    && (bundle.long_leg.exchange == fill.exchange
                        || bundle.short_leg.exchange == fill.exchange)
            })
            .unwrap_or(false)
}

struct LiveTakerSelection {
    exchange: ExchangeId,
    exchange_symbol: ExchangeSymbol,
    price: Option<f64>,
}

fn select_best_live_taker_for_fill(
    runtime: &CrossArbRuntime,
    tracked: &TrackedCrossArbOrder,
    fill: &FillEvent,
    reduce_only: bool,
) -> Option<LiveTakerSelection> {
    if reduce_only {
        return Some(LiveTakerSelection {
            exchange: tracked.taker_exchange.clone(),
            exchange_symbol: tracked.taker_exchange_symbol.clone(),
            price: tracked.planned_taker_price,
        });
    }
    let maker_price = if fill.price.is_finite() && fill.price > 0.0 {
        fill.price
    } else {
        tracked.maker_entry_price?
    };
    runtime
        .state
        .opportunities
        .iter()
        .filter(|opportunity| {
            opportunity.canonical_symbol == fill.canonical_symbol
                && opportunity.maker_exchange == tracked.exchange
                && to_execution_order_side(opportunity.maker_side) == tracked.maker_side
                && opportunity.can_open
        })
        .filter_map(|opportunity| {
            let route_thresholds = runtime
                .state
                .config
                .thresholds
                .route_thresholds(&opportunity.long_exchange, &opportunity.short_exchange);
            if opportunity.maker_taker_net_edge < route_thresholds.min_open_maker_taker_net_edge {
                return None;
            }
            let taker_price = opportunity.taker_vwap?;
            let live_edge = live_fill_net_edge(
                runtime,
                tracked,
                fill,
                opportunity,
                maker_price,
                taker_price,
            )?;
            (live_edge >= route_thresholds.min_open_maker_taker_net_edge).then(|| {
                (
                    live_edge,
                    LiveTakerSelection {
                        exchange: opportunity.taker_exchange.clone(),
                        exchange_symbol: exchange_symbol_for(
                            &opportunity.taker_exchange,
                            &opportunity.canonical_symbol,
                        ),
                        price: Some(taker_price),
                    },
                )
            })
        })
        .max_by(|(left_edge, _), (right_edge, _)| {
            left_edge
                .partial_cmp(right_edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(_, selection)| selection)
}

fn live_fill_net_edge(
    runtime: &CrossArbRuntime,
    tracked: &TrackedCrossArbOrder,
    fill: &FillEvent,
    opportunity: &Opportunity,
    maker_price: f64,
    taker_price: f64,
) -> Option<f64> {
    if maker_price <= 0.0 || taker_price <= 0.0 {
        return None;
    }
    let (long_price, short_price) = match tracked.maker_side {
        OrderSide::Buy => (maker_price, taker_price),
        OrderSide::Sell => (taker_price, maker_price),
    };
    if long_price <= 0.0 || short_price <= 0.0 {
        return None;
    }
    let notional = fill.notional().max(fill.quantity * maker_price).max(1.0);
    let fee_model = super::FeeModel::from_config(&runtime.state.config.fees);
    let round_trip_fee = fee_model
        .estimate_maker_taker_round_trip(&tracked.exchange, &opportunity.taker_exchange, notional)
        .total_normal_fee();
    Some(
        short_price / long_price
            - 1.0
            - round_trip_fee / notional
            - runtime.state.config.risk.taker_slippage_buffer
            - runtime.state.config.risk.safety_buffer,
    )
}

fn fill_for_tracked_leg(fill: &FillEvent, leg: BundleLeg) -> FillEvent {
    let mut normalized = fill.clone();
    if matches!(
        leg,
        BundleLeg::CloseLong
            | BundleLeg::CloseShort
            | BundleLeg::EmergencyCloseLong
            | BundleLeg::EmergencyCloseShort
    ) {
        normalized.reduce_only = Some(true);
        normalized.position_side = match leg {
            BundleLeg::CloseLong | BundleLeg::EmergencyCloseLong => PositionSide::Long,
            BundleLeg::CloseShort | BundleLeg::EmergencyCloseShort => PositionSide::Short,
            _ => normalized.position_side,
        };
    }
    normalized
}

pub fn execution_request_from_signal(
    state: &CrossArbRuntimeState,
    signal: &ArbSignal,
    opportunity: &Opportunity,
) -> Result<ExecutionRequest> {
    if signal.action != ArbSignalAction::Open {
        anyhow::bail!("signal {} is not an open signal", signal.signal_id);
    }
    if !opportunity.can_open {
        anyhow::bail!(
            "opportunity {} cannot open: {:?}",
            opportunity.opportunity_id,
            opportunity.reject_reasons
        );
    }
    let quantity = opportunity
        .maker_quantity
        .or_else(|| {
            quantity_from_notional(
                opportunity.executable_notional_usdt,
                opportunity.maker_price,
            )
        })
        .ok_or_else(|| {
            anyhow!(
                "opportunity {} has no executable quantity",
                opportunity.opportunity_id
            )
        })?;
    if quantity <= 0.0 || !quantity.is_finite() {
        anyhow::bail!(
            "opportunity {} has invalid quantity {quantity}",
            opportunity.opportunity_id
        );
    }

    let bundle_id = format!("bundle-{}", signal.signal_id);
    Ok(ExecutionRequest {
        request_id: format!("request-{}", signal.signal_id),
        mode: signal.mode,
        action: ExecutionAction::Open,
        bundle_id,
        canonical_symbol: opportunity.canonical_symbol.clone(),
        maker_exchange: opportunity.maker_exchange.clone(),
        taker_exchange: opportunity.taker_exchange.clone(),
        maker_exchange_symbol: exchange_symbol_for(
            &opportunity.maker_exchange,
            &opportunity.canonical_symbol,
        ),
        taker_exchange_symbol: exchange_symbol_for(
            &opportunity.taker_exchange,
            &opportunity.canonical_symbol,
        ),
        maker_side: to_execution_order_side(opportunity.maker_side),
        taker_side: to_execution_order_side(opportunity.taker_side),
        quantity,
        maker_price: Some(opportunity.maker_price),
        taker_price: opportunity.taker_vwap,
        max_slippage_pct: Some(state.config.execution.taker_ioc_slippage_limit_pct),
        generated_at: signal.generated_at,
        open_with_dual_taker: state.config.execution.open_execution_style
            == OpenExecutionStyle::DualTaker,
    })
}

fn register_open_bundle(
    runtime: &mut CrossArbRuntime,
    signal: &ArbSignal,
    opportunity: &Opportunity,
    request: &ExecutionRequest,
) -> Result<()> {
    if runtime.state.open_bundles.len() >= runtime.state.config.risk.max_open_bundles {
        anyhow::bail!("max_open_bundles reached");
    }
    let now = signal.generated_at;
    let mut bundle = ArbitrageBundle::new(
        request.bundle_id.clone(),
        signal.mode,
        opportunity.canonical_symbol.clone(),
        opportunity.long_exchange.clone(),
        opportunity.short_exchange.clone(),
        opportunity.maker_exchange.clone(),
        opportunity.taker_exchange.clone(),
        opportunity.executable_notional_usdt,
        now,
    );
    bundle.status = if request.open_with_dual_taker {
        BundleStatus::Hedging
    } else {
        BundleStatus::MakerPending
    };
    bundle.created_from_signal_id = Some(signal.signal_id.clone());
    runtime.state.register_bundle_position(
        &bundle,
        request.quantity,
        opportunity.maker_taker_net_edge,
        now,
    );
    runtime.state.open_bundles.insert(
        request.bundle_id.clone(),
        SimulatedBundleState {
            bundle_id: request.bundle_id.clone(),
            opportunity_id: opportunity.opportunity_id.clone(),
            status: if request.open_with_dual_taker {
                SimulatedBundleStatus::Hedging
            } else {
                SimulatedBundleStatus::MakerPending
            },
            route: opportunity.route(),
            target_notional_usdt: opportunity.executable_notional_usdt,
            opened_at: None,
            updated_at: now,
        },
    );
    if let Some(bundle) = runtime.state.open_bundles.get(&request.bundle_id) {
        runtime
            .storage
            .record(super::CrossArbStorageEvent::Bundle(bundle.clone()), now);
    }
    Ok(())
}

fn update_runtime_after_open_decision(runtime: &mut CrossArbRuntime, decision: &EngineDecision) {
    if let Some(reason) = &decision.blocked_reason {
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!(
                "execution-blocked-{}",
                decision.plan.created_at.timestamp_millis()
            ),
            canonical_symbol: decision
                .plan
                .commands
                .first()
                .map(|command| command.canonical_symbol.clone()),
            exchange: decision
                .plan
                .commands
                .first()
                .map(|command| command.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: reason.clone(),
            created_at: decision.plan.created_at,
        });
        if decision.plan.requires_reconcile || decision.requires_reconcile {
            if let Some(bundle_id) = decision
                .plan
                .commands
                .first()
                .map(|command| command.bundle_id.clone())
            {
                if let Some(bundle) = runtime.state.open_bundles.get_mut(&bundle_id) {
                    bundle.status = SimulatedBundleStatus::OrphanLeg;
                    bundle.updated_at = decision.plan.created_at;
                }
                let _ = runtime.state.position_manager.mark_bundle_status(
                    &bundle_id,
                    BundleStatus::ReconcileRequired,
                    decision.plan.created_at,
                );
            }
        }
    }
}

fn update_runtime_after_hedge_decision(runtime: &mut CrossArbRuntime, decision: &EngineDecision) {
    let submitted = decision.plan.commands.first().is_some_and(|command| {
        decision
            .submitted_orders
            .iter()
            .any(|ack| ack.client_order_id == command.client_order_id && order_ack_is_live(ack))
    });
    if decision.blocked_reason.is_some() || decision.requires_reconcile || !submitted {
        if let Some(command) = decision.plan.commands.first() {
            runtime.state.record_hedge_repair_required(
                command,
                decision
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "hedge order was not submitted".to_string()),
                decision.plan.created_at,
            );
            runtime.persist_hedge_repair_task(
                &hedge_repair_task_id(&command.bundle_id, command.reduce_only),
                decision.plan.created_at,
            );
            runtime.persist_hedge_record(&command.bundle_id, decision.plan.created_at);
        }
        runtime.state.risk_events.push(super::RiskEventReadModel {
            event_id: format!(
                "hedge-decision-{}",
                decision.plan.created_at.timestamp_millis()
            ),
            canonical_symbol: decision
                .plan
                .commands
                .first()
                .map(|command| command.canonical_symbol.clone()),
            exchange: decision
                .plan
                .commands
                .first()
                .map(|command| command.exchange.clone()),
            reason: super::RejectReason::RouteUnhealthy,
            message: decision
                .blocked_reason
                .clone()
                .unwrap_or_else(|| "hedge requires reconciliation".to_string()),
            created_at: decision.plan.created_at,
        });
    } else if let Some(command) = decision.plan.commands.first() {
        runtime
            .state
            .record_hedge_order_submitted(command, decision.plan.created_at);
        runtime.persist_hedge_record(&command.bundle_id, decision.plan.created_at);
    }
}

pub fn order_ack_is_live(ack: &OrderAck) -> bool {
    ack.accepted
        && matches!(
            ack.status,
            OrderCommandStatus::Submitted
                | OrderCommandStatus::Accepted
                | OrderCommandStatus::PartiallyFilled
                | OrderCommandStatus::Filled
        )
}

fn tracked_order(command: &OrderCommand) -> TrackedCrossArbOrder {
    TrackedCrossArbOrder {
        bundle_id: command.bundle_id.clone(),
        leg: command_leg(command),
        exchange: command.exchange.clone(),
        exchange_symbol: command.exchange_symbol.clone(),
        maker_side: maker_side_from_command(command),
        taker_exchange: command.exchange.clone(),
        taker_exchange_symbol: command.exchange_symbol.clone(),
        max_slippage_pct: command.max_slippage_pct,
        trigger_hedge_on_fill: is_pending_maker_command(command),
        maker_entry_price: command.price,
        planned_taker_price: command.price,
        mode: crate::market::RuntimeMode::Simulation,
        created_at: command.created_at,
    }
}

fn tracked_order_for_request(
    command: &OrderCommand,
    request: &ExecutionRequest,
) -> TrackedCrossArbOrder {
    let mut tracked = tracked_order(command);
    tracked.mode = request.mode;
    tracked.maker_side = request.maker_side;
    if command.exchange == request.maker_exchange {
        tracked.taker_exchange = request.taker_exchange.clone();
        tracked.taker_exchange_symbol = request.taker_exchange_symbol.clone();
        tracked.trigger_hedge_on_fill = !request.open_with_dual_taker;
    } else {
        tracked.taker_exchange = request.maker_exchange.clone();
        tracked.taker_exchange_symbol = request.maker_exchange_symbol.clone();
        tracked.trigger_hedge_on_fill = false;
    }
    tracked.max_slippage_pct = request.max_slippage_pct;
    tracked.maker_entry_price = request.maker_price;
    tracked.planned_taker_price = request.taker_price;
    tracked
}

fn tracked_order_for_close_candidate(
    mode: crate::market::RuntimeMode,
    command: &OrderCommand,
    candidate: &LiveCloseCandidate,
) -> TrackedCrossArbOrder {
    let mut tracked = tracked_order(command);
    tracked.mode = mode;
    tracked.leg = command_leg(command);
    tracked.maker_side = command.side;
    tracked.max_slippage_pct = command.max_slippage_pct;
    if is_pending_maker_command(command) {
        tracked.taker_exchange = candidate.taker_close_exchange.clone();
        tracked.taker_exchange_symbol = candidate.taker_close_exchange_symbol.clone();
        tracked.planned_taker_price = None;
        tracked.trigger_hedge_on_fill = true;
    } else {
        tracked.taker_exchange = command.exchange.clone();
        tracked.taker_exchange_symbol = command.exchange_symbol.clone();
        tracked.planned_taker_price = command.price;
        tracked.trigger_hedge_on_fill = false;
    }
    tracked
}

fn command_leg(command: &OrderCommand) -> BundleLeg {
    if command.intent == crate::execution::OrderIntent::CloseLongMaker {
        BundleLeg::CloseLong
    } else if command.intent == crate::execution::OrderIntent::CloseShortMaker {
        BundleLeg::CloseShort
    } else if command.post_only {
        BundleLeg::Maker
    } else if command.intent == crate::execution::OrderIntent::HedgeLongTaker {
        if command.position_side == PositionSide::Long {
            BundleLeg::Long
        } else {
            BundleLeg::Hedge
        }
    } else if command.intent == crate::execution::OrderIntent::HedgeShortTaker {
        if command.position_side == PositionSide::Short {
            BundleLeg::Short
        } else {
            BundleLeg::Hedge
        }
    } else if command.intent == crate::execution::OrderIntent::CloseLongTaker {
        BundleLeg::CloseLong
    } else if command.intent == crate::execution::OrderIntent::CloseShortTaker {
        BundleLeg::CloseShort
    } else if command.intent == crate::execution::OrderIntent::EmergencyCloseLongTaker {
        BundleLeg::EmergencyCloseLong
    } else if command.intent == crate::execution::OrderIntent::EmergencyCloseShortTaker {
        BundleLeg::EmergencyCloseShort
    } else {
        BundleLeg::Taker
    }
}

fn is_pending_maker_command(command: &OrderCommand) -> bool {
    command.post_only
        || matches!(
            command.intent,
            crate::execution::OrderIntent::OpenLongMaker
                | crate::execution::OrderIntent::OpenShortMaker
                | crate::execution::OrderIntent::CloseLongMaker
                | crate::execution::OrderIntent::CloseShortMaker
        )
}

fn is_hedge_trigger_leg(leg: BundleLeg) -> bool {
    matches!(
        leg,
        BundleLeg::Maker | BundleLeg::CloseLong | BundleLeg::CloseShort
    )
}

fn open_maker_cancel_status(
    runtime: &CrossArbRuntime,
    bundle_id: &str,
) -> (SimulatedBundleStatus, BundleStatus) {
    let Some(position) = runtime.state.position_manager.bundle(bundle_id) else {
        return (
            SimulatedBundleStatus::RiskStopped,
            BundleStatus::RiskStopped,
        );
    };
    let quantity_tolerance = runtime.state.config.reconciliation.quantity_tolerance;
    if position.is_fully_open(quantity_tolerance) {
        (
            SimulatedBundleStatus::OpenSimulated,
            BundleStatus::OpenSimulated,
        )
    } else if position.unhedged_qty() > quantity_tolerance {
        (SimulatedBundleStatus::OrphanLeg, BundleStatus::OrphanLeg)
    } else {
        (
            SimulatedBundleStatus::RiskStopped,
            BundleStatus::RiskStopped,
        )
    }
}

fn apply_open_maker_cancel_status(
    runtime: &mut CrossArbRuntime,
    bundle_id: &str,
    now: DateTime<Utc>,
) {
    let (simulated_status, position_status) = open_maker_cancel_status(runtime, bundle_id);
    if let Some(bundle) = runtime.state.open_bundles.get_mut(bundle_id) {
        bundle.status = simulated_status;
        bundle.updated_at = now;
    }
    let _ = runtime
        .state
        .position_manager
        .mark_bundle_status(bundle_id, position_status, now);
}

fn should_drop_unfilled_open_bundle(runtime: &CrossArbRuntime, bundle_id: &str) -> bool {
    let Some(position) = runtime.state.position_manager.bundle(bundle_id) else {
        return true;
    };
    let quantity_tolerance = runtime.state.config.reconciliation.quantity_tolerance;
    position.long_leg.filled_qty <= quantity_tolerance
        && position.short_leg.filled_qty <= quantity_tolerance
        && position.unhedged_qty() <= quantity_tolerance
}

fn order_state_has_fill(order: &OrderState, tolerance: f64) -> bool {
    order.filled_quantity > tolerance
        && !matches!(
            order.status,
            OrderCommandStatus::Rejected | OrderCommandStatus::Failed
        )
}

fn tracked_order_fill_key(tracked: &TrackedCrossArbOrder) -> String {
    format!(
        "{}:{}:{}:{:?}",
        tracked.exchange.as_str(),
        tracked.exchange_symbol.symbol,
        tracked.bundle_id,
        tracked.leg
    )
}

fn private_trade_key(fill: &FillEvent) -> String {
    format!(
        "{}:{}:{}:{}",
        fill.exchange.as_str(),
        fill.exchange_symbol.symbol,
        fill.exchange_order_id
            .as_deref()
            .or(fill.client_order_id.as_deref())
            .unwrap_or("unknown-order"),
        fill.trade_id
    )
}

fn fill_from_tracked_order_state_delta(
    tracked: &TrackedCrossArbOrder,
    order: &OrderState,
    delta_quantity: f64,
    cumulative_quantity: f64,
    received_at: DateTime<Utc>,
) -> FillEvent {
    let price = order
        .average_fill_price
        .filter(|price| price.is_finite() && *price > 0.0)
        .or(order.price)
        .or(tracked.maker_entry_price)
        .unwrap_or(0.0);
    FillEvent {
        exchange: order.exchange.clone(),
        canonical_symbol: order.canonical_symbol.clone(),
        exchange_symbol: order.exchange_symbol.clone(),
        trade_id: format!(
            "ws-order-{}-{:.12}",
            order
                .exchange_order_id
                .as_deref()
                .or(order.client_order_id.as_deref())
                .unwrap_or("unknown"),
            cumulative_quantity
        ),
        client_order_id: order.client_order_id.clone(),
        exchange_order_id: order.exchange_order_id.clone(),
        side: order.side,
        position_side: order.position_side,
        liquidity: if tracked.trigger_hedge_on_fill {
            FillLiquidity::Maker
        } else {
            FillLiquidity::Unknown
        },
        price,
        quantity: delta_quantity,
        quote_quantity: if price > 0.0 {
            price * delta_quantity
        } else {
            0.0
        },
        fee: None,
        fee_asset: None,
        fee_rate: None,
        realized_pnl: None,
        reduce_only: Some(order.reduce_only),
        filled_at: order.updated_at.max(received_at),
        received_at,
    }
}

fn is_unknown_order_cancel_error(message: &str) -> bool {
    message.contains("Unknown order sent")
        || message.contains("\"code\":-2011")
        || message.contains("order does not exist")
        || message.contains("Order does not exist")
        || message.contains("OrderNotFound")
        || message.contains("ORDER_NOT_FOUND")
}

fn cancelled_order_state_from_ack(
    command: &OrderCommand,
    ack: &CancelAck,
    now: DateTime<Utc>,
) -> OrderState {
    OrderState {
        exchange: command.exchange.clone(),
        canonical_symbol: command.canonical_symbol.clone(),
        exchange_symbol: command.exchange_symbol.clone(),
        client_order_id: ack
            .client_order_id
            .clone()
            .or_else(|| Some(command.client_order_id.clone())),
        exchange_order_id: ack.exchange_order_id.clone(),
        side: command.side,
        position_side: command.position_side,
        order_type: command.order_type,
        quantity: command.quantity,
        price: command.price,
        filled_quantity: 0.0,
        average_fill_price: None,
        time_in_force: command.time_in_force,
        reduce_only: command.reduce_only,
        status: crate::execution::OrderCommandStatus::Cancelled,
        updated_at: ack.acknowledged_at.max(now),
    }
}

fn fill_from_order_state(
    command: &OrderCommand,
    order: &OrderState,
    now: DateTime<Utc>,
) -> FillEvent {
    let price = order
        .average_fill_price
        .filter(|price| price.is_finite() && *price > 0.0)
        .or(order.price)
        .or(command.price)
        .unwrap_or(0.0);
    let quantity = order.filled_quantity.min(command.quantity).max(0.0);
    FillEvent {
        exchange: command.exchange.clone(),
        canonical_symbol: command.canonical_symbol.clone(),
        exchange_symbol: command.exchange_symbol.clone(),
        trade_id: format!("readback-{}", command.client_order_id),
        client_order_id: Some(command.client_order_id.clone()),
        exchange_order_id: order.exchange_order_id.clone(),
        side: command.side,
        position_side: command.position_side,
        liquidity: if command.post_only {
            FillLiquidity::Maker
        } else {
            FillLiquidity::Taker
        },
        price,
        quantity,
        quote_quantity: if price > 0.0 { price * quantity } else { 0.0 },
        fee: None,
        fee_asset: None,
        fee_rate: None,
        realized_pnl: None,
        reduce_only: Some(command.reduce_only),
        filled_at: order.updated_at.max(now),
        received_at: now,
    }
}

pub fn hedge_repair_task_id(bundle_id: &str, reduce_only: bool) -> String {
    let phase = if reduce_only { "close" } else { "open" };
    format!("repair-{phase}-{bundle_id}")
}

fn maker_side_from_command(command: &OrderCommand) -> OrderSide {
    if command_leg(command) == BundleLeg::Maker {
        command.side
    } else {
        command.side.opposite()
    }
}

fn maker_stats_key_from_command(command: &OrderCommand) -> MakerStatsKey {
    MakerStatsKey {
        canonical_symbol: command.canonical_symbol.clone(),
        exchange: command.exchange.clone(),
        side: command.side,
    }
}

fn maker_stats_key_from_opportunity(opportunity: &Opportunity) -> MakerStatsKey {
    MakerStatsKey {
        canonical_symbol: opportunity.canonical_symbol.clone(),
        exchange: opportunity.maker_exchange.clone(),
        side: to_execution_order_side(opportunity.maker_side),
    }
}

fn aggressive_ticks_for_cancels(
    config: &super::CrossExchangeArbitrageConfig,
    consecutive_ttl_cancels: u32,
) -> u32 {
    let after = config.execution.maker_aggressive_after_cancels.max(1);
    if consecutive_ttl_cancels < after {
        return 0;
    }
    let levels = consecutive_ttl_cancels / after;
    levels
        .saturating_mul(config.execution.maker_aggressive_step_ticks.max(1))
        .min(config.execution.maker_aggressive_max_ticks)
}

fn apply_maker_aggression_to_request(
    config: &super::CrossExchangeArbitrageConfig,
    opportunity: &Opportunity,
    aggressive_ticks: u32,
    request: &mut ExecutionRequest,
) -> Result<()> {
    if aggressive_ticks == 0 {
        return Ok(());
    }
    let Some(current_price) = request.maker_price else {
        return Ok(());
    };
    let Some(tick) = opportunity
        .maker_price_tick
        .filter(|tick| tick.is_finite() && *tick > 0.0)
    else {
        return Ok(());
    };
    let offset = tick * f64::from(aggressive_ticks);
    let adjusted = match request.maker_side {
        OrderSide::Buy => {
            let candidate = current_price + offset;
            let capped = opportunity
                .maker_best_opposite_price
                .filter(|price| price.is_finite() && *price > tick)
                .map(|best_ask| candidate.min(best_ask - tick))
                .unwrap_or(candidate);
            round_to_step(capped, tick, RoundingMode::Floor)
        }
        OrderSide::Sell => {
            let candidate = current_price - offset;
            let capped = opportunity
                .maker_best_opposite_price
                .filter(|price| price.is_finite() && *price > 0.0)
                .map(|best_bid| candidate.max(best_bid + tick))
                .unwrap_or(candidate);
            round_to_step(capped, tick, RoundingMode::Ceil)
        }
    };
    if !adjusted.is_finite() || adjusted <= 0.0 || (adjusted - current_price).abs() < f64::EPSILON {
        return Ok(());
    }

    if let Some(taker_price) = request.taker_price.filter(|price| *price > 0.0) {
        let adjusted_raw = match request.maker_side {
            OrderSide::Buy => taker_price / adjusted - 1.0,
            OrderSide::Sell => adjusted / taker_price - 1.0,
        };
        let adjusted_net =
            opportunity.maker_taker_net_edge + (adjusted_raw - opportunity.raw_open_spread);
        let thresholds = config
            .thresholds
            .route_thresholds(&opportunity.long_exchange, &opportunity.short_exchange);
        if adjusted_net < thresholds.min_open_maker_taker_net_edge {
            anyhow::bail!(
                "aggressive maker price would erase net edge: adjusted_net={:.8} threshold={:.8} ticks={}",
                adjusted_net,
                thresholds.min_open_maker_taker_net_edge,
                aggressive_ticks
            );
        }
    }

    request.maker_price = Some(adjusted);
    Ok(())
}

fn to_strategy_order_side(side: OrderSide) -> super::state::OrderSide {
    match side {
        OrderSide::Buy => super::state::OrderSide::Buy,
        OrderSide::Sell => super::state::OrderSide::Sell,
    }
}

fn to_execution_order_side(side: super::state::OrderSide) -> OrderSide {
    match side {
        super::state::OrderSide::Buy => OrderSide::Buy,
        super::state::OrderSide::Sell => OrderSide::Sell,
    }
}

fn quantity_from_notional(notional: f64, price: f64) -> Option<f64> {
    (notional > 0.0 && price > 0.0 && notional.is_finite() && price.is_finite())
        .then_some(notional / price)
}

trait OrderSideExt {
    fn opposite(self) -> Self;
}

impl OrderSideExt for OrderSide {
    fn opposite(self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}

pub fn maker_client_order_id(mode: crate::market::RuntimeMode, bundle_id: &str) -> String {
    deterministic_client_order_id(mode, bundle_id, BundleLeg::Maker, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        CancelAck, CancelCommand, ExchangeBalance, ExchangePosition, FillLiquidity, OrderAck,
        OrderCommandStatus, OrderQuery, OrderState, TimeInForce, TradingAdapter,
        TradingCapabilities,
    };
    use crate::market::{BookLevel, CanonicalSymbol, ExchangeSymbol, OrderBook5, RuntimeMode};
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct MockTradingAdapter {
        exchange: ExchangeId,
        place_calls: Arc<AtomicUsize>,
        cancel_calls: Arc<AtomicUsize>,
        last_cancel_exchange_order_id: Option<Arc<std::sync::Mutex<Option<String>>>>,
        cancel_error: Option<String>,
    }

    #[async_trait]
    impl TradingAdapter for MockTradingAdapter {
        fn exchange(&self) -> ExchangeId {
            self.exchange.clone()
        }

        fn capabilities(&self) -> TradingCapabilities {
            TradingCapabilities::default()
        }

        async fn place_order(&self, command: OrderCommand) -> anyhow::Result<OrderAck> {
            self.place_calls.fetch_add(1, Ordering::SeqCst);
            Ok(OrderAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: Some("exchange-order-1".to_string()),
                accepted: true,
                status: OrderCommandStatus::Accepted,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, command: CancelCommand) -> anyhow::Result<CancelAck> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(last_cancel_exchange_order_id) = &self.last_cancel_exchange_order_id {
                *last_cancel_exchange_order_id.lock().expect("lock") =
                    command.exchange_order_id.clone();
            }
            if let Some(error) = &self.cancel_error {
                anyhow::bail!("{}", error);
            }
            Ok(CancelAck {
                exchange: command.exchange,
                client_order_id: command.client_order_id,
                exchange_order_id: command.exchange_order_id,
                accepted: true,
                status: OrderCommandStatus::Cancelled,
                message: None,
                acknowledged_at: Utc::now(),
            })
        }

        async fn get_order(&self, _query: OrderQuery) -> anyhow::Result<OrderState> {
            anyhow::bail!("not used")
        }

        async fn get_open_orders(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<OrderState>> {
            Ok(Vec::new())
        }

        async fn get_positions(
            &self,
            _symbol: Option<&ExchangeSymbol>,
        ) -> anyhow::Result<Vec<ExchangePosition>> {
            Ok(Vec::new())
        }

        async fn get_balances(&self) -> anyhow::Result<Vec<ExchangeBalance>> {
            Ok(Vec::new())
        }
    }

    fn book(exchange: ExchangeId, bid: f64, ask: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol_for(&exchange, &CanonicalSymbol::new("BTC", "USDT")),
            vec![BookLevel::new(bid, 10.0)],
            vec![BookLevel::new(ask, 10.0)],
            Utc::now(),
            Utc::now(),
            Some(1),
            None,
        )
    }

    fn runtime_with_signal(now: DateTime<Utc>) -> (CrossArbRuntime, ArbSignal) {
        let mut config = super::super::CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.execution.dry_run = false;
        config.thresholds.min_open_maker_taker_net_edge = 0.001;
        let mut runtime = CrossArbRuntime::new(config, now);
        runtime
            .state
            .update_private_stream_health(ExchangeId::Binance, now, false);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let signals = runtime.on_market_snapshots(
            &symbol,
            &[
                super::super::MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
                super::super::MarketSnapshot::healthy(book(ExchangeId::Okx, 105.0, 106.0)),
            ],
            now,
        );
        let signal = signals
            .into_iter()
            .find(|signal| signal.action == ArbSignalAction::Open)
            .expect("open signal");
        (runtime, signal)
    }

    #[tokio::test]
    async fn cross_arb_execution_should_submit_open_maker_order_and_index_it() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));

        let decision = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("decision");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decision.plan.commands.len(), 1);
        assert_eq!(
            decision.plan.commands[0].time_in_force,
            TimeInForce::PostOnly
        );
        assert!(runtime
            .state
            .open_bundles
            .contains_key(&decision.plan.commands[0].bundle_id));
        assert!(coordinator
            .order_index()
            .resolve_fill(&maker_fill_from_command(&decision.plan.commands[0]))
            .is_some());
    }

    #[tokio::test]
    async fn cross_arb_execution_should_turn_maker_fill_into_hedge_order() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        let place_calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");

        let fill = maker_fill_from_command(&open.plan.commands[0]);
        let hedge = coordinator
            .hedge_candidate_from_private_event(
                &mut runtime,
                &PrivateEvent::fill(fill.clone(), fill.received_at),
            )
            .expect("hedge candidate");
        let hedge_decision = coordinator.execute_hedge_for_maker_fill(hedge).await;

        assert_eq!(place_calls.load(Ordering::SeqCst), 2);
        assert_eq!(hedge_decision.plan.commands.len(), 1);
        assert_ne!(
            hedge_decision.plan.commands[0].exchange,
            open.plan.commands[0].exchange
        );
        assert_eq!(
            hedge_decision.plan.commands[0].time_in_force,
            TimeInForce::Ioc
        );
    }

    #[tokio::test]
    async fn cross_arb_execution_should_turn_private_order_state_fill_into_hedge_order() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        let place_calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls: place_calls.clone(),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            last_cancel_exchange_order_id: None,
            cancel_error: None,
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");
        let maker = &open.plan.commands[0];

        let order = OrderState {
            exchange: maker.exchange.clone(),
            canonical_symbol: maker.canonical_symbol.clone(),
            exchange_symbol: maker.exchange_symbol.clone(),
            client_order_id: Some(maker.client_order_id.clone()),
            exchange_order_id: Some("exchange-order-1".to_string()),
            side: maker.side,
            position_side: maker.position_side,
            order_type: maker.order_type,
            quantity: maker.quantity,
            price: maker.price,
            filled_quantity: maker.quantity,
            average_fill_price: maker.price,
            time_in_force: TimeInForce::PostOnly,
            reduce_only: false,
            status: OrderCommandStatus::Filled,
            updated_at: now,
        };
        let hedge = coordinator
            .hedge_candidate_from_private_event(
                &mut runtime,
                &PrivateEvent::order(order.clone(), now),
            )
            .expect("hedge candidate from order state");
        let duplicate = coordinator
            .hedge_candidate_from_private_event(&mut runtime, &PrivateEvent::order(order, now));
        let hedge_decision = coordinator.execute_hedge_for_maker_fill(hedge).await;

        assert!(duplicate.is_none());
        assert_eq!(place_calls.load(Ordering::SeqCst), 2);
        assert_eq!(hedge_decision.plan.commands.len(), 1);
        assert_ne!(hedge_decision.plan.commands[0].exchange, maker.exchange);
        assert_eq!(
            hedge_decision.plan.commands[0].time_in_force,
            TimeInForce::Ioc
        );
    }

    #[tokio::test]
    async fn cross_arb_execution_should_cancel_expired_maker_order() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        runtime.state.config.execution.maker_order_ttl_ms = 1;
        let place_calls = Arc::new(AtomicUsize::new(0));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let last_cancel_exchange_order_id = Arc::new(std::sync::Mutex::new(None));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: cancel_calls.clone(),
            last_cancel_exchange_order_id: Some(last_cancel_exchange_order_id.clone()),
            cancel_error: None,
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls,
            cancel_calls: cancel_calls.clone(),
            last_cancel_exchange_order_id: Some(last_cancel_exchange_order_id.clone()),
            cancel_error: None,
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");

        let acks = coordinator
            .cancel_expired_maker_orders(&mut runtime, now + chrono::Duration::milliseconds(2))
            .await;

        assert_eq!(acks.len(), 1);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            last_cancel_exchange_order_id
                .lock()
                .expect("lock")
                .as_deref(),
            Some("exchange-order-1")
        );
        assert_eq!(coordinator.pending_maker_count(), 0);
        assert!(!runtime
            .state
            .open_bundles
            .contains_key(&open.plan.commands[0].bundle_id));
        assert!(!runtime
            .state
            .position_manager
            .contains_bundle(&open.plan.commands[0].bundle_id));
    }

    #[tokio::test]
    async fn cross_arb_execution_should_treat_unknown_order_cancel_as_terminal() {
        let now = Utc::now();
        let (mut runtime, signal) = runtime_with_signal(now);
        runtime.state.config.execution.maker_order_ttl_ms = 1;
        let place_calls = Arc::new(AtomicUsize::new(0));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let mut router = crate::execution::ExecutionRouter::new(false);
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Okx,
            place_calls: place_calls.clone(),
            cancel_calls: cancel_calls.clone(),
            last_cancel_exchange_order_id: None,
            cancel_error: Some(
                "API错误: 400 - {\"code\":-2011,\"msg\":\"Unknown order sent.\"}".to_string(),
            ),
        }));
        router.register_adapter(Arc::new(MockTradingAdapter {
            exchange: ExchangeId::Binance,
            place_calls,
            cancel_calls: cancel_calls.clone(),
            last_cancel_exchange_order_id: None,
            cancel_error: Some(
                "API错误: 400 - {\"code\":-2011,\"msg\":\"Unknown order sent.\"}".to_string(),
            ),
        }));
        let mut coordinator =
            CrossArbExecutionCoordinator::new(crate::execution::ExecutionEngine::new(router));
        let open = coordinator
            .execute_open_signal(&mut runtime, &signal)
            .await
            .unwrap()
            .expect("open decision");

        let acks = coordinator
            .cancel_expired_maker_orders(&mut runtime, now + chrono::Duration::milliseconds(2))
            .await;

        assert_eq!(acks.len(), 1);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(coordinator.pending_maker_count(), 0);
        assert!(!runtime
            .state
            .open_bundles
            .contains_key(&open.plan.commands[0].bundle_id));
        assert!(runtime
            .state
            .risk_events
            .iter()
            .any(|event| event.message.contains("treated as terminal")));
    }

    fn maker_fill_from_command(command: &OrderCommand) -> FillEvent {
        FillEvent {
            exchange: command.exchange.clone(),
            canonical_symbol: command.canonical_symbol.clone(),
            exchange_symbol: command.exchange_symbol.clone(),
            trade_id: format!("trade-{}", command.client_order_id),
            client_order_id: Some(command.client_order_id.clone()),
            exchange_order_id: Some("exchange-order-1".to_string()),
            side: command.side,
            position_side: command.position_side,
            liquidity: FillLiquidity::Maker,
            price: command.price.unwrap_or(100.0),
            quantity: command.quantity,
            quote_quantity: command.price.unwrap_or(100.0) * command.quantity,
            fee: Some(0.01),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0001),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: Utc::now(),
            received_at: Utc::now(),
        }
    }
}
