use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    load_jsonl_storage_events, scan_opportunities, ArbSignal, ArbitrageFillRecord,
    ArbitrageMarketSnapshotRecord, ArbitrageOrderRecord, AsyncJsonlStorageSink,
    BundleCloseMetricReadModel, BundleReadModel, CrossArbDashboardStatus, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, HedgeRecordReadModel, HedgeRecordStatus,
    HedgeRepairTaskReadModel, HedgeRepairTaskStatus, InMemoryStorageSink, MakerLegKind,
    MarketSnapshot, Opportunity, OpportunityReadModel, OrderSide as StrategyOrderSide,
    PortfolioExposureSummary, PositionManager, PrivateStreamHealthReadModel, RiskEventReadModel,
    RiskStateReadModel, RouteReadModel, SimulatedBundleState, SimulatedBundleStatus, StorageSink,
    StrategyRiskState, StrategyRoute,
};
use crate::execution::{
    AccountSyncState, ArbitrageBundle, BundleLeg, ExchangePosition, FillEvent, OrderCommand,
    OrderSide, OrderType, PositionSide, PositionSnapshot, PrivateEvent, PrivateEventKind,
    TimeInForce,
};
use crate::execution::{BundleStatus, OrderState};
use crate::market::{
    parse_compact_usdt_symbol, CanonicalSymbol, ExchangeId, RouteStatus, RuntimeMode,
};
use crate::strategies::cross_exchange_arbitrage::{
    one_way_conflict_for_open, FillApplication, PositionError, PositionReconcileDecision,
    RejectReason,
};

type PositionClaimKey = (ExchangeId, CanonicalSymbol, PositionSide);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbRuntimeState {
    pub config: CrossExchangeArbitrageConfig,
    pub opportunities: Vec<Opportunity>,
    pub signals: Vec<ArbSignal>,
    pub open_bundles: HashMap<String, SimulatedBundleState>,
    pub history: Vec<SimulatedBundleState>,
    pub risk_events: Vec<RiskEventReadModel>,
    pub hedge_records: HashMap<String, HedgeRecordReadModel>,
    pub hedge_repair_tasks: HashMap<String, HedgeRepairTaskReadModel>,
    pub position_manager: PositionManager,
    pub account_sync: HashMap<ExchangeId, AccountSyncState>,
    pub risk_state: StrategyRiskState,
    pub paused_new_entries: bool,
    pub close_only: bool,
    pub kill_switch: bool,
    pub updated_at: DateTime<Utc>,
}

impl CrossArbRuntimeState {
    pub fn new(config: CrossExchangeArbitrageConfig, now: DateTime<Utc>) -> Self {
        let start_paused_new_entries = config.controls.start_paused_new_entries;
        let start_close_only = config.controls.start_close_only;
        let mut risk_state = StrategyRiskState::new(now);
        risk_state.private_resync_blocks_new_entries =
            config.risk.private_resync_blocks_new_entries;
        let mut state = Self {
            config,
            opportunities: Vec::new(),
            signals: Vec::new(),
            open_bundles: HashMap::new(),
            history: Vec::new(),
            risk_events: Vec::new(),
            hedge_records: HashMap::new(),
            hedge_repair_tasks: HashMap::new(),
            position_manager: PositionManager::default(),
            account_sync: HashMap::new(),
            risk_state,
            paused_new_entries: false,
            close_only: false,
            kill_switch: false,
            updated_at: now,
        };
        if start_close_only {
            state.set_close_only();
        } else if start_paused_new_entries {
            state.pause_new_entries();
        }
        state
    }

    pub fn update_from_market_snapshots(
        &mut self,
        canonical_symbol: &CanonicalSymbol,
        snapshots: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> Vec<ArbSignal> {
        self.updated_at = now;
        self.sync_risk_state(now);
        self.opportunities
            .retain(|opportunity| opportunity.canonical_symbol != *canonical_symbol);
        let mut scanned = scan_opportunities(canonical_symbol, snapshots, &self.config, now);
        let existing_one_way = self.position_manager.active_one_way_exposure_keys();
        let symbols_with_unmanaged_positions = self
            .account_unpaired_exchange_positions()
            .into_iter()
            .map(|position| position.canonical_symbol)
            .collect::<HashSet<_>>();
        for opportunity in &mut scanned {
            if symbols_with_unmanaged_positions.contains(&opportunity.canonical_symbol) {
                opportunity
                    .reject_reasons
                    .push(RejectReason::UnpairedExchangePosition);
                opportunity.can_open = false;
            }
            let maker_side = match opportunity.maker_side {
                StrategyOrderSide::Buy => crate::execution::PositionSide::Long,
                StrategyOrderSide::Sell => crate::execution::PositionSide::Short,
            };
            let taker_side = match opportunity.taker_side {
                StrategyOrderSide::Buy => crate::execution::PositionSide::Long,
                StrategyOrderSide::Sell => crate::execution::PositionSide::Short,
            };
            if one_way_conflict_for_open(
                &opportunity.maker_exchange,
                &opportunity.canonical_symbol,
                maker_side,
                existing_one_way.clone(),
            ) || one_way_conflict_for_open(
                &opportunity.taker_exchange,
                &opportunity.canonical_symbol,
                taker_side,
                existing_one_way.clone(),
            ) {
                opportunity
                    .reject_reasons
                    .push(RejectReason::ExchangePositionLimitExceeded);
                opportunity.can_open = false;
            }
        }
        self.opportunities.extend(scanned);
        self.opportunities.sort_by(|left, right| {
            right
                .maker_taker_net_edge
                .partial_cmp(&left.maker_taker_net_edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let signals = self
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.canonical_symbol == *canonical_symbol)
            .map(|opportunity| {
                if !self.risk_state.decision().allow_new_entries {
                    ArbSignal::noop(self.config.mode, now)
                } else {
                    ArbSignal::from_opportunity(opportunity, self.config.mode, now)
                }
            })
            .collect::<Vec<_>>();
        self.signals = signals.clone();
        signals
    }

    pub fn dashboard_status(&self) -> CrossArbDashboardStatus {
        CrossArbDashboardStatus {
            mode: self.config.mode,
            updated_at: self.updated_at,
            enabled_symbols: self.config.universe.symbols.len(),
            enabled_exchanges: self.config.universe.enabled_exchanges.len(),
            open_bundles: self.open_bundles.len(),
            position_summary: self
                .position_manager
                .portfolio_exposure_summary(self.config.reconciliation.quantity_tolerance),
            route_health: self.route_read_models(),
            risk_state: RiskStateReadModel::from(&self.risk_state.decision()),
            private_stream_health: self.private_stream_health_read_models(),
        }
    }

    pub fn register_bundle_position(
        &mut self,
        bundle: &ArbitrageBundle,
        target_qty: f64,
        entry_edge_pct: f64,
        now: DateTime<Utc>,
    ) {
        self.position_manager
            .upsert_from_bundle(bundle, target_qty, entry_edge_pct, now);
    }

    pub fn record_hedge_started(&mut self, bundle_id: &str, now: DateTime<Utc>) {
        let Some(position) = self.position_manager.bundle(bundle_id) else {
            return;
        };
        Self::upsert_hedge_record_for_position(&mut self.hedge_records, position, None, None, now);
    }

    fn upsert_hedge_record_for_position(
        hedge_records: &mut HashMap<String, HedgeRecordReadModel>,
        position: &super::position::BundlePosition,
        opened_at: Option<DateTime<Utc>>,
        hedge_order_submitted_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) {
        if let Some(record) = hedge_records.get_mut(&position.bundle_id) {
            record.opened_at.get_or_insert(opened_at.unwrap_or(now));
            if let Some(hedge_order_submitted_at) = hedge_order_submitted_at {
                record
                    .hedge_order_submitted_at
                    .get_or_insert(hedge_order_submitted_at);
            }
            record.canonical_symbol = position.canonical_symbol.clone();
            record.long_exchange = position.long_leg.exchange.clone();
            record.short_exchange = position.short_leg.exchange.clone();
            record.entry_net_edge_pct = Some(position.entry_edge_pct);
            if !matches!(
                record.status,
                HedgeRecordStatus::RepairPending
                    | HedgeRecordStatus::RepairSubmitted
                    | HedgeRecordStatus::RepairFailed
                    | HedgeRecordStatus::Closing
                    | HedgeRecordStatus::Closed
            ) {
                record.status = HedgeRecordStatus::Hedging;
            }
            record.updated_at = now;
            return;
        }
        hedge_records.insert(
            position.bundle_id.clone(),
            HedgeRecordReadModel {
                record_id: position.bundle_id.clone(),
                bundle_id: position.bundle_id.clone(),
                opened_at: opened_at.or(position.opened_at).or(Some(now)),
                hedge_order_submitted_at,
                canonical_symbol: position.canonical_symbol.clone(),
                long_exchange: position.long_leg.exchange.clone(),
                short_exchange: position.short_leg.exchange.clone(),
                status: if position.is_fully_open(1e-9) {
                    HedgeRecordStatus::Hedged
                } else {
                    HedgeRecordStatus::Hedging
                },
                entry_net_edge_pct: Some(position.entry_edge_pct),
                close_spread_pct: None,
                close_net_profit_pct: None,
                is_closed: false,
                closed_at: None,
                updated_at: now,
            },
        );
    }

    pub fn record_hedge_order_submitted(&mut self, command: &OrderCommand, now: DateTime<Utc>) {
        let status = if command.reduce_only {
            HedgeRecordStatus::Closing
        } else {
            HedgeRecordStatus::Hedging
        };
        self.record_hedge_started(&command.bundle_id, now);
        if let Some(record) = self.hedge_records.get_mut(&command.bundle_id) {
            record.status = status;
            if !command.reduce_only {
                record.hedge_order_submitted_at.get_or_insert(now);
            }
            record.updated_at = now;
        }
        self.refresh_hedge_record_from_position(&command.bundle_id, now);
    }

    pub fn mark_hedge_record_closing(&mut self, bundle_id: &str, now: DateTime<Utc>) {
        self.record_hedge_started(bundle_id, now);
        if let Some(record) = self.hedge_records.get_mut(bundle_id) {
            record.status = HedgeRecordStatus::Closing;
            record.updated_at = now;
        }
    }

    pub fn record_hedge_repair_required(
        &mut self,
        command: &OrderCommand,
        reason: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        let reason = reason.into();
        self.record_hedge_started(&command.bundle_id, now);
        if let Some(record) = self.hedge_records.get_mut(&command.bundle_id) {
            record.status = HedgeRecordStatus::RepairPending;
            record.updated_at = now;
        }

        let task_id = repair_task_id(&command.bundle_id, command.reduce_only);
        self.hedge_repair_tasks
            .entry(task_id.clone())
            .and_modify(|task| {
                task.status = HedgeRepairTaskStatus::Pending;
                task.last_error = Some(reason.clone());
                task.updated_at = now;
                task.quantity = task.quantity.max(command.quantity);
                task.failed_exchange = command.exchange.clone();
            })
            .or_insert_with(|| HedgeRepairTaskReadModel {
                task_id,
                record_id: command.bundle_id.clone(),
                bundle_id: command.bundle_id.clone(),
                canonical_symbol: command.canonical_symbol.clone(),
                failed_exchange: command.exchange.clone(),
                last_attempt_exchange: None,
                side: command.side,
                position_side: command.position_side,
                quantity: command.quantity,
                reduce_only: command.reduce_only,
                status: HedgeRepairTaskStatus::Pending,
                attempts: 0,
                last_error: Some(reason),
                created_at: now,
                updated_at: now,
            });
    }

    pub fn record_hedge_repair_submitted(
        &mut self,
        task_id: &str,
        exchange: ExchangeId,
        now: DateTime<Utc>,
    ) {
        if let Some(task) = self.hedge_repair_tasks.get_mut(task_id) {
            task.status = HedgeRepairTaskStatus::Submitted;
            task.last_attempt_exchange = Some(exchange);
            task.attempts = task.attempts.saturating_add(1);
            task.last_error = None;
            task.updated_at = now;
            if let Some(record) = self.hedge_records.get_mut(&task.bundle_id) {
                record.status = HedgeRecordStatus::RepairSubmitted;
                if !task.reduce_only {
                    record.hedge_order_submitted_at.get_or_insert(now);
                }
                record.updated_at = now;
            }
        }
    }

    pub fn record_hedge_repair_failed(
        &mut self,
        task_id: &str,
        reason: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        if let Some(task) = self.hedge_repair_tasks.get_mut(task_id) {
            task.status = HedgeRepairTaskStatus::Failed;
            task.attempts = task.attempts.saturating_add(1);
            task.last_error = Some(reason.into());
            task.updated_at = now;
            if let Some(record) = self.hedge_records.get_mut(&task.bundle_id) {
                record.status = HedgeRecordStatus::RepairFailed;
                record.updated_at = now;
            }
        }
    }

    pub fn record_close_candidate_metrics(
        &mut self,
        bundle_id: &str,
        _close_spread_pct: f64,
        _close_profit_pct: f64,
        now: DateTime<Utc>,
    ) {
        self.record_hedge_started(bundle_id, now);
        if let Some(record) = self.hedge_records.get_mut(bundle_id) {
            record.status = HedgeRecordStatus::Closing;
            record.updated_at = now;
        }
    }

    pub fn hedge_record_read_models(&self) -> Vec<HedgeRecordReadModel> {
        let mut records = self
            .hedge_records
            .values()
            .filter(|record| {
                if record.is_closed {
                    return true;
                }
                record.hedge_order_submitted_at.is_some()
                    && should_keep_loaded_hedge_record(record)
                    && self.position_manager.contains_bundle(&record.bundle_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| {
                    left.canonical_symbol
                        .to_string()
                        .cmp(&right.canonical_symbol.to_string())
                })
                .then_with(|| left.bundle_id.cmp(&right.bundle_id))
        });
        records
    }

    pub fn hedge_repair_task_read_models(&self) -> Vec<HedgeRepairTaskReadModel> {
        let mut tasks = self
            .hedge_repair_tasks
            .values()
            .filter(|task| {
                if matches!(task.status, HedgeRepairTaskStatus::Completed) {
                    return false;
                }
                self.hedge_records
                    .get(&task.bundle_id)
                    .map(|record| {
                        !record.is_closed
                            && self.position_manager.contains_bundle(&task.bundle_id)
                            && !matches!(
                                record.status,
                                HedgeRecordStatus::Hedged | HedgeRecordStatus::Closed
                            )
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        tasks
    }

    pub fn due_hedge_repair_tasks(&self, now: DateTime<Utc>) -> Vec<HedgeRepairTaskReadModel> {
        self.hedge_repair_tasks
            .values()
            .filter(|task| {
                if self
                    .hedge_records
                    .get(&task.bundle_id)
                    .map(|record| {
                        !self.position_manager.contains_bundle(&task.bundle_id)
                            || matches!(
                                record.status,
                                HedgeRecordStatus::Hedged | HedgeRecordStatus::Closed
                            )
                    })
                    .unwrap_or(false)
                {
                    return false;
                }
                match task.status {
                    HedgeRepairTaskStatus::Pending => true,
                    HedgeRepairTaskStatus::Failed => {
                        let retry_delay_ms = if task.attempts < 3 {
                            5_000
                        } else if task.attempts < 10 {
                            30_000
                        } else {
                            300_000
                        };
                        now.signed_duration_since(task.updated_at)
                            .num_milliseconds()
                            >= retry_delay_ms
                    }
                    HedgeRepairTaskStatus::Submitted => {
                        now.signed_duration_since(task.updated_at)
                            .num_milliseconds()
                            >= 5_000
                    }
                    HedgeRepairTaskStatus::Completed => false,
                }
            })
            .cloned()
            .collect()
    }

    pub fn apply_fill_event(
        &mut self,
        bundle_id: &str,
        fill: &FillEvent,
    ) -> Result<FillApplication, PositionError> {
        let application = self.position_manager.apply_fill_event(bundle_id, fill)?;
        self.refresh_bundle_status_from_position(bundle_id, fill.filled_at);
        self.refresh_hedge_record_from_position(bundle_id, fill.filled_at);
        Ok(application)
    }

    pub fn upsert_open_hedge_record_for_bundle(
        &mut self,
        bundle_id: &str,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(position) = self.position_manager.bundle(bundle_id) else {
            return false;
        };
        Self::upsert_hedge_record_for_position(
            &mut self.hedge_records,
            position,
            position.opened_at.or(Some(now)),
            position.opened_at.or(Some(now)),
            now,
        );
        self.refresh_hedge_record_from_position(bundle_id, now);
        true
    }

    pub fn update_bundle_open_leg_route(
        &mut self,
        bundle_id: &str,
        position_side: PositionSide,
        exchange: ExchangeId,
        exchange_symbol: crate::market::ExchangeSymbol,
        intended_qty: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        self.position_manager.replace_open_leg_route(
            bundle_id,
            position_side,
            exchange.clone(),
            exchange_symbol,
            intended_qty,
            now,
        )?;
        if let Some(bundle) = self.open_bundles.get_mut(bundle_id) {
            match position_side {
                PositionSide::Long => {
                    bundle.route.long_exchange = exchange.clone();
                    if matches!(bundle.route.maker_side, StrategyOrderSide::Buy) {
                        bundle.route.maker_exchange = exchange;
                    } else {
                        bundle.route.taker_exchange = exchange;
                    }
                }
                PositionSide::Short => {
                    bundle.route.short_exchange = exchange.clone();
                    if matches!(bundle.route.maker_side, StrategyOrderSide::Sell) {
                        bundle.route.maker_exchange = exchange;
                    } else {
                        bundle.route.taker_exchange = exchange;
                    }
                }
                PositionSide::Net => {}
            }
            bundle.updated_at = now;
        }
        self.refresh_hedge_record_from_position(bundle_id, now);
        Ok(())
    }

    pub fn import_persisted_hedge_state(&mut self, now: DateTime<Utc>) -> (usize, usize) {
        let Ok(events) = load_jsonl_storage_events(&self.config.persistence.jsonl_dir) else {
            return (0, 0);
        };
        let mut records = 0usize;
        let mut tasks = 0usize;
        for stored in events {
            match stored.event {
                CrossArbStorageEvent::HedgeRecord(record) => {
                    if should_import_hedge_record(&record) {
                        self.hedge_records.insert(record.bundle_id.clone(), record);
                    }
                    records += 1;
                }
                CrossArbStorageEvent::HedgeRepairTask(task) => {
                    self.hedge_repair_tasks.insert(task.task_id.clone(), task);
                    tasks += 1;
                }
                _ => {}
            }
        }
        let bundle_ids = self.hedge_records.keys().cloned().collect::<Vec<_>>();
        for bundle_id in bundle_ids {
            self.refresh_hedge_record_from_position(&bundle_id, now);
        }
        self.remove_invalid_open_hedge_records(now);
        self.dedupe_open_hedge_records_by_pair(now);
        (records, tasks)
    }

    pub fn ingest_private_event(&mut self, event: PrivateEvent) {
        let exchange = event.exchange.clone();
        let received_at = event.received_at;
        let needs_resync = {
            let state = self.account_sync.entry(exchange.clone()).or_default();
            state.apply_private_event(event.clone());
            state.needs_resync(&exchange, received_at)
        };

        if matches!(
            &event.kind,
            PrivateEventKind::StreamDisconnected { .. } | PrivateEventKind::Error(_)
        ) {
            self.update_private_stream_disconnect(exchange.clone(), received_at);
        } else {
            self.update_private_stream_health(exchange.clone(), received_at, needs_resync);
        }
        if self.config.risk.block_on_external_account_exposure {
            self.record_private_reconciliation_from_account_state(&exchange, received_at);
        }
        self.reconcile_open_bundles_from_account_positions(received_at);

        if matches!(
            &event.kind,
            PrivateEventKind::StreamDisconnected { .. } | PrivateEventKind::Error(_)
        ) {
            self.risk_events.push(RiskEventReadModel {
                event_id: format!(
                    "private-{}-{}",
                    exchange.as_str(),
                    received_at.timestamp_millis()
                ),
                canonical_symbol: None,
                exchange: Some(exchange.clone()),
                reason: super::risk::RejectReason::RouteUnhealthy,
                message: format!("private stream event: {:?}", &event.kind),
                created_at: received_at,
            });
        }

        self.updated_at = received_at;
    }

    fn reconcile_open_bundles_from_account_positions(&mut self, checked_at: DateTime<Utc>) {
        let exchange_positions = self.account_exchange_positions();
        if exchange_positions.is_empty() {
            return;
        }
        self.restore_open_bundles_from_account_positions(&exchange_positions, checked_at);

        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let bundle_ids = self
            .open_bundles
            .iter()
            .filter(|(_, bundle)| {
                matches!(
                    bundle.status,
                    SimulatedBundleStatus::Hedging
                        | SimulatedBundleStatus::OrphanLeg
                        | SimulatedBundleStatus::MakerFilled
                        | SimulatedBundleStatus::OpenSimulated
                )
            })
            .map(|(bundle_id, _)| bundle_id.clone())
            .collect::<Vec<_>>();

        for bundle_id in bundle_ids {
            let Ok(reconciled) = self
                .position_manager
                .reconcile_bundle_open_from_exchange_positions(
                    &bundle_id,
                    &exchange_positions,
                    quantity_tolerance,
                    checked_at,
                )
            else {
                continue;
            };
            if reconciled {
                self.refresh_bundle_status_from_position(&bundle_id, checked_at);
                self.history.retain(|item| item.bundle_id != bundle_id);
            }
        }
    }

    pub fn reconcile_unpaired_exchange_positions_from_account_state(
        &mut self,
        checked_at: DateTime<Utc>,
    ) {
        let exchange_positions = self.account_exchange_positions();
        if exchange_positions.is_empty() {
            return;
        }
        self.restore_open_bundles_from_account_positions(&exchange_positions, checked_at);
        self.record_unpaired_exchange_positions(&exchange_positions, checked_at);
    }

    pub fn account_unpaired_exchange_positions(&self) -> Vec<ExchangePosition> {
        let exchange_positions = self.account_exchange_positions();
        self.unpaired_exchange_positions(&exchange_positions)
    }

    fn account_exchange_positions(&self) -> Vec<ExchangePosition> {
        self.account_sync
            .values()
            .flat_map(AccountSyncState::position_snapshots)
            .filter(|position| {
                position.quantity.abs() > self.config.reconciliation.quantity_tolerance
            })
            .map(|position| ExchangePosition {
                exchange: position.exchange.clone(),
                canonical_symbol: position.canonical_symbol.clone(),
                exchange_symbol: position.exchange_symbol.clone().unwrap_or_else(|| {
                    crate::market::exchange_symbol_for(
                        &position.exchange,
                        &position.canonical_symbol,
                    )
                }),
                position_side: position.position_side,
                quantity: position.quantity,
                entry_price: position.entry_price,
                mark_price: None,
                unrealized_pnl: position.unrealized_pnl,
                updated_at: position.updated_at,
            })
            .collect()
    }

    fn restore_open_bundles_from_account_positions(
        &mut self,
        exchange_positions: &[ExchangePosition],
        checked_at: DateTime<Utc>,
    ) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let mut claimed = HashMap::new();
        for long_position in exchange_positions.iter().filter(|position| {
            position.position_side == crate::execution::PositionSide::Long
                && position.quantity.abs() > quantity_tolerance
                && position.entry_price.unwrap_or_default() > 0.0
        }) {
            for short_position in exchange_positions.iter().filter(|position| {
                position.position_side == crate::execution::PositionSide::Short
                    && position.quantity.abs() > quantity_tolerance
                    && position.entry_price.unwrap_or_default() > 0.0
                    && position.canonical_symbol == long_position.canonical_symbol
                    && position.exchange != long_position.exchange
            }) {
                let long_available = remaining_position_qty(&claimed, long_position);
                let short_available = remaining_position_qty(&claimed, short_position);
                if long_available <= quantity_tolerance || short_available <= quantity_tolerance {
                    continue;
                }
                if !self
                    .config
                    .universe
                    .symbols
                    .contains(&long_position.canonical_symbol)
                {
                    continue;
                }
                if let Some(existing_bundle_id) =
                    self.active_bundle_id_for_position_pair(long_position, short_position)
                {
                    let Some(restore_qty) = self.bundle_restore_quantity(
                        &existing_bundle_id,
                        long_available,
                        short_available,
                    ) else {
                        continue;
                    };
                    let long_claim_position =
                        exchange_position_with_quantity(long_position, restore_qty);
                    let short_claim_position =
                        exchange_position_with_quantity(short_position, restore_qty);
                    self.ensure_position_for_existing_bundle(
                        &existing_bundle_id,
                        &long_claim_position,
                        &short_claim_position,
                        checked_at,
                    );
                    let _ = self
                        .position_manager
                        .reconcile_bundle_open_from_exchange_positions(
                            &existing_bundle_id,
                            &[long_claim_position.clone(), short_claim_position.clone()],
                            quantity_tolerance,
                            checked_at,
                        );
                    self.upsert_open_hedge_record_for_bundle(&existing_bundle_id, checked_at);
                    claim_position_qty(&mut claimed, long_position, restore_qty);
                    claim_position_qty(&mut claimed, short_position, restore_qty);
                    continue;
                }

                if let Some(record) = self
                    .preferred_open_hedge_record_for_position_pair(long_position, short_position)
                {
                    let bundle_id = record.bundle_id.clone();
                    let Some(restore_qty) =
                        self.record_restore_quantity(&record, long_available, short_available)
                    else {
                        continue;
                    };
                    let long_claim_position =
                        exchange_position_with_quantity(long_position, restore_qty);
                    let short_claim_position =
                        exchange_position_with_quantity(short_position, restore_qty);
                    self.restore_bundle_from_position_pair(
                        &bundle_id,
                        format!("restored-{}", bundle_id),
                        record.opened_at,
                        record.entry_net_edge_pct.unwrap_or_default(),
                        &long_claim_position,
                        &short_claim_position,
                        checked_at,
                    );
                    claim_position_qty(&mut claimed, long_position, restore_qty);
                    claim_position_qty(&mut claimed, short_position, restore_qty);
                    if self.config.risk.restored_position_blocks_new_entries {
                        self.set_close_only();
                    }
                    continue;
                }

                self.risk_events.push(RiskEventReadModel {
                    event_id: format!(
                        "external-position-pair-{}-{}-{}-{}",
                        long_position
                            .canonical_symbol
                            .to_string()
                            .replace('/', ""),
                        long_position.exchange.as_str(),
                        short_position.exchange.as_str(),
                        checked_at.timestamp_millis()
                    ),
                    canonical_symbol: Some(long_position.canonical_symbol.clone()),
                    exchange: None,
                    reason: RejectReason::UnpairedExchangePosition,
                    message: format!(
                        "external account position pair ignored: long {} {} qty={} short {} {} qty={}; strategy only manages its own bundles",
                        long_position.exchange,
                        long_position.canonical_symbol,
                        long_position.quantity,
                        short_position.exchange,
                        short_position.canonical_symbol,
                        short_position.quantity
                    ),
                    created_at: checked_at,
                });
                if self.config.risk.orphan_exposure_blocks_new_entries {
                    self.set_close_only();
                }
            }
        }
        self.restore_open_single_leg_bundles_from_account_positions(
            exchange_positions,
            &mut claimed,
            checked_at,
        );
        self.complete_satisfied_open_hedge_repair_tasks(checked_at);
        self.ensure_open_hedge_repair_tasks(checked_at);
        self.dedupe_open_hedge_records_by_pair(checked_at);
    }

    fn bundle_restore_quantity(
        &self,
        bundle_id: &str,
        long_available: f64,
        short_available: f64,
    ) -> Option<f64> {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let available_qty = long_available.min(short_available);
        if available_qty <= quantity_tolerance || !available_qty.is_finite() {
            return None;
        }
        let planned_qty = self
            .position_manager
            .bundle(bundle_id)
            .map(|position| planned_open_qty(position))
            .filter(|quantity| *quantity > quantity_tolerance && quantity.is_finite());
        let quantity = planned_qty.unwrap_or(available_qty).min(available_qty);
        (quantity > quantity_tolerance).then_some(quantity)
    }

    fn record_restore_quantity(
        &self,
        record: &HedgeRecordReadModel,
        long_available: f64,
        short_available: f64,
    ) -> Option<f64> {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let available_qty = long_available.min(short_available);
        if available_qty <= quantity_tolerance || !available_qty.is_finite() {
            return None;
        }
        let repair_qty = self
            .open_repair_quantity_for_record(&record.bundle_id)
            .filter(|quantity| *quantity > quantity_tolerance && quantity.is_finite());
        let quantity = repair_qty.unwrap_or(available_qty).min(available_qty);
        (quantity > quantity_tolerance).then_some(quantity)
    }

    fn preferred_open_hedge_record_for_position_pair(
        &self,
        long_position: &ExchangePosition,
        short_position: &ExchangePosition,
    ) -> Option<HedgeRecordReadModel> {
        self.hedge_records
            .values()
            .filter(|record| {
                !record.is_closed
                    && !matches!(record.status, HedgeRecordStatus::Closed)
                    && record.canonical_symbol == long_position.canonical_symbol
                    && record.long_exchange == long_position.exchange
                    && record.short_exchange == short_position.exchange
            })
            .max_by(|left, right| compare_hedge_record_reuse_priority(left, right))
            .cloned()
    }

    fn restore_open_single_leg_bundles_from_account_positions(
        &mut self,
        exchange_positions: &[ExchangePosition],
        claimed: &mut HashMap<PositionClaimKey, f64>,
        checked_at: DateTime<Utc>,
    ) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let positions = exchange_positions
            .iter()
            .filter(|position| {
                matches!(
                    position.position_side,
                    PositionSide::Long | PositionSide::Short
                ) && position.quantity.abs() > quantity_tolerance
                    && position.entry_price.unwrap_or_default() > 0.0
                    && self
                        .config
                        .universe
                        .symbols
                        .contains(&position.canonical_symbol)
                    && self
                        .config
                        .universe
                        .enabled_exchanges
                        .contains(&position.exchange)
            })
            .cloned()
            .collect::<Vec<_>>();
        for position in positions {
            if remaining_position_qty(claimed, &position) <= quantity_tolerance {
                continue;
            }
            let records = self.open_hedge_records_for_single_position(&position);
            for record in records {
                let available_qty = remaining_position_qty(claimed, &position);
                if available_qty <= quantity_tolerance {
                    break;
                }
                let restore_qty = self
                    .open_repair_quantity_for_record(&record.bundle_id)
                    .unwrap_or(available_qty)
                    .min(available_qty);
                if restore_qty <= quantity_tolerance || !restore_qty.is_finite() {
                    continue;
                }
                self.restore_bundle_from_single_position(
                    &record,
                    &position,
                    restore_qty,
                    checked_at,
                );
                claim_position_qty(claimed, &position, restore_qty);
                self.set_close_only();
            }
        }
    }

    fn open_hedge_records_for_single_position(
        &self,
        position: &ExchangePosition,
    ) -> Vec<HedgeRecordReadModel> {
        let mut records = self
            .hedge_records
            .values()
            .filter(|record| {
                should_keep_loaded_hedge_record(record)
                    && !self.position_manager.contains_bundle(&record.bundle_id)
                    && record.canonical_symbol == position.canonical_symbol
                    && self.hedge_record_can_claim_single_position_for_open_repair(record, position)
                    && (hedge_record_expects_open_repair(record)
                        || self.has_active_open_hedge_repair_task(&record.bundle_id))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| compare_hedge_record_reuse_priority(right, left));
        records
    }

    fn hedge_record_can_claim_single_position_for_open_repair(
        &self,
        record: &HedgeRecordReadModel,
        position: &ExchangePosition,
    ) -> bool {
        if !hedge_record_can_claim_single_position(record, position) {
            return false;
        }

        let active_open_tasks = self
            .hedge_repair_tasks
            .values()
            .filter(|task| {
                task.bundle_id == record.bundle_id
                    && !task.reduce_only
                    && !matches!(task.status, HedgeRepairTaskStatus::Completed)
            })
            .collect::<Vec<_>>();

        if active_open_tasks.is_empty() {
            return true;
        }

        active_open_tasks
            .iter()
            .any(|task| repair_task_expects_opposite_existing_leg(task, position))
    }

    fn open_repair_quantity_for_record(&self, bundle_id: &str) -> Option<f64> {
        self.hedge_repair_tasks
            .values()
            .filter(|task| {
                task.bundle_id == bundle_id
                    && !task.reduce_only
                    && !matches!(task.status, HedgeRepairTaskStatus::Completed)
                    && task.quantity > 0.0
                    && task.quantity.is_finite()
            })
            .map(|task| task.quantity)
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn restore_bundle_from_single_position(
        &mut self,
        record: &HedgeRecordReadModel,
        position: &ExchangePosition,
        quantity: f64,
        checked_at: DateTime<Utc>,
    ) {
        let bundle_id = record.bundle_id.clone();
        let entry_price = position.entry_price.unwrap_or_default();
        let target_notional = quantity * entry_price.max(0.0);
        if !self.position_manager.contains_bundle(&bundle_id) {
            let mut bundle = ArbitrageBundle::new(
                bundle_id.clone(),
                self.config.mode,
                position.canonical_symbol.clone(),
                record.long_exchange.clone(),
                record.short_exchange.clone(),
                position.exchange.clone(),
                single_position_opposite_exchange(record, position),
                target_notional,
                checked_at,
            );
            bundle.status = BundleStatus::OrphanLeg;
            bundle.open_time = record.opened_at.or(Some(checked_at));
            self.register_bundle_position(
                &bundle,
                quantity,
                record.entry_net_edge_pct.unwrap_or_default(),
                checked_at,
            );
        }

        let _ = self.position_manager.replace_open_leg_route(
            &bundle_id,
            position.position_side,
            position.exchange.clone(),
            position.exchange_symbol.clone(),
            quantity,
            checked_at,
        );
        let _ = self.position_manager.record_leg_fill(
            &bundle_id,
            position.position_side,
            quantity,
            entry_price,
            0.0,
            checked_at,
        );
        let _ = self.position_manager.mark_bundle_status(
            &bundle_id,
            BundleStatus::OrphanLeg,
            checked_at,
        );

        let route = route_for_single_position(record, position);
        self.open_bundles
            .entry(bundle_id.clone())
            .and_modify(|bundle| {
                bundle.status = SimulatedBundleStatus::OrphanLeg;
                bundle.route = route.clone();
                bundle.target_notional_usdt = target_notional;
                bundle.opened_at = bundle.opened_at.or(record.opened_at).or(Some(checked_at));
                bundle.updated_at = checked_at;
            })
            .or_insert_with(|| SimulatedBundleState {
                bundle_id: bundle_id.clone(),
                opportunity_id: format!("restored-{}", bundle_id),
                status: SimulatedBundleStatus::OrphanLeg,
                route,
                target_notional_usdt: target_notional,
                opened_at: record.opened_at.or(Some(checked_at)),
                updated_at: checked_at,
            });
        self.refresh_hedge_record_from_position(&bundle_id, checked_at);
    }

    fn restore_bundle_from_position_pair(
        &mut self,
        bundle_id: &str,
        opportunity_id: String,
        opened_at: Option<DateTime<Utc>>,
        entry_edge_pct: f64,
        long_position: &ExchangePosition,
        short_position: &ExchangePosition,
        checked_at: DateTime<Utc>,
    ) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let target_notional =
            long_position.quantity.abs() * long_position.entry_price.unwrap_or_default().max(0.0);
        if !self.position_manager.contains_bundle(bundle_id) {
            let mut bundle = ArbitrageBundle::new(
                bundle_id.to_string(),
                self.config.mode,
                long_position.canonical_symbol.clone(),
                long_position.exchange.clone(),
                short_position.exchange.clone(),
                long_position.exchange.clone(),
                short_position.exchange.clone(),
                target_notional,
                checked_at,
            );
            bundle.status = BundleStatus::OpenSimulated;
            bundle.open_time = opened_at.or(Some(checked_at));
            self.register_bundle_position(
                &bundle,
                long_position.quantity.abs(),
                entry_edge_pct,
                checked_at,
            );
        }
        let _ = self
            .position_manager
            .reconcile_bundle_open_from_exchange_positions(
                bundle_id,
                &[long_position.clone(), short_position.clone()],
                quantity_tolerance,
                checked_at,
            );
        self.open_bundles
            .entry(bundle_id.to_string())
            .and_modify(|bundle| {
                bundle.status = SimulatedBundleStatus::OpenSimulated;
                bundle.route.long_exchange = long_position.exchange.clone();
                bundle.route.short_exchange = short_position.exchange.clone();
                bundle.target_notional_usdt = target_notional;
                bundle.opened_at = bundle.opened_at.or(opened_at).or(Some(checked_at));
                bundle.updated_at = checked_at;
            })
            .or_insert_with(|| SimulatedBundleState {
                bundle_id: bundle_id.to_string(),
                opportunity_id,
                status: SimulatedBundleStatus::OpenSimulated,
                route: super::StrategyRoute {
                    long_exchange: long_position.exchange.clone(),
                    short_exchange: short_position.exchange.clone(),
                    maker_exchange: long_position.exchange.clone(),
                    taker_exchange: short_position.exchange.clone(),
                    maker_side: StrategyOrderSide::Buy,
                    taker_side: StrategyOrderSide::Sell,
                    maker_leg_kind: super::MakerLegKind::LongMakerBuy,
                },
                target_notional_usdt: target_notional,
                opened_at: opened_at.or(Some(checked_at)),
                updated_at: checked_at,
            });
        self.upsert_open_hedge_record_for_bundle(bundle_id, checked_at);
    }

    fn dedupe_open_hedge_records_by_pair(&mut self, now: DateTime<Utc>) {
        let mut preferred: HashMap<(CanonicalSymbol, ExchangeId, ExchangeId), String> =
            HashMap::new();
        for record in self.hedge_records.values() {
            if record.is_closed || matches!(record.status, HedgeRecordStatus::Closed) {
                continue;
            }
            let key = (
                record.canonical_symbol.clone(),
                record.long_exchange.clone(),
                record.short_exchange.clone(),
            );
            let replace = preferred
                .get(&key)
                .and_then(|current_id| self.hedge_records.get(current_id))
                .map(|current| compare_hedge_record_reuse_priority(record, current).is_gt())
                .unwrap_or(true);
            if replace {
                preferred.insert(key, record.bundle_id.clone());
            }
        }
        let preferred_ids = preferred.values().cloned().collect::<HashSet<_>>();
        let duplicate_ids = self
            .hedge_records
            .values()
            .filter(|record| {
                !record.is_closed
                    && !matches!(record.status, HedgeRecordStatus::Closed)
                    && preferred.contains_key(&(
                        record.canonical_symbol.clone(),
                        record.long_exchange.clone(),
                        record.short_exchange.clone(),
                    ))
                    && !preferred_ids.contains(&record.bundle_id)
            })
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();

        for bundle_id in duplicate_ids {
            self.remove_open_hedge_record(&bundle_id, "superseded by restored hedge record", now);
        }
    }

    fn remove_invalid_open_hedge_records(&mut self, now: DateTime<Utc>) {
        let invalid_ids = self
            .hedge_records
            .values()
            .filter(|record| !should_keep_loaded_hedge_record(record))
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();
        for bundle_id in invalid_ids {
            self.remove_open_hedge_record(&bundle_id, "invalid cross-exchange hedge record", now);
        }
    }

    fn remove_open_hedge_record(&mut self, bundle_id: &str, reason: &str, now: DateTime<Utc>) {
        self.hedge_records.remove(bundle_id);
        self.open_bundles.remove(bundle_id);
        self.position_manager.remove_bundle(bundle_id);
        for task in self
            .hedge_repair_tasks
            .values_mut()
            .filter(|task| task.bundle_id == bundle_id)
        {
            task.status = HedgeRepairTaskStatus::Completed;
            task.last_error = Some(reason.to_string());
            task.updated_at = now;
        }
    }

    fn record_unpaired_exchange_positions(
        &mut self,
        exchange_positions: &[ExchangePosition],
        checked_at: DateTime<Utc>,
    ) {
        let unpaired = self.unpaired_exchange_positions(exchange_positions);
        let should_close_only = self.config.risk.orphan_exposure_blocks_new_entries
            && unpaired.len() >= self.config.risk.orphan_close_only_after_count.max(1);
        for position in unpaired {
            self.risk_events.push(RiskEventReadModel {
                event_id: format!(
                    "unpaired-position-{}-{}-{:?}-{}",
                    position.exchange.as_str(),
                    position.canonical_symbol.to_string().replace('/', ""),
                    position.position_side,
                    checked_at.timestamp_millis()
                ),
                canonical_symbol: Some(position.canonical_symbol.clone()),
                exchange: Some(position.exchange.clone()),
                reason: RejectReason::UnpairedExchangePosition,
                message: format!(
                    "unpaired exchange position detected: exchange={} symbol={} side={:?} qty={}; {}",
                    position.exchange,
                    position.canonical_symbol,
                    position.position_side,
                    position.quantity,
                    if should_close_only {
                        "close-only enabled"
                    } else {
                        "ignored because orphan_exposure_blocks_new_entries=false"
                    }
                ),
                created_at: checked_at,
            });
            if should_close_only {
                self.set_close_only();
            }
        }
    }

    fn unpaired_exchange_positions(
        &self,
        exchange_positions: &[ExchangePosition],
    ) -> Vec<ExchangePosition> {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let mut claimed = HashMap::new();
        for bundle in self
            .position_manager
            .bundles()
            .filter(|bundle| !bundle.status.is_terminal())
        {
            if bundle.long_leg.filled_qty > quantity_tolerance {
                *claimed
                    .entry((
                        bundle.long_leg.exchange.clone(),
                        bundle.canonical_symbol.clone(),
                        PositionSide::Long,
                    ))
                    .or_default() += bundle.long_leg.filled_qty;
            }
            if bundle.short_leg.filled_qty > quantity_tolerance {
                *claimed
                    .entry((
                        bundle.short_leg.exchange.clone(),
                        bundle.canonical_symbol.clone(),
                        PositionSide::Short,
                    ))
                    .or_default() += bundle.short_leg.filled_qty;
            }
        }
        exchange_positions
            .iter()
            .filter(|position| {
                position.quantity.abs() > quantity_tolerance
                    && self
                        .config
                        .universe
                        .symbols
                        .contains(&position.canonical_symbol)
                    && self
                        .config
                        .universe
                        .enabled_exchanges
                        .contains(&position.exchange)
                    && remaining_position_qty(&claimed, position) > quantity_tolerance
            })
            .map(|position| {
                exchange_position_with_quantity(
                    position,
                    remaining_position_qty(&claimed, position),
                )
            })
            .collect()
    }

    fn has_active_bundle_for_position_pair(
        &self,
        long_position: &ExchangePosition,
        short_position: &ExchangePosition,
    ) -> bool {
        self.active_bundle_id_for_position_pair(long_position, short_position)
            .is_some()
    }

    fn active_bundle_id_for_position_pair(
        &self,
        long_position: &ExchangePosition,
        short_position: &ExchangePosition,
    ) -> Option<String> {
        if let Some(bundle) = self.position_manager.bundles().find(|bundle| {
            !bundle.status.is_terminal()
                && bundle.canonical_symbol == long_position.canonical_symbol
                && bundle.long_leg.exchange == long_position.exchange
                && bundle.short_leg.exchange == short_position.exchange
        }) {
            return Some(bundle.bundle_id.clone());
        }

        self.open_bundles
            .values()
            .find(|bundle| {
                is_active_bundle_status(bundle.status)
                    && bundle.route.long_exchange == long_position.exchange
                    && bundle.route.short_exchange == short_position.exchange
                    && self
                        .bundle_symbol(bundle)
                        .as_ref()
                        .map(|symbol| symbol == &long_position.canonical_symbol)
                        .unwrap_or(false)
            })
            .map(|bundle| bundle.bundle_id.clone())
    }

    fn ensure_position_for_existing_bundle(
        &mut self,
        bundle_id: &str,
        long_position: &ExchangePosition,
        short_position: &ExchangePosition,
        checked_at: DateTime<Utc>,
    ) {
        if self.position_manager.contains_bundle(bundle_id) {
            return;
        }
        let Some(existing) = self.open_bundles.get(bundle_id).cloned() else {
            return;
        };
        let target_notional =
            long_position.quantity.abs() * long_position.entry_price.unwrap_or_default().max(0.0);
        let mut bundle = ArbitrageBundle::new(
            existing.bundle_id.clone(),
            self.config.mode,
            long_position.canonical_symbol.clone(),
            long_position.exchange.clone(),
            short_position.exchange.clone(),
            existing.route.maker_exchange,
            existing.route.taker_exchange,
            target_notional.max(existing.target_notional_usdt),
            checked_at,
        );
        bundle.status = BundleStatus::OpenSimulated;
        bundle.open_time = existing.opened_at.or(Some(checked_at));
        self.register_bundle_position(&bundle, long_position.quantity.abs(), 0.0, checked_at);
    }

    fn bundle_symbol(&self, bundle: &SimulatedBundleState) -> Option<CanonicalSymbol> {
        self.position_manager
            .bundle(&bundle.bundle_id)
            .map(|position| position.canonical_symbol.clone())
            .or_else(|| {
                self.opportunities
                    .iter()
                    .find(|opportunity| opportunity.opportunity_id == bundle.opportunity_id)
                    .map(|opportunity| opportunity.canonical_symbol.clone())
            })
            .or_else(|| symbol_from_bundle_id(&bundle.bundle_id))
    }

    pub fn reconcile_positions(
        &self,
        exchange_positions: &[ExchangePosition],
        quantity_tolerance: f64,
        orphan_tolerance: f64,
        checked_at: DateTime<Utc>,
    ) -> Vec<PositionReconcileDecision> {
        self.position_manager.reconcile_exchange_positions(
            exchange_positions,
            quantity_tolerance,
            orphan_tolerance,
            checked_at,
        )
    }

    pub fn position_summary(&self, quantity_tolerance: f64) -> PortfolioExposureSummary {
        self.position_manager
            .portfolio_exposure_summary(quantity_tolerance)
    }

    pub fn private_positions(&self, exchange: &ExchangeId) -> Vec<PositionSnapshot> {
        self.account_sync
            .get(exchange)
            .map(AccountSyncState::position_snapshots)
            .unwrap_or_default()
    }

    pub fn opportunity_read_models(&self) -> Vec<OpportunityReadModel> {
        self.opportunities
            .iter()
            .map(OpportunityReadModel::from)
            .collect()
    }

    pub fn open_bundle_read_models(&self) -> Vec<BundleReadModel> {
        self.open_bundles
            .values()
            .filter(|bundle| bundle.status != SimulatedBundleStatus::Closed)
            .map(BundleReadModel::from)
            .collect()
    }

    pub fn history_read_models(&self) -> Vec<BundleReadModel> {
        self.history.iter().map(BundleReadModel::from).collect()
    }

    pub fn pause_new_entries(&mut self) {
        self.paused_new_entries = true;
        self.risk_state.pause_new_entries(self.updated_at);
        self.sync_local_controls();
    }

    pub fn resume_new_entries(&mut self) {
        self.paused_new_entries = false;
        self.close_only = false;
        self.risk_state.paused_new_entries = false;
        self.risk_state.close_only = false;
        self.risk_state.recompute_mode(self.updated_at);
        self.sync_local_controls();
    }

    pub fn clear_close_only(&mut self) {
        self.close_only = false;
        self.risk_state.close_only = false;
        self.risk_state.recompute_mode(self.updated_at);
        self.paused_new_entries = self.risk_state.paused_new_entries;
        self.close_only = self.risk_state.close_only;
        self.kill_switch = self.risk_state.kill_switch;
    }

    pub fn set_close_only(&mut self) {
        self.close_only = true;
        self.risk_state.set_close_only(self.updated_at);
        self.sync_local_controls();
    }

    pub fn kill_switch(&mut self) {
        self.kill_switch = true;
        self.paused_new_entries = true;
        self.close_only = true;
        self.risk_state.kill_switch(self.updated_at);
        self.sync_local_controls();
        self.mark_open_bundles_risk_stopped(self.updated_at);
    }

    pub fn update_private_stream_health(
        &mut self,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
        needs_resync: bool,
    ) {
        let stale_after_ms = self.private_ws_stale_after_ms(&exchange);
        self.risk_state
            .observe_private_event(exchange, received_at, needs_resync, stale_after_ms);
        self.updated_at = received_at;
        self.sync_local_controls();
    }

    pub fn update_private_stream_disconnect(
        &mut self,
        exchange: ExchangeId,
        disconnected_at: DateTime<Utc>,
    ) {
        let stale_after_ms = self.private_ws_stale_after_ms(&exchange);
        self.risk_state
            .observe_private_disconnect(exchange, disconnected_at, stale_after_ms);
        self.updated_at = disconnected_at;
        self.sync_local_controls();
    }

    fn private_ws_stale_after_ms(&self, exchange: &ExchangeId) -> i64 {
        self.config
            .exchanges
            .get(exchange)
            .map(|runtime| runtime.private_ws_run.stale_after_ms as i64)
            .unwrap_or(45_000)
            .max(1)
    }

    pub fn ingest_order_state(&mut self, order: OrderState) {
        let exchange = order.exchange.clone();
        let updated_at = order.updated_at;
        let state = self.account_sync.entry(exchange.clone()).or_default();
        state.apply_order_state(order);
        self.update_private_stream_health(exchange, updated_at, false);
    }

    pub fn replace_account_positions_from_rest(
        &mut self,
        exchange: ExchangeId,
        positions: &[ExchangePosition],
        fetched_at: DateTime<Utc>,
    ) {
        let snapshots = positions
            .iter()
            .filter(|position| position.exchange == exchange)
            .cloned()
            .map(PositionSnapshot::from_exchange_position)
            .collect::<Vec<_>>();
        self.account_sync
            .entry(exchange.clone())
            .or_default()
            .replace_exchange_positions(exchange.clone(), snapshots, fetched_at);
        self.update_private_stream_health(exchange, fetched_at, false);
    }

    pub fn record_reconciliation_result(
        &mut self,
        exchange: ExchangeId,
        symbol: String,
        severity: crate::execution::ReconcileSeverity,
        checked_at: DateTime<Utc>,
    ) {
        self.risk_state.record_reconciliation_severity(
            exchange,
            symbol,
            severity,
            self.config.risk.orphan_exposure_blocks_new_entries,
            checked_at,
        );
        self.updated_at = checked_at;
        self.sync_local_controls();
    }

    pub fn record_orphan_exposure(&mut self, now: DateTime<Utc>) {
        let summary = self
            .position_manager
            .portfolio_exposure_summary(self.config.reconciliation.quantity_tolerance);
        self.ensure_open_hedge_repair_tasks(now);
        self.risk_state.record_orphan_exposure(
            &summary,
            self.config.reconciliation.orphan_tolerance,
            self.config.risk.orphan_exposure_blocks_new_entries,
            self.config.risk.orphan_close_only_after_count,
            now,
        );
        self.updated_at = now;
        self.sync_local_controls();
        if self.risk_state.decision().mode == super::risk::RiskOperatingMode::Halted {
            self.mark_open_bundles_risk_stopped(now);
        }
    }

    fn ensure_open_hedge_repair_tasks(&mut self, now: DateTime<Utc>) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let commands = self
            .position_manager
            .bundles()
            .filter(|position| {
                !position.status.is_terminal() && position.unhedged_qty() > quantity_tolerance
            })
            .filter(|position| !self.has_active_open_hedge_repair_task(&position.bundle_id))
            .filter_map(|position| self.open_hedge_repair_command_for_position(position, now))
            .collect::<Vec<_>>();

        for command in commands {
            self.record_hedge_repair_required(
                &command,
                "open hedge position is still unbalanced after maker fill",
                now,
            );
        }
    }

    fn has_active_open_hedge_repair_task(&self, bundle_id: &str) -> bool {
        self.hedge_repair_tasks.values().any(|task| {
            task.bundle_id == bundle_id
                && !task.reduce_only
                && !matches!(task.status, HedgeRepairTaskStatus::Completed)
        })
    }

    fn complete_satisfied_open_hedge_repair_tasks(&mut self, now: DateTime<Utc>) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let bundle_ids = self
            .hedge_repair_tasks
            .values()
            .filter(|task| {
                !task.reduce_only && !matches!(task.status, HedgeRepairTaskStatus::Completed)
            })
            .filter_map(|task| self.position_manager.bundle(&task.bundle_id))
            .filter(|position| position.is_fully_open(quantity_tolerance))
            .map(|position| position.bundle_id.clone())
            .collect::<HashSet<_>>();

        for bundle_id in bundle_ids {
            self.refresh_hedge_record_from_position(&bundle_id, now);
        }
    }

    fn open_hedge_repair_command_for_position(
        &self,
        position: &super::position::BundlePosition,
        now: DateTime<Utc>,
    ) -> Option<OrderCommand> {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let long_qty = position.long_leg.filled_qty;
        let short_qty = position.short_leg.filled_qty;
        let (exchange, exchange_symbol, intent, side, position_side, quantity) =
            if long_qty > short_qty + quantity_tolerance {
                (
                    position.short_leg.exchange.clone(),
                    position.short_leg.exchange_symbol.clone(),
                    crate::execution::OrderIntent::HedgeShortTaker,
                    OrderSide::Sell,
                    PositionSide::Short,
                    long_qty - short_qty,
                )
            } else if short_qty > long_qty + quantity_tolerance {
                (
                    position.long_leg.exchange.clone(),
                    position.long_leg.exchange_symbol.clone(),
                    crate::execution::OrderIntent::HedgeLongTaker,
                    OrderSide::Buy,
                    PositionSide::Long,
                    short_qty - long_qty,
                )
            } else {
                return None;
            };

        Some(OrderCommand::new(
            self.config.mode,
            position.bundle_id.clone(),
            BundleLeg::Hedge,
            2,
            exchange,
            position.canonical_symbol.clone(),
            exchange_symbol,
            intent,
            side,
            position_side,
            OrderType::Market,
            quantity,
            None,
            TimeInForce::Ioc,
            false,
            false,
            Some(self.config.execution.taker_ioc_slippage_limit_pct),
            now,
        ))
    }

    pub fn refresh_risk_state(&mut self, now: DateTime<Utc>) {
        self.risk_state.evaluate_private_health(now);
        self.updated_at = now;
        self.sync_local_controls();
        if self.risk_state.decision().mode == super::risk::RiskOperatingMode::Halted {
            self.mark_open_bundles_risk_stopped(now);
        }
    }

    fn sync_risk_state(&mut self, now: DateTime<Utc>) {
        self.record_orphan_exposure(now);
        self.refresh_risk_state(now);
    }

    fn record_private_reconciliation_from_account_state(
        &mut self,
        exchange: &ExchangeId,
        checked_at: DateTime<Utc>,
    ) {
        let Some(state) = self.account_sync.get(exchange) else {
            return;
        };

        let exchange_positions = state
            .position_snapshots()
            .into_iter()
            .map(|position| ExchangePosition {
                exchange: position.exchange,
                canonical_symbol: position.canonical_symbol.clone(),
                exchange_symbol: position.exchange_symbol.unwrap_or_else(|| {
                    crate::market::exchange_symbol_for(exchange, &position.canonical_symbol)
                }),
                position_side: position.position_side,
                quantity: position.quantity,
                entry_price: position.entry_price,
                mark_price: None,
                unrealized_pnl: position.unrealized_pnl,
                updated_at: position.updated_at,
            })
            .collect::<Vec<_>>();
        let decisions = self.reconcile_positions(
            &exchange_positions,
            self.config.reconciliation.quantity_tolerance,
            self.config.reconciliation.orphan_tolerance,
            checked_at,
        );

        for decision in decisions
            .into_iter()
            .filter(|decision| decision.severity != crate::execution::ReconcileSeverity::Ok)
        {
            self.record_reconciliation_result(
                decision.exchange,
                decision.canonical_symbol.to_string(),
                decision.severity,
                checked_at,
            );
        }
    }

    fn sync_local_controls(&mut self) {
        let decision = self.risk_state.decision();
        self.paused_new_entries = !decision.allow_new_entries;
        self.close_only = matches!(
            decision.mode,
            super::risk::RiskOperatingMode::CloseOnly | super::risk::RiskOperatingMode::Halted
        );
        self.kill_switch = matches!(decision.mode, super::risk::RiskOperatingMode::Halted);
    }

    fn mark_open_bundles_risk_stopped(&mut self, now: DateTime<Utc>) {
        for bundle in self.open_bundles.values_mut() {
            if bundle.status != SimulatedBundleStatus::Closed {
                bundle.status = SimulatedBundleStatus::RiskStopped;
                bundle.updated_at = now;
            }
        }
    }

    fn refresh_bundle_status_from_position(&mut self, bundle_id: &str, now: DateTime<Utc>) {
        let Some(position) = self.position_manager.bundle(bundle_id) else {
            return;
        };
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let Some(bundle) = self.open_bundles.get_mut(bundle_id) else {
            return;
        };
        if position.is_fully_closed(quantity_tolerance) || position.status == BundleStatus::Closed {
            bundle.status = SimulatedBundleStatus::Closed;
            bundle.updated_at = now;
            self.history.push(bundle.clone());
        } else if position.is_fully_open(quantity_tolerance) {
            bundle.status = SimulatedBundleStatus::OpenSimulated;
            bundle.opened_at.get_or_insert(now);
            bundle.updated_at = now;
        } else if position.unhedged_qty() > quantity_tolerance {
            bundle.status = SimulatedBundleStatus::OrphanLeg;
            bundle.updated_at = now;
        }
    }

    fn refresh_hedge_record_from_position(&mut self, bundle_id: &str, now: DateTime<Utc>) {
        let Some(position) = self.position_manager.bundle(bundle_id) else {
            return;
        };
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let Some(record) = self.hedge_records.get_mut(bundle_id) else {
            return;
        };

        record.canonical_symbol = position.canonical_symbol.clone();
        record.long_exchange = position.long_leg.exchange.clone();
        record.short_exchange = position.short_leg.exchange.clone();
        record.entry_net_edge_pct = Some(position.entry_edge_pct);
        if position.is_fully_closed(quantity_tolerance) || position.status == BundleStatus::Closed {
            record.status = HedgeRecordStatus::Closed;
            record.is_closed = true;
            record.closed_at.get_or_insert(now);
            record.updated_at = now;
            let close_profit = bundle_close_net_profit_pct(position);
            if close_profit.is_finite() {
                record.close_net_profit_pct = Some(close_profit);
            }
            record.close_spread_pct = position.realized_close_spread_pct();
            for task in self
                .hedge_repair_tasks
                .values_mut()
                .filter(|task| task.bundle_id == bundle_id)
            {
                task.status = HedgeRepairTaskStatus::Completed;
                task.updated_at = now;
            }
        } else if position.is_fully_open(quantity_tolerance) {
            record.status = HedgeRecordStatus::Hedged;
            record.is_closed = false;
            record.closed_at = None;
            record.close_spread_pct = None;
            record.close_net_profit_pct = None;
            record.updated_at = now;
            for task in self
                .hedge_repair_tasks
                .values_mut()
                .filter(|task| task.bundle_id == bundle_id && !task.reduce_only)
            {
                task.status = HedgeRepairTaskStatus::Completed;
                task.updated_at = now;
            }
        } else if position.unhedged_qty() > quantity_tolerance
            && !matches!(
                record.status,
                HedgeRecordStatus::RepairPending
                    | HedgeRecordStatus::RepairSubmitted
                    | HedgeRecordStatus::RepairFailed
            )
        {
            record.status = HedgeRecordStatus::Hedging;
            record.updated_at = now;
        }
    }

    fn route_read_models(&self) -> Vec<RouteReadModel> {
        self.opportunities
            .iter()
            .map(|opportunity| RouteReadModel {
                exchange: opportunity.maker_exchange.clone(),
                canonical_symbol: opportunity.canonical_symbol.clone(),
                status: opportunity.route_status,
                last_book_age_ms: opportunity.book_age_ms,
                reject_reasons: opportunity.reject_reasons.clone(),
            })
            .collect()
    }

    fn private_stream_health_read_models(&self) -> Vec<PrivateStreamHealthReadModel> {
        self.risk_state
            .private_stream_health
            .values()
            .map(PrivateStreamHealthReadModel::from)
            .collect()
    }

    pub fn is_private_stream_ready(&self, exchange: &ExchangeId) -> bool {
        self.risk_state
            .private_stream_health
            .get(exchange)
            .map(|health| health.last_event_at.is_some() && !health.needs_resync)
            .unwrap_or(false)
    }
}

#[derive(Debug, Default)]
pub struct CrossArbRuntime {
    pub state: CrossArbRuntimeState,
    pub storage: InMemoryStorageSink,
    persistent_storage: Option<AsyncJsonlStorageSink>,
}

impl CrossArbRuntime {
    pub fn new(config: CrossExchangeArbitrageConfig, now: DateTime<Utc>) -> Self {
        let persistent_storage = if config.persistence.enabled {
            AsyncJsonlStorageSink::for_day(&config.persistence.jsonl_dir, now).ok()
        } else {
            None
        };
        let mut state = CrossArbRuntimeState::new(config, now);
        if state.config.mode.allows_live_orders()
            && state.config.persistence.enabled
            && state.config.persistence.stop_live_on_unavailable
            && persistent_storage.is_none()
        {
            state.risk_state.set_close_only(now);
        }
        Self {
            state,
            storage: InMemoryStorageSink::default(),
            persistent_storage,
        }
    }

    pub fn on_market_snapshots(
        &mut self,
        canonical_symbol: &CanonicalSymbol,
        snapshots: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> Vec<ArbSignal> {
        if self.state.config.persistence.persist_market_snapshots {
            for snapshot in snapshots {
                let event = CrossArbStorageEvent::MarketSnapshot(
                    ArbitrageMarketSnapshotRecord::from(snapshot),
                );
                self.storage.record(event.clone(), now);
                if let Some(storage) = self.persistent_storage.as_mut() {
                    storage.record(event, now);
                }
            }
        }
        let signals = self
            .state
            .update_from_market_snapshots(canonical_symbol, snapshots, now);
        for opportunity in self
            .state
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.canonical_symbol == *canonical_symbol)
        {
            self.storage
                .record(CrossArbStorageEvent::Opportunity(opportunity.clone()), now);
            if let Some(storage) = self.persistent_storage.as_mut() {
                storage.record(CrossArbStorageEvent::Opportunity(opportunity.clone()), now);
            }
        }
        for signal in &signals {
            self.storage
                .record(CrossArbStorageEvent::Signal(signal.clone()), now);
            if let Some(storage) = self.persistent_storage.as_mut() {
                storage.record(CrossArbStorageEvent::Signal(signal.clone()), now);
            }
        }
        signals
    }

    pub fn record_instrument(
        &mut self,
        instrument: crate::market::InstrumentMeta,
        now: DateTime<Utc>,
    ) {
        self.storage
            .record(CrossArbStorageEvent::Instrument(instrument.clone()), now);
        if let Some(storage) = self.persistent_storage.as_mut() {
            storage.record(CrossArbStorageEvent::Instrument(instrument), now);
        }
    }

    pub fn on_private_event(&mut self, event: PrivateEvent) {
        let recorded_at = event.received_at;
        log_live_private_event(&self.state.config, &event);
        let typed_event = match &event.kind {
            PrivateEventKind::Order(order) => Some(CrossArbStorageEvent::OrderTransition(
                ArbitrageOrderRecord::from(order),
            )),
            PrivateEventKind::Fill(fill) => {
                Some(CrossArbStorageEvent::Fill(ArbitrageFillRecord::from(fill)))
            }
            _ => None,
        };
        self.state.ingest_private_event(event.clone());
        self.storage.record(
            CrossArbStorageEvent::PrivateEvent(event.clone()),
            recorded_at,
        );
        if let Some(storage) = self.persistent_storage.as_mut() {
            storage.record(CrossArbStorageEvent::PrivateEvent(event), recorded_at);
        }
        if let Some(typed_event) = typed_event {
            self.storage.record(typed_event.clone(), recorded_at);
            if let Some(storage) = self.persistent_storage.as_mut() {
                storage.record(typed_event, recorded_at);
            }
        }
    }

    pub fn persist_hedge_record(&mut self, bundle_id: &str, recorded_at: DateTime<Utc>) {
        if let Some(record) = self.state.hedge_records.get(bundle_id).cloned() {
            self.storage.record(
                CrossArbStorageEvent::HedgeRecord(record.clone()),
                recorded_at,
            );
            if let Some(storage) = self.persistent_storage.as_mut() {
                storage.record(CrossArbStorageEvent::HedgeRecord(record), recorded_at);
            }
        }
    }

    pub fn persist_close_metric(
        &mut self,
        metric: BundleCloseMetricReadModel,
        recorded_at: DateTime<Utc>,
    ) {
        self.storage.record(
            CrossArbStorageEvent::CloseMetric(metric.clone()),
            recorded_at,
        );
        if let Some(storage) = self.persistent_storage.as_mut() {
            storage.record(CrossArbStorageEvent::CloseMetric(metric), recorded_at);
        }
    }

    pub fn ensure_and_persist_hedge_record(&mut self, bundle_id: &str, recorded_at: DateTime<Utc>) {
        if self
            .state
            .upsert_open_hedge_record_for_bundle(bundle_id, recorded_at)
        {
            self.persist_hedge_record(bundle_id, recorded_at);
        }
    }

    pub fn persist_hedge_repair_task(&mut self, task_id: &str, recorded_at: DateTime<Utc>) {
        if let Some(task) = self.state.hedge_repair_tasks.get(task_id).cloned() {
            self.storage.record(
                CrossArbStorageEvent::HedgeRepairTask(task.clone()),
                recorded_at,
            );
            if let Some(storage) = self.persistent_storage.as_mut() {
                storage.record(CrossArbStorageEvent::HedgeRepairTask(task), recorded_at);
            }
        }
    }
}

fn log_live_private_event(config: &CrossExchangeArbitrageConfig, event: &PrivateEvent) {
    if !config.mode.allows_live_orders() {
        return;
    }
    match &event.kind {
        PrivateEventKind::Order(order) => {
            log::warn!(
                "cross-arb live order update exchange={} symbol={} client_order_id={:?} exchange_order_id={:?} side={:?} position_side={:?} status={:?} filled_quantity={} average_fill_price={:?} updated_at={}",
                order.exchange,
                order.canonical_symbol,
                order.client_order_id,
                order.exchange_order_id,
                order.side,
                order.position_side,
                order.status,
                order.filled_quantity,
                order.average_fill_price,
                order.updated_at
            );
        }
        PrivateEventKind::Fill(fill) => {
            log::warn!(
                "cross-arb live fill exchange={} symbol={} trade_id={} client_order_id={:?} exchange_order_id={:?} side={:?} position_side={:?} liquidity={:?} price={} quantity={} fee={:?} fee_asset={:?} received_at={}",
                fill.exchange,
                fill.canonical_symbol,
                fill.trade_id,
                fill.client_order_id,
                fill.exchange_order_id,
                fill.side,
                fill.position_side,
                fill.liquidity,
                fill.price,
                fill.quantity,
                fill.fee,
                fill.fee_asset,
                fill.received_at
            );
        }
        PrivateEventKind::StreamDisconnected { reason, .. } => {
            log::error!(
                "cross-arb live private stream disconnected exchange={} reason={:?}",
                event.exchange,
                reason
            );
        }
        PrivateEventKind::Error(error) => {
            log::error!(
                "cross-arb live private stream error exchange={} class={:?} message={}",
                event.exchange,
                error.class,
                error.message
            );
        }
        _ => {}
    }
}

impl Default for CrossArbRuntimeState {
    fn default() -> Self {
        Self::new(CrossExchangeArbitrageConfig::default(), Utc::now())
    }
}

fn is_active_bundle_status(status: SimulatedBundleStatus) -> bool {
    matches!(
        status,
        SimulatedBundleStatus::MakerPending
            | SimulatedBundleStatus::MakerFilled
            | SimulatedBundleStatus::Hedging
            | SimulatedBundleStatus::OpenSimulated
            | SimulatedBundleStatus::ClosingSimulated
            | SimulatedBundleStatus::OrphanLeg
    )
}

fn symbol_from_bundle_id(bundle_id: &str) -> Option<CanonicalSymbol> {
    let normalized = bundle_id.to_ascii_lowercase();
    for marker in ["crossarb-", "restored-"] {
        let Some(after_marker) = normalized.split(marker).nth(1) else {
            continue;
        };
        let token = after_marker.split('-').next()?;
        if let Some(symbol) = parse_compact_usdt_symbol(token) {
            return Some(symbol);
        }
    }
    None
}

fn should_import_hedge_record(record: &HedgeRecordReadModel) -> bool {
    record.is_closed
        || matches!(record.status, HedgeRecordStatus::Closed)
        || should_keep_loaded_hedge_record(record)
}

fn should_keep_loaded_hedge_record(record: &HedgeRecordReadModel) -> bool {
    !record.is_closed
        && !matches!(record.status, HedgeRecordStatus::Closed)
        && record.long_exchange != record.short_exchange
}

fn position_claim_key(position: &ExchangePosition) -> PositionClaimKey {
    (
        position.exchange.clone(),
        position.canonical_symbol.clone(),
        position.position_side,
    )
}

fn remaining_position_qty(
    claimed: &HashMap<PositionClaimKey, f64>,
    position: &ExchangePosition,
) -> f64 {
    (position.quantity.abs()
        - claimed
            .get(&position_claim_key(position))
            .copied()
            .unwrap_or(0.0))
    .max(0.0)
}

fn claim_position_qty(
    claimed: &mut HashMap<PositionClaimKey, f64>,
    position: &ExchangePosition,
    quantity: f64,
) {
    let entry = claimed.entry(position_claim_key(position)).or_default();
    *entry += quantity.max(0.0);
}

fn exchange_position_with_quantity(position: &ExchangePosition, quantity: f64) -> ExchangePosition {
    let mut position = position.clone();
    position.quantity = quantity;
    position
}

fn planned_open_qty(position: &super::position::BundlePosition) -> f64 {
    position
        .long_leg
        .intended_qty
        .max(position.long_leg.filled_qty)
        .min(
            position
                .short_leg
                .intended_qty
                .max(position.short_leg.filled_qty),
        )
}

fn hedge_record_can_claim_single_position(
    record: &HedgeRecordReadModel,
    position: &ExchangePosition,
) -> bool {
    match position.position_side {
        PositionSide::Long => record.long_exchange == position.exchange,
        PositionSide::Short => record.short_exchange == position.exchange,
        PositionSide::Net => false,
    }
}

fn repair_task_expects_opposite_existing_leg(
    task: &HedgeRepairTaskReadModel,
    position: &ExchangePosition,
) -> bool {
    match task.position_side {
        PositionSide::Long => position.position_side == PositionSide::Short,
        PositionSide::Short => position.position_side == PositionSide::Long,
        PositionSide::Net => false,
    }
}

fn hedge_record_expects_open_repair(record: &HedgeRecordReadModel) -> bool {
    matches!(
        record.status,
        HedgeRecordStatus::Hedging
            | HedgeRecordStatus::RepairPending
            | HedgeRecordStatus::RepairSubmitted
            | HedgeRecordStatus::RepairFailed
    )
}

fn single_position_opposite_exchange(
    record: &HedgeRecordReadModel,
    position: &ExchangePosition,
) -> ExchangeId {
    match position.position_side {
        PositionSide::Long => record.short_exchange.clone(),
        PositionSide::Short => record.long_exchange.clone(),
        PositionSide::Net => record.short_exchange.clone(),
    }
}

fn route_for_single_position(
    record: &HedgeRecordReadModel,
    position: &ExchangePosition,
) -> StrategyRoute {
    match position.position_side {
        PositionSide::Long => StrategyRoute {
            long_exchange: record.long_exchange.clone(),
            short_exchange: record.short_exchange.clone(),
            maker_exchange: position.exchange.clone(),
            taker_exchange: record.short_exchange.clone(),
            maker_side: StrategyOrderSide::Buy,
            taker_side: StrategyOrderSide::Sell,
            maker_leg_kind: MakerLegKind::LongMakerBuy,
        },
        PositionSide::Short => StrategyRoute {
            long_exchange: record.long_exchange.clone(),
            short_exchange: record.short_exchange.clone(),
            maker_exchange: position.exchange.clone(),
            taker_exchange: record.long_exchange.clone(),
            maker_side: StrategyOrderSide::Sell,
            taker_side: StrategyOrderSide::Buy,
            maker_leg_kind: MakerLegKind::ShortMakerSell,
        },
        PositionSide::Net => StrategyRoute {
            long_exchange: record.long_exchange.clone(),
            short_exchange: record.short_exchange.clone(),
            maker_exchange: position.exchange.clone(),
            taker_exchange: record.short_exchange.clone(),
            maker_side: StrategyOrderSide::Buy,
            taker_side: StrategyOrderSide::Sell,
            maker_leg_kind: MakerLegKind::LongMakerBuy,
        },
    }
}

fn repair_task_id(bundle_id: &str, reduce_only: bool) -> String {
    let phase = if reduce_only { "close" } else { "open" };
    format!("repair-{phase}-{bundle_id}")
}

fn compare_hedge_record_reuse_priority(
    left: &HedgeRecordReadModel,
    right: &HedgeRecordReadModel,
) -> std::cmp::Ordering {
    hedge_record_reuse_rank(left)
        .cmp(&hedge_record_reuse_rank(right))
        .then_with(|| left.updated_at.cmp(&right.updated_at))
        .then_with(|| left.bundle_id.cmp(&right.bundle_id))
}

fn hedge_record_reuse_rank(record: &HedgeRecordReadModel) -> (bool, bool, u8, bool) {
    (
        !record.bundle_id.starts_with("restored-"),
        record.hedge_order_submitted_at.is_some(),
        match record.status {
            HedgeRecordStatus::Hedged => 6,
            HedgeRecordStatus::Hedging => 5,
            HedgeRecordStatus::RepairSubmitted => 4,
            HedgeRecordStatus::RepairPending => 3,
            HedgeRecordStatus::RepairFailed => 2,
            HedgeRecordStatus::Closing => 1,
            HedgeRecordStatus::Closed => 0,
        },
        record.opened_at.is_some(),
    )
}

fn bundle_close_net_profit_pct(position: &super::position::BundlePosition) -> f64 {
    let realized = position.long_leg.realized_pnl + position.short_leg.realized_pnl;
    let fees =
        position.open_fee_paid() + position.long_leg.close_fee + position.short_leg.close_fee;
    let denominator = position.target_notional_usdt.max(1.0);
    (realized + position.realized_funding_pnl() - fees) / denominator
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        ArbitrageBundle, BundleStatus, FillEvent, FillLiquidity, OrderSide as ExecutionOrderSide,
        PositionSide, PrivateEvent, PrivateEventKind,
    };
    use crate::market::{BookLevel, ExchangeSymbol, OrderBook5};
    use crate::strategies::cross_exchange_arbitrage::signal::ArbSignalAction;

    fn config_with_universe(
        symbols: Vec<CanonicalSymbol>,
        enabled_exchanges: Vec<ExchangeId>,
    ) -> CrossExchangeArbitrageConfig {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.symbols = symbols;
        config.universe.enabled_exchanges = enabled_exchanges;
        config
    }

    fn book(exchange: ExchangeId, bid: f64, ask: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            vec![BookLevel::new(bid, 10.0)],
            vec![BookLevel::new(ask, 10.0)],
            Utc::now(),
            Utc::now(),
            Some(1),
            None,
        )
    }

    #[test]
    fn runtime_should_scan_market_and_store_signals() {
        let now = Utc::now();
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0)),
        ];

        let signals = runtime.on_market_snapshots(&symbol, &snapshots, now);

        assert!(!signals.is_empty());
        assert!(!runtime.state.opportunities.is_empty());
        assert!(!runtime.storage.events().is_empty());
    }

    #[test]
    fn runtime_controls_should_block_new_open_signals() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        state.pause_new_entries();
        assert!(state.paused_new_entries);
        state.set_close_only();
        assert!(state.close_only);
        state.kill_switch();
        assert!(state.kill_switch);
    }

    #[test]
    fn runtime_clear_close_only_should_preserve_manual_pause() {
        let now = Utc::now();
        let mut config = CrossExchangeArbitrageConfig::default();
        config.controls.start_paused_new_entries = true;
        let mut state = CrossArbRuntimeState::new(config, now);

        state.set_close_only();
        state.clear_close_only();

        assert!(state.paused_new_entries);
        assert!(!state.close_only);
        assert!(!state.risk_state.decision().allow_new_entries);
    }

    #[test]
    fn runtime_should_track_position_manager_fills_and_summary() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        let bundle = ArbitrageBundle::new(
            "bundle-1",
            RuntimeMode::Simulation,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            now,
        );

        state.register_bundle_position(&bundle, 2.0, 0.015, now);

        let fill = FillEvent {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            trade_id: "trade-1".to_string(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            side: ExecutionOrderSide::Buy,
            position_side: PositionSide::Long,
            liquidity: FillLiquidity::Taker,
            price: 100.0,
            quantity: 2.0,
            quote_quantity: 200.0,
            fee: Some(0.1),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0005),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: now,
            received_at: now,
        };

        let short_fill = FillEvent {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            trade_id: "trade-2".to_string(),
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("order-2".to_string()),
            side: ExecutionOrderSide::Sell,
            position_side: PositionSide::Short,
            liquidity: FillLiquidity::Taker,
            price: 101.0,
            quantity: 2.0,
            quote_quantity: 202.0,
            fee: Some(0.1),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0005),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: now,
            received_at: now,
        };

        let application = state.apply_fill_event("bundle-1", &fill).unwrap();
        assert_eq!(
            application.status_after,
            crate::strategies::cross_exchange_arbitrage::LegStatus::Open
        );
        state.apply_fill_event("bundle-1", &short_fill).unwrap();

        let summary = state.position_summary(0.001);
        assert_eq!(summary.open_bundles, 1);
        assert_eq!(summary.orphan_bundle_count, 0);
        assert_eq!(
            state
                .position_manager
                .bundle("bundle-1")
                .unwrap()
                .long_leg
                .filled_qty,
            2.0
        );
    }

    #[test]
    fn hedge_record_read_models_should_wait_for_submitted_hedge_order() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let bundle = ArbitrageBundle::new(
            "bundle-hedge-record",
            RuntimeMode::Simulation,
            symbol.clone(),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            now,
        );
        state.register_bundle_position(&bundle, 1.0, 0.01, now);

        state.record_hedge_started("bundle-hedge-record", now);
        assert!(state.hedge_record_read_models().is_empty());

        let command = OrderCommand::new(
            RuntimeMode::Simulation,
            "bundle-hedge-record",
            crate::execution::BundleLeg::Hedge,
            1,
            ExchangeId::Okx,
            symbol.clone(),
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            crate::execution::OrderIntent::HedgeShortTaker,
            ExecutionOrderSide::Sell,
            PositionSide::Short,
            crate::execution::OrderType::Market,
            1.0,
            None,
            crate::execution::TimeInForce::Ioc,
            false,
            false,
            Some(0.001),
            now + chrono::Duration::milliseconds(1),
        );
        state.record_hedge_order_submitted(&command, command.created_at);

        let records = state.hedge_record_read_models();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, "bundle-hedge-record");
        assert_eq!(
            records[0].hedge_order_submitted_at,
            Some(command.created_at)
        );
    }

    #[test]
    fn runtime_should_allow_other_entries_when_orphan_global_block_disabled() {
        let now = Utc::now();
        let mut config = CrossExchangeArbitrageConfig::default();
        config.risk.orphan_exposure_blocks_new_entries = false;
        let mut state = CrossArbRuntimeState::new(config, now);
        let bundle = ArbitrageBundle::new(
            "bundle-orphan",
            RuntimeMode::LiveSmall,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeId::Bitget,
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Binance,
            100.0,
            now,
        );
        state.register_bundle_position(&bundle, 2.0, 0.015, now);

        state
            .apply_fill_event(
                "bundle-orphan",
                &FillEvent {
                    exchange: ExchangeId::Bitget,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                    trade_id: "trade-orphan".to_string(),
                    client_order_id: Some("client-orphan".to_string()),
                    exchange_order_id: Some("order-orphan".to_string()),
                    side: ExecutionOrderSide::Buy,
                    position_side: PositionSide::Long,
                    liquidity: FillLiquidity::Taker,
                    price: 100.0,
                    quantity: 2.0,
                    quote_quantity: 200.0,
                    fee: Some(0.1),
                    fee_asset: Some("USDT".to_string()),
                    fee_rate: Some(0.0005),
                    realized_pnl: None,
                    reduce_only: Some(false),
                    filled_at: now,
                    received_at: now,
                },
            )
            .unwrap();

        state.record_orphan_exposure(now);

        assert!(!state.close_only);
        assert!(!state.paused_new_entries);
        assert!(state.risk_state.decision().allow_new_entries);
        assert_eq!(state.position_summary(0.001).orphan_bundle_count, 1);
        assert_eq!(state.hedge_repair_tasks.len(), 1);
    }

    #[test]
    fn runtime_should_block_open_for_symbol_with_unmanaged_account_position() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut config = config_with_universe(
            vec![symbol.clone()],
            vec![ExchangeId::Binance, ExchangeId::Gate],
        );
        config.risk.orphan_exposure_blocks_new_entries = false;
        config.thresholds.min_open_raw_spread = 0.0;
        config.thresholds.min_open_maker_taker_net_edge = 0.0;
        let mut state = CrossArbRuntimeState::new(config, now);
        state.replace_account_positions_from_rest(
            ExchangeId::Gate,
            &[ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                position_side: PositionSide::Long,
                quantity: 0.1,
                entry_price: Some(100.0),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            }],
            now,
        );

        let signals = state.update_from_market_snapshots(
            &symbol,
            &[
                MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
                MarketSnapshot::healthy(book(ExchangeId::Gate, 104.0, 105.0)),
            ],
            now,
        );

        assert!(!signals.is_empty());
        assert!(signals
            .iter()
            .all(|signal| matches!(signal.action, ArbSignalAction::Noop)));
        assert!(state.opportunities.iter().any(|opportunity| {
            opportunity.canonical_symbol == symbol
                && !opportunity.can_open
                && opportunity
                    .reject_reasons
                    .contains(&RejectReason::UnpairedExchangePosition)
        }));
        assert!(!state.close_only);
    }

    #[test]
    fn runtime_should_delay_close_only_until_unpaired_threshold() {
        let now = Utc::now();
        let mut config = config_with_universe(
            vec![
                CanonicalSymbol::new("BTC", "USDT"),
                CanonicalSymbol::new("ETH", "USDT"),
                CanonicalSymbol::new("SOL", "USDT"),
            ],
            vec![ExchangeId::Gate, ExchangeId::Bitget],
        );
        config.risk.orphan_close_only_after_count = 3;
        let mut state = CrossArbRuntimeState::new(config, now);

        state.record_unpaired_exchange_positions(
            &[
                ExchangePosition {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                    position_side: PositionSide::Long,
                    quantity: 0.1,
                    entry_price: Some(100.0),
                    mark_price: Some(101.0),
                    unrealized_pnl: Some(0.1),
                    updated_at: now,
                },
                ExchangePosition {
                    exchange: ExchangeId::Bitget,
                    canonical_symbol: CanonicalSymbol::new("ETH", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "ETHUSDT"),
                    position_side: PositionSide::Short,
                    quantity: 0.2,
                    entry_price: Some(100.0),
                    mark_price: Some(99.0),
                    unrealized_pnl: Some(0.2),
                    updated_at: now,
                },
            ],
            now,
        );

        assert!(!state.close_only);
        assert_eq!(state.risk_events.len(), 2);

        state.record_unpaired_exchange_positions(
            &[
                ExchangePosition {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                    position_side: PositionSide::Long,
                    quantity: 0.1,
                    entry_price: Some(100.0),
                    mark_price: Some(101.0),
                    unrealized_pnl: Some(0.1),
                    updated_at: now,
                },
                ExchangePosition {
                    exchange: ExchangeId::Bitget,
                    canonical_symbol: CanonicalSymbol::new("ETH", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "ETHUSDT"),
                    position_side: PositionSide::Short,
                    quantity: 0.2,
                    entry_price: Some(100.0),
                    mark_price: Some(99.0),
                    unrealized_pnl: Some(0.2),
                    updated_at: now,
                },
                ExchangePosition {
                    exchange: ExchangeId::Gate,
                    canonical_symbol: CanonicalSymbol::new("SOL", "USDT"),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "SOL_USDT"),
                    position_side: PositionSide::Long,
                    quantity: 0.3,
                    entry_price: Some(100.0),
                    mark_price: Some(101.0),
                    unrealized_pnl: Some(0.3),
                    updated_at: now,
                },
            ],
            now,
        );

        assert!(state.close_only);
    }

    #[test]
    fn runtime_should_report_strategy_unpaired_account_positions() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(
            config_with_universe(
                vec![CanonicalSymbol::new("BTC", "USDT")],
                vec![ExchangeId::Gate],
            ),
            now,
        );

        state.record_unpaired_exchange_positions(
            &[ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                position_side: PositionSide::Long,
                quantity: 0.1,
                entry_price: Some(100.0),
                mark_price: Some(101.0),
                unrealized_pnl: Some(0.1),
                updated_at: now,
            }],
            now,
        );

        assert!(state.close_only);
        assert_eq!(state.risk_events.len(), 1);
    }

    #[test]
    fn runtime_should_ignore_non_strategy_quote_unpaired_account_positions() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);

        state.record_unpaired_exchange_positions(
            &[ExchangePosition {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDC"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDC"),
                position_side: PositionSide::Long,
                quantity: 0.1,
                entry_price: Some(100.0),
                mark_price: Some(101.0),
                unrealized_pnl: Some(0.1),
                updated_at: now,
            }],
            now,
        );

        assert!(!state.close_only);
        assert!(state.risk_events.is_empty());
    }

    #[test]
    fn runtime_restore_should_ignore_external_position_pair_when_no_strategy_record() {
        let now = Utc::now();
        let config = config_with_universe(
            vec![CanonicalSymbol::new("NEWT", "USDT")],
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate],
        );
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("NEWT", "USDT");
        let positions = vec![
            ExchangePosition {
                exchange: ExchangeId::Binance,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "NEWTUSDT"),
                position_side: PositionSide::Long,
                quantity: 74.0,
                entry_price: Some(0.07967),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Bitget,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "NEWTUSDT"),
                position_side: PositionSide::Short,
                quantity: 74.0,
                entry_price: Some(0.07996),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: symbol,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "NEWT_USDT"),
                position_side: PositionSide::Short,
                quantity: 140.0,
                entry_price: Some(0.079335),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
        ];

        state.restore_open_bundles_from_account_positions(&positions, now);
        let unpaired = state.unpaired_exchange_positions(&positions);

        assert_eq!(state.position_manager.bundles().count(), 0);
        assert!(state.hedge_record_read_models().is_empty());
        assert_eq!(unpaired.len(), 3);
        assert!(state.close_only);
        assert!(state.risk_events.iter().any(|event| event
            .message
            .contains("strategy only manages its own bundles")));
    }

    #[test]
    fn runtime_restore_should_reuse_persisted_hedge_record_for_position_pair() {
        let now = Utc::now();
        let config = config_with_universe(
            vec![CanonicalSymbol::new("NEWT", "USDT")],
            vec![ExchangeId::Binance, ExchangeId::Bitget],
        );
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("NEWT", "USDT");
        let original_id = "bundle-signal-crossarb-newtusdt-binance-bitget-longmakerbuy-1";
        state.hedge_records.insert(
            original_id.to_string(),
            HedgeRecordReadModel {
                record_id: original_id.to_string(),
                bundle_id: original_id.to_string(),
                opened_at: Some(now - chrono::Duration::minutes(10)),
                hedge_order_submitted_at: Some(now - chrono::Duration::minutes(10)),
                canonical_symbol: symbol.clone(),
                long_exchange: ExchangeId::Binance,
                short_exchange: ExchangeId::Bitget,
                status: HedgeRecordStatus::RepairFailed,
                entry_net_edge_pct: Some(0.004),
                close_spread_pct: None,
                close_net_profit_pct: None,
                is_closed: false,
                closed_at: None,
                updated_at: now - chrono::Duration::minutes(1),
            },
        );
        state.hedge_repair_tasks.insert(
            repair_task_id(original_id, false),
            HedgeRepairTaskReadModel {
                task_id: repair_task_id(original_id, false),
                record_id: original_id.to_string(),
                bundle_id: original_id.to_string(),
                canonical_symbol: symbol.clone(),
                failed_exchange: ExchangeId::Bitget,
                last_attempt_exchange: Some(ExchangeId::Gate),
                side: ExecutionOrderSide::Sell,
                position_side: PositionSide::Short,
                quantity: 74.0,
                reduce_only: false,
                status: HedgeRepairTaskStatus::Failed,
                attempts: 5,
                last_error: Some("old repair failure".to_string()),
                created_at: now - chrono::Duration::minutes(10),
                updated_at: now - chrono::Duration::minutes(1),
            },
        );

        let positions = vec![
            ExchangePosition {
                exchange: ExchangeId::Binance,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "NEWTUSDT"),
                position_side: PositionSide::Long,
                quantity: 74.0,
                entry_price: Some(0.07967),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Bitget,
                canonical_symbol: symbol,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "NEWTUSDT"),
                position_side: PositionSide::Short,
                quantity: 74.0,
                entry_price: Some(0.07996),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
        ];

        state.restore_open_bundles_from_account_positions(&positions, now);

        assert!(state.position_manager.contains_bundle(original_id));
        assert!(state.open_bundles.contains_key(original_id));
        assert!(!state
            .open_bundles
            .contains_key("restored-newtusdt-binance-bitget"));
        let record = state.hedge_records.get(original_id).unwrap();
        assert_eq!(record.status, HedgeRecordStatus::Hedged);
        assert_eq!(
            state.hedge_repair_tasks[&repair_task_id(original_id, false)].status,
            HedgeRepairTaskStatus::Completed
        );
        assert!(state.hedge_repair_task_read_models().is_empty());
    }

    #[test]
    fn runtime_restore_should_leave_residual_position_quantity_unpaired() {
        let now = Utc::now();
        let config = config_with_universe(
            vec![CanonicalSymbol::new("SPCX", "USDT")],
            vec![ExchangeId::Gate, ExchangeId::Bitget],
        );
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("SPCX", "USDT");
        let original_id = "bundle-signal-crossarb-spcxusdt-gate-bitget-longmakerbuy-1";
        state.hedge_records.insert(
            original_id.to_string(),
            HedgeRecordReadModel {
                record_id: original_id.to_string(),
                bundle_id: original_id.to_string(),
                opened_at: Some(now - chrono::Duration::minutes(10)),
                hedge_order_submitted_at: Some(now - chrono::Duration::minutes(10)),
                canonical_symbol: symbol.clone(),
                long_exchange: ExchangeId::Gate,
                short_exchange: ExchangeId::Bitget,
                status: HedgeRecordStatus::RepairFailed,
                entry_net_edge_pct: Some(0.004),
                close_spread_pct: None,
                close_net_profit_pct: None,
                is_closed: false,
                closed_at: None,
                updated_at: now - chrono::Duration::minutes(1),
            },
        );
        state.hedge_repair_tasks.insert(
            repair_task_id(original_id, false),
            HedgeRepairTaskReadModel {
                task_id: repair_task_id(original_id, false),
                record_id: original_id.to_string(),
                bundle_id: original_id.to_string(),
                canonical_symbol: symbol.clone(),
                failed_exchange: ExchangeId::Bitget,
                last_attempt_exchange: Some(ExchangeId::Bitget),
                side: ExecutionOrderSide::Sell,
                position_side: PositionSide::Short,
                quantity: 0.03,
                reduce_only: false,
                status: HedgeRepairTaskStatus::Failed,
                attempts: 2,
                last_error: Some("previous repair failed".to_string()),
                created_at: now - chrono::Duration::minutes(10),
                updated_at: now - chrono::Duration::minutes(1),
            },
        );

        let positions = vec![
            ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "SPCX_USDT"),
                position_side: PositionSide::Long,
                quantity: 0.03,
                entry_price: Some(199.04),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Bitget,
                canonical_symbol: symbol,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "SPCXUSDT"),
                position_side: PositionSide::Short,
                quantity: 0.06,
                entry_price: Some(205.405),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
        ];

        state.restore_open_bundles_from_account_positions(&positions, now);

        let position = state.position_manager.bundle(original_id).unwrap();
        assert_eq!(position.long_leg.filled_qty, 0.03);
        assert_eq!(position.short_leg.filled_qty, 0.03);
        assert_eq!(
            state.hedge_repair_tasks[&repair_task_id(original_id, false)].status,
            HedgeRepairTaskStatus::Completed
        );
        let unpaired = state.unpaired_exchange_positions(&positions);
        assert_eq!(unpaired.len(), 1);
        assert_eq!(unpaired[0].exchange, ExchangeId::Bitget);
        assert_eq!(unpaired[0].position_side, PositionSide::Short);
        assert!((unpaired[0].quantity - 0.03).abs() < 1e-9);
    }

    #[test]
    fn runtime_restore_should_reuse_persisted_single_leg_repair_record() {
        let now = Utc::now();
        let config = config_with_universe(
            vec![CanonicalSymbol::new("INIT", "USDT")],
            vec![ExchangeId::Gate, ExchangeId::Binance],
        );
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("INIT", "USDT");
        let bundle_id = "bundle-signal-crossarb-initusdt-gate-binance-longmakerbuy-1";
        state.hedge_records.insert(
            bundle_id.to_string(),
            HedgeRecordReadModel {
                record_id: bundle_id.to_string(),
                bundle_id: bundle_id.to_string(),
                opened_at: Some(now - chrono::Duration::minutes(10)),
                hedge_order_submitted_at: Some(now - chrono::Duration::minutes(10)),
                canonical_symbol: symbol.clone(),
                long_exchange: ExchangeId::Gate,
                short_exchange: ExchangeId::Binance,
                status: HedgeRecordStatus::RepairFailed,
                entry_net_edge_pct: Some(0.004),
                close_spread_pct: None,
                close_net_profit_pct: None,
                is_closed: false,
                closed_at: None,
                updated_at: now - chrono::Duration::minutes(1),
            },
        );
        state.hedge_repair_tasks.insert(
            repair_task_id(bundle_id, false),
            HedgeRepairTaskReadModel {
                task_id: repair_task_id(bundle_id, false),
                record_id: bundle_id.to_string(),
                bundle_id: bundle_id.to_string(),
                canonical_symbol: symbol.clone(),
                failed_exchange: ExchangeId::Binance,
                last_attempt_exchange: Some(ExchangeId::Binance),
                side: ExecutionOrderSide::Sell,
                position_side: PositionSide::Short,
                quantity: 70.0,
                reduce_only: false,
                status: HedgeRepairTaskStatus::Failed,
                attempts: 2,
                last_error: Some("hedge order was accepted but not filled".to_string()),
                created_at: now - chrono::Duration::minutes(10),
                updated_at: now - chrono::Duration::minutes(1),
            },
        );

        let positions = vec![ExchangePosition {
            exchange: ExchangeId::Gate,
            canonical_symbol: symbol.clone(),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "INIT_USDT"),
            position_side: PositionSide::Long,
            quantity: 130.0,
            entry_price: Some(0.153),
            mark_price: None,
            unrealized_pnl: None,
            updated_at: now,
        }];

        state.restore_open_bundles_from_account_positions(&positions, now);

        assert!(state.position_manager.contains_bundle(bundle_id));
        let position = state.position_manager.bundle(bundle_id).unwrap();
        assert_eq!(position.long_leg.exchange, ExchangeId::Gate);
        assert_eq!(position.short_leg.exchange, ExchangeId::Binance);
        assert_eq!(position.long_leg.filled_qty, 70.0);
        assert_eq!(position.short_leg.filled_qty, 0.0);
        assert_eq!(
            state.hedge_repair_task_read_models()[0].bundle_id,
            bundle_id
        );
        assert_eq!(
            state.unpaired_exchange_positions(&positions)[0].quantity,
            60.0
        );
        assert!(state.close_only);
    }

    #[test]
    fn runtime_restore_should_not_claim_repair_leg_as_single_existing_leg() {
        let now = Utc::now();
        let config = config_with_universe(
            vec![CanonicalSymbol::new("SPCX", "USDT")],
            vec![ExchangeId::Gate, ExchangeId::Bitget],
        );
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("SPCX", "USDT");
        let bundle_id = "bundle-signal-crossarb-spcxusdt-gate-bitget-longmakerbuy-1";
        state.hedge_records.insert(
            bundle_id.to_string(),
            HedgeRecordReadModel {
                record_id: bundle_id.to_string(),
                bundle_id: bundle_id.to_string(),
                opened_at: Some(now - chrono::Duration::minutes(10)),
                hedge_order_submitted_at: Some(now - chrono::Duration::minutes(10)),
                canonical_symbol: symbol.clone(),
                long_exchange: ExchangeId::Gate,
                short_exchange: ExchangeId::Bitget,
                status: HedgeRecordStatus::RepairFailed,
                entry_net_edge_pct: Some(0.004),
                close_spread_pct: None,
                close_net_profit_pct: None,
                is_closed: false,
                closed_at: None,
                updated_at: now - chrono::Duration::minutes(1),
            },
        );
        state.hedge_repair_tasks.insert(
            repair_task_id(bundle_id, false),
            HedgeRepairTaskReadModel {
                task_id: repair_task_id(bundle_id, false),
                record_id: bundle_id.to_string(),
                bundle_id: bundle_id.to_string(),
                canonical_symbol: symbol.clone(),
                failed_exchange: ExchangeId::Bitget,
                last_attempt_exchange: Some(ExchangeId::Bitget),
                side: ExecutionOrderSide::Sell,
                position_side: PositionSide::Short,
                quantity: 0.03,
                reduce_only: false,
                status: HedgeRepairTaskStatus::Failed,
                attempts: 2,
                last_error: Some("previous repair failed".to_string()),
                created_at: now - chrono::Duration::minutes(10),
                updated_at: now - chrono::Duration::minutes(1),
            },
        );

        let positions = vec![ExchangePosition {
            exchange: ExchangeId::Bitget,
            canonical_symbol: symbol.clone(),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "SPCXUSDT"),
            position_side: PositionSide::Short,
            quantity: 0.03,
            entry_price: Some(205.405),
            mark_price: None,
            unrealized_pnl: None,
            updated_at: now,
        }];

        state.restore_open_bundles_from_account_positions(&positions, now);

        assert!(!state.position_manager.contains_bundle(bundle_id));
        assert!(state.hedge_repair_task_read_models().is_empty());
        let unpaired = state.unpaired_exchange_positions(&positions);
        assert_eq!(unpaired.len(), 1);
        assert_eq!(unpaired[0].exchange, ExchangeId::Bitget);
    }

    #[test]
    fn runtime_should_not_keep_same_exchange_open_hedge_record() {
        let symbol = CanonicalSymbol::new("TAKE", "USDT");
        let record = HedgeRecordReadModel {
            record_id: "bad-same-exchange".to_string(),
            bundle_id: "bad-same-exchange".to_string(),
            opened_at: Some(Utc::now()),
            hedge_order_submitted_at: Some(Utc::now()),
            canonical_symbol: symbol,
            long_exchange: ExchangeId::Gate,
            short_exchange: ExchangeId::Gate,
            status: HedgeRecordStatus::Hedged,
            entry_net_edge_pct: Some(0.005),
            close_spread_pct: None,
            close_net_profit_pct: None,
            is_closed: false,
            closed_at: None,
            updated_at: Utc::now(),
        };

        assert!(!should_import_hedge_record(&record));
    }

    #[test]
    fn runtime_should_write_close_profit_only_after_closed() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let bundle = ArbitrageBundle::new(
            "bundle-close-profit",
            RuntimeMode::Simulation,
            symbol.clone(),
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Binance,
            ExchangeId::Bitget,
            100.0,
            now,
        );
        state.register_bundle_position(&bundle, 1.0, 0.01, now);
        state.record_hedge_order_submitted(
            &OrderCommand::new(
                RuntimeMode::Simulation,
                "bundle-close-profit",
                BundleLeg::Hedge,
                1,
                ExchangeId::Bitget,
                symbol.clone(),
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                crate::execution::OrderIntent::HedgeShortTaker,
                ExecutionOrderSide::Sell,
                PositionSide::Short,
                crate::execution::OrderType::Market,
                1.0,
                None,
                crate::execution::TimeInForce::Ioc,
                false,
                false,
                None,
                now,
            ),
            now,
        );

        state.record_close_candidate_metrics("bundle-close-profit", 0.1, 0.2, now);
        let record = state.hedge_records.get("bundle-close-profit").unwrap();
        assert_eq!(record.status, HedgeRecordStatus::Closing);
        assert_eq!(record.close_spread_pct, None);
        assert_eq!(record.close_net_profit_pct, None);

        state
            .apply_fill_event(
                "bundle-close-profit",
                &FillEvent {
                    exchange: ExchangeId::Binance,
                    canonical_symbol: symbol.clone(),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                    trade_id: "long-open".to_string(),
                    client_order_id: None,
                    exchange_order_id: None,
                    side: ExecutionOrderSide::Buy,
                    position_side: PositionSide::Long,
                    liquidity: FillLiquidity::Taker,
                    price: 100.0,
                    quantity: 1.0,
                    quote_quantity: 100.0,
                    fee: Some(0.01),
                    fee_asset: Some("USDT".to_string()),
                    fee_rate: None,
                    realized_pnl: None,
                    reduce_only: Some(false),
                    filled_at: now,
                    received_at: now,
                },
            )
            .unwrap();
        state
            .apply_fill_event(
                "bundle-close-profit",
                &FillEvent {
                    exchange: ExchangeId::Bitget,
                    canonical_symbol: symbol.clone(),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                    trade_id: "short-open".to_string(),
                    client_order_id: None,
                    exchange_order_id: None,
                    side: ExecutionOrderSide::Sell,
                    position_side: PositionSide::Short,
                    liquidity: FillLiquidity::Taker,
                    price: 101.0,
                    quantity: 1.0,
                    quote_quantity: 101.0,
                    fee: Some(0.01),
                    fee_asset: Some("USDT".to_string()),
                    fee_rate: None,
                    realized_pnl: None,
                    reduce_only: Some(false),
                    filled_at: now,
                    received_at: now,
                },
            )
            .unwrap();
        state
            .apply_fill_event(
                "bundle-close-profit",
                &FillEvent {
                    exchange: ExchangeId::Binance,
                    canonical_symbol: symbol.clone(),
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
                    trade_id: "long-close".to_string(),
                    client_order_id: None,
                    exchange_order_id: None,
                    side: ExecutionOrderSide::Sell,
                    position_side: PositionSide::Long,
                    liquidity: FillLiquidity::Taker,
                    price: 102.0,
                    quantity: 1.0,
                    quote_quantity: 102.0,
                    fee: Some(0.01),
                    fee_asset: Some("USDT".to_string()),
                    fee_rate: None,
                    realized_pnl: Some(2.0),
                    reduce_only: Some(true),
                    filled_at: now,
                    received_at: now,
                },
            )
            .unwrap();
        state
            .apply_fill_event(
                "bundle-close-profit",
                &FillEvent {
                    exchange: ExchangeId::Bitget,
                    canonical_symbol: symbol,
                    exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                    trade_id: "short-close".to_string(),
                    client_order_id: None,
                    exchange_order_id: None,
                    side: ExecutionOrderSide::Buy,
                    position_side: PositionSide::Short,
                    liquidity: FillLiquidity::Taker,
                    price: 99.0,
                    quantity: 1.0,
                    quote_quantity: 99.0,
                    fee: Some(0.01),
                    fee_asset: Some("USDT".to_string()),
                    fee_rate: None,
                    realized_pnl: Some(2.0),
                    reduce_only: Some(true),
                    filled_at: now,
                    received_at: now,
                },
            )
            .unwrap();

        let record = state.hedge_records.get("bundle-close-profit").unwrap();
        assert_eq!(record.status, HedgeRecordStatus::Closed);
        assert_eq!(record.close_spread_pct, Some(99.0 / 102.0 - 1.0));
        assert!(record.close_net_profit_pct.unwrap() > 0.0);
    }

    #[test]
    fn runtime_restore_should_not_duplicate_existing_position_pair() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let bundle = ArbitrageBundle::new(
            "bundle-signal-existing",
            RuntimeMode::LiveSmall,
            symbol.clone(),
            ExchangeId::Bitget,
            ExchangeId::Gate,
            ExchangeId::Bitget,
            ExchangeId::Gate,
            100.0,
            now,
        );
        state.register_bundle_position(&bundle, 1.0, 0.01, now);
        state.open_bundles.insert(
            "bundle-signal-existing".to_string(),
            SimulatedBundleState {
                bundle_id: "bundle-signal-existing".to_string(),
                opportunity_id: "crossarb-btcusdt-bitget-gate".to_string(),
                status: SimulatedBundleStatus::OpenSimulated,
                route: super::StrategyRoute {
                    long_exchange: ExchangeId::Bitget,
                    short_exchange: ExchangeId::Gate,
                    maker_exchange: ExchangeId::Bitget,
                    taker_exchange: ExchangeId::Gate,
                    maker_side: StrategyOrderSide::Buy,
                    taker_side: StrategyOrderSide::Sell,
                    maker_leg_kind: super::MakerLegKind::LongMakerBuy,
                },
                target_notional_usdt: 100.0,
                opened_at: Some(now),
                updated_at: now,
            },
        );

        let positions = vec![
            ExchangePosition {
                exchange: ExchangeId::Bitget,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                position_side: PositionSide::Long,
                quantity: 1.0,
                entry_price: Some(100.0),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: symbol,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                position_side: PositionSide::Short,
                quantity: 1.0,
                entry_price: Some(101.0),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
        ];

        state.restore_open_bundles_from_account_positions(&positions, now);

        assert_eq!(state.open_bundles.len(), 1);
        assert!(!state
            .open_bundles
            .contains_key("restored-btcusdt-bitget-gate"));
    }

    #[test]
    fn runtime_restore_should_reuse_existing_open_bundle_without_position_state() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut state = CrossArbRuntimeState::new(
            config_with_universe(
                vec![symbol.clone()],
                vec![ExchangeId::Bitget, ExchangeId::Gate],
            ),
            now,
        );
        state.open_bundles.insert(
            "bundle-signal-crossarb-btcusdt-bitget-gate-shortmakersell-1".to_string(),
            SimulatedBundleState {
                bundle_id: "bundle-signal-crossarb-btcusdt-bitget-gate-shortmakersell-1"
                    .to_string(),
                opportunity_id: "crossarb-btcusdt-bitget-gate-shortmakersell".to_string(),
                status: SimulatedBundleStatus::OpenSimulated,
                route: super::StrategyRoute {
                    long_exchange: ExchangeId::Bitget,
                    short_exchange: ExchangeId::Gate,
                    maker_exchange: ExchangeId::Gate,
                    taker_exchange: ExchangeId::Bitget,
                    maker_side: StrategyOrderSide::Sell,
                    taker_side: StrategyOrderSide::Buy,
                    maker_leg_kind: super::MakerLegKind::ShortMakerSell,
                },
                target_notional_usdt: 100.0,
                opened_at: Some(now),
                updated_at: now,
            },
        );

        let positions = vec![
            ExchangePosition {
                exchange: ExchangeId::Bitget,
                canonical_symbol: symbol.clone(),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                position_side: PositionSide::Long,
                quantity: 1.0,
                entry_price: Some(100.0),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
            ExchangePosition {
                exchange: ExchangeId::Gate,
                canonical_symbol: symbol,
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
                position_side: PositionSide::Short,
                quantity: 1.0,
                entry_price: Some(101.0),
                mark_price: None,
                unrealized_pnl: None,
                updated_at: now,
            },
        ];

        state.restore_open_bundles_from_account_positions(&positions, now);

        let existing_id = "bundle-signal-crossarb-btcusdt-bitget-gate-shortmakersell-1";
        assert_eq!(state.open_bundles.len(), 1);
        assert!(state.position_manager.contains_bundle(existing_id));
        assert!(!state
            .open_bundles
            .contains_key("restored-btcusdt-bitget-gate"));
    }

    #[test]
    fn runtime_should_record_private_events_and_update_health() {
        let now = Utc::now();
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);

        runtime.on_private_event(PrivateEvent::new(
            ExchangeId::Binance,
            PrivateEventKind::Heartbeat,
            now,
        ));

        assert_eq!(runtime.storage.events().len(), 1);
        assert!(matches!(
            runtime.storage.events()[0].event,
            CrossArbStorageEvent::PrivateEvent(_)
        ));
        assert!(runtime
            .state
            .risk_state
            .private_stream_health
            .contains_key(&ExchangeId::Binance));
    }

    #[test]
    fn runtime_should_mark_private_stream_error_as_resync_needed() {
        let now = Utc::now();
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);

        runtime.on_private_event(PrivateEvent::new(
            ExchangeId::Gate,
            PrivateEventKind::Heartbeat,
            now,
        ));
        runtime.on_private_event(PrivateEvent::new(
            ExchangeId::Gate,
            PrivateEventKind::Error(crate::execution::PrivateErrorEvent {
                class: crate::execution::ExchangeErrorClass::InvalidRequest,
                endpoint: None,
                code: None,
                message: "gate websocket subscribe status=fail".to_string(),
                client_order_id: None,
                exchange_order_id: None,
                retry_after_ms: None,
                occurred_at: now + chrono::Duration::milliseconds(1),
            }),
            now + chrono::Duration::milliseconds(1),
        ));

        let decision = runtime.state.risk_state.decision();
        assert!(decision.needs_private_resync);
        assert!(!decision.allow_new_entries);
    }

    #[test]
    fn runtime_should_block_gate_one_way_opposite_direction_conflict() {
        let now = Utc::now();
        let mut config = CrossExchangeArbitrageConfig::default();
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.thresholds.min_display_raw_spread = -1.0;
        config.risk.max_book_age_ms = 10_000;
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let existing = ArbitrageBundle::new(
            "gate-short-existing",
            RuntimeMode::LiveSmall,
            symbol.clone(),
            ExchangeId::Binance,
            ExchangeId::Gate,
            ExchangeId::Binance,
            ExchangeId::Gate,
            100.0,
            now,
        );
        state.register_bundle_position(&existing, 1.0, 0.01, now);
        state
            .position_manager
            .bundle_mut("gate-short-existing")
            .unwrap()
            .short_leg
            .filled_qty = 1.0;

        state.update_from_market_snapshots(
            &symbol,
            &[
                MarketSnapshot::healthy(book(ExchangeId::Gate, 100.0, 101.0)),
                MarketSnapshot::healthy(book(ExchangeId::Binance, 104.0, 105.0)),
            ],
            now,
        );

        let blocked = state
            .opportunities
            .iter()
            .find(|opportunity| {
                opportunity.maker_exchange == ExchangeId::Gate
                    && matches!(opportunity.maker_side, StrategyOrderSide::Buy)
            })
            .expect("gate long-maker opportunity");
        assert!(!blocked.can_open);
        assert!(blocked
            .reject_reasons
            .contains(&RejectReason::ExchangePositionLimitExceeded));
    }
}
