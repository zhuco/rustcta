use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use super::{
    append_jsonl, read_jsonl, validate_disable_request, validate_enable_request,
    validate_transition, AuditEvent, CommandStatus, ControlCommand, ControlCommandResponse,
    DisableMode, DisableSymbolRequest, DustPosition, EffectiveTradability, EnableSymbolRequest,
    EnableValidationReport, EnableValidationSnapshot, InventoryReadiness, ManagedSpotSymbol,
    MarkDustUnmanagedRequest, MarketLiquidationPlan, PassiveLiquidationSession,
    PassiveLiquidationStatus, SpotControlRuntimeSnapshot, SpotSymbolControlConfig,
    SpotSymbolLifecycleState, SymbolOperationLock, SymbolOperationLockRegistry,
    SymbolOperationOwner, SymbolOperationType, ValidationError, VersionedSymbolRequest,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ControlPlaneRecoveryReport {
    pub recovered_symbols: Vec<String>,
    pub incomplete_commands: Vec<String>,
    pub stale_locks: Vec<String>,
    pub unknown_open_orders: Vec<String>,
    pub reservation_mismatches: Vec<String>,
    pub manual_intervention_required: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpotControlReadModel {
    pub symbols: Vec<ManagedSpotSymbol>,
    pub commands: Vec<ControlCommand>,
    pub audit_events: Vec<AuditEvent>,
    pub operation_locks: Vec<SymbolOperationLock>,
    pub liquidation_plans: Vec<MarketLiquidationPlan>,
    pub passive_sessions: Vec<PassiveLiquidationSession>,
    pub dust_positions: Vec<DustPosition>,
    pub runtime_snapshots: Vec<SpotControlRuntimeSnapshot>,
    #[serde(default)]
    pub recovery_report: Option<ControlPlaneRecoveryReport>,
}

#[derive(Debug, Clone, Default)]
struct SpotControlState {
    symbols: HashMap<String, ManagedSpotSymbol>,
    commands: Vec<ControlCommand>,
    idempotency: HashMap<String, String>,
    audit_events: Vec<AuditEvent>,
    locks: SymbolOperationLockRegistry,
    validation_snapshot: EnableValidationSnapshot,
    liquidation_plans: Vec<MarketLiquidationPlan>,
    passive_sessions: Vec<PassiveLiquidationSession>,
    dust_positions: Vec<DustPosition>,
    runtime_snapshots: Vec<SpotControlRuntimeSnapshot>,
    recovery_report: Option<ControlPlaneRecoveryReport>,
}

#[derive(Debug, Clone)]
pub struct SpotControlService {
    config: SpotSymbolControlConfig,
    state: Arc<RwLock<SpotControlState>>,
    command_path: PathBuf,
    audit_path: PathBuf,
    lifecycle_path: PathBuf,
}

impl SpotControlService {
    pub fn new_in_memory(config: SpotSymbolControlConfig) -> Self {
        Self {
            command_path: PathBuf::new(),
            audit_path: PathBuf::new(),
            lifecycle_path: PathBuf::new(),
            config,
            state: Arc::new(RwLock::new(SpotControlState::default())),
        }
    }

    pub fn load_or_new(config: SpotSymbolControlConfig) -> std::io::Result<Self> {
        let command_path = PathBuf::from(&config.command_store_path);
        let audit_path = PathBuf::from(&config.audit_store_path);
        let lifecycle_path = PathBuf::from(&config.lifecycle_store_path);

        let commands = read_jsonl::<ControlCommand>(&command_path)?;
        let audit_events = read_jsonl::<AuditEvent>(&audit_path)?;
        let symbol_events = read_jsonl::<ManagedSpotSymbol>(&lifecycle_path)?;
        let mut state = SpotControlState::default();
        for command in commands {
            state
                .idempotency
                .insert(command.idempotency_key.clone(), command.command_id.clone());
            state.commands.push(command);
        }
        for event in audit_events {
            state.audit_events.push(event);
        }
        for mut symbol in symbol_events {
            symbol.normalize_in_place();
            state.symbols.insert(symbol.internal_symbol.clone(), symbol);
        }

        Ok(Self {
            config,
            state: Arc::new(RwLock::new(state)),
            command_path,
            audit_path,
            lifecycle_path,
        })
    }

    pub fn config(&self) -> &SpotSymbolControlConfig {
        &self.config
    }

    pub async fn set_validation_snapshot(&self, snapshot: EnableValidationSnapshot) {
        self.state.write().await.validation_snapshot = snapshot;
    }

    pub async fn read_model(&self) -> SpotControlReadModel {
        self.reload_lifecycle_store().await;
        let state = self.state.read().await;
        SpotControlReadModel {
            symbols: state.symbols.values().cloned().collect(),
            commands: state.commands.clone(),
            audit_events: state.audit_events.clone(),
            operation_locks: state.locks.all(),
            liquidation_plans: state.liquidation_plans.clone(),
            passive_sessions: state.passive_sessions.clone(),
            dust_positions: state.dust_positions.clone(),
            runtime_snapshots: state.runtime_snapshots.clone(),
            recovery_report: state.recovery_report.clone(),
        }
    }

    pub async fn record_runtime_snapshot(&self, snapshot: SpotControlRuntimeSnapshot) {
        let mut state = self.state.write().await;
        let event = AuditEvent::new(
            None,
            "system",
            "snapshot_built",
            snapshot.symbol.clone(),
            None,
            None,
            if snapshot.critical_errors.is_empty() {
                "fresh"
            } else {
                "has_critical_errors"
            },
            "authoritative runtime snapshot built",
            json!({
                "snapshot_id": snapshot.snapshot_id,
                "warnings": snapshot.warnings,
                "critical_errors": snapshot.critical_errors,
                "component_statuses": snapshot.component_statuses,
            }),
        )
        .with_snapshot_id(Some(snapshot.snapshot_id.clone()));
        self.record_audit_locked(&mut state, event);
        state.runtime_snapshots.push(snapshot);
        trim_snapshots(&mut state.runtime_snapshots);
    }

    pub async fn latest_runtime_snapshot(
        &self,
        symbol: &str,
    ) -> Option<SpotControlRuntimeSnapshot> {
        let symbol = super::normalize_symbol(symbol);
        self.state
            .read()
            .await
            .runtime_snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.symbol == symbol)
            .cloned()
    }

    pub async fn runtime_snapshot_by_id(
        &self,
        snapshot_id: &str,
    ) -> Option<SpotControlRuntimeSnapshot> {
        self.state
            .read()
            .await
            .runtime_snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.snapshot_id == snapshot_id)
            .cloned()
    }

    pub async fn runtime_snapshots_for_symbol(
        &self,
        symbol: &str,
        limit: usize,
    ) -> Vec<SpotControlRuntimeSnapshot> {
        let symbol = super::normalize_symbol(symbol);
        let mut snapshots = self
            .state
            .read()
            .await
            .runtime_snapshots
            .iter()
            .filter(|snapshot| snapshot.symbol == symbol)
            .cloned()
            .collect::<Vec<_>>();
        if snapshots.len() > limit {
            snapshots.drain(0..snapshots.len() - limit);
        }
        snapshots
    }

    pub async fn recover_after_restart(&self) -> ControlPlaneRecoveryReport {
        let mut state = self.state.write().await;
        let incomplete_commands = state
            .commands
            .iter()
            .filter(|command| {
                matches!(
                    command.status,
                    CommandStatus::Requested
                        | CommandStatus::Validated
                        | CommandStatus::Queued
                        | CommandStatus::Running
                        | CommandStatus::AwaitingConfirmation
                )
            })
            .map(|command| command.command_id.clone())
            .collect::<Vec<_>>();
        let stale_locks = state
            .locks
            .all()
            .into_iter()
            .filter(|lock| lock.is_stale(Utc::now()))
            .map(|lock| lock.command_id)
            .collect::<Vec<_>>();
        let manual_intervention_required =
            !incomplete_commands.is_empty() || !stale_locks.is_empty();
        if manual_intervention_required {
            for symbol in state.symbols.values_mut() {
                if matches!(
                    symbol.lifecycle_state,
                    SpotSymbolLifecycleState::CancelingOrders
                        | SpotSymbolLifecycleState::LiquidationPlanning
                        | SpotSymbolLifecycleState::LiquidatingMarket
                        | SpotSymbolLifecycleState::LiquidatingPassive
                ) {
                    symbol.lifecycle_state = SpotSymbolLifecycleState::ManualInterventionRequired;
                    symbol.version += 1;
                    symbol.updated_at = Utc::now();
                }
            }
        }
        let report = ControlPlaneRecoveryReport {
            recovered_symbols: state.symbols.keys().cloned().collect(),
            incomplete_commands,
            stale_locks,
            unknown_open_orders: Vec::new(),
            reservation_mismatches: Vec::new(),
            manual_intervention_required,
        };
        state.recovery_report = Some(report.clone());
        self.record_audit_locked(
            &mut state,
            AuditEvent::new(
                None,
                "system",
                "restart_recovery",
                "*",
                None,
                None,
                if manual_intervention_required {
                    "manual_intervention_required"
                } else {
                    "clean"
                },
                "control plane restart recovery completed",
                json!(report),
            ),
        );
        report
    }

    pub async fn symbol(&self, symbol: &str) -> Option<ManagedSpotSymbol> {
        self.state
            .read()
            .await
            .symbols
            .get(&super::normalize_symbol(symbol))
            .cloned()
    }

    pub async fn command(&self, command_id: &str) -> Option<ControlCommand> {
        self.state
            .read()
            .await
            .commands
            .iter()
            .find(|command| command.command_id == command_id)
            .cloned()
    }

    pub async fn enable(
        &self,
        request: EnableSymbolRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&request.symbol);
        let mut command = self.prepare_command(
            SymbolOperationType::Enable,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }

        let previous = state.symbols.get(&symbol).cloned();
        if let Some(error) = expected_version_error(previous.as_ref(), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], previous.as_ref());
        }

        let runtime_snapshot = state
            .runtime_snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.symbol == symbol)
            .cloned();
        command.snapshot_id = runtime_snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot_id.clone());
        let authoritative_validation = runtime_snapshot
            .as_ref()
            .map(super::enable_validation_from_runtime_snapshot);
        let snapshot_errors = runtime_snapshot
            .as_ref()
            .map(|snapshot| {
                super::validate_snapshot_freshness(snapshot, &self.config.runtime_snapshot)
            })
            .unwrap_or_default();
        let report = validate_enable_request(
            &request,
            authoritative_validation
                .as_ref()
                .unwrap_or(&state.validation_snapshot),
            self.config.allow_future_small_live,
        );
        let mut report_errors = report.errors.clone();
        report_errors.extend(
            snapshot_errors
                .into_iter()
                .map(|error| ValidationError::critical("runtime_snapshot_not_fresh", error)),
        );
        let mut managed = previous.unwrap_or_else(|| {
            ManagedSpotSymbol::new_disabled(
                symbol.clone(),
                request.quote_asset.clone(),
                request.requested_by.clone(),
                Utc::now(),
            )
        });
        let previous_state = managed.lifecycle_state;
        let requested_state = if report_errors.iter().any(|error| error.critical) {
            if report_errors.iter().any(|error| {
                error.code == "unmanaged_inventory_conflict" || error.code == "inventory_not_ready"
            }) {
                SpotSymbolLifecycleState::InventoryReviewRequired
            } else {
                SpotSymbolLifecycleState::Failed
            }
        } else {
            SpotSymbolLifecycleState::Active
        };

        let command_id = command.command_id.clone();
        if let Err(error) = transition_symbol(
            &mut managed,
            SpotSymbolLifecycleState::EnableRequested,
            &command_id,
        )
        .and_then(|_| {
            transition_symbol(
                &mut managed,
                SpotSymbolLifecycleState::EnableValidating,
                &command_id,
            )
        })
        .and_then(|_| transition_symbol(&mut managed, requested_state, &command_id))
        {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical("invalid_transition", error)],
                Some(&managed),
            );
        }

        if requested_state == SpotSymbolLifecycleState::Active {
            managed.selected_exchanges =
                super::normalize_exchange_list(&request.selected_exchanges);
            managed.allowed_directions = report.permitted_directions.clone();
            managed.enable_mode = request.mode;
            managed.enabled_at_optional = Some(Utc::now());
            managed.disabled_at_optional = None;
            managed.disabled_reason_optional = None;
        }
        managed.requested_by = request.requested_by.clone();
        managed.normalize_in_place();

        self.accept_symbol_update(
            &mut state,
            command,
            managed,
            previous_state,
            requested_state,
            report_errors,
            json!({
                "snapshot_id": runtime_snapshot.as_ref().map(|snapshot| snapshot.snapshot_id.clone()),
                "inventory_readiness": report.inventory_readiness,
                "permitted_directions": report.permitted_directions,
                "live_submission_blocked": true,
            }),
        )
    }

    pub async fn pause(
        &self,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        self.simple_transition(
            SymbolOperationType::Pause,
            symbol,
            request,
            idempotency_key,
            SpotSymbolLifecycleState::Paused,
            "paused by operator",
        )
        .await
    }

    pub async fn resume(
        &self,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        self.simple_transition(
            SymbolOperationType::Resume,
            symbol,
            request,
            idempotency_key,
            SpotSymbolLifecycleState::Active,
            "resumed by operator",
        )
        .await
    }

    pub async fn cancel_orders(
        &self,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&symbol);
        let command = self.prepare_command(
            SymbolOperationType::CancelOrders,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }
        let previous = state.symbols.get(&symbol).cloned();
        if let Some(error) = expected_version_error(previous.as_ref(), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], previous.as_ref());
        }
        self.accept_command_only(
            &mut state,
            command,
            previous.as_ref(),
            CommandStatus::Completed,
            json!({
                "cancel_orders_plan": "live_dry_run_only",
                "would_cancel_strategy_orders": true,
                "live_submission_blocked": true
            }),
        )
    }

    pub async fn disable(
        &self,
        request: DisableSymbolRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&request.symbol);
        let operation = match request.mode {
            DisableMode::FreezeOnly => SymbolOperationType::Disable,
            DisableMode::MarketLiquidate => SymbolOperationType::MarketLiquidate,
            DisableMode::PassiveAskLiquidate => SymbolOperationType::PassiveLiquidate,
        };
        let mut command = self.prepare_command(
            operation,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }

        let previous = state.symbols.get(&symbol).cloned();
        let Some(mut managed) = previous.clone() else {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical(
                    "symbol_not_managed",
                    "symbol must be managed before it can be disabled",
                )],
                None,
            );
        };
        if let Some(error) = expected_version_error(Some(&managed), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], Some(&managed));
        }
        let errors = validate_disable_request(&request);
        let runtime_snapshot = state
            .runtime_snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.symbol == symbol)
            .cloned();
        command.snapshot_id = runtime_snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot_id.clone());
        let snapshot_errors = runtime_snapshot
            .as_ref()
            .map(|snapshot| {
                super::validate_snapshot_freshness(snapshot, &self.config.runtime_snapshot)
            })
            .unwrap_or_default();
        let mut errors = errors;
        errors.extend(
            snapshot_errors
                .into_iter()
                .map(|error| ValidationError::critical("runtime_snapshot_not_fresh", error)),
        );
        if errors.iter().any(|error| error.critical) {
            return self.reject_locked(&mut state, command, errors, Some(&managed));
        }

        let command_id = command.command_id.clone();
        let previous_state = managed.lifecycle_state;
        if let Err(error) = state.locks.acquire(
            &symbol,
            SymbolOperationOwner::DisableWorkflow,
            &command_id,
            60_000,
            Utc::now(),
        ) {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical("operation_lock_conflict", error)],
                Some(&managed),
            );
        }

        let result = match request.mode {
            DisableMode::FreezeOnly => self.plan_freeze(&mut managed, &command_id, &request),
            DisableMode::MarketLiquidate => self.plan_market_liquidation(
                &mut state,
                &mut managed,
                &command_id,
                &request,
                runtime_snapshot.as_ref(),
            ),
            DisableMode::PassiveAskLiquidate => self.plan_passive_liquidation(
                &mut state,
                &mut managed,
                &command_id,
                &request,
                runtime_snapshot.as_ref(),
            ),
        };
        let _ = state.locks.release(&symbol, &command_id);
        match result {
            Ok((status, summary)) => self.accept_symbol_update(
                &mut state,
                command,
                managed,
                previous_state,
                status,
                Vec::new(),
                summary,
            ),
            Err(error) => self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical("disable_workflow_failed", error)],
                previous.as_ref(),
            ),
        }
    }

    pub async fn mark_dust_unmanaged(
        &self,
        symbol: String,
        request: MarkDustUnmanagedRequest,
        idempotency_key: String,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&symbol);
        let command = self.prepare_command(
            SymbolOperationType::MarkUnmanaged,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }
        let previous = state.symbols.get(&symbol).cloned();
        if let Some(error) = expected_version_error(previous.as_ref(), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], previous.as_ref());
        }
        for dust in &mut state.dust_positions {
            if dust.symbol == symbol
                && dust.exchange == super::normalize_exchange(&request.exchange)
                && dust.asset.eq_ignore_ascii_case(&request.asset)
            {
                dust.managed_or_unmanaged = "unmanaged".to_string();
                if let Some(reason) = &request.reason {
                    dust.reason = reason.clone();
                }
            }
        }
        self.accept_command_only(
            &mut state,
            command,
            previous.as_ref(),
            CommandStatus::Completed,
            json!({"marked_dust_unmanaged": true}),
        )
    }

    pub async fn manual_control_command(
        &self,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
        reason: &'static str,
        status: CommandStatus,
        summary: Value,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&symbol);
        let command = self.prepare_command(
            SymbolOperationType::ManualReconcile,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }
        let previous = state.symbols.get(&symbol).cloned();
        if let Some(error) = expected_version_error(previous.as_ref(), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], previous.as_ref());
        }
        let mut final_summary = summary;
        final_summary["reason"] = json!(reason);
        self.accept_command_only(
            &mut state,
            command,
            previous.as_ref(),
            status,
            final_summary,
        )
    }

    pub async fn complete_market_liquidation(
        &self,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
        final_state: SpotSymbolLifecycleState,
        summary: Value,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&symbol);
        let command = self.prepare_command(
            SymbolOperationType::MarketLiquidate,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }
        let previous = state.symbols.get(&symbol).cloned();
        let Some(mut managed) = previous.clone() else {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical(
                    "symbol_not_managed",
                    "symbol is not managed by spot control",
                )],
                None,
            );
        };
        if let Some(error) = expected_version_error(Some(&managed), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], Some(&managed));
        }
        if !matches!(
            final_state,
            SpotSymbolLifecycleState::DisabledClean
                | SpotSymbolLifecycleState::DustRemaining
                | SpotSymbolLifecycleState::ManualInterventionRequired
        ) {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical(
                    "invalid_liquidation_final_state",
                    "market liquidation may only complete as DisabledClean, DustRemaining, or ManualInterventionRequired",
                )],
                Some(&managed),
            );
        }
        let previous_state = managed.lifecycle_state;
        if let Err(error) = transition_symbol(
            &mut managed,
            SpotSymbolLifecycleState::LiquidatingMarket,
            &command.command_id,
        )
        .and_then(|_| transition_symbol(&mut managed, final_state, &command.command_id))
        {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical("invalid_transition", error)],
                Some(&managed),
            );
        }
        managed.disabled_at_optional = Some(Utc::now());
        managed.disabled_reason_optional =
            Some("MarketLiquidate completed by strategy runtime".to_string());
        self.accept_symbol_update(
            &mut state,
            command,
            managed,
            previous_state,
            final_state,
            Vec::new(),
            summary,
        )
    }

    pub async fn sync_runtime_active_symbol(
        &self,
        symbol: String,
        selected_exchanges: Vec<String>,
        allowed_directions: Vec<super::EnabledDirection>,
        requested_by: impl Into<String>,
    ) -> Option<ManagedSpotSymbol> {
        let symbol = super::normalize_symbol(&symbol);
        let requested_by = requested_by.into();
        let mut state = self.state.write().await;
        let previous = state.symbols.get(&symbol).cloned();
        if previous
            .as_ref()
            .is_some_and(|item| item.lifecycle_state.blocks_new_arbitrage())
        {
            return previous;
        }
        let now = Utc::now();
        let mut managed = previous.clone().unwrap_or_else(|| {
            ManagedSpotSymbol::new_disabled(symbol.clone(), "USDT", requested_by.clone(), now)
        });
        let previous_state = managed.lifecycle_state;
        managed.lifecycle_state = SpotSymbolLifecycleState::Active;
        managed.selected_exchanges = super::normalize_exchange_list(&selected_exchanges);
        managed.allowed_directions = allowed_directions
            .into_iter()
            .map(super::EnabledDirection::normalized)
            .collect();
        managed.enable_mode = self.config.default_enable_mode;
        managed.enabled_at_optional.get_or_insert(now);
        managed.disabled_at_optional = None;
        managed.disabled_reason_optional = None;
        managed.requested_by = requested_by.clone();
        managed.version += 1;
        managed.updated_at = now;
        managed.normalize_in_place();
        state
            .symbols
            .insert(managed.internal_symbol.clone(), managed.clone());
        self.persist_symbol(&managed);
        self.record_audit_locked(
            &mut state,
            AuditEvent::new(
                None,
                requested_by,
                "RuntimeActiveSync",
                managed.internal_symbol.clone(),
                Some(previous_state),
                Some(managed.lifecycle_state),
                "synced",
                "strategy runtime synced active arbitraging symbol",
                json!({
                    "selected_exchanges": managed.selected_exchanges,
                    "allowed_directions": managed.allowed_directions,
                }),
            ),
        );
        Some(managed)
    }

    pub async fn sync_runtime_closed_symbol(
        &self,
        symbol: String,
        requested_by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Option<ManagedSpotSymbol> {
        let symbol = super::normalize_symbol(&symbol);
        let requested_by = requested_by.into();
        let reason = reason.into();
        let mut state = self.state.write().await;
        let Some(mut managed) = state.symbols.get(&symbol).cloned() else {
            return None;
        };
        if managed.lifecycle_state != SpotSymbolLifecycleState::Active {
            return Some(managed);
        }
        let previous_state = managed.lifecycle_state;
        managed.lifecycle_state = SpotSymbolLifecycleState::DisabledClean;
        managed.disabled_at_optional = Some(Utc::now());
        managed.disabled_reason_optional = Some(reason.clone());
        managed.version += 1;
        managed.updated_at = Utc::now();
        state
            .symbols
            .insert(managed.internal_symbol.clone(), managed.clone());
        self.persist_symbol(&managed);
        self.record_audit_locked(
            &mut state,
            AuditEvent::new(
                None,
                requested_by,
                "RuntimeClosedSync",
                managed.internal_symbol.clone(),
                Some(previous_state),
                Some(managed.lifecycle_state),
                "synced",
                "strategy runtime synced closed arbitraging symbol",
                json!({ "reason": reason }),
            ),
        );
        Some(managed)
    }

    pub async fn effective_tradability(
        &self,
        symbol: &str,
        buy_exchange: &str,
        sell_exchange: &str,
        strategy_mode: &str,
    ) -> EffectiveTradability {
        self.reload_lifecycle_store().await;
        let state = self.state.read().await;
        let symbol = super::normalize_symbol(symbol);
        let Some(managed) = state.symbols.get(&symbol) else {
            return EffectiveTradability::evaluate(
                self.config
                    .default_enable_mode
                    .allows_strategy_mode(strategy_mode),
                true,
                true,
                true,
                true,
            );
        };
        let lifecycle_allows = managed.lifecycle_state.allows_new_arbitrage()
            && managed.allows_direction(buy_exchange, sell_exchange)
            && managed.enable_mode.allows_strategy_mode(strategy_mode);
        EffectiveTradability::evaluate(
            lifecycle_allows,
            true,
            true,
            true,
            state.locks.operation_allows_arbitrage(&symbol),
        )
    }

    pub async fn reload_lifecycle_store(&self) {
        if self.lifecycle_path.as_os_str().is_empty() {
            return;
        }
        match read_jsonl::<ManagedSpotSymbol>(&self.lifecycle_path) {
            Ok(symbol_events) => {
                let mut state = self.state.write().await;
                for mut symbol in symbol_events {
                    symbol.normalize_in_place();
                    let should_update = state
                        .symbols
                        .get(&symbol.internal_symbol)
                        .map(|existing| {
                            symbol.version > existing.version
                                || symbol.updated_at > existing.updated_at
                        })
                        .unwrap_or(true);
                    if should_update {
                        state.symbols.insert(symbol.internal_symbol.clone(), symbol);
                    }
                }
            }
            Err(error) => {
                log::warn!(
                    "failed to reload spot control lifecycle store {}: {}",
                    self.lifecycle_path.display(),
                    error
                );
            }
        }
    }

    fn plan_freeze(
        &self,
        managed: &mut ManagedSpotSymbol,
        command_id: &str,
        request: &DisableSymbolRequest,
    ) -> Result<(SpotSymbolLifecycleState, Value), String> {
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::DisableRequested,
            command_id,
        )?;
        if request.cancel_active_orders {
            transition_symbol(
                managed,
                SpotSymbolLifecycleState::CancelingOrders,
                command_id,
            )?;
        }
        transition_symbol(managed, SpotSymbolLifecycleState::Frozen, command_id)?;
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::DisabledWithInventory,
            command_id,
        )?;
        managed.disabled_at_optional = Some(Utc::now());
        managed.disabled_reason_optional = Some("FreezeOnly requested by operator".to_string());
        Ok((
            SpotSymbolLifecycleState::DisabledWithInventory,
            json!({
                "mode": DisableMode::FreezeOnly,
                "cancel_active_orders": request.cancel_active_orders,
                "sold_inventory": false,
                "preserved_managed_inventory": true,
                "preserved_unmanaged_inventory": true
            }),
        ))
    }

    fn plan_market_liquidation(
        &self,
        state: &mut SpotControlState,
        managed: &mut ManagedSpotSymbol,
        command_id: &str,
        request: &DisableSymbolRequest,
        runtime_snapshot: Option<&SpotControlRuntimeSnapshot>,
    ) -> Result<(SpotSymbolLifecycleState, Value), String> {
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::DisableRequested,
            command_id,
        )?;
        if request.cancel_active_orders {
            transition_symbol(
                managed,
                SpotSymbolLifecycleState::CancelingOrders,
                command_id,
            )?;
        }
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::LiquidationPlanning,
            command_id,
        )?;

        let plans = runtime_snapshot
            .map(|snapshot| {
                snapshot
                    .liquidation_preview
                    .market_plans
                    .iter()
                    .cloned()
                    .map(|mut plan| {
                        plan.command_id = command_id.to_string();
                        plan.would_submit_order = false;
                        plan
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                request
                    .selected_exchanges
                    .iter()
                    .map(|exchange| MarketLiquidationPlan {
                        command_id: command_id.to_string(),
                        exchange: super::normalize_exchange(exchange),
                        symbol: managed.internal_symbol.clone(),
                        managed_sellable_quantity: 0.0,
                        rounded_quantity: 0.0,
                        best_bid: 0.0,
                        executable_vwap: 0.0,
                        worst_allowed_price: 0.0,
                        estimated_fee: 0.0,
                        estimated_slippage_bps: request
                            .maximum_slippage_bps
                            .unwrap_or(self.config.disable_defaults.maximum_slippage_bps),
                        estimated_proceeds: 0.0,
                        estimated_loss: 0.0,
                        validation_status: "missing_runtime_snapshot".to_string(),
                        would_submit_order: false,
                        rejection_reason_optional: Some(
                            "runtime snapshot unavailable; no live submission".to_string(),
                        ),
                    })
                    .collect::<Vec<_>>()
            });
        state.liquidation_plans.extend(plans.clone());
        managed.disabled_at_optional = None;
        if self.config.require_liquidation_confirmation {
            Ok((
                SpotSymbolLifecycleState::LiquidationPlanning,
                json!({
                    "mode": DisableMode::MarketLiquidate,
                    "requires_confirmation": true,
                    "would_submit_order": false,
                    "order_type": "IOC limit sell",
                    "snapshot_id": runtime_snapshot.map(|snapshot| snapshot.snapshot_id.clone()),
                    "plans": plans
                }),
            ))
        } else {
            Ok((
                SpotSymbolLifecycleState::ManualInterventionRequired,
                json!({
                    "mode": DisableMode::MarketLiquidate,
                    "live_submission_blocked": true,
                    "reason": "real liquidation execution is not enabled"
                }),
            ))
        }
    }

    fn plan_passive_liquidation(
        &self,
        state: &mut SpotControlState,
        managed: &mut ManagedSpotSymbol,
        command_id: &str,
        request: &DisableSymbolRequest,
        runtime_snapshot: Option<&SpotControlRuntimeSnapshot>,
    ) -> Result<(SpotSymbolLifecycleState, Value), String> {
        if !self.config.passive_liquidation.enabled {
            return Err("passive liquidation is disabled by configuration".to_string());
        }
        if !self.config.passive_liquidation.post_only_required {
            return Err(
                "PostOnly downgrade is forbidden; post_only_required must remain true".to_string(),
            );
        }
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::DisableRequested,
            command_id,
        )?;
        if request.cancel_active_orders {
            transition_symbol(
                managed,
                SpotSymbolLifecycleState::CancelingOrders,
                command_id,
            )?;
        }
        transition_symbol(
            managed,
            SpotSymbolLifecycleState::LiquidationPlanning,
            command_id,
        )?;
        let sessions = runtime_snapshot
            .map(|snapshot| {
                snapshot
                    .liquidation_preview
                    .passive_sessions
                    .iter()
                    .cloned()
                    .map(|mut session| {
                        session.command_id = command_id.to_string();
                        session
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                request
                    .selected_exchanges
                    .iter()
                    .map(|exchange| PassiveLiquidationSession {
                        session_id: Uuid::new_v4().to_string(),
                        command_id: command_id.to_string(),
                        exchange: super::normalize_exchange(exchange),
                        symbol: managed.internal_symbol.clone(),
                        initial_managed_quantity: 0.0,
                        remaining_quantity: 0.0,
                        filled_quantity: 0.0,
                        average_sell_price: 0.0,
                        fees_paid: 0.0,
                        realized_proceeds: 0.0,
                        active_order_id_optional: None,
                        started_at: Utc::now(),
                        last_reprice_at: None,
                        status: PassiveLiquidationStatus::Planning,
                        stop_reason_optional: Some(
                            "runtime snapshot unavailable; no live submission".to_string(),
                        ),
                    })
                    .collect::<Vec<_>>()
            });
        state.passive_sessions.extend(sessions.clone());
        Ok((
            SpotSymbolLifecycleState::LiquidationPlanning,
            json!({
                "mode": DisableMode::PassiveAskLiquidate,
                "requires_confirmation": self.config.require_liquidation_confirmation,
                "post_only_required": true,
                "would_submit_order": false,
                "snapshot_id": runtime_snapshot.map(|snapshot| snapshot.snapshot_id.clone()),
                "sessions": sessions
            }),
        ))
    }

    async fn simple_transition(
        &self,
        operation: SymbolOperationType,
        symbol: String,
        request: VersionedSymbolRequest,
        idempotency_key: String,
        next_state: SpotSymbolLifecycleState,
        reason: &str,
    ) -> ControlCommandResponse {
        let payload = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let symbol = super::normalize_symbol(&symbol);
        let command = self.prepare_command(
            operation,
            &symbol,
            payload,
            &request.requested_by,
            request.expected_version,
            idempotency_key,
        );
        let mut state = self.state.write().await;
        if let Some(response) = self.duplicate_response(&state, &command.idempotency_key) {
            return response;
        }
        let previous = state.symbols.get(&symbol).cloned();
        let Some(mut managed) = previous.clone() else {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical(
                    "symbol_not_managed",
                    "symbol is not managed by spot control",
                )],
                None,
            );
        };
        if let Some(error) = expected_version_error(Some(&managed), request.expected_version) {
            return self.reject_locked(&mut state, command, vec![error], Some(&managed));
        }
        let previous_state = managed.lifecycle_state;
        if let Err(error) = transition_symbol(&mut managed, next_state, &command.command_id) {
            return self.reject_locked(
                &mut state,
                command,
                vec![ValidationError::critical("invalid_transition", error)],
                Some(&managed),
            );
        }
        self.accept_symbol_update(
            &mut state,
            command,
            managed,
            previous_state,
            next_state,
            Vec::new(),
            json!({ "reason": reason }),
        )
    }

    fn prepare_command(
        &self,
        operation: SymbolOperationType,
        symbol: &str,
        payload: Value,
        requested_by: &str,
        expected_version: u64,
        idempotency_key: String,
    ) -> ControlCommand {
        ControlCommand {
            command_id: Uuid::new_v4().to_string(),
            idempotency_key,
            operation,
            symbol: symbol.to_string(),
            payload,
            requested_by: requested_by.to_string(),
            requested_at: Utc::now(),
            expected_version,
            snapshot_id: None,
            status: CommandStatus::Requested,
            validation_errors: Vec::new(),
            started_at_optional: None,
            completed_at_optional: None,
            result_summary_optional: None,
        }
    }

    fn duplicate_response(
        &self,
        state: &SpotControlState,
        idempotency_key: &str,
    ) -> Option<ControlCommandResponse> {
        let command_id = state.idempotency.get(idempotency_key)?;
        let command = state
            .commands
            .iter()
            .find(|command| &command.command_id == command_id)?;
        let symbol = state.symbols.get(&command.symbol);
        Some(ControlCommandResponse {
            command_id: command.command_id.clone(),
            status: command.status,
            symbol: command.symbol.clone(),
            lifecycle_state: symbol.map(|symbol| symbol.lifecycle_state),
            current_version: symbol.map(|symbol| symbol.version),
            validation_errors: command.validation_errors.clone(),
            result_summary: command.result_summary_optional.clone(),
        })
    }

    fn reject_locked(
        &self,
        state: &mut SpotControlState,
        mut command: ControlCommand,
        errors: Vec<ValidationError>,
        symbol: Option<&ManagedSpotSymbol>,
    ) -> ControlCommandResponse {
        command.status = CommandStatus::Rejected;
        command.validation_errors = errors.clone();
        command.completed_at_optional = Some(Utc::now());
        let response = ControlCommandResponse {
            command_id: command.command_id.clone(),
            status: command.status,
            symbol: command.symbol.clone(),
            lifecycle_state: symbol.map(|item| item.lifecycle_state),
            current_version: symbol.map(|item| item.version),
            validation_errors: errors,
            result_summary: None,
        };
        self.record_command_locked(state, command.clone());
        self.record_audit_locked(
            state,
            AuditEvent::new(
                Some(command.command_id),
                command.requested_by,
                format!("{:?}", command.operation),
                command.symbol,
                symbol.map(|item| item.lifecycle_state),
                symbol.map(|item| item.lifecycle_state),
                "rejected",
                "validation failed",
                json!({ "validation_errors": response.validation_errors }),
            )
            .with_snapshot_id(command.snapshot_id.clone()),
        );
        response
    }

    #[allow(clippy::too_many_arguments)]
    fn accept_symbol_update(
        &self,
        state: &mut SpotControlState,
        mut command: ControlCommand,
        mut managed: ManagedSpotSymbol,
        previous_state: SpotSymbolLifecycleState,
        new_state: SpotSymbolLifecycleState,
        errors: Vec<ValidationError>,
        summary: Value,
    ) -> ControlCommandResponse {
        command.status = if errors.iter().any(|error| error.critical) {
            CommandStatus::Failed
        } else if matches!(new_state, SpotSymbolLifecycleState::LiquidationPlanning)
            && summary
                .get("requires_confirmation")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            CommandStatus::AwaitingConfirmation
        } else {
            CommandStatus::Completed
        };
        command.validation_errors = errors.clone();
        command.started_at_optional = Some(command.requested_at);
        command.completed_at_optional = Some(Utc::now());
        command.result_summary_optional = Some(summary.clone());
        managed.lifecycle_state = new_state;
        managed.version += 1;
        managed.updated_at = Utc::now();
        managed.last_command_id = Some(command.command_id.clone());
        let response = ControlCommandResponse {
            command_id: command.command_id.clone(),
            status: command.status,
            symbol: managed.internal_symbol.clone(),
            lifecycle_state: Some(managed.lifecycle_state),
            current_version: Some(managed.version),
            validation_errors: errors,
            result_summary: Some(summary.clone()),
        };
        state
            .symbols
            .insert(managed.internal_symbol.clone(), managed.clone());
        self.persist_symbol(&managed);
        self.record_command_locked(state, command.clone());
        self.record_audit_locked(
            state,
            AuditEvent::new(
                Some(command.command_id),
                command.requested_by,
                format!("{:?}", command.operation),
                managed.internal_symbol,
                Some(previous_state),
                Some(new_state),
                format!("{:?}", command.status),
                "accepted by control processor",
                summary,
            )
            .with_snapshot_id(command.snapshot_id.clone()),
        );
        response
    }

    fn accept_command_only(
        &self,
        state: &mut SpotControlState,
        mut command: ControlCommand,
        symbol: Option<&ManagedSpotSymbol>,
        status: CommandStatus,
        summary: Value,
    ) -> ControlCommandResponse {
        command.status = status;
        command.started_at_optional = Some(command.requested_at);
        command.completed_at_optional = Some(Utc::now());
        command.result_summary_optional = Some(summary.clone());
        let response = ControlCommandResponse {
            command_id: command.command_id.clone(),
            status: command.status,
            symbol: command.symbol.clone(),
            lifecycle_state: symbol.map(|item| item.lifecycle_state),
            current_version: symbol.map(|item| item.version),
            validation_errors: Vec::new(),
            result_summary: Some(summary.clone()),
        };
        self.record_command_locked(state, command.clone());
        self.record_audit_locked(
            state,
            AuditEvent::new(
                Some(command.command_id),
                command.requested_by,
                format!("{:?}", command.operation),
                command.symbol,
                symbol.map(|item| item.lifecycle_state),
                symbol.map(|item| item.lifecycle_state),
                format!("{:?}", command.status),
                "accepted by control processor",
                summary,
            )
            .with_snapshot_id(command.snapshot_id.clone()),
        );
        response
    }

    fn record_command_locked(&self, state: &mut SpotControlState, command: ControlCommand) {
        state
            .idempotency
            .insert(command.idempotency_key.clone(), command.command_id.clone());
        self.persist_command(&command);
        state.commands.push(command);
    }

    fn record_audit_locked(&self, state: &mut SpotControlState, event: AuditEvent) {
        self.persist_audit(&event);
        state.audit_events.push(event);
    }

    fn persist_command(&self, command: &ControlCommand) {
        if !self.command_path.as_os_str().is_empty() {
            let _ = append_jsonl(&self.command_path, command);
        }
    }

    fn persist_audit(&self, event: &AuditEvent) {
        if !self.audit_path.as_os_str().is_empty() {
            let _ = append_jsonl(&self.audit_path, event);
        }
    }

    fn persist_symbol(&self, symbol: &ManagedSpotSymbol) {
        if !self.lifecycle_path.as_os_str().is_empty() {
            let _ = append_jsonl(&self.lifecycle_path, symbol);
        }
    }
}

fn transition_symbol(
    symbol: &mut ManagedSpotSymbol,
    next: SpotSymbolLifecycleState,
    command_id: &str,
) -> Result<(), String> {
    validate_transition(symbol.lifecycle_state, next)?;
    symbol.lifecycle_state = next;
    symbol.last_command_id = Some(command_id.to_string());
    symbol.updated_at = Utc::now();
    Ok(())
}

fn expected_version_error(
    symbol: Option<&ManagedSpotSymbol>,
    expected_version: u64,
) -> Option<ValidationError> {
    let current = symbol.map(|symbol| symbol.version).unwrap_or(0);
    if current == expected_version {
        None
    } else {
        Some(ValidationError::critical(
            "stale_lifecycle_version",
            format!("expected lifecycle version {expected_version}, current version is {current}"),
        ))
    }
}

fn has_error(report: &EnableValidationReport, code: &str) -> bool {
    report.errors.iter().any(|error| error.code == code)
}

pub fn control_store_path_enabled(path: &Path) -> bool {
    !path.as_os_str().is_empty()
}

fn trim_snapshots(snapshots: &mut Vec<SpotControlRuntimeSnapshot>) {
    const MAX_SNAPSHOTS: usize = 256;
    if snapshots.len() > MAX_SNAPSHOTS {
        let excess = snapshots.len() - MAX_SNAPSHOTS;
        snapshots.drain(0..excess);
    }
}
