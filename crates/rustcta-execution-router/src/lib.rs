// Router errors embed the public execution protocol error so callers can retain
// the full serialized failure payload.
#![allow(clippy::result_large_err)]

use chrono::{DateTime, Utc};
use rustcta_event_ledger::{
    AssetBalanceRecord, AuditActor, AuditActorType, AuditOutcome, AuditRecord,
    BalanceSnapshotRecord, EventIdentity, EventKind, FillLedgerRecord, LedgerEvent, LedgerPayload,
    LedgerWriter, OrderLifecycleRecord,
};
use rustcta_exchange_api::{
    CancelAllOrdersRequest, CancelOrderRequest, PlaceOrderRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    ExchangeGateway, GatewayClient, GatewayError, GatewayOperation, GatewayProtocolRequest,
    GatewayRequest, GatewayRequestPayload, GatewayResponse, GatewayResponsePayload,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use rustcta_execution_api::{
    BundleSubmissionPath, BundleSubmitAck, BundleSubmitCommand, CancelAck, CancelAllAck,
    CancelAllCommand, CancelCommand, ExecutionApiError, ExecutionDecisionOutcome,
    ExecutionFeeAssetMode, ExecutionFeeRole, ExecutionFeeSource, ExecutionMutationKind,
    FeeEstimate, FeeModelDecision, FeeRateSnapshot, FillEvent, IdempotencyDecision,
    LiveDryRunDecision, MutationIdentity, NormalizedUserStreamEvent, OrderAck, OrderCommand,
    OrderReconciliationSummary, OrderState, ReconciliationEvent, RejectionDecision,
    ReservationDecision, RiskDecision, RiskDecisionEvent, EXECUTION_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, ExchangeBalance, OrderSide, OrderStatus, PositionSide, RunId, TenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouterMode {
    DryRun,
    LiveDryRun,
    Live,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIdentity {
    pub tenant_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub run_id: String,
    pub command_id: String,
    pub idempotency_key: String,
    pub risk_profile_id: String,
}

impl ExecutionIdentity {
    pub fn validate(&self) -> Result<(), ExecutionRouterError> {
        for (name, value) in [
            ("tenant_id", &self.tenant_id),
            ("account_id", &self.account_id),
            ("strategy_id", &self.strategy_id),
            ("run_id", &self.run_id),
            ("command_id", &self.command_id),
            ("idempotency_key", &self.idempotency_key),
            ("risk_profile_id", &self.risk_profile_id),
        ] {
            if value.trim().is_empty() {
                return Err(ExecutionRouterError::MissingIdentityField {
                    field: name.to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedExecutionCommand {
    pub identity: ExecutionIdentity,
    pub operation: String,
    #[serde(default)]
    pub payload: Value,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedExecutionAck {
    pub identity: ExecutionIdentity,
    pub accepted: bool,
    pub dry_run: bool,
    #[serde(default)]
    pub payload: Value,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ExecutionRouterError {
    #[error("missing execution identity field {field}")]
    MissingIdentityField { field: String },
    #[error("gateway error: {0}")]
    Gateway(#[from] GatewayError),
    #[error("execution API error: {0}")]
    ExecutionApi(#[from] ExecutionApiError),
    #[error("gateway payload mismatch: {0}")]
    GatewayPayload(String),
    #[error("ledger error: {0}")]
    Ledger(#[from] rustcta_event_ledger::LedgerError),
    #[error("router state error: {0}")]
    State(String),
}

#[derive(Debug, Clone)]
pub struct ExecutionRouterConfig {
    pub mode: RouterMode,
    pub enforce_idempotency: bool,
}

impl ExecutionRouterConfig {
    pub fn dry_run() -> Self {
        Self {
            mode: RouterMode::DryRun,
            enforce_idempotency: false,
        }
    }

    pub fn live() -> Self {
        Self {
            mode: RouterMode::Live,
            enforce_idempotency: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastRiskConfig {
    pub max_exchange_notional: Option<f64>,
    pub max_symbol_notional: Option<f64>,
    pub max_strategy_notional: Option<f64>,
    pub max_run_notional: Option<f64>,
    pub max_unhedged_exposure: Option<f64>,
    pub max_order_rate_per_window: Option<u32>,
    pub max_cancel_rate_per_window: Option<u32>,
    pub rate_window_ms: i64,
}

impl Default for FastRiskConfig {
    fn default() -> Self {
        Self {
            max_exchange_notional: None,
            max_symbol_notional: None,
            max_strategy_notional: None,
            max_run_notional: None,
            max_unhedged_exposure: None,
            max_order_rate_per_window: None,
            max_cancel_rate_per_window: None,
            rate_window_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastRiskContext {
    pub config: FastRiskConfig,
    #[serde(default)]
    pub kill_switch_active: bool,
    #[serde(default)]
    pub disabled_exchanges: HashSet<String>,
    #[serde(default)]
    pub disabled_symbols: HashSet<String>,
    #[serde(default)]
    pub disabled_exchange_symbols: HashSet<String>,
    #[serde(default)]
    pub exchange_cooldowns: HashMap<String, String>,
    #[serde(default)]
    pub symbol_cooldowns: HashMap<String, String>,
}

impl FastRiskContext {
    pub fn new(config: FastRiskConfig) -> Self {
        Self {
            config,
            kill_switch_active: false,
            disabled_exchanges: HashSet::new(),
            disabled_symbols: HashSet::new(),
            disabled_exchange_symbols: HashSet::new(),
            exchange_cooldowns: HashMap::new(),
            symbol_cooldowns: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastRiskReservation {
    pub reservation_id: String,
    pub command_id: String,
    pub exchange_id: rustcta_types::ExchangeId,
    pub market_type: rustcta_types::MarketType,
    pub canonical_symbol: rustcta_types::CanonicalSymbol,
    pub strategy_id: rustcta_types::StrategyId,
    pub run_id: rustcta_types::RunId,
    pub notional: f64,
    pub reserved_at: DateTime<Utc>,
    pub reconcile_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastRiskOrderDecision {
    pub risk: RiskDecision,
    pub reservation: ReservationDecision,
    pub handle: Option<FastRiskReservation>,
}

#[derive(Debug, Clone, Default)]
struct FastRiskCounters {
    exchange_notional: HashMap<String, f64>,
    symbol_notional: HashMap<String, f64>,
    strategy_notional: HashMap<String, f64>,
    run_notional: HashMap<String, f64>,
    unhedged_exposure: f64,
    order_timestamps: VecDeque<DateTime<Utc>>,
    cancel_timestamps: VecDeque<DateTime<Utc>>,
    reservations: HashMap<String, FastRiskReservation>,
}

#[derive(Debug)]
pub struct FastRiskEngine {
    context: FastRiskContext,
    counters: Mutex<FastRiskCounters>,
}

impl FastRiskEngine {
    pub fn new(context: FastRiskContext) -> Self {
        Self {
            context,
            counters: Mutex::new(FastRiskCounters::default()),
        }
    }

    pub fn reserve_order(&self, command: &OrderCommand) -> FastRiskOrderDecision {
        let decided_at = Utc::now();
        let decision_id = risk_decision_id(&command.command_id, decided_at);
        let notional = order_notional(command);

        if let Some(reason) = self.static_order_block_reason(command, notional) {
            return rejected_order_decision(command, decision_id, reason, decided_at, notional);
        }

        let mut counters = match self.counters.lock() {
            Ok(counters) => counters,
            Err(_) => {
                return rejected_order_decision(
                    command,
                    decision_id,
                    "fast risk state lock poisoned".to_string(),
                    decided_at,
                    notional,
                );
            }
        };
        prune_window(
            &mut counters.order_timestamps,
            decided_at,
            self.context.config.rate_window_ms,
        );

        if let Some(limit) = self.context.config.max_order_rate_per_window {
            if counters.order_timestamps.len() >= limit as usize {
                return rejected_order_decision(
                    command,
                    decision_id,
                    format!("order-rate limit exceeded: {limit} per window"),
                    decided_at,
                    notional,
                );
            }
        }

        let exchange_key = exchange_key(&command.exchange_id);
        let symbol_key = symbol_key(&command.canonical_symbol);
        let strategy_key = command.identity.strategy_id.to_string();
        let run_key = run_key(&command.identity);
        if let Some(reason) = self.limit_block_reason(
            &counters,
            &exchange_key,
            &symbol_key,
            &strategy_key,
            &run_key,
            notional,
        ) {
            return rejected_order_decision(command, decision_id, reason, decided_at, notional);
        }

        counters.order_timestamps.push_back(decided_at);
        *counters.exchange_notional.entry(exchange_key).or_default() += notional;
        *counters.symbol_notional.entry(symbol_key).or_default() += notional;
        *counters.strategy_notional.entry(strategy_key).or_default() += notional;
        *counters.run_notional.entry(run_key).or_default() += notional;
        counters.unhedged_exposure += notional;

        let handle = FastRiskReservation {
            reservation_id: format!("{}:{}", command.identity.run_id, command.command_id),
            command_id: command.command_id.clone(),
            exchange_id: command.exchange_id.clone(),
            market_type: command.market_type,
            canonical_symbol: command.canonical_symbol.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            notional,
            reserved_at: decided_at,
            reconcile_pending: false,
        };
        counters
            .reservations
            .insert(command.command_id.clone(), handle.clone());

        FastRiskOrderDecision {
            risk: RiskDecision::Approved {
                risk_profile_id: command.identity.risk_profile_id.clone(),
                decision_id,
                decided_at,
            },
            reservation: reservation_decision(
                command,
                Some(notional),
                Some(notional),
                ExecutionDecisionOutcome::Approved,
                None,
                decided_at,
            ),
            handle: Some(handle),
        }
    }

    pub fn record_cancel(&self, command: &CancelCommand) -> RiskDecision {
        let decided_at = Utc::now();
        let decision_id = risk_decision_id(&command.command_id, decided_at);

        if let Some(reason) = self.static_cancel_block_reason(command) {
            return RiskDecision::Rejected {
                risk_profile_id: command.identity.risk_profile_id.clone(),
                decision_id,
                reason,
                decided_at,
            };
        }

        let Ok(mut counters) = self.counters.lock() else {
            return RiskDecision::Rejected {
                risk_profile_id: command.identity.risk_profile_id.clone(),
                decision_id,
                reason: "fast risk state lock poisoned".to_string(),
                decided_at,
            };
        };
        prune_window(
            &mut counters.cancel_timestamps,
            decided_at,
            self.context.config.rate_window_ms,
        );
        if let Some(limit) = self.context.config.max_cancel_rate_per_window {
            if counters.cancel_timestamps.len() >= limit as usize {
                return RiskDecision::Rejected {
                    risk_profile_id: command.identity.risk_profile_id.clone(),
                    decision_id,
                    reason: format!("cancel-rate limit exceeded: {limit} per window"),
                    decided_at,
                };
            }
        }
        counters.cancel_timestamps.push_back(decided_at);
        RiskDecision::Approved {
            risk_profile_id: command.identity.risk_profile_id.clone(),
            decision_id,
            decided_at,
        }
    }

    pub fn release_reservation(&self, reservation: &FastRiskReservation) {
        self.adjust_reservation(reservation, false);
    }

    pub fn mark_reconcile_pending(&self, reservation: &FastRiskReservation) {
        if let Ok(mut counters) = self.counters.lock() {
            if let Some(existing) = counters.reservations.get_mut(&reservation.command_id) {
                existing.reconcile_pending = true;
            }
        }
    }

    pub fn apply_cooldown(&self, _scope: &str, _reason: &str) {
        // Cooldown mutations are intentionally configured outside the hot path.
    }

    pub fn reserved_notional_for_command(&self, command_id: &str) -> Option<f64> {
        self.counters
            .lock()
            .ok()
            .and_then(|counters| counters.reservations.get(command_id).map(|r| r.notional))
    }

    fn adjust_reservation(&self, reservation: &FastRiskReservation, keep_pending: bool) {
        let Ok(mut counters) = self.counters.lock() else {
            return;
        };
        if !keep_pending {
            counters.reservations.remove(&reservation.command_id);
        }
        subtract_counter(
            &mut counters.exchange_notional,
            &exchange_key(&reservation.exchange_id),
            reservation.notional,
        );
        subtract_counter(
            &mut counters.symbol_notional,
            &symbol_key(&reservation.canonical_symbol),
            reservation.notional,
        );
        subtract_counter(
            &mut counters.strategy_notional,
            &reservation.strategy_id.to_string(),
            reservation.notional,
        );
        subtract_counter(
            &mut counters.run_notional,
            &format!("{}|{}", reservation.strategy_id, reservation.run_id),
            reservation.notional,
        );
        counters.unhedged_exposure = (counters.unhedged_exposure - reservation.notional).max(0.0);
    }

    fn static_order_block_reason(&self, command: &OrderCommand, notional: f64) -> Option<String> {
        if self.context.kill_switch_active {
            return Some("local kill switch active".to_string());
        }
        if !notional.is_finite() || notional <= 0.0 {
            return Some("order notional must be finite and positive".to_string());
        }
        let exchange = exchange_key(&command.exchange_id);
        let symbol = symbol_key(&command.canonical_symbol);
        let exchange_symbol = exchange_symbol_key(&command.exchange_id, &command.canonical_symbol);
        if self.context.disabled_exchanges.contains(&exchange) {
            return Some(format!("exchange disabled: {exchange}"));
        }
        if self.context.disabled_symbols.contains(&symbol) {
            return Some(format!("symbol disabled: {symbol}"));
        }
        if self
            .context
            .disabled_exchange_symbols
            .contains(&exchange_symbol)
        {
            return Some(format!("exchange-symbol disabled: {exchange_symbol}"));
        }
        if let Some(reason) = self.context.exchange_cooldowns.get(&exchange) {
            return Some(format!("exchange cooldown active: {reason}"));
        }
        if let Some(reason) = self.context.symbol_cooldowns.get(&exchange_symbol) {
            return Some(format!("symbol cooldown active: {reason}"));
        }
        None
    }

    fn static_cancel_block_reason(&self, command: &CancelCommand) -> Option<String> {
        if self.context.kill_switch_active {
            return Some("local kill switch active".to_string());
        }
        let exchange = exchange_key(&command.exchange_id);
        let exchange_symbol = exchange_symbol_key(&command.exchange_id, &command.canonical_symbol);
        if let Some(reason) = self.context.exchange_cooldowns.get(&exchange) {
            return Some(format!("exchange cooldown active: {reason}"));
        }
        if let Some(reason) = self.context.symbol_cooldowns.get(&exchange_symbol) {
            return Some(format!("symbol cooldown active: {reason}"));
        }
        None
    }

    fn limit_block_reason(
        &self,
        counters: &FastRiskCounters,
        exchange_key: &str,
        symbol_key: &str,
        strategy_key: &str,
        run_key: &str,
        notional: f64,
    ) -> Option<String> {
        limit_reason(
            "exchange notional",
            self.context.config.max_exchange_notional,
            counters
                .exchange_notional
                .get(exchange_key)
                .copied()
                .unwrap_or(0.0),
            notional,
        )
        .or_else(|| {
            limit_reason(
                "symbol notional",
                self.context.config.max_symbol_notional,
                counters
                    .symbol_notional
                    .get(symbol_key)
                    .copied()
                    .unwrap_or(0.0),
                notional,
            )
        })
        .or_else(|| {
            limit_reason(
                "strategy notional",
                self.context.config.max_strategy_notional,
                counters
                    .strategy_notional
                    .get(strategy_key)
                    .copied()
                    .unwrap_or(0.0),
                notional,
            )
        })
        .or_else(|| {
            limit_reason(
                "run notional",
                self.context.config.max_run_notional,
                counters.run_notional.get(run_key).copied().unwrap_or(0.0),
                notional,
            )
        })
        .or_else(|| {
            limit_reason(
                "unhedged exposure",
                self.context.config.max_unhedged_exposure,
                counters.unhedged_exposure,
                notional,
            )
        })
    }
}

pub struct ExecutionRouter<G> {
    config: ExecutionRouterConfig,
    gateway: G,
    ledger: Option<Arc<dyn LedgerWriter>>,
    idempotency_index: Arc<Mutex<HashMap<String, String>>>,
    fast_risk: Option<Arc<FastRiskEngine>>,
}

impl<G> ExecutionRouter<G> {
    pub fn new(config: ExecutionRouterConfig, gateway: G) -> Self {
        Self {
            config,
            gateway,
            ledger: None,
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            fast_risk: None,
        }
    }

    pub fn with_ledger(
        config: ExecutionRouterConfig,
        gateway: G,
        ledger: Arc<dyn LedgerWriter>,
    ) -> Self {
        Self {
            config,
            gateway,
            ledger: Some(ledger),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            fast_risk: None,
        }
    }

    pub fn with_fast_risk_engine(mut self, fast_risk: Arc<FastRiskEngine>) -> Self {
        self.fast_risk = Some(fast_risk);
        self
    }

    pub async fn record_fill(&self, event: FillEvent) -> Result<(), ExecutionRouterError> {
        event.validate()?;
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = FillLedgerRecord::new(
            event_identity_for_fill(&event),
            event.exchange_id.clone(),
            event.market_type,
            event.canonical_symbol.clone(),
            event.side,
            PositionSide::None,
            event.liquidity,
            event.price,
            event.quantity,
            event.filled_at,
        );
        record.client_order_id = event.client_order_id.clone();
        record.exchange_order_id = event.exchange_order_id.clone();
        record.fill_id = Some(event.fill_id.clone());
        record.fee_asset = event.fee_asset.clone();
        record.fee_amount = event.fee_amount;
        record.metadata = serde_json::json!({
            "exchange_symbol": event.exchange_symbol,
            "trade_id": event.trade_id,
            "received_at": event.received_at,
        });
        ledger.append(LedgerEvent::fill(record)).await?;
        Ok(())
    }

    pub async fn record_account_snapshot(
        &self,
        snapshot: ExchangeBalance,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let identity = EventIdentity::new(
            snapshot.tenant_id.clone(),
            "rustcta-execution-router",
            snapshot.observed_at,
        )
        .with_account(snapshot.account_id.clone());
        let balances = snapshot
            .balances
            .iter()
            .map(|balance| {
                AssetBalanceRecord::new(
                    balance.asset.clone(),
                    balance.total,
                    balance.available,
                    balance.locked,
                    balance.locally_reserved,
                )
            })
            .collect();
        let record = BalanceSnapshotRecord::new(
            identity,
            snapshot.exchange_id,
            snapshot.market_type,
            balances,
            snapshot.observed_at,
        );
        ledger.append(LedgerEvent::balance_snapshot(record)).await?;
        Ok(())
    }

    pub async fn record_reconciliation(
        &self,
        event: ReconciliationEvent,
    ) -> Result<(), ExecutionRouterError> {
        event.validate()?;
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut identity = EventIdentity::new(
            event.tenant_id.clone(),
            "rustcta-execution-router",
            event.reconciled_at,
        )
        .with_account(event.account_id.clone());
        if let (Some(strategy_id), Some(run_id)) = (event.strategy_id.clone(), event.run_id.clone())
        {
            identity = identity.with_strategy_run(strategy_id, run_id);
        }
        if let Some(client_order_id) = &event.client_order_id {
            identity = identity.with_command(client_order_id.clone());
        }
        ledger
            .append(LedgerEvent::reconciliation(identity, event))
            .await?;
        Ok(())
    }

    pub async fn record_order_reconciliation(
        &self,
        event: OrderReconciliationSummary,
    ) -> Result<(), ExecutionRouterError> {
        event.validate()?;
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut identity = EventIdentity::new(
            event.tenant_id.clone(),
            "rustcta-execution-router",
            event.reconciled_at,
        )
        .with_account(event.account_id.clone());
        if let (Some(strategy_id), Some(run_id)) = (event.strategy_id.clone(), event.run_id.clone())
        {
            identity = identity.with_strategy_run(strategy_id, run_id);
        }
        if let Some(client_order_id) = &event.client_order_id {
            identity = identity.with_command(client_order_id.clone());
        }
        ledger
            .append(LedgerEvent::order_reconciliation(identity, event))
            .await?;
        Ok(())
    }

    pub async fn record_user_stream_event(
        &self,
        event: NormalizedUserStreamEvent,
    ) -> Result<(), ExecutionRouterError> {
        event.validate()?;
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut identity = EventIdentity::new(
            event.tenant_id.clone(),
            "rustcta-execution-router",
            event.received_at,
        )
        .with_account(event.account_id.clone());
        if let (Some(strategy_id), Some(run_id)) = (event.strategy_id.clone(), event.run_id.clone())
        {
            identity = identity.with_strategy_run(strategy_id, run_id);
        }
        if let Some(event_id) = &event.event_id {
            identity = identity.with_correlation_id(event_id.clone());
        }
        ledger
            .append(LedgerEvent::user_stream_event(identity, event))
            .await?;
        Ok(())
    }
}

impl<G> ExecutionRouter<G>
where
    G: ExchangeGateway,
{
    pub async fn route(
        &self,
        command: RoutedExecutionCommand,
    ) -> Result<RoutedExecutionAck, ExecutionRouterError> {
        command.identity.validate()?;
        rustcta_event_ledger::reject_secret_fields(&command)?;
        self.append_routed_command_event(&command).await?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            let ack = RoutedExecutionAck {
                identity: command.identity,
                accepted: false,
                dry_run: true,
                payload: command.payload,
                message: Some("execution router dry-run: gateway mutation not called".to_string()),
                acknowledged_at: Utc::now(),
            };
            self.append_routed_ack_event(&ack).await?;
            return Ok(ack);
        }

        let response: GatewayResponse = self
            .gateway
            .handle(GatewayRequest {
                request_id: command.identity.command_id.clone(),
                tenant_id: command.identity.tenant_id.clone(),
                account_id: Some(command.identity.account_id.clone()),
                operation: command.operation,
                payload: command.payload,
                requested_at: command.requested_at,
            })
            .await?;

        let ack = RoutedExecutionAck {
            identity: command.identity,
            accepted: response.accepted,
            dry_run: false,
            payload: response.payload,
            message: response.error,
            acknowledged_at: response.responded_at,
        };
        self.append_routed_ack_event(&ack).await?;
        Ok(ack)
    }

    async fn append_routed_command_event(
        &self,
        command: &RoutedExecutionCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = AuditRecord::new(
            event_identity_for_routed(&command.identity, command.requested_at),
            AuditActor::new(AuditActorType::ExecutionRouter, "rustcta-execution-router"),
            format!("execution.{}", command.operation),
            AuditOutcome::Accepted,
        );
        record.message = Some("routed execution mutation requested".to_string());
        record.metadata = serde_json::json!({
            "operation": command.operation,
            "payload": command.payload,
            "requested_at": command.requested_at
        });
        ledger.append(LedgerEvent::operator_command(record)).await?;
        Ok(())
    }

    async fn append_routed_ack_event(
        &self,
        ack: &RoutedExecutionAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = AuditRecord::new(
            event_identity_for_routed(&ack.identity, ack.acknowledged_at),
            AuditActor::new(AuditActorType::ExecutionRouter, "rustcta-execution-router"),
            "execution.routed_ack",
            if ack.accepted {
                AuditOutcome::Succeeded
            } else {
                AuditOutcome::Rejected
            },
        );
        record.message = ack.message.clone();
        record.metadata = serde_json::json!({
            "accepted": ack.accepted,
            "dry_run": ack.dry_run,
            "payload": ack.payload,
            "acknowledged_at": ack.acknowledged_at
        });
        ledger.append(LedgerEvent::audit(record)).await?;
        Ok(())
    }
}

impl<G> ExecutionRouter<G>
where
    G: GatewayClient,
{
    pub async fn submit_order_bundle(
        &self,
        command: BundleSubmitCommand,
    ) -> Result<BundleSubmitAck, ExecutionRouterError> {
        command.validate()?;
        rustcta_event_ledger::reject_secret_fields(&command)?;

        let submission_path = if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            BundleSubmissionPath::DryRun
        } else {
            BundleSubmissionPath::GatewaySequential
        };

        if let Some(existing_command_id) = self
            .record_idempotency_decision(
                &command.identity,
                &command.bundle_id,
                ExecutionMutationKind::SubmitBundle,
            )
            .await?
        {
            let message =
                format!("duplicate idempotency_key already used by command {existing_command_id}");
            self.append_rejection_event(
                &command.identity,
                &command.bundle_id,
                ExecutionMutationKind::Idempotency,
                message.clone(),
            )
            .await?;
            let order_acks = command
                .legs
                .iter()
                .map(|leg| OrderAck::rejected(&leg.command, message.clone(), Utc::now()))
                .collect();
            let ack =
                BundleSubmitAck::from_order_acks(&command, submission_path, order_acks, Utc::now());
            self.append_bundle_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let mut order_acks = Vec::with_capacity(command.legs.len());
        for leg in &command.legs {
            match self.place_order(leg.command.clone()).await {
                Ok(ack) => order_acks.push(ack),
                Err(error) => order_acks.push(OrderAck::rejected(
                    &leg.command,
                    error.to_string(),
                    Utc::now(),
                )),
            }
        }

        let ack =
            BundleSubmitAck::from_order_acks(&command, submission_path, order_acks, Utc::now());
        self.append_bundle_ack_event(&command, &ack).await?;
        Ok(ack)
    }

    pub async fn place_order(
        &self,
        command: OrderCommand,
    ) -> Result<OrderAck, ExecutionRouterError> {
        command.validate()?;
        rustcta_event_ledger::reject_secret_fields(&command)?;
        if let Some(existing_command_id) = self
            .record_idempotency_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::PlaceOrder,
            )
            .await?
        {
            let message =
                format!("duplicate idempotency_key already used by command {existing_command_id}");
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Idempotency,
                message.clone(),
            )
            .await?;
            return Ok(OrderAck::rejected(&command, message, Utc::now()));
        }
        let fast_risk_decision = self.evaluate_fast_order_risk(&command).await?;
        if !fast_risk_decision.risk.is_approved() {
            let message = risk_rejection_reason(&fast_risk_decision.risk)
                .unwrap_or_else(|| "fast risk rejected order".to_string());
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Rejection,
                message.clone(),
            )
            .await?;
            return Ok(OrderAck::rejected(&command, message, Utc::now()));
        }
        self.append_order_decision_events(&command, Some(&fast_risk_decision))
            .await?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_order_command_event(&command).await?;
            self.append_live_dry_run_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::PlaceOrder,
            )
            .await?;
            let ack = OrderAck::rejected(
                &command,
                "execution router dry-run: gateway mutation not called",
                Utc::now(),
            );
            self.append_order_ack_event(&command, &ack).await?;
            self.release_fast_reservation(fast_risk_decision.handle.as_ref());
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_place_order(&command);
        self.append_order_command_event(&command).await?;
        let response = match self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.mark_fast_reservation_reconcile_pending(fast_risk_decision.handle.as_ref());
                return Err(error.into());
            }
        };
        let place_order = match response.payload {
            GatewayResponsePayload::PlaceOrder(response) => response,
            other => {
                self.mark_fast_reservation_reconcile_pending(fast_risk_decision.handle.as_ref());
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected place_order response, got {other:?}"
                )));
            }
        };

        let ack = OrderAck {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            command_id: command.command_id.clone(),
            exchange_id: command.exchange_id.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: place_order.order.exchange_order_id,
            idempotency_key: command.identity.idempotency_key.clone(),
            accepted: response.accepted && place_order.order.status != OrderStatus::Rejected,
            state: execution_order_state(place_order.order.status),
            message: response.error.map(|error| error.message),
            acknowledged_at: response.responded_at,
        };
        self.append_order_ack_event(&command, &ack).await?;
        if !ack.accepted {
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Rejection,
                ack.message
                    .clone()
                    .unwrap_or_else(|| "order rejected by gateway".to_string()),
            )
            .await?;
            self.release_fast_reservation(fast_risk_decision.handle.as_ref());
        }
        Ok(ack)
    }

    pub async fn cancel_order(
        &self,
        command: CancelCommand,
    ) -> Result<CancelAck, ExecutionRouterError> {
        command.validate()?;
        rustcta_event_ledger::reject_secret_fields(&command)?;
        if let Some(existing_command_id) = self
            .record_idempotency_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::CancelOrder,
            )
            .await?
        {
            let message =
                format!("duplicate idempotency_key already used by command {existing_command_id}");
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Idempotency,
                message.clone(),
            )
            .await?;
            return Ok(cancel_ack(
                &command,
                false,
                OrderState::Rejected,
                Some(message),
                Utc::now(),
            ));
        }
        let risk_decision = self.evaluate_fast_cancel_risk(&command).await?;
        if !risk_decision.is_approved() {
            let message = risk_rejection_reason(&risk_decision)
                .unwrap_or_else(|| "fast risk rejected cancel".to_string());
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Rejection,
                message.clone(),
            )
            .await?;
            return Ok(cancel_ack(
                &command,
                false,
                OrderState::Rejected,
                Some(message),
                Utc::now(),
            ));
        }
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_cancel_command_event(&command).await?;
            self.append_live_dry_run_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::CancelOrder,
            )
            .await?;
            let ack = cancel_ack(
                &command,
                false,
                OrderState::Rejected,
                Some("execution router dry-run: gateway mutation not called".to_string()),
                Utc::now(),
            );
            self.append_cancel_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_cancel_order(&command);
        self.append_cancel_command_event(&command).await?;
        let response = self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await?;
        let cancel_order = match response.payload {
            GatewayResponsePayload::CancelOrder(response) => response,
            other => {
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected cancel_order response, got {other:?}"
                )));
            }
        };

        let ack = cancel_ack(
            &command,
            response.accepted && cancel_order.cancelled,
            execution_order_state(cancel_order.order.status),
            response.error.map(|error| error.message),
            response.responded_at,
        );
        self.append_cancel_ack_event(&command, &ack).await?;
        if !ack.accepted {
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Rejection,
                ack.message
                    .clone()
                    .unwrap_or_else(|| "cancel rejected by gateway".to_string()),
            )
            .await?;
        }
        Ok(ack)
    }

    pub async fn cancel_all_orders(
        &self,
        command: CancelAllCommand,
    ) -> Result<CancelAllAck, ExecutionRouterError> {
        command.validate()?;
        rustcta_event_ledger::reject_secret_fields(&command)?;
        if let Some(existing_command_id) = self
            .record_idempotency_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::CancelAllOrders,
            )
            .await?
        {
            let message =
                format!("duplicate idempotency_key already used by command {existing_command_id}");
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Idempotency,
                message.clone(),
            )
            .await?;
            return Ok(cancel_all_ack(
                &command,
                false,
                0,
                Some(message),
                Utc::now(),
            ));
        }
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_cancel_all_command_event(&command).await?;
            self.append_live_dry_run_decision(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::CancelAllOrders,
            )
            .await?;
            let ack = cancel_all_ack(
                &command,
                false,
                0,
                Some("execution router dry-run: gateway mutation not called".to_string()),
                Utc::now(),
            );
            self.append_cancel_all_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_cancel_all_orders(&command);
        self.append_cancel_all_command_event(&command).await?;
        let response = self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await?;
        let cancel_all = match response.payload {
            GatewayResponsePayload::CancelAllOrders(response) => response,
            other => {
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected cancel_all_orders response, got {other:?}"
                )));
            }
        };

        let ack = cancel_all_ack(
            &command,
            response.accepted,
            cancel_all.cancelled_count,
            response.error.map(|error| error.message),
            response.responded_at,
        );
        self.append_cancel_all_ack_event(&command, &ack).await?;
        if !ack.accepted {
            self.append_rejection_event(
                &command.identity,
                &command.command_id,
                ExecutionMutationKind::Rejection,
                ack.message
                    .clone()
                    .unwrap_or_else(|| "cancel-all rejected by gateway".to_string()),
            )
            .await?;
        }
        Ok(ack)
    }

    async fn record_idempotency_decision(
        &self,
        identity: &MutationIdentity,
        command_id: &str,
        mutation_kind: ExecutionMutationKind,
    ) -> Result<Option<String>, ExecutionRouterError> {
        let key = idempotency_scope_key(identity);
        let existing = {
            let mut index = self.idempotency_index.lock().map_err(|_| {
                ExecutionRouterError::State(
                    "execution router idempotency lock poisoned".to_string(),
                )
            })?;
            if let Some(existing_command_id) = index.get(&key) {
                Some(existing_command_id.clone())
            } else {
                index.insert(key, command_id.to_string());
                None
            }
        };

        let outcome = if existing.is_some() {
            ExecutionDecisionOutcome::Rejected
        } else {
            ExecutionDecisionOutcome::Recorded
        };
        self.append_idempotency_decision(
            identity,
            command_id,
            mutation_kind,
            outcome,
            existing.clone(),
        )
        .await?;

        if self.config.enforce_idempotency {
            Ok(existing)
        } else {
            Ok(None)
        }
    }

    async fn append_order_decision_events(
        &self,
        command: &OrderCommand,
        fast_risk_decision: Option<&FastRiskOrderDecision>,
    ) -> Result<(), ExecutionRouterError> {
        self.append_fee_model_decision(command).await?;
        self.append_reservation_decision(command, fast_risk_decision)
            .await
    }

    async fn evaluate_fast_order_risk(
        &self,
        command: &OrderCommand,
    ) -> Result<FastRiskOrderDecision, ExecutionRouterError> {
        let Some(fast_risk) = &self.fast_risk else {
            return Ok(skipped_fast_risk_order_decision(command));
        };
        let decision = fast_risk.reserve_order(command);
        self.append_risk_decision(command, &decision.risk).await?;
        if !decision.risk.is_approved() {
            self.append_reservation_decision(command, Some(&decision))
                .await?;
        }
        Ok(decision)
    }

    async fn evaluate_fast_cancel_risk(
        &self,
        command: &CancelCommand,
    ) -> Result<RiskDecision, ExecutionRouterError> {
        let Some(fast_risk) = &self.fast_risk else {
            return Ok(RiskDecision::Approved {
                risk_profile_id: command.identity.risk_profile_id.clone(),
                decision_id: risk_decision_id(&command.command_id, Utc::now()),
                decided_at: Utc::now(),
            });
        };
        let decision = fast_risk.record_cancel(command);
        self.append_cancel_risk_decision(command, &decision).await?;
        Ok(decision)
    }

    fn release_fast_reservation(&self, reservation: Option<&FastRiskReservation>) {
        if let (Some(fast_risk), Some(reservation)) = (&self.fast_risk, reservation) {
            fast_risk.release_reservation(reservation);
        }
    }

    fn mark_fast_reservation_reconcile_pending(&self, reservation: Option<&FastRiskReservation>) {
        if let (Some(fast_risk), Some(reservation)) = (&self.fast_risk, reservation) {
            fast_risk.mark_reconcile_pending(reservation);
        }
    }

    async fn append_risk_decision(
        &self,
        command: &OrderCommand,
        decision: &RiskDecision,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = RiskDecisionEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            command_id: command.command_id.clone(),
            idempotency_key: command.identity.idempotency_key.clone(),
            decision: decision.clone(),
        };
        ledger
            .append(LedgerEvent::new(
                EventKind::RiskDecisionEvent,
                event_identity_for_order(command),
                LedgerPayload::Raw(serde_json::to_value(event).map_err(|error| {
                    ExecutionRouterError::State(format!(
                        "failed to encode risk decision event: {error}"
                    ))
                })?),
            ))
            .await?;
        Ok(())
    }

    async fn append_cancel_risk_decision(
        &self,
        command: &CancelCommand,
        decision: &RiskDecision,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = RiskDecisionEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            command_id: command.command_id.clone(),
            idempotency_key: command.identity.idempotency_key.clone(),
            decision: decision.clone(),
        };
        ledger
            .append(LedgerEvent::new(
                EventKind::RiskDecisionEvent,
                event_identity_for_cancel(command),
                LedgerPayload::Raw(serde_json::to_value(event).map_err(|error| {
                    ExecutionRouterError::State(format!(
                        "failed to encode risk decision event: {error}"
                    ))
                })?),
            ))
            .await?;
        Ok(())
    }

    async fn append_fee_model_decision(
        &self,
        command: &OrderCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let decided_at = Utc::now();
        let role = if command.post_only {
            ExecutionFeeRole::Maker
        } else {
            ExecutionFeeRole::Taker
        };
        let rate = default_fee_rate_snapshot(command, decided_at);
        let estimate = FeeEstimate::from_rate(&rate, role, order_notional(command), decided_at)?;
        let decision = FeeModelDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: command.identity.clone(),
            command_id: command.command_id.clone(),
            exchange_id: command.exchange_id.clone(),
            market_type: command.market_type,
            canonical_symbol: Some(command.canonical_symbol.clone()),
            maker_fee_rate: Some(rate.maker_fee_rate),
            taker_fee_rate: Some(rate.taker_fee_rate),
            estimated_fee_asset: estimate.fee_asset,
            estimated_fee_amount: Some(estimate.estimated_fee_amount),
            decided_at,
        };
        decision.validate()?;
        ledger
            .append(LedgerEvent::fee_model_decision(
                event_identity_for_order(command),
                decision,
            ))
            .await?;
        Ok(())
    }

    async fn append_reservation_decision(
        &self,
        command: &OrderCommand,
        fast_risk_decision: Option<&FastRiskOrderDecision>,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let decision = fast_risk_decision
            .map(|decision| decision.reservation.clone())
            .unwrap_or_else(|| {
                reservation_decision(
                    command,
                    Some(command.quantity),
                    None,
                    ExecutionDecisionOutcome::Skipped,
                    Some("router compatibility path does not reserve balances".to_string()),
                    Utc::now(),
                )
            });
        decision.validate()?;
        ledger
            .append(LedgerEvent::reservation_decision(
                event_identity_for_order(command),
                decision,
            ))
            .await?;
        Ok(())
    }

    async fn append_idempotency_decision(
        &self,
        identity: &MutationIdentity,
        command_id: &str,
        mutation_kind: ExecutionMutationKind,
        outcome: ExecutionDecisionOutcome,
        existing_command_id: Option<String>,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let decision = IdempotencyDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity.clone(),
            command_id: command_id.to_string(),
            mutation_kind,
            outcome,
            existing_command_id,
            reason: None,
            decided_at: Utc::now(),
        };
        decision.validate()?;
        ledger
            .append(LedgerEvent::idempotency_decision(
                event_identity_for_identity(identity, command_id),
                decision,
            ))
            .await?;
        Ok(())
    }

    async fn append_bundle_ack_event(
        &self,
        command: &BundleSubmitCommand,
        ack: &BundleSubmitAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        ledger
            .append(LedgerEvent::bundle(
                event_identity_for_bundle(command),
                ack.clone(),
            ))
            .await?;
        Ok(())
    }

    async fn append_live_dry_run_decision(
        &self,
        identity: &MutationIdentity,
        command_id: &str,
        mutation_kind: ExecutionMutationKind,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let decision = LiveDryRunDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity.clone(),
            command_id: command_id.to_string(),
            mutation_kind,
            gateway_mutation_blocked: true,
            message: "execution router dry-run: gateway mutation not called".to_string(),
            decided_at: Utc::now(),
        };
        decision.validate()?;
        ledger
            .append(LedgerEvent::live_dry_run_decision(
                event_identity_for_identity(identity, command_id),
                decision,
            ))
            .await?;
        Ok(())
    }

    async fn append_rejection_event(
        &self,
        identity: &MutationIdentity,
        command_id: &str,
        mutation_kind: ExecutionMutationKind,
        reason: String,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let decision = RejectionDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity.clone(),
            command_id: command_id.to_string(),
            mutation_kind,
            reason,
            rejected_at: Utc::now(),
            metadata: Value::Null,
        };
        decision.validate()?;
        ledger
            .append(LedgerEvent::rejection(
                event_identity_for_identity(identity, command_id),
                decision,
            ))
            .await?;
        Ok(())
    }

    async fn append_order_command_event(
        &self,
        command: &OrderCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = order_lifecycle_record(
            EventKind::OrderCommandEvent,
            event_identity_for_order(command),
            command.exchange_id.clone(),
            command.market_type,
            command.canonical_symbol.clone(),
            command.client_order_id.clone(),
            command.side,
            command.position_side,
            OrderStatus::New,
            command.quantity,
        );
        record.requested_price = command.price;
        ledger.append(LedgerEvent::order(record)).await?;
        Ok(())
    }

    async fn append_order_ack_event(
        &self,
        command: &OrderCommand,
        ack: &OrderAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = order_lifecycle_record(
            EventKind::OrderAckEvent,
            event_identity_for_order(command),
            ack.exchange_id.clone(),
            command.market_type,
            command.canonical_symbol.clone(),
            ack.client_order_id.clone(),
            command.side,
            command.position_side,
            gateway_order_status_from_execution_state(ack.state),
            command.quantity,
        );
        record.requested_price = command.price;
        record.exchange_order_id = ack.exchange_order_id.clone();
        record.message = ack.message.clone();
        record.metadata = serde_json::json!({
            "accepted": ack.accepted
        });
        ledger.append(LedgerEvent::order(record)).await?;
        Ok(())
    }

    async fn append_cancel_command_event(
        &self,
        command: &CancelCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelCommandEvent,
            event_identity_for_cancel(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": command.command_id,
                "exchange_id": command.exchange_id,
                "market_type": command.market_type,
                "canonical_symbol": command.canonical_symbol,
                "exchange_symbol": command.exchange_symbol,
                "client_order_id": command.cancellation_ids.client_order_id,
                "exchange_order_id": command.cancellation_ids.exchange_order_id,
                "reason": command.reason,
                "requested_at": command.identity.requested_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_ack_event(
        &self,
        command: &CancelCommand,
        ack: &CancelAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelAckEvent,
            event_identity_for_cancel(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": ack.command_id,
                "exchange_id": ack.exchange_id,
                "client_order_id": ack.client_order_id,
                "exchange_order_id": ack.exchange_order_id,
                "accepted": ack.accepted,
                "state": ack.state,
                "message": ack.message,
                "acknowledged_at": ack.acknowledged_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_all_command_event(
        &self,
        command: &CancelAllCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelCommandEvent,
            event_identity_for_cancel_all(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": command.command_id,
                "exchange_id": command.exchange_id,
                "market_type": command.market_type,
                "canonical_symbol": command.canonical_symbol,
                "exchange_symbol": command.exchange_symbol,
                "cancel_all_id": command.cancel_all_id,
                "reason": command.reason,
                "requested_at": command.identity.requested_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_all_ack_event(
        &self,
        command: &CancelAllCommand,
        ack: &CancelAllAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelAckEvent,
            event_identity_for_cancel_all(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": ack.command_id,
                "exchange_id": ack.exchange_id,
                "cancel_all_id": ack.cancel_all_id,
                "accepted": ack.accepted,
                "cancelled_orders": ack.cancelled_orders,
                "message": ack.message,
                "acknowledged_at": ack.acknowledged_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }
}

fn gateway_request_for_place_order(command: &OrderCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::PlaceOrder,
        payload: GatewayRequestPayload::PlaceOrder(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            symbol: SymbolScope {
                exchange: command.exchange_id.clone(),
                market_type: command.market_type,
                canonical_symbol: Some(command.canonical_symbol.clone()),
                exchange_symbol: command.exchange_symbol.clone(),
            },
            client_order_id: Some(command.client_order_id.clone()),
            side: command.side,
            position_side: Some(command.position_side),
            order_type: command.order_type,
            time_in_force: Some(command.time_in_force),
            quantity: command.quantity.to_string(),
            price: command.price.map(|price| price.to_string()),
            quote_quantity: None,
            reduce_only: command.reduce_only,
            post_only: command.post_only,
        }),
        requested_at: command.identity.requested_at,
    }
}

fn gateway_request_for_cancel_order(command: &CancelCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::CancelOrder,
        payload: GatewayRequestPayload::CancelOrder(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            symbol: SymbolScope {
                exchange: command.exchange_id.clone(),
                market_type: command.market_type,
                canonical_symbol: Some(command.canonical_symbol.clone()),
                exchange_symbol: command.exchange_symbol.clone(),
            },
            client_order_id: command.cancellation_ids.client_order_id.clone(),
            exchange_order_id: command.cancellation_ids.exchange_order_id.clone(),
        }),
        requested_at: command.identity.requested_at,
    }
}

fn gateway_request_for_cancel_all_orders(command: &CancelAllCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::CancelAllOrders,
        payload: GatewayRequestPayload::CancelAllOrders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            exchange: command.exchange_id.clone(),
            market_type: Some(command.market_type),
            symbol: command
                .exchange_symbol
                .clone()
                .map(|exchange_symbol| SymbolScope {
                    exchange: command.exchange_id.clone(),
                    market_type: command.market_type,
                    canonical_symbol: command.canonical_symbol.clone(),
                    exchange_symbol,
                }),
        }),
        requested_at: command.identity.requested_at,
    }
}

fn request_context<C>(command: &C) -> RequestContext
where
    C: CommandContext,
{
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(command.tenant_id()),
        account_id: Some(command.account_id()),
        run_id: Some(command.run_id()),
        request_id: Some(command.command_id()),
        requested_at: command.requested_at(),
    }
}

trait CommandContext {
    fn tenant_id(&self) -> TenantId;
    fn account_id(&self) -> AccountId;
    fn run_id(&self) -> RunId;
    fn command_id(&self) -> String;
    fn requested_at(&self) -> DateTime<Utc>;
}

impl CommandContext for OrderCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

impl CommandContext for CancelCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

impl CommandContext for CancelAllCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

fn cancel_ack(
    command: &CancelCommand,
    accepted: bool,
    state: OrderState,
    message: Option<String>,
    acknowledged_at: DateTime<Utc>,
) -> CancelAck {
    CancelAck {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        tenant_id: command.identity.tenant_id.clone(),
        account_id: command.identity.account_id.clone(),
        strategy_id: command.identity.strategy_id.clone(),
        run_id: command.identity.run_id.clone(),
        command_id: command.command_id.clone(),
        exchange_id: command.exchange_id.clone(),
        client_order_id: command.cancellation_ids.client_order_id.clone(),
        exchange_order_id: command.cancellation_ids.exchange_order_id.clone(),
        idempotency_key: command.identity.idempotency_key.clone(),
        accepted,
        state,
        message,
        acknowledged_at,
    }
}

fn cancel_all_ack(
    command: &CancelAllCommand,
    accepted: bool,
    cancelled_orders: u32,
    message: Option<String>,
    acknowledged_at: DateTime<Utc>,
) -> CancelAllAck {
    CancelAllAck {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        tenant_id: command.identity.tenant_id.clone(),
        account_id: command.identity.account_id.clone(),
        strategy_id: command.identity.strategy_id.clone(),
        run_id: command.identity.run_id.clone(),
        command_id: command.command_id.clone(),
        exchange_id: command.exchange_id.clone(),
        cancel_all_id: command.cancel_all_id.clone(),
        idempotency_key: command.identity.idempotency_key.clone(),
        accepted,
        cancelled_orders,
        message,
        acknowledged_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn order_lifecycle_record(
    event_kind: EventKind,
    identity: EventIdentity,
    exchange_id: rustcta_types::ExchangeId,
    market_type: rustcta_types::MarketType,
    canonical_symbol: rustcta_types::CanonicalSymbol,
    client_order_id: impl Into<String>,
    side: OrderSide,
    position_side: PositionSide,
    status: OrderStatus,
    requested_quantity: f64,
) -> OrderLifecycleRecord {
    OrderLifecycleRecord::new(
        identity,
        event_kind,
        exchange_id,
        market_type,
        canonical_symbol,
        client_order_id,
        side,
        position_side,
        status,
        requested_quantity,
    )
}

fn event_identity_for_order(command: &OrderCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn event_identity_for_cancel(command: &CancelCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn event_identity_for_cancel_all(command: &CancelAllCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn event_identity_for_identity(identity: &MutationIdentity, command_id: &str) -> EventIdentity {
    EventIdentity::new(
        identity.tenant_id.clone(),
        "rustcta-execution-router",
        identity.requested_at,
    )
    .with_account(identity.account_id.clone())
    .with_strategy_run(identity.strategy_id.clone(), identity.run_id.clone())
    .with_command(command_id.to_string())
    .with_idempotency_key(identity.idempotency_key.clone())
}

fn event_identity_for_bundle(command: &BundleSubmitCommand) -> EventIdentity {
    let mut identity = EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.bundle_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone());
    if let Some(correlation_id) = &command.correlation_id {
        identity = identity.with_correlation_id(correlation_id.clone());
    }
    identity
}

fn event_identity_for_routed(
    identity: &ExecutionIdentity,
    occurred_at: DateTime<Utc>,
) -> EventIdentity {
    EventIdentity::new(
        TenantId::unchecked(identity.tenant_id.clone()),
        "rustcta-execution-router",
        occurred_at,
    )
    .with_account(AccountId::unchecked(identity.account_id.clone()))
    .with_strategy_run(
        rustcta_types::StrategyId::unchecked(identity.strategy_id.clone()),
        RunId::unchecked(identity.run_id.clone()),
    )
    .with_command(identity.command_id.clone())
    .with_idempotency_key(identity.idempotency_key.clone())
}

fn event_identity_for_fill(event: &FillEvent) -> EventIdentity {
    let mut identity = EventIdentity::new(
        event.tenant_id.clone(),
        "rustcta-execution-router",
        event.received_at,
    )
    .with_account(event.account_id.clone())
    .with_strategy_run(event.strategy_id.clone(), event.run_id.clone());
    if let Some(client_order_id) = &event.client_order_id {
        identity = identity.with_command(client_order_id.clone());
    }
    identity
}

fn idempotency_scope_key(identity: &MutationIdentity) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        identity.tenant_id,
        identity.account_id,
        identity.strategy_id,
        identity.run_id,
        identity.idempotency_key
    )
}

fn skipped_fast_risk_order_decision(command: &OrderCommand) -> FastRiskOrderDecision {
    let decided_at = Utc::now();
    FastRiskOrderDecision {
        risk: RiskDecision::Approved {
            risk_profile_id: command.identity.risk_profile_id.clone(),
            decision_id: risk_decision_id(&command.command_id, decided_at),
            decided_at,
        },
        reservation: reservation_decision(
            command,
            Some(command.quantity),
            None,
            ExecutionDecisionOutcome::Skipped,
            Some("fast risk engine not configured".to_string()),
            decided_at,
        ),
        handle: None,
    }
}

fn rejected_order_decision(
    command: &OrderCommand,
    decision_id: String,
    reason: String,
    decided_at: DateTime<Utc>,
    requested_notional: f64,
) -> FastRiskOrderDecision {
    FastRiskOrderDecision {
        risk: RiskDecision::Rejected {
            risk_profile_id: command.identity.risk_profile_id.clone(),
            decision_id,
            reason: reason.clone(),
            decided_at,
        },
        reservation: reservation_decision(
            command,
            requested_notional.is_finite().then_some(requested_notional),
            None,
            ExecutionDecisionOutcome::Rejected,
            Some(reason),
            decided_at,
        ),
        handle: None,
    }
}

fn reservation_decision(
    command: &OrderCommand,
    requested_quantity: Option<f64>,
    reserved_quantity: Option<f64>,
    outcome: ExecutionDecisionOutcome,
    reason: Option<String>,
    decided_at: DateTime<Utc>,
) -> ReservationDecision {
    ReservationDecision {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        identity: command.identity.clone(),
        command_id: command.command_id.clone(),
        exchange_id: command.exchange_id.clone(),
        market_type: command.market_type,
        canonical_symbol: Some(command.canonical_symbol.clone()),
        asset: Some(reservation_asset(command)),
        requested_quantity,
        reserved_quantity,
        outcome,
        reason,
        decided_at,
    }
}

fn reservation_asset(command: &OrderCommand) -> String {
    match command.side {
        OrderSide::Buy => command.canonical_symbol.quote_asset().to_string(),
        OrderSide::Sell => command.canonical_symbol.base_asset().to_string(),
    }
}

fn order_notional(command: &OrderCommand) -> f64 {
    command
        .price
        .map(|price| price * command.quantity)
        .unwrap_or(command.quantity)
}

fn default_fee_rate_snapshot(
    command: &OrderCommand,
    observed_at: DateTime<Utc>,
) -> FeeRateSnapshot {
    let (maker_fee_rate, taker_fee_rate) = match command.market_type {
        rustcta_types::MarketType::Spot => (0.0010, 0.0010),
        rustcta_types::MarketType::Perpetual | rustcta_types::MarketType::Futures => {
            (0.0002, 0.0005)
        }
        rustcta_types::MarketType::Margin | rustcta_types::MarketType::Option => (0.0010, 0.0010),
    };
    FeeRateSnapshot {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        exchange_id: command.exchange_id.clone(),
        market_type: command.market_type,
        canonical_symbol: Some(command.canonical_symbol.clone()),
        maker_fee_rate,
        taker_fee_rate,
        fee_asset: Some(command.canonical_symbol.quote_asset().to_string()),
        fee_asset_mode: ExecutionFeeAssetMode::Quote,
        source: ExecutionFeeSource::Fallback,
        observed_at,
    }
}

fn risk_decision_id(command_id: &str, decided_at: DateTime<Utc>) -> String {
    format!(
        "{command_id}:{}",
        decided_at.timestamp_nanos_opt().unwrap_or_default()
    )
}

fn risk_rejection_reason(decision: &RiskDecision) -> Option<String> {
    match decision {
        RiskDecision::Rejected { reason, .. } | RiskDecision::Reduced { reason, .. } => {
            Some(reason.clone())
        }
        RiskDecision::Approved { .. } => None,
    }
}

fn limit_reason(label: &str, limit: Option<f64>, current: f64, requested: f64) -> Option<String> {
    let limit = limit?;
    if current + requested > limit + 1e-12 {
        Some(format!(
            "{label} limit exceeded: current={current} requested={requested} limit={limit}"
        ))
    } else {
        None
    }
}

fn exchange_key(exchange_id: &rustcta_types::ExchangeId) -> String {
    exchange_id.to_string().to_ascii_lowercase()
}

fn symbol_key(symbol: &rustcta_types::CanonicalSymbol) -> String {
    symbol.to_string().to_ascii_uppercase()
}

fn exchange_symbol_key(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_types::CanonicalSymbol,
) -> String {
    format!("{}|{}", exchange_key(exchange_id), symbol_key(symbol))
}

fn run_key(identity: &MutationIdentity) -> String {
    format!("{}|{}", identity.strategy_id, identity.run_id)
}

fn prune_window(timestamps: &mut VecDeque<DateTime<Utc>>, now: DateTime<Utc>, window_ms: i64) {
    let window_ms = window_ms.max(1);
    while timestamps.front().is_some_and(|timestamp| {
        now.signed_duration_since(*timestamp).num_milliseconds() >= window_ms
    }) {
        timestamps.pop_front();
    }
}

fn subtract_counter(counters: &mut HashMap<String, f64>, key: &str, amount: f64) {
    if let Some(value) = counters.get_mut(key) {
        *value = (*value - amount).max(0.0);
        if *value <= 1e-12 {
            counters.remove(key);
        }
    }
}

fn gateway_order_status_from_execution_state(state: OrderState) -> OrderStatus {
    match state {
        OrderState::Planned => OrderStatus::New,
        OrderState::Submitted => OrderStatus::New,
        OrderState::Accepted => OrderStatus::Open,
        OrderState::PartiallyFilled => OrderStatus::PartiallyFilled,
        OrderState::Filled => OrderStatus::Filled,
        OrderState::CancelRequested => OrderStatus::PendingCancel,
        OrderState::Cancelled => OrderStatus::Cancelled,
        OrderState::Rejected => OrderStatus::Rejected,
        OrderState::Expired => OrderStatus::Expired,
        OrderState::Failed | OrderState::Unknown => OrderStatus::Unknown,
    }
}

fn execution_order_state(status: OrderStatus) -> OrderState {
    match status {
        OrderStatus::New => OrderState::Submitted,
        OrderStatus::Open => OrderState::Accepted,
        OrderStatus::PartiallyFilled => OrderState::PartiallyFilled,
        OrderStatus::Filled => OrderState::Filled,
        OrderStatus::PendingCancel => OrderState::CancelRequested,
        OrderStatus::Cancelled => OrderState::Cancelled,
        OrderStatus::Rejected => OrderState::Rejected,
        OrderStatus::Expired => OrderState::Expired,
        OrderStatus::Unknown => OrderState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustcta_event_ledger::{InMemoryLedger, LedgerPayload, LedgerReader};
    use rustcta_exchange_gateway::{
        CredentialBoundary, GatewayIdentity, GatewayMode, GatewayStatus, InProcessGatewayClient,
        MockExchangeGateway,
    };
    use rustcta_execution_api::{
        BundleLegKind, BundleOrderLeg, CancellationIds, MutationIdentity,
        NormalizedUserStreamEvent, NormalizedUserStreamEventKind, OrderReconciliationOutcome,
        OrderReconciliationSummary, ReconciliationSeverity, ReconciliationTrigger,
    };
    use rustcta_types::{
        AccountId, AssetBalance, CanonicalSymbol, ExchangeId, ExchangeSymbol, LiquidityRole,
        MarketType, OrderSide, OrderType, PositionSide, RunId, SchemaVersion, StrategyId, TenantId,
        TimeInForce,
    };
    use std::sync::Arc;

    struct MockGateway;

    #[async_trait]
    impl ExchangeGateway for MockGateway {
        async fn status(&self) -> Result<GatewayStatus, GatewayError> {
            Ok(GatewayStatus {
                api_version: "test".to_string(),
                identity: GatewayIdentity {
                    gateway_id: "mock".to_string(),
                    mode: GatewayMode::Local,
                    credential_boundary: CredentialBoundary::GatewayOnly,
                    started_at: Utc::now(),
                },
                exchanges: Vec::new(),
            })
        }

        async fn handle(&self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
            Ok(GatewayResponse {
                request_id: request.request_id,
                accepted: true,
                payload: Value::Null,
                error: None,
                responded_at: Utc::now(),
            })
        }
    }

    #[tokio::test]
    async fn dry_run_router_should_not_call_gateway_mutation() {
        let router = ExecutionRouter::new(ExecutionRouterConfig::dry_run(), MockGateway);
        let ack = router
            .route(RoutedExecutionCommand {
                identity: ExecutionIdentity {
                    tenant_id: "tenant".to_string(),
                    account_id: "account".to_string(),
                    strategy_id: "strategy".to_string(),
                    run_id: "run".to_string(),
                    command_id: "cmd".to_string(),
                    idempotency_key: "idem".to_string(),
                    risk_profile_id: "risk".to_string(),
                },
                operation: "place_order".to_string(),
                payload: Value::Null,
                requested_at: Utc::now(),
            })
            .await
            .expect("dry-run ack");

        assert!(!ack.accepted);
        assert!(ack.dry_run);
    }

    #[tokio::test]
    async fn routed_legacy_mutation_should_append_command_and_ack_to_ledger() {
        let ledger = Arc::new(InMemoryLedger::new());
        let router = ExecutionRouter::with_ledger(
            ExecutionRouterConfig::dry_run(),
            MockGateway,
            ledger.clone(),
        );

        let ack = router
            .route(RoutedExecutionCommand {
                identity: ExecutionIdentity {
                    tenant_id: "tenant".to_string(),
                    account_id: "account".to_string(),
                    strategy_id: "strategy".to_string(),
                    run_id: "run".to_string(),
                    command_id: "cmd".to_string(),
                    idempotency_key: "idem".to_string(),
                    risk_profile_id: "risk".to_string(),
                },
                operation: "place_order".to_string(),
                payload: serde_json::json!({
                    "client_order_id": "client-1",
                    "symbol": "BTCUSDT"
                }),
                requested_at: Utc::now(),
            })
            .await
            .expect("dry-run ack");

        assert!(!ack.accepted);
        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![EventKind::OperatorCommandEvent, EventKind::AuditEvent]
        );
        assert!(events
            .iter()
            .all(|event| event.identity.command_id.as_deref() == Some("cmd")));
    }

    #[tokio::test]
    async fn live_router_should_send_typed_order_and_cancel_through_gateway_client() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let router = ExecutionRouter::new(ExecutionRouterConfig::live(), client);

        let order = order_command();
        let ack = router.place_order(order.clone()).await.expect("order ack");
        assert!(ack.accepted);
        assert_eq!(ack.state, OrderState::Accepted);
        assert_eq!(ack.client_order_id, "cli-typed-1");
        assert!(ack.exchange_order_id.is_some());

        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        let cancel_ack = router.cancel_order(cancel).await.expect("cancel ack");
        assert!(cancel_ack.accepted);
        assert_eq!(cancel_ack.state, OrderState::Cancelled);

        let second_order = OrderCommand::new(
            identity(),
            "cmd-order-2",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            "cli-typed-2",
            OrderSide::Buy,
            PositionSide::None,
            OrderType::Limit,
            TimeInForce::GTC,
            0.01,
            Some(100.0),
        );
        let second_ack = router
            .place_order(second_order)
            .await
            .expect("second order ack");
        assert!(second_ack.accepted);

        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        )
        .for_symbol(canonical_symbol(), exchange_symbol());
        let cancel_all_ack = router
            .cancel_all_orders(cancel_all)
            .await
            .expect("cancel-all ack");
        assert!(cancel_all_ack.accepted);
        assert_eq!(cancel_all_ack.cancelled_orders, 1);
    }

    #[tokio::test]
    async fn dry_run_bundle_should_route_legs_and_append_bundle_event_to_ledger() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::dry_run(), client, ledger.clone());
        let bundle = bundle_command("bundle-1", "idem-bundle-1");

        let ack = router
            .submit_order_bundle(bundle)
            .await
            .expect("bundle ack");

        assert_eq!(ack.submission_path, BundleSubmissionPath::DryRun);
        assert_eq!(ack.order_acks.len(), 2);
        assert!(ack.requires_reconcile);
        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::OrderCommandEvent)
                .count(),
            2
        );
        assert!(events
            .iter()
            .any(|event| event.kind == EventKind::BundleEvent));
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LedgerPayload::IdempotencyDecision(decision)
                if decision.mutation_kind == ExecutionMutationKind::SubmitBundle
                    && decision.command_id == "bundle-1"
        )));
    }

    #[tokio::test]
    async fn bundle_idempotency_enforcement_should_reject_duplicate_before_leg_mutation() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router = ExecutionRouter::with_ledger(
            ExecutionRouterConfig {
                mode: RouterMode::DryRun,
                enforce_idempotency: true,
            },
            client,
            ledger.clone(),
        );

        let first = bundle_command("bundle-1", "idem-bundle-1");
        let second = first.clone();

        let first_ack = router
            .submit_order_bundle(first)
            .await
            .expect("first bundle ack");
        let second_ack = router
            .submit_order_bundle(second)
            .await
            .expect("second bundle ack");

        assert_eq!(first_ack.order_acks.len(), 2);
        assert!(first_ack.requires_reconcile);
        assert!(second_ack.order_acks.iter().all(|ack| !ack.accepted));
        assert!(second_ack
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("requires reconciliation"));

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::OrderCommandEvent)
                .count(),
            2
        );
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LedgerPayload::Rejection(decision)
                if decision.mutation_kind == ExecutionMutationKind::Idempotency
                    && decision.command_id == "bundle-1"
        )));
    }

    #[tokio::test]
    async fn live_router_should_append_order_and_cancel_events_to_ledger() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::live(), client, ledger.clone());

        let order = order_command();
        router
            .place_order(order.clone())
            .await
            .expect("order should route");
        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        router
            .cancel_order(cancel)
            .await
            .expect("cancel should route");
        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        );
        router
            .cancel_all_orders(cancel_all)
            .await
            .expect("cancel-all should route");

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                EventKind::IdempotencyDecisionEvent,
                EventKind::FeeModelDecisionEvent,
                EventKind::ReservationDecisionEvent,
                EventKind::OrderCommandEvent,
                EventKind::OrderAckEvent,
                EventKind::IdempotencyDecisionEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
                EventKind::IdempotencyDecisionEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        );
        assert!(events
            .iter()
            .all(|event| event.identity.command_id.is_some()));
        let fee_event = events
            .iter()
            .find(|event| event.kind == EventKind::FeeModelDecisionEvent)
            .expect("fee model event");
        assert!(matches!(
            &fee_event.payload,
            LedgerPayload::FeeModelDecision(decision)
                if decision.maker_fee_rate.is_some()
                    && decision.taker_fee_rate.is_some()
                    && decision.estimated_fee_amount.is_some()
        ));
    }

    #[tokio::test]
    async fn dry_run_typed_router_should_append_rejected_order_and_cancel_events_to_ledger() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::dry_run(), client, ledger.clone());

        let order = order_command();
        let order_ack = router
            .place_order(order.clone())
            .await
            .expect("dry-run order should route");
        assert!(!order_ack.accepted);
        assert_eq!(order_ack.state, OrderState::Rejected);

        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        let cancel_ack = router
            .cancel_order(cancel)
            .await
            .expect("dry-run cancel should route");
        assert!(!cancel_ack.accepted);
        assert_eq!(cancel_ack.state, OrderState::Rejected);

        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        );
        let cancel_all_ack = router
            .cancel_all_orders(cancel_all)
            .await
            .expect("dry-run cancel-all should route");
        assert!(!cancel_all_ack.accepted);
        assert_eq!(cancel_all_ack.cancelled_orders, 0);

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                EventKind::IdempotencyDecisionEvent,
                EventKind::FeeModelDecisionEvent,
                EventKind::ReservationDecisionEvent,
                EventKind::OrderCommandEvent,
                EventKind::LiveDryRunDecisionEvent,
                EventKind::OrderAckEvent,
                EventKind::IdempotencyDecisionEvent,
                EventKind::CancelCommandEvent,
                EventKind::LiveDryRunDecisionEvent,
                EventKind::CancelAckEvent,
                EventKind::IdempotencyDecisionEvent,
                EventKind::CancelCommandEvent,
                EventKind::LiveDryRunDecisionEvent,
                EventKind::CancelAckEvent,
            ]
        );
        assert!(events
            .iter()
            .all(|event| event.identity.idempotency_key.is_some()));
    }

    #[tokio::test]
    async fn router_should_record_fill_account_and_reconciliation_events() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::live(), client, ledger.clone());

        router.record_fill(fill_event()).await.expect("record fill");
        router
            .record_account_snapshot(balance_snapshot())
            .await
            .expect("record account");
        router
            .record_reconciliation(reconciliation_event())
            .await
            .expect("record reconciliation");

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                EventKind::FillEvent,
                EventKind::BalanceSnapshotEvent,
                EventKind::ReconciliationEvent,
            ]
        );
    }

    #[tokio::test]
    async fn router_should_record_order_reconciliation_summary() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::live(), client, ledger.clone());

        router
            .record_order_reconciliation(OrderReconciliationSummary {
                schema_version: EXECUTION_API_SCHEMA_VERSION,
                tenant_id: TenantId::new("tenant").expect("tenant id"),
                account_id: AccountId::new("account").expect("account id"),
                strategy_id: Some(StrategyId::new("strategy").expect("strategy id")),
                run_id: Some(RunId::new("run").expect("run id")),
                exchange_id: exchange_id(),
                market_type: MarketType::Spot,
                canonical_symbol: Some(canonical_symbol()),
                exchange_symbol: Some(exchange_symbol()),
                client_order_id: Some("cli-typed-1".to_string()),
                exchange_order_id: Some("paper-1".to_string()),
                trigger: ReconciliationTrigger::PrivateStreamDisconnected,
                outcome: OrderReconciliationOutcome::Filled,
                severity: ReconciliationSeverity::Info,
                attempts: 1,
                message: "filled during reconciliation".to_string(),
                reconciled_at: Utc::now(),
            })
            .await
            .expect("record order reconciliation");

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::ReconciliationEvent);
        assert!(matches!(
            &events[0].payload,
            LedgerPayload::OrderReconciliation(summary)
                if summary.client_order_id.as_deref() == Some("cli-typed-1")
        ));
    }

    #[tokio::test]
    async fn router_should_record_normalized_user_stream_events() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::live(), client, ledger.clone());
        let fill = fill_event();
        let event = NormalizedUserStreamEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: fill.tenant_id.clone(),
            account_id: fill.account_id.clone(),
            strategy_id: Some(fill.strategy_id.clone()),
            run_id: Some(fill.run_id.clone()),
            exchange_id: fill.exchange_id.clone(),
            market_type: fill.market_type,
            canonical_symbol: Some(fill.canonical_symbol.clone()),
            exchange_symbol: Some(fill.exchange_symbol.clone()),
            event_id: Some("user-stream-event-1".to_string()),
            exchange_sequence: Some(7),
            received_at: fill.received_at,
            kind: NormalizedUserStreamEventKind::Fill(fill),
            raw: None,
        };

        router
            .record_user_stream_event(event)
            .await
            .expect("record user stream event");

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::UserStreamEvent);
        assert!(matches!(
            &events[0].payload,
            LedgerPayload::UserStream(NormalizedUserStreamEvent {
                event_id: Some(event_id),
                ..
            }) if event_id == "user-stream-event-1"
        ));
    }

    #[tokio::test]
    async fn legacy_router_should_reject_secret_fields_before_gateway_mutation() {
        let router = ExecutionRouter::new(ExecutionRouterConfig::dry_run(), MockGateway);

        let error = router
            .route(RoutedExecutionCommand {
                identity: ExecutionIdentity {
                    tenant_id: "tenant".to_string(),
                    account_id: "account".to_string(),
                    strategy_id: "strategy".to_string(),
                    run_id: "run".to_string(),
                    command_id: "cmd".to_string(),
                    idempotency_key: "idem".to_string(),
                    risk_profile_id: "risk".to_string(),
                },
                operation: "place_order".to_string(),
                payload: serde_json::json!({
                    "client_order_id": "cli-secret",
                    "api_secret": "do-not-route"
                }),
                requested_at: Utc::now(),
            })
            .await
            .expect_err("secret-like field should reject");

        assert!(matches!(error, ExecutionRouterError::Ledger(_)));
    }

    #[tokio::test]
    async fn idempotency_enforcement_should_append_rejection_without_gateway_mutation() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router = ExecutionRouter::with_ledger(
            ExecutionRouterConfig {
                mode: RouterMode::Live,
                enforce_idempotency: true,
            },
            client,
            ledger.clone(),
        );

        let first = order_command();
        let mut second = order_command();
        second.command_id = "cmd-order-duplicate".to_string();
        second.client_order_id = "cli-typed-duplicate".to_string();

        let first_ack = router.place_order(first).await.expect("first accepted");
        let second_ack = router.place_order(second).await.expect("second rejected");

        assert!(first_ack.accepted);
        assert!(!second_ack.accepted);
        assert_eq!(second_ack.state, OrderState::Rejected);

        let events = ledger.replay(None).await.expect("replay events");
        assert!(events
            .iter()
            .any(|event| event.kind == EventKind::RejectionEvent));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::OrderCommandEvent)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fast_risk_reservation_should_be_atomic_for_notional_limits() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let fast_risk = Arc::new(FastRiskEngine::new(FastRiskContext::new(FastRiskConfig {
            max_exchange_notional: Some(1.5),
            ..FastRiskConfig::default()
        })));
        let router = ExecutionRouter::new(ExecutionRouterConfig::live(), client)
            .with_fast_risk_engine(fast_risk.clone());

        let first = router
            .place_order(order_command())
            .await
            .expect("first ack");
        let mut second = order_command();
        second.command_id = "cmd-order-2".to_string();
        second.client_order_id = "cli-typed-2".to_string();
        second.identity.idempotency_key = "idem-typed-2".to_string();
        let second_ack = router.place_order(second).await.expect("second ack");

        assert!(first.accepted);
        assert!(!second_ack.accepted);
        assert!(second_ack
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("exchange notional limit exceeded"));
        assert_eq!(
            fast_risk.reserved_notional_for_command("cmd-order"),
            Some(1.0)
        );
        assert_eq!(fast_risk.reserved_notional_for_command("cmd-order-2"), None);
    }

    #[tokio::test]
    async fn dry_run_rejection_should_release_fast_risk_reservation() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let fast_risk = Arc::new(FastRiskEngine::new(FastRiskContext::new(FastRiskConfig {
            max_exchange_notional: Some(1.5),
            ..FastRiskConfig::default()
        })));
        let router = ExecutionRouter::new(ExecutionRouterConfig::dry_run(), client)
            .with_fast_risk_engine(fast_risk.clone());

        let ack = router
            .place_order(order_command())
            .await
            .expect("dry-run ack");

        assert!(!ack.accepted);
        assert_eq!(fast_risk.reserved_notional_for_command("cmd-order"), None);
    }

    #[tokio::test]
    async fn fast_risk_should_block_order_rate_cancel_rate_and_unhedged_exposure() {
        let fast_risk = FastRiskEngine::new(FastRiskContext::new(FastRiskConfig {
            max_unhedged_exposure: Some(1.5),
            max_order_rate_per_window: Some(1),
            max_cancel_rate_per_window: Some(1),
            rate_window_ms: 60_000,
            ..FastRiskConfig::default()
        }));

        let first = fast_risk.reserve_order(&order_command());
        let mut second = order_command();
        second.command_id = "cmd-order-2".to_string();
        second.identity.idempotency_key = "idem-typed-2".to_string();
        let rate_block = fast_risk.reserve_order(&second);

        assert!(first.risk.is_approved());
        assert!(
            matches!(rate_block.risk, RiskDecision::Rejected { ref reason, .. } if reason.contains("order-rate"))
        );

        let fast_risk = FastRiskEngine::new(FastRiskContext::new(FastRiskConfig {
            max_unhedged_exposure: Some(1.5),
            ..FastRiskConfig::default()
        }));
        let first = fast_risk.reserve_order(&order_command());
        let mut second = order_command();
        second.command_id = "cmd-order-2".to_string();
        second.client_order_id = "cli-typed-2".to_string();
        second.identity.idempotency_key = "idem-typed-2".to_string();
        let exposure_block = fast_risk.reserve_order(&second);
        assert!(first.risk.is_approved());
        assert!(
            matches!(exposure_block.risk, RiskDecision::Rejected { ref reason, .. } if reason.contains("unhedged exposure"))
        );

        let fast_risk = FastRiskEngine::new(FastRiskContext::new(FastRiskConfig {
            max_cancel_rate_per_window: Some(1),
            rate_window_ms: 60_000,
            ..FastRiskConfig::default()
        }));
        let cancel = cancel_command("cancel-1", "idem-cancel-1");
        let first_cancel = fast_risk.record_cancel(&cancel);
        let second_cancel = fast_risk.record_cancel(&cancel_command("cancel-2", "idem-cancel-2"));
        assert!(first_cancel.is_approved());
        assert!(
            matches!(second_cancel, RiskDecision::Rejected { ref reason, .. } if reason.contains("cancel-rate"))
        );
    }

    #[tokio::test]
    async fn fast_risk_exchange_cooldown_should_block_before_gateway_mutation() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let mut context = FastRiskContext::new(FastRiskConfig::default());
        context
            .exchange_cooldowns
            .insert("paper".to_string(), "api 429".to_string());
        let fast_risk = Arc::new(FastRiskEngine::new(context));
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::live(), client, ledger.clone())
                .with_fast_risk_engine(fast_risk);

        let ack = router.place_order(order_command()).await.expect("risk ack");

        assert!(!ack.accepted);
        assert!(ack
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("exchange cooldown active"));
        let events = ledger.replay(None).await.expect("events");
        assert!(events
            .iter()
            .any(|event| event.kind == EventKind::RiskDecisionEvent));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::OrderCommandEvent)
                .count(),
            0
        );
    }

    fn order_command() -> OrderCommand {
        OrderCommand::new(
            identity(),
            "cmd-order",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            "cli-typed-1",
            OrderSide::Buy,
            PositionSide::None,
            OrderType::Limit,
            TimeInForce::GTC,
            0.01,
            Some(100.0),
        )
    }

    fn cancel_command(command_id: &str, idempotency_key: &str) -> CancelCommand {
        let mut identity = identity();
        identity.idempotency_key = idempotency_key.to_string();
        CancelCommand::new(
            identity,
            command_id,
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        )
    }

    fn bundle_command(bundle_id: &str, bundle_idempotency_key: &str) -> BundleSubmitCommand {
        let mut bundle_identity = identity();
        bundle_identity.idempotency_key = bundle_idempotency_key.to_string();
        let mut first = order_command();
        first.identity.idempotency_key = format!("{bundle_idempotency_key}-leg-1");
        first.source_intent_id = Some(bundle_id.to_string());
        let mut second = order_command();
        second.command_id = format!("{bundle_id}-cmd-order-2");
        second.client_order_id = format!("{bundle_id}-cli-typed-2");
        second.identity.idempotency_key = format!("{bundle_idempotency_key}-leg-2");
        second.source_intent_id = Some(bundle_id.to_string());
        BundleSubmitCommand {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: bundle_identity,
            bundle_id: bundle_id.to_string(),
            opportunity_id: Some(format!("{bundle_id}-opportunity")),
            legs: vec![
                BundleOrderLeg {
                    leg_id: "long".to_string(),
                    leg_kind: BundleLegKind::Long,
                    command: first,
                },
                BundleOrderLeg {
                    leg_id: "short".to_string(),
                    leg_kind: BundleLegKind::Short,
                    command: second,
                },
            ],
            correlation_id: Some(format!("{bundle_id}-corr")),
            requested_at: Utc::now(),
        }
    }

    fn identity() -> MutationIdentity {
        MutationIdentity {
            tenant_id: TenantId::new("tenant").expect("tenant id"),
            account_id: AccountId::new("account").expect("account id"),
            strategy_id: StrategyId::new("strategy").expect("strategy id"),
            run_id: RunId::new("run").expect("run id"),
            idempotency_key: "idem-typed".to_string(),
            risk_profile_id: "risk".to_string(),
            requested_at: Utc::now(),
        }
    }

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("paper").expect("exchange id")
    }

    fn canonical_symbol() -> CanonicalSymbol {
        CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")
    }

    fn exchange_symbol() -> ExchangeSymbol {
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT").expect("exchange symbol")
    }

    fn fill_event() -> FillEvent {
        FillEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: TenantId::new("tenant").expect("tenant id"),
            account_id: AccountId::new("account").expect("account id"),
            strategy_id: StrategyId::new("strategy").expect("strategy id"),
            run_id: RunId::new("run").expect("run id"),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: canonical_symbol(),
            exchange_symbol: exchange_symbol(),
            client_order_id: Some("cli-typed-1".to_string()),
            exchange_order_id: Some("paper-1".to_string()),
            fill_id: "fill-1".to_string(),
            trade_id: Some("trade-1".to_string()),
            side: OrderSide::Buy,
            liquidity: LiquidityRole::Taker,
            price: 100.0,
            quantity: 0.01,
            fee_asset: Some("USDT".to_string()),
            fee_amount: Some(0.01),
            exchange_fill: None,
            filled_at: Utc::now(),
            received_at: Utc::now(),
        }
    }

    fn balance_snapshot() -> ExchangeBalance {
        ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: TenantId::new("tenant").expect("tenant id"),
            account_id: AccountId::new("account").expect("account id"),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            balances: vec![AssetBalance::new("USDT", 100.0, 90.0, 10.0)
                .expect("asset balance")
                .with_reservation(1.0)
                .expect("reservation")],
            observed_at: Utc::now(),
        }
    }

    fn reconciliation_event() -> ReconciliationEvent {
        ReconciliationEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: TenantId::new("tenant").expect("tenant id"),
            account_id: AccountId::new("account").expect("account id"),
            strategy_id: Some(StrategyId::new("strategy").expect("strategy id")),
            run_id: Some(RunId::new("run").expect("run id")),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol()),
            exchange_symbol: Some(exchange_symbol()),
            client_order_id: Some("cli-typed-1".to_string()),
            exchange_order_id: Some("paper-1".to_string()),
            expected_state: Some(OrderState::Accepted),
            observed_state: Some(OrderState::Filled),
            balance_snapshot: vec![balance_snapshot()],
            discrepancy: Some("filled during reconciliation".to_string()),
            reconciled_at: Utc::now(),
        }
    }
}
