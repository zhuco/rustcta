//! Risk gates and strategy-level risk state for cross-exchange arbitrage.

use super::config::{CrossExchangeArbitrageConfig, RiskConfig};
use super::funding::FundingEstimate;
use super::position::PortfolioExposureSummary;
use super::simulation::TakerVwapResult;
use super::types::OpportunityRecord;
use crate::execution::{PositionSide, ReconcileSeverity};
use crate::market::{CanonicalSymbol, ExchangeId, OrderBook5, RouteStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RejectReason {
    RawSpreadTooSmall,
    NetEdgeTooSmall,
    StaleBook,
    RouteUnhealthy,
    DepthInsufficient,
    FundingDangerous,
    FundingWindowTooClose,
    NotionalOverLimit,
    SlippageTooHigh,
    BadOrderBook,
    PrecisionInvalid,
    AbnormalCrossExchangeSpread,
    ExchangeCapacityExceeded,
    ExchangePositionLimitExceeded,
    UnpairedExchangePosition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGateDecision {
    pub can_open: bool,
    pub reject_reasons: Vec<RejectReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProductionRiskReason {
    MaxSymbolNotional,
    MaxExchangeNotional,
    MaxTotalNotional,
    MaxSingleLegExposure,
    MaxOpenPositions,
    MaxLossPerTrade,
    MaxDailyLoss,
    MaxDrawdown,
    SpreadStopLoss,
    TimeStopLoss,
    ExchangeDataStale,
    RestFailureRate,
    OrderRejectRate,
    CancelFailureRate,
    PrivateStreamDelay,
    BalanceReconciliationMismatch,
    GlobalKillSwitch,
    SymbolDisabled,
    ExchangeDisabled,
    ResidualExposure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskAction {
    Allow,
    RejectOpen,
    StopOpening,
    ReduceExposure,
    ClosePosition,
    EmergencyFlatten,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProductionRiskDecision {
    pub action: RiskAction,
    pub reasons: Vec<ProductionRiskReason>,
    pub allow_new_position: bool,
    pub should_reduce_exposure: bool,
    pub should_close_position: bool,
    pub should_flatten: bool,
    pub evaluated_at: DateTime<Utc>,
}

impl ProductionRiskDecision {
    pub fn allow(evaluated_at: DateTime<Utc>) -> Self {
        Self {
            action: RiskAction::Allow,
            reasons: Vec::new(),
            allow_new_position: true,
            should_reduce_exposure: false,
            should_close_position: false,
            should_flatten: false,
            evaluated_at,
        }
    }

    fn from_reasons(
        action: RiskAction,
        reasons: Vec<ProductionRiskReason>,
        evaluated_at: DateTime<Utc>,
    ) -> Self {
        let action = if reasons.is_empty() {
            RiskAction::Allow
        } else {
            action
        };
        Self {
            action,
            reasons,
            allow_new_position: action == RiskAction::Allow,
            should_reduce_exposure: matches!(
                action,
                RiskAction::ReduceExposure
                    | RiskAction::ClosePosition
                    | RiskAction::EmergencyFlatten
            ),
            should_close_position: matches!(
                action,
                RiskAction::ClosePosition | RiskAction::EmergencyFlatten
            ),
            should_flatten: action == RiskAction::EmergencyFlatten,
            evaluated_at,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PortfolioRiskSnapshot {
    pub symbol_notional_usdt: HashMap<CanonicalSymbol, f64>,
    pub exchange_notional_usdt: HashMap<ExchangeId, f64>,
    pub total_notional_usdt: f64,
    pub max_single_leg_exposure_usdt: f64,
    pub open_positions: usize,
    pub residual_exposure_usdt: f64,
}

impl PortfolioRiskSnapshot {
    pub fn projected_symbol_notional(
        &self,
        symbol: &CanonicalSymbol,
        additional_notional_usdt: f64,
    ) -> f64 {
        self.symbol_notional_usdt
            .get(symbol)
            .copied()
            .unwrap_or_default()
            + additional_notional_usdt.max(0.0)
    }

    pub fn projected_exchange_notional(
        &self,
        exchange: &ExchangeId,
        additional_notional_usdt: f64,
    ) -> f64 {
        self.exchange_notional_usdt
            .get(exchange)
            .copied()
            .unwrap_or_default()
            + additional_notional_usdt.max(0.0)
    }

    pub fn projected_total_notional(&self, additional_notional_usdt: f64) -> f64 {
        self.total_notional_usdt + additional_notional_usdt.max(0.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LossRiskSnapshot {
    pub trade_pnl_usdt: f64,
    pub daily_pnl_usdt: f64,
    pub peak_equity_usdt: f64,
    pub current_equity_usdt: f64,
}

impl Default for LossRiskSnapshot {
    fn default() -> Self {
        Self {
            trade_pnl_usdt: 0.0,
            daily_pnl_usdt: 0.0,
            peak_equity_usdt: 0.0,
            current_equity_usdt: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenPositionRiskSnapshot {
    pub symbol: CanonicalSymbol,
    pub buy_exchange: ExchangeId,
    pub sell_exchange: ExchangeId,
    pub opened_at: DateTime<Utc>,
    pub entry_spread_bps: f64,
    pub current_spread_bps: f64,
    pub residual_exposure_usdt: f64,
}

impl OpenPositionRiskSnapshot {
    pub fn adverse_spread_move_bps(&self) -> f64 {
        (self.entry_spread_bps - self.current_spread_bps).max(0.0)
    }

    pub fn hold_seconds(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.opened_at)
            .num_seconds()
            .max(0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeHealthSnapshot {
    pub exchange: ExchangeId,
    pub ws_stale: bool,
    pub rest_failure_rate: f64,
    pub order_reject_rate: f64,
    pub cancel_failure_rate: f64,
    pub private_stream_delay_ms: i64,
    pub balance_reconciliation_mismatch_usdt: f64,
}

impl ExchangeHealthSnapshot {
    pub fn healthy(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            ws_stale: false,
            rest_failure_rate: 0.0,
            order_reject_rate: 0.0,
            cancel_failure_rate: 0.0,
            private_stream_delay_ms: 0,
            balance_reconciliation_mismatch_usdt: 0.0,
        }
    }

    fn risk_reasons(&self, config: &RiskConfig) -> Vec<ProductionRiskReason> {
        let mut reasons = Vec::new();
        if self.ws_stale {
            reasons.push(ProductionRiskReason::ExchangeDataStale);
        }
        if self.rest_failure_rate > config.max_rest_failure_rate {
            reasons.push(ProductionRiskReason::RestFailureRate);
        }
        if self.order_reject_rate > config.max_order_reject_rate {
            reasons.push(ProductionRiskReason::OrderRejectRate);
        }
        if self.cancel_failure_rate > config.max_cancel_failure_rate {
            reasons.push(ProductionRiskReason::CancelFailureRate);
        }
        if self.private_stream_delay_ms > config.max_private_stream_delay_ms {
            reasons.push(ProductionRiskReason::PrivateStreamDelay);
        }
        if self.balance_reconciliation_mismatch_usdt
            > config.max_balance_reconciliation_mismatch_usdt
        {
            reasons.push(ProductionRiskReason::BalanceReconciliationMismatch);
        }
        reasons
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KillSwitchState {
    pub global_disabled: bool,
    pub disabled_symbols: HashSet<CanonicalSymbol>,
    pub disabled_exchanges: HashSet<ExchangeId>,
    pub emergency_flatten: bool,
}

pub fn evaluate_pre_trade_risk(
    config: &RiskConfig,
    opportunity: &OpportunityRecord,
    portfolio: &PortfolioRiskSnapshot,
    health: &[ExchangeHealthSnapshot],
    kill_switch: &KillSwitchState,
    now: DateTime<Utc>,
) -> ProductionRiskDecision {
    let mut reasons = Vec::new();
    let notional = opportunity.estimated_notional.max(0.0);

    if kill_switch.global_disabled {
        reasons.push(ProductionRiskReason::GlobalKillSwitch);
    }
    if kill_switch.emergency_flatten {
        reasons.push(ProductionRiskReason::GlobalKillSwitch);
    }
    if kill_switch.disabled_symbols.contains(&opportunity.symbol) {
        reasons.push(ProductionRiskReason::SymbolDisabled);
    }
    if kill_switch
        .disabled_exchanges
        .contains(&opportunity.buy_exchange)
        || kill_switch
            .disabled_exchanges
            .contains(&opportunity.sell_exchange)
    {
        reasons.push(ProductionRiskReason::ExchangeDisabled);
    }
    if notional > config.max_notional_per_symbol_usdt
        || portfolio.projected_symbol_notional(&opportunity.symbol, notional)
            > config.max_notional_per_symbol_usdt
    {
        reasons.push(ProductionRiskReason::MaxSymbolNotional);
    }
    if portfolio.projected_exchange_notional(&opportunity.buy_exchange, notional)
        > config.max_notional_per_exchange_usdt
        || portfolio.projected_exchange_notional(&opportunity.sell_exchange, notional)
            > config.max_notional_per_exchange_usdt
    {
        reasons.push(ProductionRiskReason::MaxExchangeNotional);
    }
    if portfolio.projected_total_notional(notional) > config.max_total_notional_usdt {
        reasons.push(ProductionRiskReason::MaxTotalNotional);
    }
    if portfolio.max_single_leg_exposure_usdt.max(notional) > config.max_single_leg_exposure_usdt {
        reasons.push(ProductionRiskReason::MaxSingleLegExposure);
    }
    if portfolio.open_positions >= config.max_open_positions {
        reasons.push(ProductionRiskReason::MaxOpenPositions);
    }
    if portfolio.residual_exposure_usdt > config.max_single_leg_exposure_usdt {
        reasons.push(ProductionRiskReason::ResidualExposure);
    }

    add_route_health_reasons(
        config,
        health,
        [&opportunity.buy_exchange, &opportunity.sell_exchange],
        &mut reasons,
    );

    let action = if kill_switch.emergency_flatten
        || reasons.contains(&ProductionRiskReason::ResidualExposure)
    {
        RiskAction::EmergencyFlatten
    } else if reasons.iter().any(is_exchange_health_reason)
        || kill_switch.global_disabled
        || reasons.contains(&ProductionRiskReason::ExchangeDisabled)
        || reasons.contains(&ProductionRiskReason::SymbolDisabled)
    {
        RiskAction::StopOpening
    } else {
        RiskAction::RejectOpen
    };
    let decision = ProductionRiskDecision::from_reasons(action, reasons, now);
    log_risk_rejection("pre_trade", &decision);
    decision
}

pub fn evaluate_after_fill_risk(
    config: &RiskConfig,
    position: &OpenPositionRiskSnapshot,
    losses: &LossRiskSnapshot,
    health: &[ExchangeHealthSnapshot],
    kill_switch: &KillSwitchState,
    now: DateTime<Utc>,
) -> ProductionRiskDecision {
    let mut reasons = Vec::new();

    if kill_switch.global_disabled || kill_switch.emergency_flatten {
        reasons.push(ProductionRiskReason::GlobalKillSwitch);
    }
    if kill_switch.disabled_symbols.contains(&position.symbol) {
        reasons.push(ProductionRiskReason::SymbolDisabled);
    }
    if kill_switch
        .disabled_exchanges
        .contains(&position.buy_exchange)
        || kill_switch
            .disabled_exchanges
            .contains(&position.sell_exchange)
    {
        reasons.push(ProductionRiskReason::ExchangeDisabled);
    }
    if losses.trade_pnl_usdt < -config.max_loss_per_trade_usdt {
        reasons.push(ProductionRiskReason::MaxLossPerTrade);
    }
    if losses.daily_pnl_usdt < -config.max_daily_loss_usdt {
        reasons.push(ProductionRiskReason::MaxDailyLoss);
    }
    if (losses.peak_equity_usdt - losses.current_equity_usdt) > config.max_drawdown_usdt {
        reasons.push(ProductionRiskReason::MaxDrawdown);
    }
    if position.adverse_spread_move_bps() > config.max_spread_loss_bps {
        reasons.push(ProductionRiskReason::SpreadStopLoss);
    }
    if position.hold_seconds(now) > config.max_hold_seconds {
        reasons.push(ProductionRiskReason::TimeStopLoss);
    }
    if position.residual_exposure_usdt > config.max_single_leg_exposure_usdt {
        reasons.push(ProductionRiskReason::ResidualExposure);
    }

    add_route_health_reasons(
        config,
        health,
        [&position.buy_exchange, &position.sell_exchange],
        &mut reasons,
    );

    let action = if kill_switch.emergency_flatten
        || reasons.contains(&ProductionRiskReason::ResidualExposure)
        || reasons.contains(&ProductionRiskReason::ExchangeDisabled)
        || reasons.iter().any(|reason| {
            matches!(
                reason,
                ProductionRiskReason::RestFailureRate
                    | ProductionRiskReason::OrderRejectRate
                    | ProductionRiskReason::CancelFailureRate
                    | ProductionRiskReason::PrivateStreamDelay
                    | ProductionRiskReason::BalanceReconciliationMismatch
            )
        }) {
        RiskAction::EmergencyFlatten
    } else if reasons.contains(&ProductionRiskReason::ExchangeDataStale) {
        RiskAction::ReduceExposure
    } else if reasons.iter().any(|reason| {
        matches!(
            reason,
            ProductionRiskReason::MaxLossPerTrade
                | ProductionRiskReason::MaxDailyLoss
                | ProductionRiskReason::MaxDrawdown
                | ProductionRiskReason::SpreadStopLoss
                | ProductionRiskReason::TimeStopLoss
                | ProductionRiskReason::GlobalKillSwitch
                | ProductionRiskReason::SymbolDisabled
        )
    }) {
        RiskAction::ClosePosition
    } else {
        RiskAction::Allow
    };
    let decision = ProductionRiskDecision::from_reasons(action, reasons, now);
    log_risk_rejection("after_fill", &decision);
    decision
}

fn add_route_health_reasons<'a>(
    config: &RiskConfig,
    health: &[ExchangeHealthSnapshot],
    exchanges: impl IntoIterator<Item = &'a ExchangeId>,
    reasons: &mut Vec<ProductionRiskReason>,
) {
    for exchange in exchanges {
        if let Some(snapshot) = health
            .iter()
            .find(|snapshot| snapshot.exchange == *exchange)
        {
            reasons.extend(snapshot.risk_reasons(config));
        }
    }
    dedup_reasons(reasons);
}

fn dedup_reasons(reasons: &mut Vec<ProductionRiskReason>) {
    let mut seen = HashSet::new();
    reasons.retain(|reason| seen.insert(*reason));
}

fn is_exchange_health_reason(reason: &ProductionRiskReason) -> bool {
    matches!(
        reason,
        ProductionRiskReason::ExchangeDataStale
            | ProductionRiskReason::RestFailureRate
            | ProductionRiskReason::OrderRejectRate
            | ProductionRiskReason::CancelFailureRate
            | ProductionRiskReason::PrivateStreamDelay
            | ProductionRiskReason::BalanceReconciliationMismatch
    )
}

fn log_risk_rejection(context: &str, decision: &ProductionRiskDecision) {
    if decision.action == RiskAction::Allow {
        return;
    }
    log::warn!(
        target: "cross_exchange_arbitrage_risk",
        "risk_rejection context={} action={:?} allow_new_position={} reduce_exposure={} close_position={} flatten={} reasons={:?}",
        context,
        decision.action,
        decision.allow_new_position,
        decision.should_reduce_exposure,
        decision.should_close_position,
        decision.should_flatten,
        decision.reasons
    );
    if decision.reasons.iter().any(|reason| {
        matches!(
            reason,
            ProductionRiskReason::SpreadStopLoss
                | ProductionRiskReason::TimeStopLoss
                | ProductionRiskReason::MaxLossPerTrade
                | ProductionRiskReason::MaxDailyLoss
                | ProductionRiskReason::MaxDrawdown
        )
    }) {
        log::warn!(
            target: "cross_exchange_arbitrage_risk",
            "cross-arb stop loss context={} action={:?} close_position={} flatten={} reasons={:?}",
            context,
            decision.action,
            decision.should_close_position,
            decision.should_flatten,
            decision.reasons
        );
    }
}

impl RiskGateDecision {
    pub fn allow() -> Self {
        Self {
            can_open: true,
            reject_reasons: Vec::new(),
        }
    }

    pub fn from_reasons(reject_reasons: Vec<RejectReason>) -> Self {
        Self {
            can_open: reject_reasons.is_empty(),
            reject_reasons,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskOperatingMode {
    Normal,
    Degraded,
    CloseOnly,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskTriggerKind {
    PrivateStreamStale,
    PrivateStreamDisconnected,
    ReconciliationDrift,
    OrphanExposure,
    RouteDegraded,
    ManualPause,
    ManualCloseOnly,
    KillSwitch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskTriggerRecord {
    pub trigger_id: String,
    pub kind: RiskTriggerKind,
    pub exchange: Option<ExchangeId>,
    pub symbol: Option<String>,
    pub message: String,
    pub triggered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamHealth {
    pub exchange: ExchangeId,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub stale_after_ms: i64,
    pub needs_resync: bool,
    pub consecutive_stale_checks: u32,
    pub resync_requested_at: Option<DateTime<Utc>>,
}

impl PrivateStreamHealth {
    pub fn new(exchange: ExchangeId, stale_after_ms: i64) -> Self {
        Self {
            exchange,
            last_event_at: None,
            last_disconnect_at: None,
            stale_after_ms,
            needs_resync: false,
            consecutive_stale_checks: 0,
            resync_requested_at: None,
        }
    }

    pub fn observe_event(&mut self, received_at: DateTime<Utc>) {
        self.last_event_at = Some(received_at);
        self.needs_resync = false;
        self.consecutive_stale_checks = 0;
        self.resync_requested_at = None;
    }

    pub fn observe_disconnect(&mut self, disconnected_at: DateTime<Utc>) {
        self.last_disconnect_at = Some(disconnected_at);
        self.needs_resync = true;
        self.resync_requested_at.get_or_insert(disconnected_at);
    }

    pub fn evaluate(&mut self, now: DateTime<Utc>) -> bool {
        let stale = self
            .last_event_at
            .map(|last| now.signed_duration_since(last).num_milliseconds() > self.stale_after_ms)
            .unwrap_or(true);
        if stale {
            self.consecutive_stale_checks = self.consecutive_stale_checks.saturating_add(1);
            self.needs_resync = true;
            self.resync_requested_at.get_or_insert(now);
        } else {
            self.consecutive_stale_checks = 0;
        }
        stale
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskState {
    pub mode: RiskOperatingMode,
    pub paused_new_entries: bool,
    pub close_only: bool,
    pub kill_switch: bool,
    #[serde(default = "default_true")]
    pub private_resync_blocks_new_entries: bool,
    pub private_stream_health: HashMap<ExchangeId, PrivateStreamHealth>,
    pub triggers: Vec<RiskTriggerRecord>,
    pub updated_at: DateTime<Utc>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskDecision {
    pub mode: RiskOperatingMode,
    pub allow_new_entries: bool,
    pub allow_closes: bool,
    pub needs_reconciliation: bool,
    pub needs_private_resync: bool,
    pub trigger_count: usize,
}

impl StrategyRiskState {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            mode: RiskOperatingMode::Normal,
            paused_new_entries: false,
            close_only: false,
            kill_switch: false,
            private_resync_blocks_new_entries: true,
            private_stream_health: HashMap::new(),
            triggers: Vec::new(),
            updated_at: now,
        }
    }

    pub fn observe_private_event(
        &mut self,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
        needs_resync: bool,
        stale_after_ms: i64,
    ) {
        let health = self
            .private_stream_health
            .entry(exchange.clone())
            .or_insert_with(|| PrivateStreamHealth::new(exchange.clone(), stale_after_ms));
        health.stale_after_ms = stale_after_ms;
        health.observe_event(received_at);
        if needs_resync {
            health.needs_resync = true;
            health.resync_requested_at.get_or_insert(received_at);
            self.raise_trigger(
                RiskTriggerKind::PrivateStreamStale,
                Some(exchange.clone()),
                None,
                "private stream requested resync".to_string(),
                received_at,
            );
        }
        self.updated_at = received_at;
        self.recompute_mode(received_at);
    }

    pub fn observe_private_disconnect(
        &mut self,
        exchange: ExchangeId,
        disconnected_at: DateTime<Utc>,
        stale_after_ms: i64,
    ) {
        let health = self
            .private_stream_health
            .entry(exchange.clone())
            .or_insert_with(|| PrivateStreamHealth::new(exchange.clone(), stale_after_ms));
        health.stale_after_ms = stale_after_ms;
        health.observe_disconnect(disconnected_at);
        self.raise_trigger(
            RiskTriggerKind::PrivateStreamDisconnected,
            Some(exchange.clone()),
            None,
            "private websocket disconnected".to_string(),
            disconnected_at,
        );
        self.updated_at = disconnected_at;
        self.recompute_mode(disconnected_at);
    }

    pub fn evaluate_private_health(&mut self, now: DateTime<Utc>) -> bool {
        let mut any_stale = false;
        for health in self.private_stream_health.values_mut() {
            any_stale |= health.evaluate(now);
        }
        if any_stale {
            self.recompute_mode(now);
        }
        any_stale
    }

    pub fn record_orphan_exposure(
        &mut self,
        summary: &PortfolioExposureSummary,
        orphan_threshold: f64,
        block_new_entries: bool,
        close_only_after_count: usize,
        now: DateTime<Utc>,
    ) {
        if summary.orphan_qty > orphan_threshold {
            self.raise_trigger(
                RiskTriggerKind::OrphanExposure,
                None,
                None,
                format!("orphan exposure {:.8}", summary.orphan_qty),
                now,
            );
            if block_new_entries && summary.orphan_bundle_count >= close_only_after_count.max(1) {
                self.close_only = true;
            }
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn record_reconciliation_severity(
        &mut self,
        exchange: ExchangeId,
        symbol: String,
        severity: ReconcileSeverity,
        orphan_exposure_blocks_new_entries: bool,
        now: DateTime<Utc>,
    ) {
        match severity {
            ReconcileSeverity::Ok => {}
            ReconcileSeverity::MinorDrift => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange.clone()),
                    Some(symbol.clone()),
                    "minor reconciliation drift".to_string(),
                    now,
                );
                self.paused_new_entries = true;
            }
            ReconcileSeverity::OrderDrift | ReconcileSeverity::PositionDrift => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange.clone()),
                    Some(symbol.clone()),
                    format!("reconciliation drift: {:?}", severity),
                    now,
                );
                self.close_only = true;
            }
            ReconcileSeverity::OrphanExposure => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange.clone()),
                    Some(symbol.clone()),
                    format!("reconciliation drift: {:?}", severity),
                    now,
                );
                if orphan_exposure_blocks_new_entries {
                    self.close_only = true;
                }
            }
            ReconcileSeverity::UnknownCritical => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange),
                    Some(symbol),
                    format!("reconciliation severity: {:?}", severity),
                    now,
                );
                self.close_only = true;
            }
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn record_route_status(
        &mut self,
        exchange: ExchangeId,
        status: RouteStatus,
        now: DateTime<Utc>,
    ) {
        if !status.allows_new_entries() {
            self.raise_trigger(
                RiskTriggerKind::RouteDegraded,
                Some(exchange.clone()),
                None,
                format!("route status {:?}", status),
                now,
            );
        }
        if !status.allows_closes() {
            self.kill_switch = true;
            self.raise_trigger(
                RiskTriggerKind::KillSwitch,
                Some(exchange),
                None,
                "route cannot safely close".to_string(),
                now,
            );
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn pause_new_entries(&mut self, now: DateTime<Utc>) {
        self.paused_new_entries = true;
        self.raise_trigger(
            RiskTriggerKind::ManualPause,
            None,
            None,
            "manual pause".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn set_close_only(&mut self, now: DateTime<Utc>) {
        self.close_only = true;
        self.raise_trigger(
            RiskTriggerKind::ManualCloseOnly,
            None,
            None,
            "manual close-only".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn kill_switch(&mut self, now: DateTime<Utc>) {
        self.kill_switch = true;
        self.paused_new_entries = true;
        self.close_only = true;
        self.raise_trigger(
            RiskTriggerKind::KillSwitch,
            None,
            None,
            "kill switch".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn decision(&self) -> RiskDecision {
        let needs_private_resync = self
            .private_stream_health
            .values()
            .any(|health| health.needs_resync);
        RiskDecision {
            mode: self.mode.clone(),
            allow_new_entries: !self.paused_new_entries
                && !self.close_only
                && !self.kill_switch
                && (!needs_private_resync || !self.private_resync_blocks_new_entries),
            allow_closes: !self.kill_switch,
            needs_reconciliation: self.close_only
                || self.paused_new_entries
                || (needs_private_resync && self.private_resync_blocks_new_entries),
            needs_private_resync,
            trigger_count: self.triggers.len(),
        }
    }

    pub fn recompute_mode(&mut self, now: DateTime<Utc>) {
        let any_resync = self
            .private_stream_health
            .values()
            .any(|health| health.needs_resync);
        self.mode = if self.kill_switch {
            RiskOperatingMode::Halted
        } else if self.close_only || (any_resync && self.private_resync_blocks_new_entries) {
            RiskOperatingMode::CloseOnly
        } else if self.paused_new_entries {
            RiskOperatingMode::Degraded
        } else {
            RiskOperatingMode::Normal
        };
        self.updated_at = now;
    }

    fn raise_trigger(
        &mut self,
        kind: RiskTriggerKind,
        exchange: Option<ExchangeId>,
        symbol: Option<String>,
        message: String,
        triggered_at: DateTime<Utc>,
    ) {
        let trigger_id = format!(
            "risk-{}-{}",
            triggered_at.timestamp_millis(),
            self.triggers.len()
        );
        self.triggers.push(RiskTriggerRecord {
            trigger_id,
            kind,
            exchange,
            symbol,
            message,
            triggered_at,
        });
        if self.triggers.len() > 1000 {
            let drain = self.triggers.len() - 1000;
            self.triggers.drain(0..drain);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGate;

impl RiskGate {
    pub fn evaluate_open(
        maker_book: &OrderBook5,
        taker_book: &OrderBook5,
        maker_route_status: RouteStatus,
        taker_route_status: RouteStatus,
        taker_vwap: &TakerVwapResult,
        funding: &FundingEstimate,
        target_notional_usdt: f64,
        config: &CrossExchangeArbitrageConfig,
        now: DateTime<Utc>,
    ) -> RiskGateDecision {
        let mut reasons = Vec::new();

        if !maker_book.is_usable() || !taker_book.is_usable() {
            reasons.push(RejectReason::BadOrderBook);
        }
        if maker_book.quality.stale
            || taker_book.quality.stale
            || book_age_ms(maker_book, now) > config.risk.max_book_age_ms
            || book_age_ms(taker_book, now) > config.risk.max_book_age_ms
        {
            reasons.push(RejectReason::StaleBook);
        }
        if !maker_route_status.allows_new_entries() || !taker_route_status.allows_new_entries() {
            reasons.push(RejectReason::RouteUnhealthy);
        }
        if !taker_vwap.depth_enough {
            reasons.push(RejectReason::DepthInsufficient);
        }
        if funding.dangerous {
            reasons.push(RejectReason::FundingDangerous);
        }
        if funding.near_negative_settlement {
            reasons.push(RejectReason::FundingWindowTooClose);
        }
        if target_notional_usdt > config.risk.max_notional_per_symbol_usdt {
            reasons.push(RejectReason::NotionalOverLimit);
        }
        if taker_vwap.slippage_pct > config.risk.max_taker_slippage_pct {
            reasons.push(RejectReason::SlippageTooHigh);
        }

        RiskGateDecision::from_reasons(reasons)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OneWayExposureKey {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub side: PositionSide,
}

pub fn one_way_conflict_for_open(
    exchange: &ExchangeId,
    canonical_symbol: &CanonicalSymbol,
    proposed_side: PositionSide,
    existing: impl IntoIterator<Item = OneWayExposureKey>,
) -> bool {
    if *exchange != ExchangeId::Gate {
        return false;
    }
    existing.into_iter().any(|item| {
        item.exchange == *exchange
            && item.canonical_symbol == *canonical_symbol
            && item.side != proposed_side
    })
}

pub fn book_age_ms(book: &OrderBook5, now: DateTime<Utc>) -> i64 {
    now.signed_duration_since(book.recv_ts).num_milliseconds()
}

impl Default for StrategyRiskState {
    fn default() -> Self {
        Self::new(Utc::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{BookLevel, CanonicalSymbol, ExchangeSymbol};
    use crate::strategies::cross_exchange_arbitrage::simulation::calculate_taker_vwap;
    use crate::strategies::cross_exchange_arbitrage::state::OrderSide;

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
    fn strategy_risk_state_should_enter_close_only_on_resync() {
        let now = Utc::now();
        let mut state = StrategyRiskState::new(now);
        state.observe_private_event(ExchangeId::Binance, now, true, 45_000);

        let decision = state.decision();

        assert_eq!(decision.mode, RiskOperatingMode::CloseOnly);
        assert!(decision.needs_private_resync);
        assert!(!decision.allow_new_entries);
        assert!(decision.allow_closes);
    }

    #[test]
    fn strategy_risk_state_should_recover_after_healthy_private_event() {
        let now = Utc::now();
        let mut state = StrategyRiskState::new(now);
        state.observe_private_event(ExchangeId::Binance, now, true, 45_000);
        state.observe_private_event(
            ExchangeId::Binance,
            now + chrono::Duration::milliseconds(500),
            false,
            45_000,
        );

        let decision = state.decision();

        assert_eq!(decision.mode, RiskOperatingMode::Normal);
        assert!(!decision.needs_private_resync);
        assert!(decision.allow_new_entries);
    }

    #[test]
    fn risk_gate_should_reject_stale_books() {
        let now = Utc::now();
        let config = CrossExchangeArbitrageConfig::default();
        let mut maker = book(ExchangeId::Binance, 100.0, 101.0);
        maker.recv_ts = now - chrono::Duration::seconds(10);
        let taker = book(ExchangeId::Okx, 100.5, 101.5);
        let funding = FundingEstimate {
            long_leg_funding: 0.0,
            short_leg_funding: 0.0,
            net_funding: 0.0,
            net_funding_rate: 0.0,
            next_funding_time: None,
            minutes_to_funding: None,
            dangerous: false,
            near_negative_settlement: false,
        };
        let vwap = calculate_taker_vwap(&taker, OrderSide::Buy, 100.0);

        let decision = RiskGate::evaluate_open(
            &maker,
            &taker,
            RouteStatus::Healthy,
            RouteStatus::Healthy,
            &vwap,
            &funding,
            config.risk.max_notional_per_symbol_usdt,
            &config,
            now,
        );

        assert!(decision.reject_reasons.contains(&RejectReason::StaleBook));
    }

    fn open_position(opened_at: DateTime<Utc>) -> OpenPositionRiskSnapshot {
        OpenPositionRiskSnapshot {
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            buy_exchange: ExchangeId::Binance,
            sell_exchange: ExchangeId::Okx,
            opened_at,
            entry_spread_bps: 20.0,
            current_spread_bps: 20.0,
            residual_exposure_usdt: 0.0,
        }
    }

    fn opportunity() -> OpportunityRecord {
        OpportunityRecord {
            timestamp: Utc::now(),
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            buy_exchange: ExchangeId::Binance,
            sell_exchange: ExchangeId::Okx,
            buy_price: 100.0,
            sell_price: 101.0,
            raw_spread_bps: 100.0,
            estimated_net_spread_bps: 80.0,
            estimated_notional: 100.0,
            decision: super::super::OpportunityDecision::Accepted,
            reason: "accepted".to_string(),
        }
    }

    #[test]
    fn spread_stop_loss_should_close_position() {
        let mut config = RiskConfig::default();
        config.max_spread_loss_bps = 15.0;
        let now = Utc::now();
        let mut position = open_position(now);
        position.current_spread_bps = 0.0;

        let decision = evaluate_after_fill_risk(
            &config,
            &position,
            &LossRiskSnapshot::default(),
            &[],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::ClosePosition);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::SpreadStopLoss));
    }

    #[test]
    fn time_stop_loss_should_close_position() {
        let mut config = RiskConfig::default();
        config.max_hold_seconds = 30;
        let now = Utc::now();
        let position = open_position(now - chrono::Duration::seconds(31));

        let decision = evaluate_after_fill_risk(
            &config,
            &position,
            &LossRiskSnapshot::default(),
            &[],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::ClosePosition);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::TimeStopLoss));
    }

    #[test]
    fn max_daily_loss_should_lock_out_new_positions() {
        let mut config = RiskConfig::default();
        config.max_daily_loss_usdt = 50.0;
        let now = Utc::now();
        let position = open_position(now);
        let losses = LossRiskSnapshot {
            daily_pnl_usdt: -51.0,
            ..LossRiskSnapshot::default()
        };

        let decision = evaluate_after_fill_risk(
            &config,
            &position,
            &losses,
            &[],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::ClosePosition);
        assert!(!decision.allow_new_position);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::MaxDailyLoss));
    }

    #[test]
    fn stale_exchange_should_lock_out_new_positions() {
        let config = RiskConfig::default();
        let now = Utc::now();
        let health = ExchangeHealthSnapshot {
            ws_stale: true,
            ..ExchangeHealthSnapshot::healthy(ExchangeId::Binance)
        };

        let decision = evaluate_pre_trade_risk(
            &config,
            &opportunity(),
            &PortfolioRiskSnapshot::default(),
            &[health],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::StopOpening);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::ExchangeDataStale));
    }

    #[test]
    fn cancel_failure_path_should_flatten_after_fill() {
        let mut config = RiskConfig::default();
        config.max_cancel_failure_rate = 0.01;
        let now = Utc::now();
        let health = ExchangeHealthSnapshot {
            cancel_failure_rate: 0.25,
            ..ExchangeHealthSnapshot::healthy(ExchangeId::Okx)
        };

        let decision = evaluate_after_fill_risk(
            &config,
            &open_position(now),
            &LossRiskSnapshot::default(),
            &[health],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::EmergencyFlatten);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::CancelFailureRate));
    }

    #[test]
    fn residual_exposure_should_trigger_kill_switch_flatten() {
        let mut config = RiskConfig::default();
        config.max_single_leg_exposure_usdt = 100.0;
        let now = Utc::now();
        let mut position = open_position(now);
        position.residual_exposure_usdt = 150.0;

        let decision = evaluate_after_fill_risk(
            &config,
            &position,
            &LossRiskSnapshot::default(),
            &[],
            &KillSwitchState::default(),
            now,
        );

        assert_eq!(decision.action, RiskAction::EmergencyFlatten);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::ResidualExposure));
    }

    #[test]
    fn emergency_flatten_path_should_override_pre_trade() {
        let config = RiskConfig::default();
        let mut kill_switch = KillSwitchState::default();
        kill_switch.emergency_flatten = true;

        let decision = evaluate_pre_trade_risk(
            &config,
            &opportunity(),
            &PortfolioRiskSnapshot::default(),
            &[],
            &kill_switch,
            Utc::now(),
        );

        assert_eq!(decision.action, RiskAction::EmergencyFlatten);
        assert!(decision.should_flatten);
        assert!(decision
            .reasons
            .contains(&ProductionRiskReason::GlobalKillSwitch));
    }
}
