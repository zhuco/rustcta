use crate::execution::{
    OrderAck, OrderCommand, OrderCommandStatus, OrderIntent, OrderSide, OrderType, TimeInForce,
};
use crate::market::{CanonicalSymbol, ExchangeId, RuntimeMode};
use anyhow::{bail, ensure, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleStatus {
    Observing,
    MakerPending,
    MakerTimeout,
    MakerFilled,
    Hedging,
    OpenSimulated,
    ClosingSimulated,
    Closed,
    Expired,
    OrphanLeg,
    OneSidedExposure,
    RiskStopped,
    DepthInsufficient,
    ReconcileRequired,
}

impl BundleStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Expired | Self::RiskStopped)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleLeg {
    Long,
    Short,
    Maker,
    Taker,
    Hedge,
    CloseLong,
    CloseShort,
    EmergencyCloseLong,
    EmergencyCloseShort,
}

impl BundleLeg {
    pub fn as_slug(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
            Self::Maker => "maker",
            Self::Taker => "taker",
            Self::Hedge => "hedge",
            Self::CloseLong => "close-long",
            Self::CloseShort => "close-short",
            Self::EmergencyCloseLong => "emergency-close-long",
            Self::EmergencyCloseShort => "emergency-close-short",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CloseReason {
    TargetReached,
    StopLoss,
    SpreadMeanReverted,
    FundingRisk,
    RouteDegraded,
    Manual,
    MakerTimeout,
    OrphanRecovered,
    ReconcileRecovery,
    EmergencyRisk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageBundle {
    pub bundle_id: String,
    pub mode: RuntimeMode,
    pub canonical_symbol: CanonicalSymbol,
    pub status: BundleStatus,
    pub open_time: Option<DateTime<Utc>>,
    pub close_time: Option<DateTime<Utc>>,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub long_entry_vwap: Option<f64>,
    pub short_entry_vwap: Option<f64>,
    pub long_qty: f64,
    pub short_qty: f64,
    pub target_notional: f64,
    pub entry_spread: Option<f64>,
    pub current_spread: Option<f64>,
    pub open_fee: f64,
    pub close_fee: f64,
    pub funding_pnl: f64,
    pub gross_spread_pnl: f64,
    pub net_pnl: f64,
    pub max_adverse_spread: Option<f64>,
    pub max_favorable_spread: Option<f64>,
    pub orphan_loss: f64,
    pub close_reason: Option<CloseReason>,
    pub last_reconcile_at: Option<DateTime<Utc>>,
    pub created_from_signal_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ArbitrageBundle {
    pub fn new(
        bundle_id: impl Into<String>,
        mode: RuntimeMode,
        canonical_symbol: CanonicalSymbol,
        long_exchange: ExchangeId,
        short_exchange: ExchangeId,
        maker_exchange: ExchangeId,
        taker_exchange: ExchangeId,
        target_notional: f64,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            bundle_id: bundle_id.into(),
            mode,
            canonical_symbol,
            status: BundleStatus::Observing,
            open_time: None,
            close_time: None,
            long_exchange,
            short_exchange,
            maker_exchange,
            taker_exchange,
            long_entry_vwap: None,
            short_entry_vwap: None,
            long_qty: 0.0,
            short_qty: 0.0,
            target_notional,
            entry_spread: None,
            current_spread: None,
            open_fee: 0.0,
            close_fee: 0.0,
            funding_pnl: 0.0,
            gross_spread_pnl: 0.0,
            net_pnl: 0.0,
            max_adverse_spread: None,
            max_favorable_spread: None,
            orphan_loss: 0.0,
            close_reason: None,
            last_reconcile_at: None,
            created_from_signal_id: None,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn has_balanced_open_qty(&self, tolerance: f64) -> bool {
        (self.long_qty - self.short_qty).abs() <= tolerance
    }

    pub fn unhedged_qty(&self) -> f64 {
        (self.long_qty - self.short_qty).abs()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleSubmissionPath {
    Batch,
    Concurrent,
    DryRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleExecutionStatus {
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    RequiresReconcile,
    OneSidedExposure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleLegEventKind {
    Submitted,
    Acked,
    Rejected,
    Failed,
    PartiallyFilled,
    Filled,
    RequiresReconcile,
    OneSidedExposure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OneSidedExposureKind {
    LongBase,
    ShortBase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleRecoveryMode {
    NoLoss,
    ProfitCovered,
    EmergencyRisk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OneSidedRecoveryPolicy {
    pub no_loss_safety_bps: u32,
    pub profit_floor_usdt: f64,
    pub allow_emergency_risk: bool,
}

impl Default for OneSidedRecoveryPolicy {
    fn default() -> Self {
        Self {
            no_loss_safety_bps: 8,
            profit_floor_usdt: 0.0,
            allow_emergency_risk: false,
        }
    }
}

impl OneSidedRecoveryPolicy {
    pub fn allowed_modes(&self) -> Vec<BundleRecoveryMode> {
        let mut modes = vec![
            BundleRecoveryMode::NoLoss,
            BundleRecoveryMode::ProfitCovered,
        ];
        if self.allow_emergency_risk {
            modes.push(BundleRecoveryMode::EmergencyRisk);
        }
        modes
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleOrderLeg {
    pub leg_id: String,
    pub command: OrderCommand,
}

impl BundleOrderLeg {
    pub fn new(leg: BundleLeg, command: OrderCommand) -> Self {
        Self {
            leg_id: leg.as_slug().to_string(),
            command,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleSubmitCommand {
    pub mode: RuntimeMode,
    pub bundle_id: String,
    pub opportunity_id: String,
    pub idempotency_key: String,
    pub legs: Vec<BundleOrderLeg>,
    pub recovery_policy: OneSidedRecoveryPolicy,
    pub requested_at: DateTime<Utc>,
}

impl BundleSubmitCommand {
    pub fn new(
        mode: RuntimeMode,
        bundle_id: impl Into<String>,
        opportunity_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        legs: impl IntoIterator<Item = BundleOrderLeg>,
        requested_at: DateTime<Utc>,
    ) -> Self {
        Self {
            mode,
            bundle_id: bundle_id.into(),
            opportunity_id: opportunity_id.into(),
            idempotency_key: idempotency_key.into(),
            legs: legs.into_iter().collect(),
            recovery_policy: OneSidedRecoveryPolicy::default(),
            requested_at,
        }
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.bundle_id.trim().is_empty(), "bundle_id is required");
        ensure!(
            !self.opportunity_id.trim().is_empty(),
            "opportunity_id is required"
        );
        ensure!(
            !self.idempotency_key.trim().is_empty(),
            "idempotency_key is required"
        );
        ensure!(
            self.legs.len() == 2,
            "spot taker-taker bundle requires exactly two legs"
        );

        let mut leg_ids = HashSet::new();
        let mut client_order_ids = HashSet::new();
        for leg in &self.legs {
            ensure!(!leg.leg_id.trim().is_empty(), "leg_id is required");
            ensure!(
                leg_ids.insert(leg.leg_id.clone()),
                "duplicate bundle leg_id {}",
                leg.leg_id
            );
            ensure!(
                leg.command.bundle_id == self.bundle_id,
                "leg {} has mismatched bundle_id {}",
                leg.leg_id,
                leg.command.bundle_id
            );
            ensure!(
                client_order_ids.insert(leg.command.client_order_id.clone()),
                "duplicate client_order_id {} in bundle {}",
                leg.command.client_order_id,
                self.bundle_id
            );
            ensure!(
                leg.command.quantity.is_finite() && leg.command.quantity > 0.0,
                "leg {} has invalid quantity",
                leg.leg_id
            );
            if self.mode.allows_live_orders() && is_taker_intent(leg.command.intent) {
                validate_live_taker_order_safety(&leg.command)?;
            }
        }

        Ok(())
    }

    pub fn same_exchange(&self) -> Option<ExchangeId> {
        let first = self.legs.first()?.command.exchange.clone();
        if self.legs.iter().all(|leg| leg.command.exchange == first) {
            Some(first)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OneSidedExposureState {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub idempotency_key: String,
    pub leg_id: String,
    pub kind: OneSidedExposureKind,
    pub exchange: ExchangeId,
    pub quantity: f64,
    pub cost_price: Option<f64>,
    pub recovery_modes: Vec<BundleRecoveryMode>,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleLegEvent {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub idempotency_key: String,
    pub leg_id: String,
    pub exchange: ExchangeId,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub kind: BundleLegEventKind,
    pub accepted: bool,
    pub status: OrderCommandStatus,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleLegSubmitResult {
    pub leg_id: String,
    pub command: OrderCommand,
    pub ack: Option<OrderAck>,
    pub error: Option<String>,
}

impl BundleLegSubmitResult {
    pub fn acked(leg: BundleOrderLeg, ack: OrderAck) -> Self {
        Self {
            leg_id: leg.leg_id,
            command: leg.command,
            ack: Some(ack),
            error: None,
        }
    }

    pub fn failed(leg: BundleOrderLeg, error: impl Into<String>) -> Self {
        Self {
            leg_id: leg.leg_id,
            command: leg.command,
            ack: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleSubmitReport {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub idempotency_key: String,
    pub submission_path: BundleSubmissionPath,
    pub status: BundleExecutionStatus,
    pub order_acks: Vec<OrderAck>,
    pub events: Vec<BundleLegEvent>,
    pub one_sided_exposure: Option<OneSidedExposureState>,
    pub requires_reconcile: bool,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl BundleSubmitReport {
    pub fn from_results(
        command: &BundleSubmitCommand,
        submission_path: BundleSubmissionPath,
        results: Vec<BundleLegSubmitResult>,
        acknowledged_at: DateTime<Utc>,
    ) -> Self {
        let mut events = Vec::new();
        let mut order_acks = Vec::new();

        for result in &results {
            match (&result.ack, &result.error) {
                (Some(ack), _) => {
                    events.push(event_from_ack(command, result, ack));
                    order_acks.push(ack.clone());
                }
                (None, Some(error)) => {
                    events.push(event_from_error(command, result, error, acknowledged_at));
                }
                (None, None) => {
                    events.push(event_from_error(
                        command,
                        result,
                        "bundle leg finished without ack or error",
                        acknowledged_at,
                    ));
                }
            }
        }

        let one_sided_exposure = classify_one_sided_exposure(command, &results, acknowledged_at);
        if let Some(exposure) = &one_sided_exposure {
            events.push(BundleLegEvent {
                bundle_id: command.bundle_id.clone(),
                opportunity_id: command.opportunity_id.clone(),
                idempotency_key: command.idempotency_key.clone(),
                leg_id: exposure.leg_id.clone(),
                exchange: exposure.exchange.clone(),
                client_order_id: results
                    .iter()
                    .find(|result| result.leg_id == exposure.leg_id)
                    .map(|result| result.command.client_order_id.clone())
                    .unwrap_or_default(),
                exchange_order_id: None,
                kind: BundleLegEventKind::OneSidedExposure,
                accepted: false,
                status: OrderCommandStatus::PartiallyFilled,
                message: Some(
                    "single-leg or partial fill entered one-sided exposure recovery".to_string(),
                ),
                occurred_at: acknowledged_at,
            });
        }

        let has_partial = results.iter().any(|result| {
            result
                .ack
                .as_ref()
                .is_some_and(|ack| ack.status == OrderCommandStatus::PartiallyFilled)
        });
        let has_failure = results.iter().any(|result| {
            result.error.is_some()
                || result.ack.as_ref().is_some_and(|ack| {
                    !ack.accepted
                        || matches!(
                            ack.status,
                            OrderCommandStatus::Rejected | OrderCommandStatus::Failed
                        )
                })
        });
        let all_filled = results.iter().all(|result| {
            result
                .ack
                .as_ref()
                .is_some_and(|ack| ack.status == OrderCommandStatus::Filled)
        });
        let all_accepted = results.iter().all(|result| {
            result.ack.as_ref().is_some_and(|ack| {
                ack.accepted
                    && matches!(
                        ack.status,
                        OrderCommandStatus::Submitted
                            | OrderCommandStatus::Accepted
                            | OrderCommandStatus::Filled
                    )
            })
        });

        let status = if one_sided_exposure.is_some() {
            BundleExecutionStatus::OneSidedExposure
        } else if has_failure {
            BundleExecutionStatus::RequiresReconcile
        } else if has_partial {
            BundleExecutionStatus::PartiallyFilled
        } else if all_filled {
            BundleExecutionStatus::Filled
        } else if all_accepted {
            BundleExecutionStatus::Accepted
        } else {
            BundleExecutionStatus::Submitted
        };
        let requires_reconcile = matches!(
            status,
            BundleExecutionStatus::RequiresReconcile
                | BundleExecutionStatus::PartiallyFilled
                | BundleExecutionStatus::OneSidedExposure
        );

        Self {
            bundle_id: command.bundle_id.clone(),
            opportunity_id: command.opportunity_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
            submission_path,
            status,
            order_acks,
            events,
            one_sided_exposure,
            requires_reconcile,
            message: if requires_reconcile {
                Some(format!(
                    "bundle {} requires reconciliation",
                    command.bundle_id
                ))
            } else {
                None
            },
            acknowledged_at,
        }
    }
}

fn event_from_ack(
    command: &BundleSubmitCommand,
    result: &BundleLegSubmitResult,
    ack: &OrderAck,
) -> BundleLegEvent {
    BundleLegEvent {
        bundle_id: command.bundle_id.clone(),
        opportunity_id: command.opportunity_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        leg_id: result.leg_id.clone(),
        exchange: ack.exchange.clone(),
        client_order_id: ack.client_order_id.clone(),
        exchange_order_id: ack.exchange_order_id.clone(),
        kind: event_kind_for_ack(ack),
        accepted: ack.accepted,
        status: ack.status,
        message: ack.message.clone(),
        occurred_at: ack.acknowledged_at,
    }
}

fn event_from_error(
    command: &BundleSubmitCommand,
    result: &BundleLegSubmitResult,
    error: impl Into<String>,
    occurred_at: DateTime<Utc>,
) -> BundleLegEvent {
    BundleLegEvent {
        bundle_id: command.bundle_id.clone(),
        opportunity_id: command.opportunity_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        leg_id: result.leg_id.clone(),
        exchange: result.command.exchange.clone(),
        client_order_id: result.command.client_order_id.clone(),
        exchange_order_id: None,
        kind: BundleLegEventKind::Failed,
        accepted: false,
        status: OrderCommandStatus::Failed,
        message: Some(error.into()),
        occurred_at,
    }
}

fn event_kind_for_ack(ack: &OrderAck) -> BundleLegEventKind {
    match (ack.accepted, ack.status) {
        (_, OrderCommandStatus::PartiallyFilled) => BundleLegEventKind::PartiallyFilled,
        (_, OrderCommandStatus::Filled) => BundleLegEventKind::Filled,
        (false, OrderCommandStatus::Rejected) => BundleLegEventKind::Rejected,
        (false, OrderCommandStatus::Failed) => BundleLegEventKind::Failed,
        (false, _) => BundleLegEventKind::Rejected,
        (true, _) => BundleLegEventKind::Acked,
    }
}

fn classify_one_sided_exposure(
    command: &BundleSubmitCommand,
    results: &[BundleLegSubmitResult],
    started_at: DateTime<Utc>,
) -> Option<OneSidedExposureState> {
    let exposed = results.iter().find(|result| {
        result.ack.as_ref().is_some_and(|ack| {
            matches!(
                ack.status,
                OrderCommandStatus::PartiallyFilled | OrderCommandStatus::Filled
            )
        })
    })?;
    let filled_or_partial_count = results
        .iter()
        .filter(|result| {
            result.ack.as_ref().is_some_and(|ack| {
                matches!(
                    ack.status,
                    OrderCommandStatus::PartiallyFilled | OrderCommandStatus::Filled
                )
            })
        })
        .count();
    let all_legs_have_fill_signal = filled_or_partial_count == results.len();
    let any_failure = results.iter().any(|result| {
        result.error.is_some()
            || result.ack.as_ref().is_some_and(|ack| {
                !ack.accepted
                    || matches!(
                        ack.status,
                        OrderCommandStatus::Rejected | OrderCommandStatus::Failed
                    )
            })
    });
    let any_partial = results.iter().any(|result| {
        result
            .ack
            .as_ref()
            .is_some_and(|ack| ack.status == OrderCommandStatus::PartiallyFilled)
    });

    if all_legs_have_fill_signal && !any_failure && !any_partial {
        return None;
    }
    if !any_partial && !(filled_or_partial_count == 1 && any_failure) {
        return None;
    }

    Some(OneSidedExposureState {
        bundle_id: command.bundle_id.clone(),
        opportunity_id: command.opportunity_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        leg_id: exposed.leg_id.clone(),
        kind: match exposed.command.side {
            OrderSide::Buy => OneSidedExposureKind::LongBase,
            OrderSide::Sell => OneSidedExposureKind::ShortBase,
        },
        exchange: exposed.command.exchange.clone(),
        quantity: exposed.command.quantity,
        cost_price: exposed.command.price,
        recovery_modes: command.recovery_policy.allowed_modes(),
        started_at,
    })
}

fn is_taker_intent(intent: OrderIntent) -> bool {
    matches!(
        intent,
        OrderIntent::HedgeLongTaker
            | OrderIntent::HedgeShortTaker
            | OrderIntent::CloseLongTaker
            | OrderIntent::CloseShortTaker
            | OrderIntent::EmergencyCloseLongTaker
            | OrderIntent::EmergencyCloseShortTaker
    )
}

fn validate_live_taker_order_safety(command: &OrderCommand) -> Result<()> {
    if command.order_type != OrderType::Limit || command.time_in_force != TimeInForce::Ioc {
        bail!(
            "live taker order {} must be submitted as IOC limit",
            command.command_id
        );
    }
    if command
        .price
        .filter(|price| price.is_finite() && price > &0.0)
        .is_none()
    {
        bail!(
            "live taker order {} requires explicit positive limit price",
            command.command_id
        );
    }
    if command
        .max_slippage_pct
        .filter(|slippage| slippage.is_finite() && *slippage > 0.0)
        .is_none()
    {
        bail!(
            "live taker order {} requires max slippage protection",
            command.command_id
        );
    }
    Ok(())
}
