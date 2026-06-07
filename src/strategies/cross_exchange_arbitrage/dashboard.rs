//! Read models for dashboard/API workers. This module has no server side effects.

use super::opportunity::Opportunity;
use super::position::PortfolioExposureSummary;
use super::risk::{
    PrivateStreamHealth, RejectReason, RiskDecision, RiskOperatingMode, StrategyRiskState,
};
use super::state::{SimulatedBundleState, SimulatedBundleStatus};
use crate::execution::{OrderSide, PositionSide};
use crate::market::{BookQuality, CanonicalSymbol, ExchangeId, RouteStatus, RuntimeMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbDashboardStatus {
    pub mode: RuntimeMode,
    pub updated_at: DateTime<Utc>,
    pub enabled_symbols: usize,
    pub enabled_exchanges: usize,
    pub open_bundles: usize,
    pub position_summary: PortfolioExposureSummary,
    pub route_health: Vec<RouteReadModel>,
    pub risk_state: RiskStateReadModel,
    pub private_stream_health: Vec<PrivateStreamHealthReadModel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteReadModel {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub status: RouteStatus,
    pub last_book_age_ms: i64,
    pub reject_reasons: Vec<RejectReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityReadModel {
    pub opportunity_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: super::state::OrderSide,
    pub maker_price: f64,
    pub maker_price_tick: Option<f64>,
    pub maker_best_opposite_price: Option<f64>,
    pub maker_book_spread_pct: f64,
    pub raw_open_spread: f64,
    pub maker_taker_net_edge: f64,
    pub target_notional_usdt: f64,
    pub executable_notional_usdt: f64,
    pub maker_quantity: Option<f64>,
    pub taker_quantity: Option<f64>,
    pub maker_notional_usdt: Option<f64>,
    pub taker_notional_usdt: Option<f64>,
    pub open_fee_est_usdt: f64,
    pub close_fee_est_usdt: f64,
    pub expected_funding_usdt: f64,
    pub slippage_pct: f64,
    pub depth_notional_usdt: f64,
    pub book_age_ms: i64,
    pub can_open: bool,
    pub reject_reasons: Vec<RejectReason>,
    pub created_at: DateTime<Utc>,
}

impl From<&Opportunity> for OpportunityReadModel {
    fn from(opportunity: &Opportunity) -> Self {
        Self {
            opportunity_id: opportunity.opportunity_id.clone(),
            canonical_symbol: opportunity.canonical_symbol.clone(),
            long_exchange: opportunity.long_exchange.clone(),
            short_exchange: opportunity.short_exchange.clone(),
            maker_exchange: opportunity.maker_exchange.clone(),
            taker_exchange: opportunity.taker_exchange.clone(),
            maker_side: opportunity.maker_side,
            maker_price: opportunity.maker_price,
            maker_price_tick: opportunity.maker_price_tick,
            maker_best_opposite_price: opportunity.maker_best_opposite_price,
            maker_book_spread_pct: opportunity.maker_book_spread_pct,
            raw_open_spread: opportunity.raw_open_spread,
            maker_taker_net_edge: opportunity.maker_taker_net_edge,
            target_notional_usdt: opportunity.target_notional_usdt,
            executable_notional_usdt: opportunity.executable_notional_usdt,
            maker_quantity: opportunity.maker_quantity,
            taker_quantity: opportunity.taker_quantity,
            maker_notional_usdt: opportunity.maker_notional_usdt,
            taker_notional_usdt: opportunity.taker_notional_usdt,
            open_fee_est_usdt: opportunity.open_fee_est_usdt,
            close_fee_est_usdt: opportunity.close_fee_est_usdt,
            expected_funding_usdt: opportunity.expected_funding_usdt,
            slippage_pct: opportunity.slippage_pct,
            depth_notional_usdt: opportunity.depth_notional_usdt,
            book_age_ms: opportunity.book_age_ms,
            can_open: opportunity.can_open,
            reject_reasons: opportunity.reject_reasons.clone(),
            created_at: opportunity.created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleReadModel {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub status: SimulatedBundleStatus,
    pub target_notional_usdt: f64,
    pub updated_at: DateTime<Utc>,
}

impl From<&SimulatedBundleState> for BundleReadModel {
    fn from(bundle: &SimulatedBundleState) -> Self {
        Self {
            bundle_id: bundle.bundle_id.clone(),
            opportunity_id: bundle.opportunity_id.clone(),
            status: bundle.status,
            target_notional_usdt: bundle.target_notional_usdt,
            updated_at: bundle.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcileReadModel {
    pub report_id: String,
    pub severity: String,
    pub message: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskEventReadModel {
    pub event_id: String,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange: Option<ExchangeId>,
    pub reason: RejectReason,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MakerExecutionStatsReadModel {
    pub canonical_symbol: CanonicalSymbol,
    pub exchange: ExchangeId,
    pub side: super::state::OrderSide,
    pub consecutive_ttl_cancels: u32,
    pub total_ttl_cancels: u64,
    pub total_fills: u64,
    pub current_aggressive_ticks: u32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgeRecordStatus {
    Hedging,
    Hedged,
    RepairPending,
    RepairSubmitted,
    Closing,
    Closed,
    RepairFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgeRecordReadModel {
    pub record_id: String,
    pub bundle_id: String,
    #[serde(default)]
    pub opened_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub hedge_order_submitted_at: Option<DateTime<Utc>>,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub status: HedgeRecordStatus,
    pub entry_net_edge_pct: Option<f64>,
    pub close_spread_pct: Option<f64>,
    pub close_net_profit_pct: Option<f64>,
    pub is_closed: bool,
    pub closed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleCloseMetricReadModel {
    pub bundle_id: String,
    pub symbol: String,
    pub long_exchange: String,
    pub short_exchange: String,
    pub quantity: f64,
    pub target_notional_usdt: f64,
    pub entry_edge_pct: Option<f64>,
    pub open_fee_paid_usdt: f64,
    pub close_fee_est_usdt: f64,
    pub gross_spread_pnl_usdt: f64,
    pub realized_funding_pnl_usdt: f64,
    pub close_spread_pct: f64,
    pub close_profit_pct: f64,
    pub close_threshold_pct: f64,
    pub close_maker_exchange: String,
    pub close_maker_side: String,
    pub close_maker_price: f64,
    pub close_maker_book_spread_pct: f64,
    pub closeable: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgeRepairTaskStatus {
    Pending,
    Submitted,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgeRepairTaskReadModel {
    pub task_id: String,
    pub record_id: String,
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub failed_exchange: ExchangeId,
    pub last_attempt_exchange: Option<ExchangeId>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: f64,
    pub reduce_only: bool,
    pub status: HedgeRepairTaskStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookQualityReadModel {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub book_age_ms: i64,
    pub source_route: Option<String>,
    pub quality: BookQuality,
    pub usable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskStateReadModel {
    pub mode: RiskOperatingMode,
    pub allow_new_entries: bool,
    pub allow_closes: bool,
    pub needs_reconciliation: bool,
    pub needs_private_resync: bool,
    pub trigger_count: usize,
}

impl From<&RiskDecision> for RiskStateReadModel {
    fn from(decision: &RiskDecision) -> Self {
        Self {
            mode: decision.mode.clone(),
            allow_new_entries: decision.allow_new_entries,
            allow_closes: decision.allow_closes,
            needs_reconciliation: decision.needs_reconciliation,
            needs_private_resync: decision.needs_private_resync,
            trigger_count: decision.trigger_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamHealthReadModel {
    pub exchange: ExchangeId,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub stale_after_ms: i64,
    pub needs_resync: bool,
    pub consecutive_stale_checks: u32,
    pub resync_requested_at: Option<DateTime<Utc>>,
}

impl From<&PrivateStreamHealth> for PrivateStreamHealthReadModel {
    fn from(health: &PrivateStreamHealth) -> Self {
        Self {
            exchange: health.exchange.clone(),
            last_event_at: health.last_event_at,
            last_disconnect_at: health.last_disconnect_at,
            stale_after_ms: health.stale_after_ms,
            needs_resync: health.needs_resync,
            consecutive_stale_checks: health.consecutive_stale_checks,
            resync_requested_at: health.resync_requested_at,
        }
    }
}
