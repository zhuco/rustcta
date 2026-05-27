//! Read models for dashboard/API workers. This module has no server side effects.

use super::opportunity::Opportunity;
use super::position::PortfolioExposureSummary;
use super::risk::RejectReason;
use super::state::{SimulatedBundleState, SimulatedBundleStatus};
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
