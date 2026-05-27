//! Risk gates for pure opportunity filtering.

use super::config::CrossExchangeArbitrageConfig;
use super::funding::FundingEstimate;
use super::simulation::TakerVwapResult;
use crate::market::{OrderBook5, RouteStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGateDecision {
    pub can_open: bool,
    pub reject_reasons: Vec<RejectReason>,
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

pub fn book_age_ms(book: &OrderBook5, now: DateTime<Utc>) -> i64 {
    now.signed_duration_since(book.recv_ts).num_milliseconds()
}
