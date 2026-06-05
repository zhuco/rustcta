use serde::{Deserialize, Serialize};

use super::relationship::ConfidenceLevel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskScoreComponents {
    pub expected_return_on_capital_per_hour_score: f64,
    pub executable_depth_score: f64,
    pub book_freshness_score: f64,
    pub fee_confidence_score: f64,
    pub convergence_confidence_score: f64,
    pub funding_confidence_score: f64,
    pub residual_risk_penalty: f64,
    pub adverse_selection_penalty: f64,
    pub capital_fragmentation_penalty: f64,
    pub stale_book_penalty: f64,
    pub fee_fallback_penalty: f64,
    pub total_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskScoreInput {
    pub expected_return_on_capital_per_hour: f64,
    pub full_depth_available: bool,
    pub max_book_age_ms: i64,
    pub fee_fallback_used: bool,
    pub convergence_confidence: ConfidenceLevel,
    pub funding_confidence: ConfidenceLevel,
    pub residual_risk_bps: f64,
    pub adverse_selection_bps: f64,
    pub capital_fragmentation_penalty_usdt: f64,
    pub stale_book: bool,
}

pub fn score_opportunity(input: &RiskScoreInput) -> RiskScoreComponents {
    let expected_return_on_capital_per_hour_score =
        (input.expected_return_on_capital_per_hour * 10_000.0).clamp(-100.0, 100.0);
    let executable_depth_score = if input.full_depth_available {
        15.0
    } else {
        -30.0
    };
    let book_freshness_score = if input.max_book_age_ms <= 250 {
        15.0
    } else if input.max_book_age_ms <= 1_000 {
        8.0
    } else {
        -25.0
    };
    let fee_confidence_score = if input.fee_fallback_used { -10.0 } else { 10.0 };
    let convergence_confidence_score = confidence_points(input.convergence_confidence);
    let funding_confidence_score = confidence_points(input.funding_confidence);
    let residual_risk_penalty = input.residual_risk_bps.max(0.0).min(50.0);
    let adverse_selection_penalty = input.adverse_selection_bps.max(0.0).min(50.0);
    let capital_fragmentation_penalty = (input.capital_fragmentation_penalty_usdt / 10.0).min(25.0);
    let stale_book_penalty = if input.stale_book { 50.0 } else { 0.0 };
    let fee_fallback_penalty = if input.fee_fallback_used { 15.0 } else { 0.0 };
    let total_score = expected_return_on_capital_per_hour_score
        + executable_depth_score
        + book_freshness_score
        + fee_confidence_score
        + convergence_confidence_score
        + funding_confidence_score
        - residual_risk_penalty
        - adverse_selection_penalty
        - capital_fragmentation_penalty
        - stale_book_penalty
        - fee_fallback_penalty;
    RiskScoreComponents {
        expected_return_on_capital_per_hour_score,
        executable_depth_score,
        book_freshness_score,
        fee_confidence_score,
        convergence_confidence_score,
        funding_confidence_score,
        residual_risk_penalty,
        adverse_selection_penalty,
        capital_fragmentation_penalty,
        stale_book_penalty,
        fee_fallback_penalty,
        total_score,
    }
}

fn confidence_points(confidence: ConfidenceLevel) -> f64 {
    match confidence {
        ConfidenceLevel::Low => 0.0,
        ConfidenceLevel::Medium => 5.0,
        ConfidenceLevel::High => 10.0,
    }
}
