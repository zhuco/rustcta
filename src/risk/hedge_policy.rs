use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgePolicyMode {
    NoHedge,
    ManualFixedRatio,
    DynamicRegimeRecommendation,
    EmergencyHedgeRecommendation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    StrongUptrend,
    ModerateUptrend,
    Neutral,
    HighVolatility,
    WeakeningTrend,
    Downtrend,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgeConfidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpotInventoryRiskSnapshot {
    pub symbol: String,
    pub total_managed_spot_quantity: f64,
    pub inventory_value_usdt: f64,
    pub inventory_cost_basis_optional: Option<f64>,
    pub unrealized_pnl_optional: Option<f64>,
    pub exchange_distribution: BTreeMap<String, f64>,
    pub concentration_score: f64,
    pub volatility_estimate: f64,
    pub drawdown_estimate: f64,
    pub beta_to_btc_optional: Option<f64>,
    pub hedge_instrument_available: bool,
    pub hedge_market_depth_optional: Option<f64>,
    pub funding_rate_optional: Option<f64>,
    pub basis_bps_optional: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HedgeRecommendation {
    pub symbol: String,
    pub current_policy: HedgePolicyMode,
    pub recommended_policy: HedgePolicyMode,
    pub recommended_hedge_ratio: f64,
    pub recommended_hedge_exchange_optional: Option<String>,
    pub recommended_hedge_instrument_optional: Option<String>,
    pub estimated_hedge_fee_bps: f64,
    pub estimated_funding_cost_bps: f64,
    pub estimated_basis_risk_bps: f64,
    pub estimated_margin_required: f64,
    pub expected_risk_reduction: f64,
    pub confidence: HedgeConfidence,
    pub reasons: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HedgeVenueCapability {
    pub exchange: String,
    pub supports_perpetual: bool,
    pub supports_unified_account: bool,
    pub supports_spot_as_collateral: bool,
    pub supports_cross_margin: bool,
    pub supports_isolated_margin: bool,
    pub supports_portfolio_margin: bool,
    pub supports_internal_transfer: bool,
    pub collateral_haircuts_known: bool,
    pub funding_data_available: bool,
    pub mark_price_available: bool,
    pub index_price_available: bool,
    pub supported_hedge_symbols: Vec<String>,
    pub account_structure: String,
    pub collateral_rules: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketRegimeSnapshot {
    pub regime: MarketRegime,
    pub confidence: HedgeConfidence,
    pub btc_trend_score: Option<f64>,
    pub drawdown_score: Option<f64>,
    pub realized_volatility_score: Option<f64>,
    pub funding_score: Option<f64>,
    pub breadth_score: Option<f64>,
    pub inventory_concentration_score: Option<f64>,
    pub reasons: Vec<String>,
    pub missing_inputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HedgePolicyReadModel {
    pub enabled: bool,
    pub recommendation_only: bool,
    pub inventory_risk: Vec<SpotInventoryRiskSnapshot>,
    pub recommendations: Vec<HedgeRecommendation>,
    pub venue_capabilities: Vec<HedgeVenueCapability>,
    pub market_regime: Option<MarketRegimeSnapshot>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketRegimeInputs {
    pub btc_above_fast_ma: Option<bool>,
    pub btc_above_slow_ma: Option<bool>,
    pub drawdown_bps: Option<f64>,
    pub realized_volatility_bps: Option<f64>,
    pub funding_rate_bps: Option<f64>,
    pub inventory_concentration_score: Option<f64>,
}

pub fn classify_market_regime(inputs: &MarketRegimeInputs) -> MarketRegimeSnapshot {
    let mut missing = Vec::new();
    let fast = inputs.btc_above_fast_ma.or_else(|| {
        missing.push("btc_above_fast_ma".to_string());
        None
    });
    let slow = inputs.btc_above_slow_ma.or_else(|| {
        missing.push("btc_above_slow_ma".to_string());
        None
    });
    let vol = inputs.realized_volatility_bps.or_else(|| {
        missing.push("realized_volatility_bps".to_string());
        None
    });
    let drawdown = inputs.drawdown_bps.or_else(|| {
        missing.push("drawdown_bps".to_string());
        None
    });

    let regime = match (fast, slow, vol, drawdown) {
        (Some(true), Some(true), Some(v), Some(d)) if v < 250.0 && d < 500.0 => {
            MarketRegime::StrongUptrend
        }
        (Some(true), Some(true), _, _) => MarketRegime::ModerateUptrend,
        (_, _, Some(v), _) if v >= 600.0 => MarketRegime::HighVolatility,
        (Some(false), Some(true), _, _) => MarketRegime::WeakeningTrend,
        (Some(false), Some(false), _, Some(d)) if d >= 500.0 => MarketRegime::Downtrend,
        (Some(_), Some(_), Some(_), Some(_)) => MarketRegime::Neutral,
        _ => MarketRegime::Unknown,
    };
    let confidence = if missing.is_empty() {
        HedgeConfidence::Medium
    } else {
        HedgeConfidence::Low
    };
    MarketRegimeSnapshot {
        regime,
        confidence,
        btc_trend_score: fast.zip(slow).map(|(f, s)| match (f, s) {
            (true, true) => 100.0,
            (true, false) => 60.0,
            (false, true) => 40.0,
            (false, false) => 0.0,
        }),
        drawdown_score: drawdown,
        realized_volatility_score: vol,
        funding_score: inputs.funding_rate_bps,
        breadth_score: None,
        inventory_concentration_score: inputs.inventory_concentration_score,
        reasons: vec!["explainable rule-based classification; not an execution signal".to_string()],
        missing_inputs: missing,
    }
}

pub fn recommend_hedge(
    risk: &SpotInventoryRiskSnapshot,
    current_policy: HedgePolicyMode,
    regime: &MarketRegimeSnapshot,
    venues: &[HedgeVenueCapability],
) -> HedgeRecommendation {
    let mut reasons = Vec::new();
    let mut blockers = vec!["hedge execution is disabled; recommendation only".to_string()];
    let venue = venues.iter().find(|venue| {
        venue.supports_perpetual
            && venue.funding_data_available
            && venue.mark_price_available
            && venue.index_price_available
    });
    if venue.is_none() {
        blockers.push("no fully verified hedge venue capability".to_string());
    }
    let ratio = match regime.regime {
        MarketRegime::StrongUptrend => {
            reasons.push(
                "strong uptrend favors no or low hedge subject to inventory limits".to_string(),
            );
            if risk.concentration_score > 75.0 {
                0.25
            } else {
                0.0
            }
        }
        MarketRegime::ModerateUptrend => {
            reasons.push(
                "moderate uptrend uses partial hedge only for elevated concentration".to_string(),
            );
            if risk.concentration_score > 60.0 {
                0.25
            } else {
                0.0
            }
        }
        MarketRegime::HighVolatility | MarketRegime::WeakeningTrend => {
            reasons.push(
                "volatility or weakening trend supports partial hedge consideration".to_string(),
            );
            0.5
        }
        MarketRegime::Downtrend => {
            reasons.push(
                "downtrend supports strong hedge consideration or inventory reduction".to_string(),
            );
            0.75
        }
        MarketRegime::Neutral | MarketRegime::Unknown => {
            reasons.push("neutral or unknown regime keeps default no-hedge policy".to_string());
            0.0
        }
    };
    HedgeRecommendation {
        symbol: risk.symbol.clone(),
        current_policy,
        recommended_policy: if ratio > 0.0 {
            HedgePolicyMode::DynamicRegimeRecommendation
        } else {
            HedgePolicyMode::NoHedge
        },
        recommended_hedge_ratio: ratio,
        recommended_hedge_exchange_optional: venue.map(|venue| venue.exchange.clone()),
        recommended_hedge_instrument_optional: venue
            .and_then(|venue| venue.supported_hedge_symbols.first().cloned()),
        estimated_hedge_fee_bps: 5.0,
        estimated_funding_cost_bps: risk.funding_rate_optional.unwrap_or(0.0).abs(),
        estimated_basis_risk_bps: risk.basis_bps_optional.unwrap_or(0.0).abs(),
        estimated_margin_required: risk.inventory_value_usdt * ratio,
        expected_risk_reduction: ratio * 0.7,
        confidence: if regime.regime == MarketRegime::Unknown {
            HedgeConfidence::Low
        } else {
            regime.confidence
        },
        reasons,
        blockers,
    }
}
