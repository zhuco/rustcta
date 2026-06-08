use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    default_exchange_roles, ExchangeOperationalRole, ExchangePairCoverage, FeeSource,
    SymbolCoverageRecord,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiveExchangeSpotScannerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_roles")]
    pub exchanges: BTreeMap<String, ExchangeOperationalRole>,
    #[serde(default = "default_notionals")]
    pub notionals_usdt: Vec<f64>,
    #[serde(default = "default_min_coverage")]
    pub minimum_exchange_coverage: usize,
    #[serde(default = "default_true")]
    pub require_fresh_books: bool,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default)]
    pub execution_analysis: ExecutionAnalysisConfig,
    #[serde(default)]
    pub statistics: ScannerStatisticsConfig,
    #[serde(default)]
    pub hedge_policy: ScannerHedgePolicyConfig,
    #[serde(default)]
    pub regime: ScannerRegimeConfig,
}

impl Default for FiveExchangeSpotScannerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            exchanges: default_roles(),
            notionals_usdt: default_notionals(),
            minimum_exchange_coverage: 2,
            require_fresh_books: true,
            max_book_age_ms: 1_000,
            execution_analysis: ExecutionAnalysisConfig::default(),
            statistics: ScannerStatisticsConfig::default(),
            hedge_policy: ScannerHedgePolicyConfig::default(),
            regime: ScannerRegimeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionAnalysisConfig {
    #[serde(default = "default_true")]
    pub taker_taker: bool,
    #[serde(default = "default_true")]
    pub maker_taker_expected_value: bool,
    #[serde(default)]
    pub maker_taker_execution: bool,
    #[serde(default)]
    pub live_execution: bool,
}

impl Default for ExecutionAnalysisConfig {
    fn default() -> Self {
        Self {
            taker_taker: true,
            maker_taker_expected_value: true,
            maker_taker_execution: false,
            live_execution: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerStatisticsConfig {
    #[serde(default = "default_medium_samples")]
    pub minimum_samples_for_medium_confidence: u64,
    #[serde(default = "default_high_samples")]
    pub minimum_samples_for_high_confidence: u64,
}

impl Default for ScannerStatisticsConfig {
    fn default() -> Self {
        Self {
            minimum_samples_for_medium_confidence: 1_000,
            minimum_samples_for_high_confidence: 10_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerHedgePolicyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub recommendation_only: bool,
    #[serde(default = "default_no_hedge")]
    pub default_policy: String,
    #[serde(default)]
    pub allow_real_hedge_execution: bool,
}

impl Default for ScannerHedgePolicyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            recommendation_only: true,
            default_policy: "no_hedge".to_string(),
            allow_real_hedge_execution: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerRegimeConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub use_explainable_rules_only: bool,
}

impl Default for ScannerRegimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            use_explainable_rules_only: true,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_roles() -> BTreeMap<String, ExchangeOperationalRole> {
    default_exchange_roles()
}
fn default_notionals() -> Vec<f64> {
    vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0]
}
fn default_min_coverage() -> usize {
    2
}
fn default_max_book_age_ms() -> u64 {
    1_000
}
fn default_medium_samples() -> u64 {
    1_000
}
fn default_high_samples() -> u64 {
    10_000
}
fn default_no_hedge() -> String {
    "no_hedge".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityExecutionEligibility {
    pub analytical_only: bool,
    pub live_dry_run_eligible: bool,
    pub future_live_candidate: bool,
    pub currently_live_executable: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FiveExchangeSpotOpportunity {
    pub opportunity_id: String,
    pub observed_at: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_exchange_role: ExchangeOperationalRole,
    pub sell_exchange_role: ExchangeOperationalRole,
    pub target_notional: f64,
    pub executable_quantity: f64,
    pub buy_vwap: f64,
    pub sell_vwap: f64,
    pub raw_spread_bps: f64,
    pub total_taker_fee_bps: f64,
    pub tt_net_bps: f64,
    pub tt_net_pnl: f64,
    pub maker_buy_taker_sell_theoretical_bps: f64,
    pub maker_buy_taker_sell_expected_bps: f64,
    pub taker_buy_maker_sell_theoretical_bps: f64,
    pub taker_buy_maker_sell_expected_bps: f64,
    pub estimated_rebalance_cost_bps: f64,
    pub expected_residual_loss_bps: f64,
    pub capital_required: f64,
    pub expected_return_on_capital: f64,
    pub book_ages: BTreeMap<String, i64>,
    pub fee_sources: BTreeMap<String, FeeSource>,
    pub confidence: ConfidenceLevel,
    pub execution_eligibility: OpportunityExecutionEligibility,
    pub warnings: Vec<String>,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangePairStatistics {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub symbol_optional: Option<String>,
    pub target_notional: f64,
    pub observation_count: u64,
    pub fresh_book_observation_count: u64,
    pub tt_positive_count: u64,
    pub tt_positive_rate: f64,
    pub average_raw_spread_bps: f64,
    pub average_tt_net_bps: f64,
    pub median_tt_net_bps: f64,
    pub p90_tt_net_bps: f64,
    pub maximum_tt_net_bps: f64,
    pub average_positive_duration_ms: f64,
    pub maximum_positive_duration_ms: u64,
    pub average_executable_notional: f64,
    pub insufficient_depth_count: u64,
    pub stale_book_count: u64,
    pub fee_fallback_count: u64,
    pub maker_taker_theoretical_positive_count: u64,
    pub maker_taker_expected_positive_count: u64,
    pub estimated_daily_opportunity_count: f64,
    pub estimated_daily_net_pnl_by_notional: BTreeMap<String, f64>,
    pub confidence: ConfidenceLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SymbolCandidateRecommendedAction {
    Observe,
    ConsiderLiveDryRun,
    Avoid,
    InsufficientData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpotSymbolCandidateScore {
    pub symbol: String,
    pub exchange_pair: String,
    pub target_notional: f64,
    pub tt_profitability_score: f64,
    pub maker_taker_expected_score: f64,
    pub opportunity_frequency_score: f64,
    pub opportunity_duration_score: f64,
    pub executable_depth_score: f64,
    pub fee_confidence_score: f64,
    pub book_health_score: f64,
    pub inventory_efficiency_score: f64,
    pub rebalance_cost_score: f64,
    pub residual_risk_score: f64,
    pub total_score: f64,
    pub confidence: ConfidenceLevel,
    pub warnings: Vec<String>,
    pub recommended_action: SymbolCandidateRecommendedAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FiveExchangeScannerReadModel {
    pub exchange_roles: BTreeMap<String, ExchangeOperationalRole>,
    pub symbol_coverage: Vec<SymbolCoverageRecord>,
    pub exchange_pair_coverage: Vec<ExchangePairCoverage>,
    pub opportunities: Vec<FiveExchangeSpotOpportunity>,
    pub pair_statistics: Vec<ExchangePairStatistics>,
    pub symbol_scores: Vec<SpotSymbolCandidateScore>,
    pub recommendations: Vec<SpotSymbolCandidateScore>,
}
