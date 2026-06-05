use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderBookSnapshot, OrderSide, SymbolRule};
use crate::execution::{FeeLookupKey, FeeModel, FeeRole, FeeSource};
use crate::strategies::arbitrage_core::{
    executable_vwap, maker_buy_taker_sell, taker_buy_maker_sell, ExecutablePriceRequest,
    MakerTakerExpectedValueModel,
};

use crate::scanner::{
    build_pair_coverage, build_symbol_coverage, default_exchange_roles, normalize_exchange_name,
    role_for, ExchangeOperationalRole, ExchangePairCoverage, SymbolCoverageRecord,
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

pub fn scan_five_exchange_spot(
    config: &FiveExchangeSpotScannerConfig,
    rules: &[SymbolRule],
    books: &[OrderBookSnapshot],
    fee_model: &FeeModel,
) -> FiveExchangeScannerReadModel {
    let coverage = build_symbol_coverage(rules, &config.exchanges);
    let pair_coverage = build_pair_coverage(&coverage);
    let rules_by_key = rules
        .iter()
        .map(|rule| {
            (
                (
                    normalize_exchange_name(&rule.exchange),
                    rule.internal_symbol.clone(),
                ),
                rule,
            )
        })
        .collect::<HashMap<_, _>>();
    let books_by_key = books
        .iter()
        .map(|book| {
            (
                (normalize_exchange_name(&book.exchange), book.symbol.clone()),
                book,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut opportunities = Vec::new();
    for record in &coverage {
        if record.scan_eligible_exchanges.len() < config.minimum_exchange_coverage {
            continue;
        }
        for buy in &record.scan_eligible_exchanges {
            for sell in &record.scan_eligible_exchanges {
                if buy == sell {
                    continue;
                }
                for target_notional in &config.notionals_usdt {
                    opportunities.push(evaluate_directed_pair(
                        config,
                        record,
                        buy,
                        sell,
                        *target_notional,
                        &rules_by_key,
                        &books_by_key,
                        fee_model,
                    ));
                }
            }
        }
    }
    let pair_statistics = build_pair_statistics(&opportunities, &config.statistics);
    let symbol_scores = score_symbols(&pair_statistics);
    let recommendations = symbol_scores
        .iter()
        .filter(|score| {
            matches!(
                score.recommended_action,
                SymbolCandidateRecommendedAction::ConsiderLiveDryRun
                    | SymbolCandidateRecommendedAction::Observe
            )
        })
        .cloned()
        .collect();
    FiveExchangeScannerReadModel {
        exchange_roles: config.exchanges.clone(),
        symbol_coverage: coverage,
        exchange_pair_coverage: pair_coverage,
        opportunities,
        pair_statistics,
        symbol_scores,
        recommendations,
    }
}

fn evaluate_directed_pair(
    config: &FiveExchangeSpotScannerConfig,
    record: &SymbolCoverageRecord,
    buy: &str,
    sell: &str,
    target_notional: f64,
    rules: &HashMap<(String, String), &SymbolRule>,
    books: &HashMap<(String, String), &OrderBookSnapshot>,
    fee_model: &FeeModel,
) -> FiveExchangeSpotOpportunity {
    let now = Utc::now();
    let symbol = &record.internal_symbol;
    let buy_role = role_for(&config.exchanges, buy);
    let sell_role = role_for(&config.exchanges, sell);
    let mut warnings = Vec::new();
    let mut rejections = Vec::new();
    let mut book_ages = BTreeMap::new();

    let buy_rule = rules.get(&(buy.to_string(), symbol.clone()));
    let sell_rule = rules.get(&(sell.to_string(), symbol.clone()));
    let buy_book = books.get(&(buy.to_string(), symbol.clone()));
    let sell_book = books.get(&(sell.to_string(), symbol.clone()));

    let buy_eval = if let (Some(rule), Some(book)) = (buy_rule, buy_book) {
        book_ages.insert(buy.to_string(), book_age_ms(book));
        executable_vwap(
            book,
            rule,
            ExecutablePriceRequest {
                side: OrderSide::Buy,
                target_quantity: None,
                target_notional: Some(target_notional),
                stale_book_ms: config.max_book_age_ms,
                quantity_rounding_tolerance: 1e-3,
            },
        )
    } else {
        rejections.push(format!("missing buy rule or book for {buy} {symbol}"));
        crate::strategies::arbitrage_core::ExecutablePriceResult::rejected(
            OrderSide::Buy,
            "missing buy rule or book",
        )
    };
    let target_quantity = buy_eval.executable_quantity;
    let sell_eval = if let (Some(rule), Some(book)) = (sell_rule, sell_book) {
        book_ages.insert(sell.to_string(), book_age_ms(book));
        executable_vwap(
            book,
            rule,
            ExecutablePriceRequest {
                side: OrderSide::Sell,
                target_quantity: Some(target_quantity),
                target_notional: None,
                stale_book_ms: config.max_book_age_ms,
                quantity_rounding_tolerance: 1e-3,
            },
        )
    } else {
        rejections.push(format!("missing sell rule or book for {sell} {symbol}"));
        crate::strategies::arbitrage_core::ExecutablePriceResult::rejected(
            OrderSide::Sell,
            "missing sell rule or book",
        )
    };
    if !buy_eval.full_fill_possible {
        rejections.push(
            buy_eval
                .rejection_reason_optional
                .clone()
                .unwrap_or_else(|| "buy leg insufficient".to_string()),
        );
    }
    if !sell_eval.full_fill_possible {
        rejections.push(
            sell_eval
                .rejection_reason_optional
                .clone()
                .unwrap_or_else(|| "sell leg insufficient".to_string()),
        );
    }

    let buy_vwap = buy_eval.vwap.unwrap_or(0.0);
    let sell_vwap = sell_eval.vwap.unwrap_or(0.0);
    let executable_quantity = buy_eval
        .executable_quantity
        .min(sell_eval.executable_quantity);
    let raw_spread_bps = if buy_vwap > 0.0 {
        (sell_vwap - buy_vwap) / buy_vwap * 10_000.0
    } else {
        0.0
    };
    let buy_fee = fee_model.calculate_fee(
        &FeeLookupKey {
            exchange: buy.to_string(),
            market_type: MarketType::Spot,
            symbol: Some(symbol.clone()),
            liquidity_role: FeeRole::Taker,
        },
        buy_eval.executable_notional,
    );
    let sell_fee = fee_model.calculate_fee(
        &FeeLookupKey {
            exchange: sell.to_string(),
            market_type: MarketType::Spot,
            symbol: Some(symbol.clone()),
            liquidity_role: FeeRole::Taker,
        },
        sell_eval.executable_notional,
    );
    let total_taker_fee_bps = buy_fee.effective_fee_bps + sell_fee.effective_fee_bps;
    let estimated_rebalance_cost_bps = 2.0;
    let expected_residual_loss_bps = 2.0;
    let tt_net_bps = raw_spread_bps
        - total_taker_fee_bps
        - estimated_rebalance_cost_bps
        - expected_residual_loss_bps;
    let tt_net_pnl = buy_eval.executable_notional * tt_net_bps / 10_000.0;

    let ev = MakerTakerExpectedValueModel::default();
    let mbts = maker_buy_taker_sell(
        buy_book.and_then(|book| book.best_bid).unwrap_or(buy_vwap),
        &sell_eval,
        fee_model
            .lookup(&FeeLookupKey {
                exchange: buy.to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.clone()),
                liquidity_role: FeeRole::Maker,
            })
            .fee_bps,
        sell_fee.effective_fee_bps,
        &ev,
    );
    let tbms = taker_buy_maker_sell(
        &buy_eval,
        sell_book
            .and_then(|book| book.best_ask)
            .unwrap_or(sell_vwap),
        fee_model
            .lookup(&FeeLookupKey {
                exchange: sell.to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.clone()),
                liquidity_role: FeeRole::Maker,
            })
            .fee_bps,
        buy_fee.effective_fee_bps,
        &ev,
    );
    let mut fee_sources = BTreeMap::new();
    fee_sources.insert(buy.to_string(), buy_fee.source);
    fee_sources.insert(sell.to_string(), sell_fee.source);
    if matches!(buy_fee.source, FeeSource::Fallback)
        || matches!(sell_fee.source, FeeSource::Fallback)
    {
        warnings.push("fallback fee used; confidence reduced".to_string());
    }
    let eligibility = execution_eligibility(buy, sell, buy_role, sell_role);
    FiveExchangeSpotOpportunity {
        opportunity_id: format!(
            "fxs-{}-{}-{}-{}-{}",
            symbol,
            buy,
            sell,
            target_notional,
            now.timestamp_millis()
        ),
        observed_at: now,
        symbol: symbol.clone(),
        buy_exchange: buy.to_string(),
        sell_exchange: sell.to_string(),
        buy_exchange_role: buy_role,
        sell_exchange_role: sell_role,
        target_notional,
        executable_quantity,
        buy_vwap,
        sell_vwap,
        raw_spread_bps,
        total_taker_fee_bps,
        tt_net_bps,
        tt_net_pnl,
        maker_buy_taker_sell_theoretical_bps: mbts.theoretical_net_bps,
        maker_buy_taker_sell_expected_bps: mbts.expected_net_bps,
        taker_buy_maker_sell_theoretical_bps: tbms.theoretical_net_bps,
        taker_buy_maker_sell_expected_bps: tbms.expected_net_bps,
        estimated_rebalance_cost_bps,
        expected_residual_loss_bps,
        capital_required: target_notional,
        expected_return_on_capital: if target_notional > 0.0 {
            tt_net_pnl / target_notional
        } else {
            0.0
        },
        book_ages,
        fee_sources,
        confidence: if rejections.is_empty() && warnings.is_empty() {
            ConfidenceLevel::Medium
        } else {
            ConfidenceLevel::Low
        },
        execution_eligibility: eligibility,
        warnings,
        rejection_reasons: rejections,
    }
}

fn execution_eligibility(
    buy: &str,
    sell: &str,
    buy_role: ExchangeOperationalRole,
    sell_role: ExchangeOperationalRole,
) -> OpportunityExecutionEligibility {
    let mut blockers = Vec::new();
    let live_dry = buy_role.live_dry_run_eligible()
        && sell_role.live_dry_run_eligible()
        && ((buy == "gateio" && sell == "bitget") || (buy == "bitget" && sell == "gateio"));
    if !live_dry {
        blockers.push("only Gate.io <-> Bitget FutureLiveExecutionCandidate pairs may be live_dry_run eligible".to_string());
    }
    blockers.push("live execution remains disabled for this task".to_string());
    OpportunityExecutionEligibility {
        analytical_only: true,
        live_dry_run_eligible: live_dry,
        future_live_candidate: live_dry,
        currently_live_executable: false,
        blockers,
    }
}

fn build_pair_statistics(
    opportunities: &[FiveExchangeSpotOpportunity],
    config: &ScannerStatisticsConfig,
) -> Vec<ExchangePairStatistics> {
    let mut grouped: BTreeMap<
        (String, String, Option<String>, String),
        Vec<&FiveExchangeSpotOpportunity>,
    > = BTreeMap::new();
    for opportunity in opportunities {
        grouped
            .entry((
                opportunity.buy_exchange.clone(),
                opportunity.sell_exchange.clone(),
                Some(opportunity.symbol.clone()),
                format!("{:.8}", opportunity.target_notional),
            ))
            .or_default()
            .push(opportunity);
        grouped
            .entry((
                opportunity.buy_exchange.clone(),
                opportunity.sell_exchange.clone(),
                None,
                format!("{:.8}", opportunity.target_notional),
            ))
            .or_default()
            .push(opportunity);
    }
    grouped
        .into_iter()
        .map(|((buy, sell, symbol, notional), values)| {
            statistics_for_group(
                buy,
                sell,
                symbol,
                notional.parse().unwrap_or(0.0),
                values,
                config,
            )
        })
        .collect()
}

fn statistics_for_group(
    buy: String,
    sell: String,
    symbol: Option<String>,
    notional: f64,
    values: Vec<&FiveExchangeSpotOpportunity>,
    config: &ScannerStatisticsConfig,
) -> ExchangePairStatistics {
    let observation_count = values.len() as u64;
    let tt_positive_count = values.iter().filter(|item| item.tt_net_bps > 0.0).count() as u64;
    let mut nets = values
        .iter()
        .map(|item| item.tt_net_bps)
        .collect::<Vec<_>>();
    nets.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let avg = |items: Vec<f64>| {
        if items.is_empty() {
            0.0
        } else {
            items.iter().sum::<f64>() / items.len() as f64
        }
    };
    let confidence = if observation_count >= config.minimum_samples_for_high_confidence {
        ConfidenceLevel::High
    } else if observation_count >= config.minimum_samples_for_medium_confidence {
        ConfidenceLevel::Medium
    } else {
        ConfidenceLevel::Low
    };
    ExchangePairStatistics {
        buy_exchange: buy,
        sell_exchange: sell,
        symbol_optional: symbol,
        target_notional: notional,
        observation_count,
        fresh_book_observation_count: values
            .iter()
            .filter(|item| item.rejection_reasons.is_empty())
            .count() as u64,
        tt_positive_count,
        tt_positive_rate: if observation_count > 0 {
            tt_positive_count as f64 / observation_count as f64
        } else {
            0.0
        },
        average_raw_spread_bps: avg(values.iter().map(|item| item.raw_spread_bps).collect()),
        average_tt_net_bps: avg(values.iter().map(|item| item.tt_net_bps).collect()),
        median_tt_net_bps: percentile(&nets, 0.5),
        p90_tt_net_bps: percentile(&nets, 0.9),
        maximum_tt_net_bps: nets.last().copied().unwrap_or(0.0),
        average_positive_duration_ms: 0.0,
        maximum_positive_duration_ms: 0,
        average_executable_notional: avg(values
            .iter()
            .map(|item| item.buy_vwap * item.executable_quantity)
            .collect()),
        insufficient_depth_count: values
            .iter()
            .filter(|item| {
                item.rejection_reasons
                    .iter()
                    .any(|reason| reason.contains("insufficient"))
            })
            .count() as u64,
        stale_book_count: values
            .iter()
            .filter(|item| {
                item.rejection_reasons
                    .iter()
                    .any(|reason| reason.contains("book age"))
            })
            .count() as u64,
        fee_fallback_count: values
            .iter()
            .filter(|item| {
                item.fee_sources
                    .values()
                    .any(|source| *source == FeeSource::Fallback)
            })
            .count() as u64,
        maker_taker_theoretical_positive_count: values
            .iter()
            .filter(|item| {
                item.maker_buy_taker_sell_theoretical_bps > 0.0
                    || item.taker_buy_maker_sell_theoretical_bps > 0.0
            })
            .count() as u64,
        maker_taker_expected_positive_count: values
            .iter()
            .filter(|item| {
                item.maker_buy_taker_sell_expected_bps > 0.0
                    || item.taker_buy_maker_sell_expected_bps > 0.0
            })
            .count() as u64,
        estimated_daily_opportunity_count: tt_positive_count as f64,
        estimated_daily_net_pnl_by_notional: BTreeMap::from([(
            format!("{notional:.2}"),
            values.iter().map(|item| item.tt_net_pnl.max(0.0)).sum(),
        )]),
        confidence,
    }
}

pub fn score_symbols(statistics: &[ExchangePairStatistics]) -> Vec<SpotSymbolCandidateScore> {
    statistics
        .iter()
        .filter_map(|stats| {
            let symbol = stats.symbol_optional.clone()?;
            let tt_profitability_score = clamp_score(stats.average_tt_net_bps / 10.0);
            let maker_taker_expected_score = clamp_score(
                stats.maker_taker_expected_positive_count as f64
                    / stats.observation_count.max(1) as f64
                    * 100.0,
            );
            let opportunity_frequency_score = clamp_score(stats.tt_positive_rate * 100.0);
            let executable_depth_score = clamp_score(
                (1.0 - stats.insufficient_depth_count as f64
                    / stats.observation_count.max(1) as f64)
                    * 100.0,
            );
            let fee_confidence_score = clamp_score(
                (1.0 - stats.fee_fallback_count as f64 / stats.observation_count.max(1) as f64)
                    * 100.0,
            );
            let book_health_score = clamp_score(
                (1.0 - stats.stale_book_count as f64 / stats.observation_count.max(1) as f64)
                    * 100.0,
            );
            let sample_penalty = match stats.confidence {
                ConfidenceLevel::Low => 0.4,
                ConfidenceLevel::Medium => 0.8,
                ConfidenceLevel::High => 1.0,
            };
            let total_score = (tt_profitability_score * 0.2
                + maker_taker_expected_score * 0.1
                + opportunity_frequency_score * 0.15
                + executable_depth_score * 0.15
                + fee_confidence_score * 0.1
                + book_health_score * 0.1
                + 50.0 * 0.2)
                * sample_penalty;
            let recommended_action = if stats.confidence == ConfidenceLevel::Low {
                SymbolCandidateRecommendedAction::InsufficientData
            } else if total_score >= 75.0 {
                SymbolCandidateRecommendedAction::ConsiderLiveDryRun
            } else if total_score >= 45.0 {
                SymbolCandidateRecommendedAction::Observe
            } else {
                SymbolCandidateRecommendedAction::Avoid
            };
            Some(SpotSymbolCandidateScore {
                symbol,
                exchange_pair: format!("{}->{}", stats.buy_exchange, stats.sell_exchange),
                target_notional: stats.target_notional,
                tt_profitability_score,
                maker_taker_expected_score,
                opportunity_frequency_score,
                opportunity_duration_score: 0.0,
                executable_depth_score,
                fee_confidence_score,
                book_health_score,
                inventory_efficiency_score: 50.0,
                rebalance_cost_score: 50.0,
                residual_risk_score: 50.0,
                total_score,
                confidence: stats.confidence,
                warnings: (stats.confidence == ConfidenceLevel::Low)
                    .then(|| "sample count too low for profitability claims".to_string())
                    .into_iter()
                    .collect(),
                recommended_action,
            })
        })
        .collect()
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn clamp_score(value: f64) -> f64 {
    value.clamp(0.0, 100.0)
}

fn book_age_ms(book: &OrderBookSnapshot) -> i64 {
    Utc::now()
        .signed_duration_since(book.received_at)
        .num_milliseconds()
        .max(0)
}
