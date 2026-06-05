use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderSide};
use crate::execution::FeeSource;

use super::capital_model::{
    calculate_capital_requirement, expected_return_on_capital, expected_return_on_capital_per_hour,
    AccountStructure, CapitalModelInput, ExchangeCapitalCapabilities,
};
use super::executable_price::{enforce_same_quantity, executable_vwap, ExecutablePriceRequest};
use super::lifecycle_model::{analyze_lifecycle, analyze_taker_taker_entry, LifecycleModelConfig};
use super::maker_taker_model::{
    maker_buy_taker_sell, taker_buy_maker_sell, MakerTakerExpectedValueModel,
};
use super::opportunity::ArbitrageOpportunityAnalysis;
use super::relationship::{
    ArbitrageRelationship, ArbitrageRelationshipType, ConfidenceLevel, ExecutionModeCandidate,
};
use super::scoring::{score_opportunity, RiskScoreInput};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageScannerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub relationships: RelationshipScanToggles,
    #[serde(default)]
    pub execution_modes: ExecutionModeToggles,
    #[serde(default = "default_notionals")]
    pub notionals_usdt: Vec<f64>,
    #[serde(default)]
    pub minimums: ScannerMinimums,
    #[serde(default)]
    pub lifecycle: LifecycleModelConfig,
    #[serde(default)]
    pub maker_taker: MakerTakerExpectedValueModel,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_quantity_tolerance")]
    pub quantity_rounding_tolerance: f64,
    #[serde(default)]
    pub output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipScanToggles {
    pub spot_spot: bool,
    pub spot_perp: bool,
    pub perp_perp: bool,
}

impl Default for RelationshipScanToggles {
    fn default() -> Self {
        Self {
            spot_spot: true,
            spot_perp: true,
            perp_perp: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionModeToggles {
    pub taker_taker: bool,
    pub maker_taker_analysis: bool,
    pub maker_taker_execution: bool,
}

impl Default for ExecutionModeToggles {
    fn default() -> Self {
        Self {
            taker_taker: true,
            maker_taker_analysis: true,
            maker_taker_execution: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScannerMinimums {
    pub tt_immediate_net_bps: f64,
    pub lifecycle_expected_net_bps: f64,
    pub return_on_capital_per_hour_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbitrageStatisticsSnapshot {
    pub spread_duration_samples: usize,
    pub basis_duration_samples: usize,
    pub spread_mean_bps: Option<f64>,
    pub spread_std_bps: Option<f64>,
    pub spread_percentile_bps: Option<f64>,
    pub maximum_adverse_spread_expansion_bps: Option<f64>,
    pub time_to_convergence_seconds: Option<f64>,
    pub convergence_probability: Option<f64>,
    pub maker_fill_probability: Option<f64>,
    pub maker_time_to_fill_ms: Option<f64>,
    pub spread_after_maker_fill_bps: Option<f64>,
    pub hedge_slippage_after_maker_fill_bps: Option<f64>,
    pub residual_occurrence_rate: Option<f64>,
    pub residual_loss_bps_p95: Option<f64>,
    pub funding_rate_mean_bps: Option<f64>,
    pub funding_rate_volatility_bps: Option<f64>,
    pub confidence: ConfidenceLevel,
}

impl Default for ArbitrageScannerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            relationships: RelationshipScanToggles::default(),
            execution_modes: ExecutionModeToggles::default(),
            notionals_usdt: default_notionals(),
            minimums: ScannerMinimums::default(),
            lifecycle: LifecycleModelConfig::default(),
            maker_taker: MakerTakerExpectedValueModel::default(),
            stale_book_ms: default_stale_book_ms(),
            quantity_rounding_tolerance: default_quantity_tolerance(),
            output_path: None,
        }
    }
}

pub fn analyze_relationship(
    relationship: &ArbitrageRelationship,
    target_notional: f64,
    config: &ArbitrageScannerConfig,
    stats: Option<&ArbitrageStatisticsSnapshot>,
) -> ArbitrageOpportunityAnalysis {
    let mut warnings = Vec::new();
    let mut rejection_reasons = Vec::new();
    if config.execution_modes.maker_taker_execution {
        warnings.push(
            "maker_taker_execution flag ignored; live maker/taker execution is disabled"
                .to_string(),
        );
    }
    let buy = executable_vwap(
        &relationship.buy_leg.order_book,
        &relationship.buy_leg.symbol_rule,
        ExecutablePriceRequest {
            side: OrderSide::Buy,
            target_quantity: None,
            target_notional: Some(target_notional),
            stale_book_ms: config.stale_book_ms,
            quantity_rounding_tolerance: config.quantity_rounding_tolerance,
        },
    );
    let sell = executable_vwap(
        &relationship.sell_leg.order_book,
        &relationship.sell_leg.symbol_rule,
        ExecutablePriceRequest {
            side: OrderSide::Sell,
            target_quantity: Some(buy.requested_quantity),
            target_notional: None,
            stale_book_ms: config.stale_book_ms,
            quantity_rounding_tolerance: config.quantity_rounding_tolerance,
        },
    );
    if let Err(reason) = enforce_same_quantity(&buy, &sell, config.quantity_rounding_tolerance) {
        rejection_reasons.push(reason);
    }
    if let Some(reason) = &buy.rejection_reason_optional {
        rejection_reasons.push(format!("buy: {reason}"));
    }
    if let Some(reason) = &sell.rejection_reason_optional {
        rejection_reasons.push(format!("sell: {reason}"));
    }
    let fee_sources = vec![
        relationship.buy_leg.fee_rate.source,
        relationship.sell_leg.fee_rate.source,
    ];
    let fee_fallback_used = fee_sources
        .iter()
        .any(|source| matches!(source, FeeSource::Fallback));
    if fee_fallback_used {
        warnings.push("conservative fallback fee used".to_string());
    }
    if relationship.buy_leg.funding_rate_optional.is_none()
        && relationship.buy_leg.market_type == MarketType::Perpetual
    {
        warnings.push("buy leg funding unavailable; confidence lowered".to_string());
    }
    if relationship.sell_leg.funding_rate_optional.is_none()
        && relationship.sell_leg.market_type == MarketType::Perpetual
    {
        warnings.push("sell leg funding unavailable; confidence lowered".to_string());
    }
    let tt = analyze_taker_taker_entry(
        &buy,
        &sell,
        relationship.buy_leg.fee_rate.taker_fee_bps,
        relationship.sell_leg.fee_rate.taker_fee_bps,
        config.lifecycle.safety_buffer_bps,
        config.lifecycle.expected_residual_loss_bps,
    );
    let mut lifecycle_config = config.lifecycle.clone();
    lifecycle_config.expected_convergence_bps = match relationship.relationship_type {
        ArbitrageRelationshipType::SpotSpot => tt.raw_executable_spread_bps,
        ArbitrageRelationshipType::SpotPerp => {
            tt.raw_executable_spread_bps - config.lifecycle.expected_exit_basis_bps
        }
        ArbitrageRelationshipType::PerpPerp => {
            tt.raw_executable_spread_bps - config.lifecycle.expected_exit_spread_bps
        }
    };
    lifecycle_config.expected_funding_net_bps = funding_net_bps(relationship);
    let lifecycle = analyze_lifecycle(
        relationship.relationship_type,
        &tt,
        buy.executable_notional,
        &lifecycle_config,
    );
    let maker_buy_price = relationship
        .buy_leg
        .order_book
        .best_bid
        .or_else(|| {
            relationship
                .buy_leg
                .order_book
                .bids
                .first()
                .map(|level| level.price)
        })
        .unwrap_or_else(|| buy.best_price.unwrap_or_default());
    let maker_sell_price = relationship
        .sell_leg
        .order_book
        .best_ask
        .or_else(|| {
            relationship
                .sell_leg
                .order_book
                .asks
                .first()
                .map(|level| level.price)
        })
        .unwrap_or_else(|| sell.best_price.unwrap_or_default());
    let mut ev = config.maker_taker.clone();
    if ev.expected_spread_at_fill_bps == 0.0 {
        ev.expected_spread_at_fill_bps = tt.raw_executable_spread_bps;
    }
    if let Some(stats) = stats {
        if let Some(fill_probability) = stats.maker_fill_probability {
            ev.fill_probability = fill_probability;
        }
    }
    let mbts = maker_buy_taker_sell(
        maker_buy_price,
        &sell,
        relationship.buy_leg.fee_rate.maker_fee_bps,
        relationship.sell_leg.fee_rate.taker_fee_bps,
        &ev,
    );
    let tbms = taker_buy_maker_sell(
        &buy,
        maker_sell_price,
        relationship.sell_leg.fee_rate.maker_fee_bps,
        relationship.buy_leg.fee_rate.taker_fee_bps,
        &ev,
    );
    let account_structure = AccountStructure::SeparateAccounts;
    let capital = calculate_capital_requirement(&CapitalModelInput {
        relationship_type: relationship.relationship_type,
        buy_market_type: relationship.buy_leg.market_type,
        sell_market_type: relationship.sell_leg.market_type,
        target_notional_usdt: target_notional,
        account_structure,
        collateral_assets: Vec::new(),
        buy_exchange_capabilities: ExchangeCapitalCapabilities::default(),
        sell_exchange_capabilities: ExchangeCapitalCapabilities::default(),
        initial_margin_rate: 0.1,
        maintenance_margin_rate: 0.005,
        liquidation_buffer_bps: 100.0,
        fragmented_capital_penalty_bps: 10.0,
    });
    let roc = expected_return_on_capital(lifecycle.expected_lifecycle_net_pnl, &capital);
    let roc_per_hour = expected_return_on_capital_per_hour(roc, lifecycle.expected_holding_seconds);
    let max_book_age = book_age_ms(&relationship.buy_leg.order_book)
        .max(book_age_ms(&relationship.sell_leg.order_book));
    let confidence = confidence(stats, relationship, fee_fallback_used);
    let score_components = score_opportunity(&RiskScoreInput {
        expected_return_on_capital_per_hour: roc_per_hour,
        full_depth_available: buy.full_fill_possible && sell.full_fill_possible,
        max_book_age_ms: max_book_age,
        fee_fallback_used,
        convergence_confidence: confidence,
        funding_confidence: funding_confidence(relationship),
        residual_risk_bps: config.lifecycle.expected_residual_loss_bps,
        adverse_selection_bps: config.maker_taker.expected_adverse_selection_bps,
        capital_fragmentation_penalty_usdt: capital.fragmented_capital_penalty_usdt,
        stale_book: relationship.buy_leg.order_book.is_stale
            || relationship.sell_leg.order_book.is_stale,
    });
    let accepted = rejection_reasons.is_empty()
        && tt.immediate_net_bps >= config.minimums.tt_immediate_net_bps
        && lifecycle.expected_lifecycle_net_bps >= config.minimums.lifecycle_expected_net_bps
        && roc_per_hour * 10_000.0 >= config.minimums.return_on_capital_per_hour_bps;
    if !accepted && rejection_reasons.is_empty() {
        rejection_reasons.push("below configured minimum analytics thresholds".to_string());
    }
    ArbitrageOpportunityAnalysis {
        opportunity_id: format!(
            "arb-{}-{}-{}-{}",
            relationship.buy_leg.exchange,
            relationship.sell_leg.exchange,
            relationship.buy_leg.internal_symbol,
            Utc::now().timestamp_micros()
        ),
        timestamp: Utc::now(),
        relationship_type: relationship.relationship_type,
        symbol: relationship.buy_leg.internal_symbol.clone(),
        buy_exchange: relationship.buy_leg.exchange.clone(),
        buy_market_type: relationship.buy_leg.market_type,
        sell_exchange: relationship.sell_leg.exchange.clone(),
        sell_market_type: relationship.sell_leg.market_type,
        target_quantity: buy.requested_quantity,
        target_notional,
        buy_best_price: buy.best_price,
        sell_best_price: sell.best_price,
        buy_vwap: buy.vwap,
        sell_vwap: sell.vwap,
        buy_slippage_bps: buy.slippage_bps,
        sell_slippage_bps: sell.slippage_bps,
        raw_executable_spread_bps: tt.raw_executable_spread_bps,
        tt_immediate_net_bps: tt.immediate_net_bps,
        tt_immediate_net_pnl: tt.immediate_net_pnl,
        tt_lifecycle_expected_net_bps: lifecycle.expected_lifecycle_net_bps,
        tt_lifecycle_expected_net_pnl: lifecycle.expected_lifecycle_net_pnl,
        maker_buy_taker_sell_theoretical_net_bps: Some(mbts.theoretical_net_bps),
        maker_buy_taker_sell_expected_net_bps: Some(mbts.expected_net_bps),
        taker_buy_maker_sell_theoretical_net_bps: Some(tbms.theoretical_net_bps),
        taker_buy_maker_sell_expected_net_bps: Some(tbms.expected_net_bps),
        expected_funding_net_bps: lifecycle.expected_funding_net_bps,
        expected_rebalance_cost_bps: lifecycle.expected_inventory_rebalance_cost_bps,
        expected_exit_fee_bps: lifecycle.expected_exit_fee_bps,
        expected_exit_slippage_bps: lifecycle.expected_exit_slippage_bps,
        expected_residual_loss_bps: lifecycle.expected_residual_loss_bps,
        safety_buffer_bps: lifecycle.safety_buffer_bps,
        expected_holding_seconds: lifecycle.expected_holding_seconds,
        required_capital_usdt: capital.effective_total_capital_required_usdt,
        expected_return_on_capital: roc,
        expected_return_on_capital_per_hour: roc_per_hour,
        risk_adjusted_score: score_components.total_score,
        score_components,
        fee_sources,
        book_ages: vec![
            book_age_ms(&relationship.buy_leg.order_book),
            book_age_ms(&relationship.sell_leg.order_book),
        ],
        book_latencies: vec![
            relationship.buy_leg.order_book.latency_ms,
            relationship.sell_leg.order_book.latency_ms,
        ],
        account_structure,
        confidence,
        warnings,
        rejection_reasons,
        accepted,
    }
}

pub fn record_analysis_jsonl(
    path: impl AsRef<Path>,
    analysis: &ArbitrageOpportunityAnalysis,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    serde_json::to_writer(&mut file, analysis)?;
    writeln!(file)?;
    Ok(())
}

pub fn execution_mode_candidates(config: &ArbitrageScannerConfig) -> Vec<ExecutionModeCandidate> {
    let mut values = Vec::new();
    if config.execution_modes.taker_taker {
        values.push(ExecutionModeCandidate::TakerTaker);
    }
    if config.execution_modes.maker_taker_analysis {
        values.push(ExecutionModeCandidate::MakerBuyTakerSell);
        values.push(ExecutionModeCandidate::TakerBuyMakerSell);
    }
    values
}

fn funding_net_bps(relationship: &ArbitrageRelationship) -> f64 {
    let buy = relationship
        .buy_leg
        .funding_rate_optional
        .as_ref()
        .map(|funding| funding.funding_rate_bps)
        .unwrap_or(0.0);
    let sell = relationship
        .sell_leg
        .funding_rate_optional
        .as_ref()
        .map(|funding| funding.funding_rate_bps)
        .unwrap_or(0.0);
    match relationship.relationship_type {
        ArbitrageRelationshipType::SpotSpot => 0.0,
        ArbitrageRelationshipType::SpotPerp => sell - buy,
        ArbitrageRelationshipType::PerpPerp => sell - buy,
    }
}

fn confidence(
    stats: Option<&ArbitrageStatisticsSnapshot>,
    relationship: &ArbitrageRelationship,
    fee_fallback_used: bool,
) -> ConfidenceLevel {
    if fee_fallback_used {
        return ConfidenceLevel::Low;
    }
    if relationship.relationship_type != ArbitrageRelationshipType::SpotSpot
        && (relationship.buy_leg.funding_rate_optional.is_none()
            || relationship.sell_leg.funding_rate_optional.is_none())
    {
        return ConfidenceLevel::Low;
    }
    stats
        .map(|stats| stats.confidence)
        .unwrap_or(ConfidenceLevel::Low)
}

fn funding_confidence(relationship: &ArbitrageRelationship) -> ConfidenceLevel {
    if relationship.relationship_type == ArbitrageRelationshipType::SpotSpot {
        ConfidenceLevel::High
    } else if relationship.buy_leg.funding_rate_optional.is_some()
        && relationship.sell_leg.funding_rate_optional.is_some()
    {
        ConfidenceLevel::Medium
    } else {
        ConfidenceLevel::Low
    }
}

fn book_age_ms(book: &crate::exchanges::unified::OrderBookSnapshot) -> i64 {
    chrono::Utc::now()
        .signed_duration_since(book.received_at)
        .num_milliseconds()
        .max(0)
}

fn default_notionals() -> Vec<f64> {
    vec![5.0, 10.0, 25.0, 50.0]
}
fn default_stale_book_ms() -> u64 {
    1_000
}
fn default_quantity_tolerance() -> f64 {
    1e-8
}
