use crate::smart_money::{
    aggregate_alpha, classify_wallet, construct_target_portfolio, evaluate_target_portfolio,
    filter_position, filter_wallet_profile, AlphaAggregationConfig, ClusterAssignment,
    ClusterConfig, Direction, FilterDecision, MarketRegimeSnapshot, PortfolioConstraints,
    PortfolioConstructionConfig, PortfolioRiskConfig, RiskDecision, ScoreWeights, TargetPortfolio,
    TraderCluster, WalletFilterConfig, WalletOpinion, WalletPositionSnapshot, WalletProfile,
    WalletScoreMatrix,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletResearchSnapshot {
    pub profile: WalletProfile,
    pub score: WalletScoreMatrix,
    pub clusters: Vec<ClusterAssignment>,
    pub filter: FilterDecision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SmartMoneyPipelineOutput {
    pub as_of: DateTime<Utc>,
    pub wallet_research: Vec<WalletResearchSnapshot>,
    pub opinions: Vec<WalletOpinion>,
    pub alpha: Vec<crate::smart_money::AggregatedAlpha>,
    pub target_portfolio: TargetPortfolio,
    pub risk_decision: RiskDecision,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SmartMoneyPipelineConfig {
    pub wallet_filter: WalletFilterConfig,
    pub cluster: ClusterConfig,
    pub score_weights: ScoreWeights,
    pub alpha: AlphaAggregationConfig,
    pub portfolio: PortfolioConstructionConfig,
    pub risk: PortfolioRiskConfig,
    pub opinion_ttl_secs: i64,
}

impl Default for SmartMoneyPipelineConfig {
    fn default() -> Self {
        Self {
            wallet_filter: WalletFilterConfig::default(),
            cluster: ClusterConfig::default(),
            score_weights: ScoreWeights::default(),
            alpha: AlphaAggregationConfig::default(),
            portfolio: PortfolioConstructionConfig::default(),
            risk: PortfolioRiskConfig::default(),
            opinion_ttl_secs: 300,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run_smart_money_pipeline(
    profiles: &[WalletProfile],
    positions: &[WalletPositionSnapshot],
    regime: &MarketRegimeSnapshot,
    as_of: DateTime<Utc>,
    nav_usdt: Decimal,
    constraints: &PortfolioConstraints,
    config: &SmartMoneyPipelineConfig,
    daily_pnl_pct: Decimal,
    current_drawdown_pct: Decimal,
    market_data_stale: bool,
) -> SmartMoneyPipelineOutput {
    let mut wallet_research = Vec::new();
    let mut accepted_by_wallet = HashMap::new();

    for profile in profiles {
        let filter = filter_wallet_profile(profile, &config.wallet_filter);
        let score = crate::smart_money::score_wallet(profile, regime, &config.score_weights);
        let clusters = classify_wallet(profile, &config.cluster);
        accepted_by_wallet.insert(profile.wallet_id.clone(), filter.accepted);
        wallet_research.push(WalletResearchSnapshot {
            profile: profile.clone(),
            score,
            clusters,
            filter,
        });
    }

    let score_by_wallet = wallet_research
        .iter()
        .map(|snapshot| (snapshot.profile.wallet_id.clone(), snapshot.score.clone()))
        .collect::<HashMap<_, _>>();
    let profile_by_wallet = wallet_research
        .iter()
        .map(|snapshot| (snapshot.profile.wallet_id.clone(), snapshot.profile.clone()))
        .collect::<HashMap<_, _>>();
    let clusters_by_wallet = wallet_research
        .iter()
        .map(|snapshot| {
            (
                snapshot.profile.wallet_id.clone(),
                snapshot
                    .clusters
                    .iter()
                    .map(|assignment| assignment.cluster.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    let opinions = positions
        .iter()
        .filter(|position| {
            accepted_by_wallet
                .get(&position.wallet_id)
                .copied()
                .unwrap_or(false)
        })
        .filter(|position| filter_position(position, as_of, &config.wallet_filter).accepted)
        .filter_map(|position| {
            let profile = profile_by_wallet.get(&position.wallet_id)?;
            let score = score_by_wallet.get(&position.wallet_id)?;
            Some(position_to_opinion(
                position,
                profile,
                score,
                clusters_by_wallet
                    .get(&position.wallet_id)
                    .cloned()
                    .unwrap_or_default(),
                as_of,
                config.opinion_ttl_secs,
            ))
        })
        .collect::<Vec<_>>();

    let alpha = aggregate_alpha(&opinions, as_of, &config.alpha);
    let target_portfolio =
        construct_target_portfolio(&alpha, nav_usdt, as_of, constraints, &config.portfolio);
    let wallet_data_stale = opinions.is_empty() && !positions.is_empty();
    let risk_decision = evaluate_target_portfolio(
        &target_portfolio,
        constraints,
        &config.risk,
        daily_pnl_pct,
        current_drawdown_pct,
        market_data_stale,
        wallet_data_stale,
    );

    SmartMoneyPipelineOutput {
        as_of,
        wallet_research,
        opinions,
        alpha,
        target_portfolio,
        risk_decision,
    }
}

fn position_to_opinion(
    position: &WalletPositionSnapshot,
    profile: &WalletProfile,
    score: &WalletScoreMatrix,
    clusters: Vec<TraderCluster>,
    as_of: DateTime<Utc>,
    ttl_secs: i64,
) -> WalletOpinion {
    let conviction = if profile.equity_usdt > Decimal::ZERO {
        (position.notional_usdt.abs() / profile.equity_usdt).min(Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    WalletOpinion {
        wallet_id: position.wallet_id.clone(),
        symbol: position.symbol.clone(),
        direction: if position.direction == Direction::Flat {
            Direction::Flat
        } else {
            position.direction
        },
        confidence: score.final_score,
        conviction,
        dynamic_score: score.final_score,
        wallet_equity_usdt: profile.equity_usdt,
        clusters,
        observed_at: position.observed_at,
        expires_at: as_of + Duration::seconds(ttl_secs.max(1)),
    }
}
