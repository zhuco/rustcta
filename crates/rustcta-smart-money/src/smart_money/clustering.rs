use crate::smart_money::{TraderCluster, WalletProfile};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterAssignment {
    pub cluster: TraderCluster,
    pub confidence: Decimal,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub whale_equity_threshold_usdt: Decimal,
    pub scalper_max_holding_secs: i64,
    pub intraday_max_holding_secs: i64,
    pub swing_max_holding_secs: i64,
    pub high_frequency_daily_trades: Decimal,
    pub high_leverage_threshold: Decimal,
    pub institutional_max_leverage: Decimal,
    pub consistent_min_win_rate: Decimal,
    pub consistent_max_drawdown: Decimal,
    pub new_alpha_max_history_days: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            whale_equity_threshold_usdt: Decimal::new(1_000_000, 0),
            scalper_max_holding_secs: 30 * 60,
            intraday_max_holding_secs: 24 * 60 * 60,
            swing_max_holding_secs: 7 * 24 * 60 * 60,
            high_frequency_daily_trades: Decimal::new(20, 0),
            high_leverage_threshold: Decimal::new(10, 0),
            institutional_max_leverage: Decimal::new(5, 0),
            consistent_min_win_rate: Decimal::new(55, 2),
            consistent_max_drawdown: Decimal::new(15, 2),
            new_alpha_max_history_days: 60,
        }
    }
}

pub fn classify_wallet(profile: &WalletProfile, config: &ClusterConfig) -> Vec<ClusterAssignment> {
    let mut assignments = Vec::new();

    if profile.equity_usdt >= config.whale_equity_threshold_usdt {
        assignments.push(assign(
            TraderCluster::Whale,
            Decimal::new(90, 2),
            "equity exceeds whale threshold",
        ));
    }
    if profile.average_holding_secs > 0
        && profile.average_holding_secs <= config.scalper_max_holding_secs
        && profile.trade_frequency_daily >= config.high_frequency_daily_trades
    {
        assignments.push(assign(
            TraderCluster::Scalper,
            Decimal::new(85, 2),
            "short holding period with high trade frequency",
        ));
    } else if profile.average_holding_secs > 0
        && profile.average_holding_secs <= config.intraday_max_holding_secs
    {
        assignments.push(assign(
            TraderCluster::Intraday,
            Decimal::new(75, 2),
            "positions usually close intraday",
        ));
    } else if profile.average_holding_secs <= config.swing_max_holding_secs {
        assignments.push(assign(
            TraderCluster::Swing,
            Decimal::new(70, 2),
            "holding period fits swing profile",
        ));
    } else {
        assignments.push(assign(
            TraderCluster::Trend,
            Decimal::new(70, 2),
            "long holding period indicates trend behavior",
        ));
    }

    if profile.return_90d > Decimal::ZERO && profile.signal_reproducibility > Decimal::new(60, 2) {
        assignments.push(assign(
            TraderCluster::Momentum,
            Decimal::new(70, 2),
            "positive 90d return with reproducible signals",
        ));
    }
    if profile.win_rate > Decimal::new(60, 2)
        && profile.average_holding_secs <= config.swing_max_holding_secs
        && profile.max_drawdown <= Decimal::new(20, 2)
    {
        assignments.push(assign(
            TraderCluster::MeanReversion,
            Decimal::new(60, 2),
            "high win rate and controlled drawdown",
        ));
    }
    if profile.average_leverage <= config.institutional_max_leverage
        && profile.position_concentration <= Decimal::new(35, 2)
        && profile.max_drawdown <= Decimal::new(20, 2)
    {
        assignments.push(assign(
            TraderCluster::InstitutionalStyle,
            Decimal::new(80, 2),
            "low leverage, diversified exposure, controlled drawdown",
        ));
    }
    if profile.win_rate >= config.consistent_min_win_rate
        && profile.max_drawdown <= config.consistent_max_drawdown
        && profile.behavior_stability >= Decimal::new(60, 2)
    {
        assignments.push(assign(
            TraderCluster::ConsistentAlpha,
            Decimal::new(85, 2),
            "consistent win rate, drawdown, and behavior stability",
        ));
    }
    if profile.maximum_leverage >= config.high_leverage_threshold
        || profile.max_drawdown >= Decimal::new(30, 2)
    {
        assignments.push(assign(
            TraderCluster::Gambler,
            Decimal::new(75, 2),
            "high leverage or large drawdown",
        ));
    }
    if profile.history_days <= config.new_alpha_max_history_days
        && profile.recent_performance > Decimal::new(10, 2)
        && profile.closed_trades >= 20
    {
        assignments.push(assign(
            TraderCluster::NewAlpha,
            Decimal::new(65, 2),
            "short history with strong recent performance",
        ));
    }

    dedupe_assignments(assignments)
}

fn assign(
    cluster: TraderCluster,
    confidence: Decimal,
    reason: impl Into<String>,
) -> ClusterAssignment {
    ClusterAssignment {
        cluster,
        confidence,
        reason: reason.into(),
    }
}

fn dedupe_assignments(assignments: Vec<ClusterAssignment>) -> Vec<ClusterAssignment> {
    let mut deduped = Vec::<ClusterAssignment>::new();
    for assignment in assignments {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|item| item.cluster == assignment.cluster)
        {
            if assignment.confidence > existing.confidence {
                *existing = assignment;
            }
        } else {
            deduped.push(assignment);
        }
    }
    deduped
}
