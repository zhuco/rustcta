use crate::smart_money::{PortfolioConstraints, TargetPortfolio};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioRiskConfig {
    pub daily_reduce_threshold: Decimal,
    pub daily_close_only_threshold: Decimal,
    pub daily_kill_threshold: Decimal,
    pub drawdown_scale_threshold: Decimal,
    pub drawdown_close_only_threshold: Decimal,
    pub drawdown_kill_threshold: Decimal,
    pub max_wallet_pressure_share: Decimal,
    pub max_cluster_pressure_share: Decimal,
}

impl Default for PortfolioRiskConfig {
    fn default() -> Self {
        Self {
            daily_reduce_threshold: Decimal::new(2, 2),
            daily_close_only_threshold: Decimal::new(3, 2),
            daily_kill_threshold: Decimal::new(5, 2),
            drawdown_scale_threshold: Decimal::new(8, 2),
            drawdown_close_only_threshold: Decimal::new(12, 2),
            drawdown_kill_threshold: Decimal::new(15, 2),
            max_wallet_pressure_share: Decimal::new(20, 2),
            max_cluster_pressure_share: Decimal::new(40, 2),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskDecisionKind {
    Approved,
    Scaled,
    Rejected,
    CloseOnly,
    KillSwitch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskRejectReason {
    NonPositiveNav,
    GrossNotionalLimit,
    LeverageLimit,
    DailyLossLimit,
    DrawdownLimit,
    StaleMarketData,
    StaleWalletData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskDecision {
    pub kind: RiskDecisionKind,
    pub target: Option<TargetPortfolio>,
    pub reasons: Vec<RiskRejectReason>,
}

pub fn evaluate_target_portfolio(
    target: &TargetPortfolio,
    constraints: &PortfolioConstraints,
    config: &PortfolioRiskConfig,
    daily_pnl_pct: Decimal,
    current_drawdown_pct: Decimal,
    market_data_stale: bool,
    wallet_data_stale: bool,
) -> RiskDecision {
    let mut reasons = Vec::new();
    if target.nav_usdt <= Decimal::ZERO {
        reasons.push(RiskRejectReason::NonPositiveNav);
    }
    if target.gross_notional_usdt > constraints.max_gross_notional_usdt {
        reasons.push(RiskRejectReason::GrossNotionalLimit);
    }
    if target.leverage > constraints.max_leverage {
        reasons.push(RiskRejectReason::LeverageLimit);
    }
    if daily_pnl_pct <= -config.daily_kill_threshold
        || current_drawdown_pct >= config.drawdown_kill_threshold
    {
        reasons.push(RiskRejectReason::DailyLossLimit);
        return RiskDecision {
            kind: RiskDecisionKind::KillSwitch,
            target: Some(flatten_target(target)),
            reasons,
        };
    }
    if daily_pnl_pct <= -config.daily_close_only_threshold
        || current_drawdown_pct >= config.drawdown_close_only_threshold
    {
        return RiskDecision {
            kind: RiskDecisionKind::CloseOnly,
            target: Some(flatten_target(target)),
            reasons,
        };
    }
    if market_data_stale {
        reasons.push(RiskRejectReason::StaleMarketData);
    }
    if wallet_data_stale {
        reasons.push(RiskRejectReason::StaleWalletData);
    }
    if !reasons.is_empty() {
        return RiskDecision {
            kind: RiskDecisionKind::Rejected,
            target: None,
            reasons,
        };
    }
    if daily_pnl_pct <= -config.daily_reduce_threshold
        || current_drawdown_pct >= config.drawdown_scale_threshold
    {
        return RiskDecision {
            kind: RiskDecisionKind::Scaled,
            target: Some(scale_target(target, Decimal::new(50, 2))),
            reasons,
        };
    }
    RiskDecision {
        kind: RiskDecisionKind::Approved,
        target: Some(target.clone()),
        reasons,
    }
}

fn flatten_target(target: &TargetPortfolio) -> TargetPortfolio {
    let mut flattened = target.clone();
    flattened.target_notional.clear();
    flattened.target_weights.clear();
    flattened.gross_notional_usdt = Decimal::ZERO;
    flattened.leverage = Decimal::ZERO;
    flattened.cash_weight = Decimal::ONE;
    flattened
}

fn scale_target(target: &TargetPortfolio, scale: Decimal) -> TargetPortfolio {
    let mut scaled = target.clone();
    for notional in scaled.target_notional.values_mut() {
        *notional *= scale;
    }
    for weight in scaled.target_weights.values_mut() {
        *weight *= scale;
    }
    scaled.gross_notional_usdt *= scale;
    scaled.leverage *= scale;
    scaled.cash_weight = Decimal::ONE - scaled.leverage;
    scaled
}
