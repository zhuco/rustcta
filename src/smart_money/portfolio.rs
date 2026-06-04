use crate::smart_money::{
    AggregatedAlpha, Direction, PortfolioAction, PortfolioConstraints, PortfolioTransition,
    TargetPortfolio,
};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::Signed;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioConstructionConfig {
    pub min_alpha_confidence: Decimal,
    pub min_consensus_strength: Decimal,
    pub volatility_floor: Decimal,
}

impl Default for PortfolioConstructionConfig {
    fn default() -> Self {
        Self {
            min_alpha_confidence: Decimal::new(5, 2),
            min_consensus_strength: Decimal::new(10, 2),
            volatility_floor: Decimal::new(2, 2),
        }
    }
}

pub fn construct_target_portfolio(
    alpha: &[AggregatedAlpha],
    current_nav_usdt: Decimal,
    as_of: DateTime<Utc>,
    constraints: &PortfolioConstraints,
    config: &PortfolioConstructionConfig,
) -> TargetPortfolio {
    let nav = if current_nav_usdt > Decimal::ZERO {
        current_nav_usdt
    } else {
        constraints.initial_capital_usdt
    };
    let mut target_notional = BTreeMap::new();
    let asset_cap = constraints.max_gross_notional_usdt * constraints.max_single_asset_gross_share;

    for item in alpha {
        if item.alpha_confidence_score < config.min_alpha_confidence
            || item.consensus_strength < config.min_consensus_strength
        {
            continue;
        }
        let direction = Direction::from_signed_notional(item.net_alpha_score);
        if direction == Direction::Flat {
            continue;
        }
        let confidence_scale = item.alpha_confidence_score.min(Decimal::ONE);
        let consensus_scale = item.consensus_strength.min(Decimal::ONE);
        let raw_notional = constraints.standard_entry_notional_usdt
            * confidence_scale
            * consensus_scale
            * direction.sign();
        let capped = clamp_abs(raw_notional, asset_cap);
        if capped != Decimal::ZERO {
            target_notional.insert(item.symbol.clone(), capped);
        }
    }

    scale_to_constraints(&mut target_notional, nav, constraints);
    build_target_portfolio(target_notional, nav, as_of, constraints.max_leverage)
}

pub fn plan_portfolio_transitions(
    current_notional: &BTreeMap<String, Decimal>,
    target_notional: &BTreeMap<String, Decimal>,
) -> Vec<PortfolioTransition> {
    let symbols = current_notional
        .keys()
        .chain(target_notional.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    symbols
        .into_iter()
        .filter_map(|symbol| {
            let current = *current_notional.get(&symbol).unwrap_or(&Decimal::ZERO);
            let target = *target_notional.get(&symbol).unwrap_or(&Decimal::ZERO);
            let delta = target - current;
            let action = classify_transition(current, target);
            if action == PortfolioAction::Noop {
                None
            } else {
                Some(PortfolioTransition {
                    symbol,
                    action,
                    current_notional: current,
                    target_notional: target,
                    delta_notional: delta,
                })
            }
        })
        .collect()
}

pub fn classify_transition(current: Decimal, target: Decimal) -> PortfolioAction {
    if current == target {
        return PortfolioAction::Noop;
    }
    if current == Decimal::ZERO && target != Decimal::ZERO {
        return PortfolioAction::Open;
    }
    if current != Decimal::ZERO && target == Decimal::ZERO {
        return PortfolioAction::Close;
    }
    if current.signum() != target.signum() {
        return PortfolioAction::Reverse;
    }
    if target.abs() > current.abs() {
        PortfolioAction::ScaleIn
    } else if target.abs() < current.abs() {
        PortfolioAction::ScaleOut
    } else {
        PortfolioAction::Reduce
    }
}

fn build_target_portfolio(
    target_notional: BTreeMap<String, Decimal>,
    nav: Decimal,
    as_of: DateTime<Utc>,
    max_leverage: Decimal,
) -> TargetPortfolio {
    let gross = target_notional.values().map(|v| v.abs()).sum::<Decimal>();
    let leverage = if nav > Decimal::ZERO {
        gross / nav
    } else {
        Decimal::ZERO
    };
    let target_weights = target_notional
        .iter()
        .map(|(symbol, notional)| {
            let weight = if nav > Decimal::ZERO {
                *notional / nav
            } else {
                Decimal::ZERO
            };
            (symbol.clone(), weight)
        })
        .collect::<BTreeMap<_, _>>();
    let cash_weight = Decimal::ONE - leverage.min(max_leverage);
    TargetPortfolio {
        portfolio_id: Uuid::new_v4(),
        as_of,
        nav_usdt: nav,
        gross_notional_usdt: gross,
        leverage,
        target_weights,
        target_notional,
        cash_weight,
    }
}

fn scale_to_constraints(
    target_notional: &mut BTreeMap<String, Decimal>,
    nav: Decimal,
    constraints: &PortfolioConstraints,
) {
    let gross = target_notional.values().map(|v| v.abs()).sum::<Decimal>();
    if gross <= Decimal::ZERO {
        return;
    }
    let leverage_cap_gross = nav * constraints.max_leverage;
    let allowed_gross = constraints.max_gross_notional_usdt.min(leverage_cap_gross);
    if gross <= allowed_gross {
        return;
    }
    let scale = allowed_gross / gross;
    for notional in target_notional.values_mut() {
        *notional *= scale;
    }
}

fn clamp_abs(value: Decimal, max_abs: Decimal) -> Decimal {
    if value.abs() > max_abs {
        max_abs * value.signum()
    } else {
        value
    }
}
