use crate::smart_money::{MarketRegimeSnapshot, WalletProfile, WalletScoreMatrix};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct ScoreWeights {
    pub current_regime: Decimal,
    pub recent_performance: Decimal,
    pub consistency: Decimal,
    pub signal_quality: Decimal,
    pub execution_quality: Decimal,
    pub capital_efficiency: Decimal,
    pub risk_adjusted_return: Decimal,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            current_regime: Decimal::new(22, 2),
            recent_performance: Decimal::new(18, 2),
            consistency: Decimal::new(15, 2),
            signal_quality: Decimal::new(15, 2),
            execution_quality: Decimal::new(10, 2),
            capital_efficiency: Decimal::new(10, 2),
            risk_adjusted_return: Decimal::new(10, 2),
        }
    }
}

pub fn score_wallet(
    profile: &WalletProfile,
    regime: &MarketRegimeSnapshot,
    weights: &ScoreWeights,
) -> WalletScoreMatrix {
    let trend_score = clamp01(
        normalized_return(profile.return_90d) * Decimal::new(60, 2)
            + profile.signal_reproducibility * Decimal::new(40, 2),
    );
    let range_score = clamp01(
        profile.win_rate * Decimal::new(50, 2)
            + inverse01(profile.max_drawdown, Decimal::new(40, 2)) * Decimal::new(50, 2),
    );
    let high_volatility_score = clamp01(
        inverse01(profile.maximum_leverage, Decimal::new(15, 0)) * Decimal::new(45, 2)
            + profile.behavior_stability * Decimal::new(55, 2),
    );
    let low_volatility_score = clamp01(
        profile.capital_efficiency * Decimal::new(55, 2)
            + profile.signal_reproducibility * Decimal::new(45, 2),
    );
    let risk_on_score = clamp01(
        normalized_return(profile.return_30d) * Decimal::new(65, 2)
            + profile.capital_efficiency * Decimal::new(35, 2),
    );
    let risk_off_score = clamp01(
        inverse01(profile.max_drawdown, Decimal::new(40, 2)) * Decimal::new(60, 2)
            + profile.behavior_stability * Decimal::new(40, 2),
    );

    let current_regime_score = weighted_regime_score(
        regime,
        trend_score,
        range_score,
        high_volatility_score,
        low_volatility_score,
        risk_on_score,
        risk_off_score,
    );
    let recent_performance_score = clamp01(normalized_return(profile.recent_performance));
    let consistency_score = clamp01(
        profile.behavior_stability * Decimal::new(50, 2)
            + profile.signal_reproducibility * Decimal::new(50, 2),
    );
    let signal_quality_score = clamp01(
        profile.win_rate * Decimal::new(35, 2)
            + normalize_profit_factor(profile.profit_factor) * Decimal::new(35, 2)
            + profile.signal_reproducibility * Decimal::new(30, 2),
    );
    let execution_quality_score = clamp01(
        inverse01(profile.position_concentration, Decimal::ONE) * Decimal::new(35, 2)
            + inverse01(profile.risk_per_trade, Decimal::new(10, 2)) * Decimal::new(35, 2)
            + profile.capital_efficiency * Decimal::new(30, 2),
    );
    let risk_adjusted_return_score = clamp01(
        profile
            .sharpe
            .map(|s| normalize_signed(s, Decimal::new(3, 0)))
            .unwrap_or_else(|| normalized_return(profile.total_return)),
    );

    let penalty = deterioration_penalty(profile);
    let final_score = clamp01(
        current_regime_score * weights.current_regime
            + recent_performance_score * weights.recent_performance
            + consistency_score * weights.consistency
            + signal_quality_score * weights.signal_quality
            + execution_quality_score * weights.execution_quality
            + profile.capital_efficiency * weights.capital_efficiency
            + risk_adjusted_return_score * weights.risk_adjusted_return
            - penalty,
    );

    WalletScoreMatrix {
        wallet_id: profile.wallet_id.clone(),
        as_of: profile.as_of,
        trend_score,
        range_score,
        high_volatility_score,
        low_volatility_score,
        risk_on_score,
        risk_off_score,
        current_regime_score,
        recent_performance_score,
        consistency_score,
        signal_quality_score,
        execution_quality_score,
        final_score,
    }
}

pub fn decayed_weight(age_days: f64, lambda: f64) -> Decimal {
    Decimal::from_f64((-lambda * age_days).exp()).unwrap_or(Decimal::ZERO)
}

fn weighted_regime_score(
    regime: &MarketRegimeSnapshot,
    trend_score: Decimal,
    range_score: Decimal,
    high_volatility_score: Decimal,
    low_volatility_score: Decimal,
    risk_on_score: Decimal,
    risk_off_score: Decimal,
) -> Decimal {
    let trend_weight = regime.bull_trend.max(regime.bear_trend);
    let total = trend_weight
        + regime.range
        + regime.volatility_expansion
        + regime.volatility_compression
        + regime.risk_on
        + regime.risk_off;
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    clamp01(
        (trend_score * trend_weight
            + range_score * regime.range
            + high_volatility_score * regime.volatility_expansion
            + low_volatility_score * regime.volatility_compression
            + risk_on_score * regime.risk_on
            + risk_off_score * regime.risk_off)
            / total,
    )
}

fn deterioration_penalty(profile: &WalletProfile) -> Decimal {
    let mut penalty = Decimal::ZERO;
    if profile.recent_performance < Decimal::ZERO {
        penalty += normalized_return(profile.recent_performance.abs()) * Decimal::new(12, 2);
    }
    if profile.maximum_leverage > Decimal::new(10, 0) {
        penalty += Decimal::new(5, 2);
    }
    if profile.max_drawdown > Decimal::new(25, 2) {
        penalty += Decimal::new(8, 2);
    }
    penalty
}

fn normalize_profit_factor(value: Decimal) -> Decimal {
    clamp01((value - Decimal::ONE) / Decimal::new(2, 0))
}

fn normalized_return(value: Decimal) -> Decimal {
    normalize_signed(value, Decimal::new(100, 2))
}

fn normalize_signed(value: Decimal, scale: Decimal) -> Decimal {
    if scale <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    clamp01((value / scale + Decimal::ONE) / Decimal::new(2, 0))
}

fn inverse01(value: Decimal, max_value: Decimal) -> Decimal {
    if max_value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    clamp01(Decimal::ONE - value / max_value)
}

pub fn clamp01(value: Decimal) -> Decimal {
    if value < Decimal::ZERO {
        Decimal::ZERO
    } else if value > Decimal::ONE {
        Decimal::ONE
    } else {
        value
    }
}

pub fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}
