use crate::smart_money::{Direction, WalletId, WalletProfile, WalletTrade};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct EquityPoint {
    pub ts: DateTime<Utc>,
    pub equity_usdt: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClosedPositionSample {
    pub symbol: String,
    pub direction: Direction,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
    pub entry_notional_usdt: Decimal,
    pub realized_pnl_usdt: Decimal,
    pub max_leverage: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WalletProfileInput {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub equity_curve: Vec<EquityPoint>,
    pub trades: Vec<WalletTrade>,
    pub closed_positions: Vec<ClosedPositionSample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfileComputationConfig {
    pub annualization_days: Decimal,
}

impl Default for ProfileComputationConfig {
    fn default() -> Self {
        Self {
            annualization_days: Decimal::new(365, 0),
        }
    }
}

pub fn compute_wallet_profile(
    input: &WalletProfileInput,
    config: &ProfileComputationConfig,
) -> WalletProfile {
    let mut equity_curve = input.equity_curve.clone();
    equity_curve.sort_by_key(|point| point.ts);
    let first_equity = equity_curve
        .first()
        .map(|point| point.equity_usdt)
        .unwrap_or(Decimal::ZERO);
    let latest_equity = equity_curve
        .last()
        .map(|point| point.equity_usdt)
        .unwrap_or(Decimal::ZERO);
    let history_days = equity_curve
        .first()
        .map(|first| {
            input
                .as_of
                .signed_duration_since(first.ts)
                .num_days()
                .max(0) as u32
        })
        .unwrap_or(0);

    let total_return = percent_return(first_equity, latest_equity);
    let return_30d = trailing_return(&equity_curve, input.as_of, 30);
    let return_90d = trailing_return(&equity_curve, input.as_of, 90);
    let return_180d = trailing_return(&equity_curve, input.as_of, 180);
    let returns = period_returns(&equity_curve);
    let sharpe = sharpe_ratio(&returns, config.annualization_days);
    let sortino = sortino_ratio(&returns, config.annualization_days);
    let max_drawdown = max_drawdown(&equity_curve);
    let calmar = if max_drawdown > Decimal::ZERO {
        Some(total_return / max_drawdown)
    } else {
        None
    };

    let closed_trades = input.closed_positions.len() as u32;
    let wins = input
        .closed_positions
        .iter()
        .filter(|position| position.realized_pnl_usdt > Decimal::ZERO)
        .count();
    let gross_profit = input
        .closed_positions
        .iter()
        .map(|position| position.realized_pnl_usdt)
        .filter(|pnl| *pnl > Decimal::ZERO)
        .sum::<Decimal>();
    let gross_loss = input
        .closed_positions
        .iter()
        .map(|position| position.realized_pnl_usdt)
        .filter(|pnl| *pnl < Decimal::ZERO)
        .map(|pnl| pnl.abs())
        .sum::<Decimal>();
    let win_rate = if closed_trades > 0 {
        Decimal::from(wins as u64) / Decimal::from(closed_trades)
    } else {
        Decimal::ZERO
    };
    let profit_factor = if gross_loss > Decimal::ZERO {
        gross_profit / gross_loss
    } else if gross_profit > Decimal::ZERO {
        Decimal::new(10, 0)
    } else {
        Decimal::ZERO
    };

    let average_holding_secs = average_holding_secs(&input.closed_positions);
    let average_leverage = average_decimal(
        input
            .closed_positions
            .iter()
            .map(|position| position.max_leverage),
    );
    let maximum_leverage = input
        .closed_positions
        .iter()
        .map(|position| position.max_leverage)
        .max()
        .unwrap_or(Decimal::ZERO);
    let active_days = active_days(&input.trades);
    let trade_frequency_daily = if active_days > 0 {
        Decimal::from(input.trades.len() as u64) / Decimal::from(active_days)
    } else {
        Decimal::ZERO
    };
    let position_concentration = position_concentration(&input.closed_positions);
    let risk_per_trade = if latest_equity > Decimal::ZERO {
        average_decimal(
            input
                .closed_positions
                .iter()
                .map(|position| position.realized_pnl_usdt.min(Decimal::ZERO).abs()),
        ) / latest_equity
    } else {
        Decimal::ZERO
    };
    let signal_reproducibility = reproducibility_score(&input.closed_positions);
    let capital_efficiency = if latest_equity > Decimal::ZERO {
        (gross_profit - gross_loss).max(Decimal::ZERO) / latest_equity
    } else {
        Decimal::ZERO
    }
    .min(Decimal::ONE);
    let behavior_stability = behavior_stability(&returns);
    let recent_performance = return_30d;

    WalletProfile {
        wallet_id: input.wallet_id.clone(),
        as_of: input.as_of,
        total_return,
        return_30d,
        return_90d,
        return_180d,
        sharpe,
        sortino,
        calmar,
        max_drawdown,
        win_rate,
        profit_factor,
        average_holding_secs,
        average_leverage,
        maximum_leverage,
        trade_frequency_daily,
        position_concentration,
        risk_per_trade,
        signal_reproducibility,
        capital_efficiency,
        behavior_stability,
        recent_performance,
        closed_trades,
        active_days,
        history_days,
        equity_usdt: latest_equity,
    }
}

fn percent_return(start: Decimal, end: Decimal) -> Decimal {
    if start > Decimal::ZERO {
        (end - start) / start
    } else {
        Decimal::ZERO
    }
}

fn trailing_return(curve: &[EquityPoint], as_of: DateTime<Utc>, days: i64) -> Decimal {
    let end = curve
        .iter()
        .rev()
        .find(|point| point.ts <= as_of)
        .map(|point| point.equity_usdt)
        .unwrap_or(Decimal::ZERO);
    let cutoff = as_of - chrono::Duration::days(days);
    let start = curve
        .iter()
        .rev()
        .find(|point| point.ts <= cutoff)
        .or_else(|| curve.first())
        .map(|point| point.equity_usdt)
        .unwrap_or(Decimal::ZERO);
    percent_return(start, end)
}

fn period_returns(curve: &[EquityPoint]) -> Vec<Decimal> {
    curve
        .windows(2)
        .filter_map(|pair| {
            let start = pair[0].equity_usdt;
            let end = pair[1].equity_usdt;
            if start > Decimal::ZERO {
                Some((end - start) / start)
            } else {
                None
            }
        })
        .collect()
}

fn sharpe_ratio(returns: &[Decimal], annualization_days: Decimal) -> Option<Decimal> {
    let mean = average_decimal(returns.iter().copied());
    let vol = stddev_decimal(returns, mean)?;
    if vol > Decimal::ZERO {
        Some(mean / vol * decimal_sqrt(annualization_days))
    } else {
        None
    }
}

fn sortino_ratio(returns: &[Decimal], annualization_days: Decimal) -> Option<Decimal> {
    let mean = average_decimal(returns.iter().copied());
    let downside = returns
        .iter()
        .copied()
        .filter(|value| *value < Decimal::ZERO)
        .collect::<Vec<_>>();
    let downside_vol = stddev_decimal(&downside, Decimal::ZERO)?;
    if downside_vol > Decimal::ZERO {
        Some(mean / downside_vol * decimal_sqrt(annualization_days))
    } else {
        None
    }
}

fn max_drawdown(curve: &[EquityPoint]) -> Decimal {
    let mut peak = Decimal::ZERO;
    let mut max_dd = Decimal::ZERO;
    for point in curve {
        if point.equity_usdt > peak {
            peak = point.equity_usdt;
        }
        if peak > Decimal::ZERO {
            let dd = (peak - point.equity_usdt) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

fn average_holding_secs(positions: &[ClosedPositionSample]) -> i64 {
    if positions.is_empty() {
        return 0;
    }
    let total = positions
        .iter()
        .map(|position| {
            position
                .closed_at
                .signed_duration_since(position.opened_at)
                .num_seconds()
                .max(0)
        })
        .sum::<i64>();
    total / positions.len() as i64
}

fn active_days(trades: &[WalletTrade]) -> u32 {
    trades
        .iter()
        .map(|trade| trade.executed_at.date_naive())
        .collect::<std::collections::HashSet<_>>()
        .len() as u32
}

fn position_concentration(positions: &[ClosedPositionSample]) -> Decimal {
    let total = positions
        .iter()
        .map(|position| position.entry_notional_usdt.abs())
        .sum::<Decimal>();
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut by_symbol = HashMap::<String, Decimal>::new();
    for position in positions {
        *by_symbol.entry(position.symbol.clone()).or_default() +=
            position.entry_notional_usdt.abs();
    }
    by_symbol
        .values()
        .map(|notional| {
            let share = *notional / total;
            share * share
        })
        .sum()
}

fn reproducibility_score(positions: &[ClosedPositionSample]) -> Decimal {
    if positions.is_empty() {
        return Decimal::ZERO;
    }
    let mut by_symbol = HashMap::<String, (u32, u32)>::new();
    for position in positions {
        let entry = by_symbol.entry(position.symbol.clone()).or_default();
        entry.1 += 1;
        if position.realized_pnl_usdt > Decimal::ZERO {
            entry.0 += 1;
        }
    }
    average_decimal(by_symbol.values().map(|(wins, total)| {
        if *total > 0 {
            Decimal::from(*wins) / Decimal::from(*total)
        } else {
            Decimal::ZERO
        }
    }))
}

fn behavior_stability(returns: &[Decimal]) -> Decimal {
    if returns.is_empty() {
        return Decimal::ZERO;
    }
    let mean = average_decimal(returns.iter().copied());
    match stddev_decimal(returns, mean) {
        Some(vol) => (Decimal::ONE - vol.abs()).clamp(Decimal::ZERO, Decimal::ONE),
        None => Decimal::ZERO,
    }
}

fn average_decimal(values: impl Iterator<Item = Decimal>) -> Decimal {
    let mut count = 0u64;
    let mut sum = Decimal::ZERO;
    for value in values {
        sum += value;
        count += 1;
    }
    if count > 0 {
        sum / Decimal::from(count)
    } else {
        Decimal::ZERO
    }
}

fn stddev_decimal(values: &[Decimal], mean: Decimal) -> Option<Decimal> {
    if values.len() < 2 {
        return None;
    }
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<Decimal>()
        / Decimal::from(values.len() as u64 - 1);
    Some(decimal_sqrt(variance))
}

fn decimal_sqrt(value: Decimal) -> Decimal {
    value
        .to_f64()
        .and_then(|v| rust_decimal::Decimal::from_f64_retain(v.sqrt()))
        .unwrap_or(Decimal::ZERO)
}
