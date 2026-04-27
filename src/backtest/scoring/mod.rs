use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TrendScoreInput {
    pub roi_pct: f64,
    pub max_drawdown_pct: f64,
    pub profit_factor: f64,
    pub trades: usize,
    pub win_rate_pct: f64,
    pub min_trades: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScore {
    pub input: TrendScoreInput,
    pub composite_score: f64,
    pub return_drawdown_ratio: f64,
    pub risk_flags: Vec<String>,
}

pub fn score_trend_run(input: TrendScoreInput) -> TrendScore {
    let drawdown_abs = input.max_drawdown_pct.abs().max(0.1);
    let return_drawdown_ratio = input.roi_pct / drawdown_abs;
    let trade_ratio = if input.min_trades == 0 {
        1.0
    } else {
        (input.trades as f64 / input.min_trades as f64).min(1.0)
    };
    let profit_factor_score = input.profit_factor.min(5.0) * 8.0;
    let win_rate_score = (input.win_rate_pct - 50.0) * 0.25;
    let drawdown_penalty = drawdown_abs * 1.5;
    let low_trade_penalty = if input.trades < input.min_trades {
        (input.min_trades - input.trades) as f64 * 2.0
    } else {
        0.0
    };
    let composite_score = input.roi_pct * 4.0
        + return_drawdown_ratio * 12.0
        + profit_factor_score
        + win_rate_score
        + trade_ratio * 10.0
        - drawdown_penalty
        - low_trade_penalty;

    let mut risk_flags = Vec::new();
    if input.trades < input.min_trades {
        risk_flags.push("low_trade_count".to_string());
    }
    if input.max_drawdown_pct < -10.0 {
        risk_flags.push("high_drawdown".to_string());
    }
    if input.profit_factor < 1.0 {
        risk_flags.push("weak_profit_factor".to_string());
    }

    TrendScore {
        input,
        composite_score,
        return_drawdown_ratio,
        risk_flags,
    }
}
