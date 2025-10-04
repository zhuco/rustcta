use super::config::{ASATRConfig, ASEMAConfig, ASRSIConfig, ASVWAPConfig};
use super::state::IndicatorState;

/// 计算价格/数量精度
pub(super) fn calculate_precision(step: f64) -> usize {
    if step <= 0.0 {
        return 0;
    }
    let step_str = format!("{:.10}", step);
    step_str
        .trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len())
        .unwrap_or(0)
}

pub(super) fn update_ema(state: &mut IndicatorState, price: f64, config: &ASEMAConfig) {
    let fast_alpha = 2.0 / (config.fast_period as f64 + 1.0);
    let slow_alpha = 2.0 / (config.slow_period as f64 + 1.0);

    if state.ema_fast == 0.0 {
        state.ema_fast = price;
        state.ema_slow = price;
    } else {
        state.ema_fast = fast_alpha * price + (1.0 - fast_alpha) * state.ema_fast;
        state.ema_slow = slow_alpha * price + (1.0 - slow_alpha) * state.ema_slow;
    }
}

pub(super) fn update_rsi(state: &mut IndicatorState, config: &ASRSIConfig) {
    let period = config.period;
    if state.price_history.len() < period + 1 {
        state.rsi = 50.0;
        return;
    }

    let prices: Vec<f64> = state
        .price_history
        .iter()
        .rev()
        .take(period + 1)
        .rev()
        .cloned()
        .collect();

    let mut gains = Vec::with_capacity(period as usize);
    let mut losses = Vec::with_capacity(period as usize);
    for i in 1..prices.len() {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            gains.push(change);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(change.abs());
        }
    }

    if state.prev_avg_gain == 0.0 || state.prev_avg_loss == 0.0 {
        state.prev_avg_gain = gains.iter().sum::<f64>() / period as f64;
        state.prev_avg_loss = losses.iter().sum::<f64>() / period as f64;
    } else {
        let latest_gain = gains.last().copied().unwrap_or(0.0);
        let latest_loss = losses.last().copied().unwrap_or(0.0);
        state.prev_avg_gain =
            (state.prev_avg_gain * (period - 1) as f64 + latest_gain) / period as f64;
        state.prev_avg_loss =
            (state.prev_avg_loss * (period - 1) as f64 + latest_loss) / period as f64;
    }

    let avg_loss = if state.prev_avg_loss < 1e-7 {
        1e-7
    } else {
        state.prev_avg_loss
    };
    let avg_gain = state.prev_avg_gain;
    let rs = avg_gain / avg_loss;
    state.rsi = 100.0 - (100.0 / (1.0 + rs));
    state.rsi = state.rsi.max(0.0).min(100.0);
}

pub(super) fn update_vwap(state: &mut IndicatorState, config: &ASVWAPConfig) {
    if state.price_history.is_empty() {
        return;
    }
    let recent: Vec<f64> = state
        .price_history
        .iter()
        .rev()
        .take(config.period)
        .cloned()
        .collect();
    if recent.is_empty() {
        return;
    }
    state.vwap = recent.iter().sum::<f64>() / recent.len() as f64;
}

pub(super) fn update_atr(state: &mut IndicatorState, config: &ASATRConfig) {
    if state.price_history.len() < config.period + 1 {
        return;
    }

    let prices: Vec<f64> = state
        .price_history
        .iter()
        .rev()
        .take(config.period + 1)
        .rev()
        .cloned()
        .collect();

    let mut true_ranges = Vec::with_capacity(config.period as usize);
    for i in 1..prices.len() {
        let tr = (prices[i] - prices[i - 1]).abs();
        true_ranges.push(tr);
    }

    if !true_ranges.is_empty() {
        state.atr = true_ranges.iter().sum::<f64>() / true_ranges.len() as f64;
    }
}
