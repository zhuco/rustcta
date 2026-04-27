use crate::core::types::Kline;

pub fn ema(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() < period {
        return output;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut current = values[..period].iter().sum::<f64>() / period as f64;
    output[period - 1] = Some(current);
    for index in period..values.len() {
        current = values[index] * alpha + current * (1.0 - alpha);
        output[index] = Some(current);
    }
    output
}

pub fn sma(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() < period {
        return output;
    }
    let mut sum = 0.0;
    for (index, value) in values.iter().enumerate() {
        sum += value;
        if index >= period {
            sum -= values[index - period];
        }
        if index + 1 >= period {
            output[index] = Some(sum / period as f64);
        }
    }
    output
}

pub fn rsi(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() <= period {
        return output;
    }
    let mut gains = vec![0.0; values.len()];
    let mut losses = vec![0.0; values.len()];
    for index in 1..values.len() {
        let change = values[index] - values[index - 1];
        gains[index] = change.max(0.0);
        losses[index] = (-change).max(0.0);
    }
    let mut avg_gain = gains[1..=period].iter().sum::<f64>() / period as f64;
    let mut avg_loss = losses[1..=period].iter().sum::<f64>() / period as f64;
    output[period] = Some(rsi_from_avgs(avg_gain, avg_loss));
    for index in period + 1..values.len() {
        avg_gain = (avg_gain * (period as f64 - 1.0) + gains[index]) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + losses[index]) / period as f64;
        output[index] = Some(rsi_from_avgs(avg_gain, avg_loss));
    }
    output
}

pub fn atr(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    atr_adx(candles, period).0
}

pub fn adx(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    atr_adx(candles, period).1
}

pub fn macd_histogram(
    values: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> Vec<Option<f64>> {
    let fast = ema(values, fast_period);
    let slow = ema(values, slow_period);
    let mut compact = Vec::new();
    let mut indexes = Vec::new();
    for (index, (fast_value, slow_value)) in fast.iter().zip(slow.iter()).enumerate() {
        if let (Some(fast_value), Some(slow_value)) = (fast_value, slow_value) {
            compact.push(fast_value - slow_value);
            indexes.push(index);
        }
    }
    let signal = ema(&compact, signal_period);
    let mut output = vec![None; values.len()];
    for (compact_index, signal_value) in signal.iter().enumerate() {
        if let Some(signal_value) = signal_value {
            output[indexes[compact_index]] = Some(compact[compact_index] - signal_value);
        }
    }
    output
}

pub fn donchian_high(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    rolling_extreme(candles, period, true)
}

pub fn donchian_low(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    rolling_extreme(candles, period, false)
}

pub fn bollinger_width(values: &[f64], period: usize, std_dev: f64) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() < period {
        return output;
    }
    for index in period - 1..values.len() {
        let window = &values[index + 1 - period..=index];
        let mean = window.iter().sum::<f64>() / period as f64;
        if mean.abs() <= f64::EPSILON {
            continue;
        }
        let variance = window
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / period as f64;
        let band_width = 2.0 * std_dev * variance.sqrt() / mean.abs();
        output[index] = Some(band_width);
    }
    output
}

pub fn supertrend_direction(candles: &[Kline], period: usize, multiplier: f64) -> Vec<Option<i8>> {
    let atr_values = atr(candles, period);
    let mut output = vec![None; candles.len()];
    if candles.is_empty() {
        return output;
    }
    let mut upper_band = 0.0;
    let mut lower_band = 0.0;
    let mut direction = 1i8;
    for index in 0..candles.len() {
        let Some(atr_now) = atr_values[index] else {
            continue;
        };
        let hl2 = (candles[index].high + candles[index].low) / 2.0;
        let basic_upper = hl2 + multiplier * atr_now;
        let basic_lower = hl2 - multiplier * atr_now;
        if output[..index].iter().all(Option::is_none) {
            upper_band = basic_upper;
            lower_band = basic_lower;
        } else {
            let previous_close = candles[index - 1].close;
            upper_band = if basic_upper < upper_band || previous_close > upper_band {
                basic_upper
            } else {
                upper_band
            };
            lower_band = if basic_lower > lower_band || previous_close < lower_band {
                basic_lower
            } else {
                lower_band
            };
        }
        if candles[index].close > upper_band {
            direction = 1;
        } else if candles[index].close < lower_band {
            direction = -1;
        }
        output[index] = Some(direction);
    }
    output
}

pub fn volume_ratio(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    let volumes = candles
        .iter()
        .map(|candle| candle.volume)
        .collect::<Vec<_>>();
    let average = sma(&volumes, period);
    volumes
        .iter()
        .zip(average.iter())
        .map(|(volume, avg)| avg.and_then(|value| (value > 0.0).then_some(volume / value)))
        .collect()
}

pub fn roc(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() <= period {
        return output;
    }
    for index in period..values.len() {
        let previous = values[index - period];
        if previous.abs() > f64::EPSILON {
            output[index] = Some((values[index] / previous - 1.0) * 100.0);
        }
    }
    output
}

#[derive(Debug, Clone)]
pub struct KeltnerChannelSeries {
    pub upper: Vec<Option<f64>>,
    pub middle: Vec<Option<f64>>,
    pub lower: Vec<Option<f64>>,
}

pub fn ema_slope(values: &[f64], period: usize, lookback: usize) -> Vec<Option<f64>> {
    let ema_values = ema(values, period);
    let mut output = vec![None; values.len()];
    if lookback == 0 || values.len() <= lookback {
        return output;
    }
    for index in lookback..values.len() {
        let Some(current) = ema_values[index] else {
            continue;
        };
        let Some(previous) = ema_values[index - lookback] else {
            continue;
        };
        if previous.abs() > f64::EPSILON {
            output[index] = Some((current / previous - 1.0) * 100.0);
        }
    }
    output
}

pub fn keltner_channel(
    candles: &[Kline],
    ema_period: usize,
    atr_period: usize,
    multiplier: f64,
) -> KeltnerChannelSeries {
    let closes = candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let middle = ema(&closes, ema_period);
    let atr_values = atr(candles, atr_period);
    let mut upper = vec![None; candles.len()];
    let mut lower = vec![None; candles.len()];
    for index in 0..candles.len() {
        if let (Some(mid), Some(atr_now)) = (middle[index], atr_values[index]) {
            upper[index] = Some(mid + atr_now * multiplier);
            lower[index] = Some(mid - atr_now * multiplier);
        }
    }
    KeltnerChannelSeries {
        upper,
        middle,
        lower,
    }
}

pub fn vwap_deviation(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; candles.len()];
    if period == 0 || candles.len() < period {
        return output;
    }
    for index in period - 1..candles.len() {
        let window = &candles[index + 1 - period..=index];
        let volume_sum = window.iter().map(|candle| candle.volume).sum::<f64>();
        if volume_sum <= 0.0 {
            continue;
        }
        let vwap = window
            .iter()
            .map(|candle| typical_price(candle) * candle.volume)
            .sum::<f64>()
            / volume_sum;
        if vwap.abs() > f64::EPSILON {
            output[index] = Some((candles[index].close / vwap - 1.0) * 100.0);
        }
    }
    output
}

pub fn mfi(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; candles.len()];
    if period == 0 || candles.len() <= period {
        return output;
    }
    let typical = candles.iter().map(typical_price).collect::<Vec<_>>();
    let money_flow = candles
        .iter()
        .zip(typical.iter())
        .map(|(candle, price)| price * candle.volume)
        .collect::<Vec<_>>();
    for index in period..candles.len() {
        let mut positive = 0.0;
        let mut negative = 0.0;
        for cursor in index + 1 - period..=index {
            if typical[cursor] > typical[cursor - 1] {
                positive += money_flow[cursor];
            } else if typical[cursor] < typical[cursor - 1] {
                negative += money_flow[cursor];
            }
        }
        output[index] = Some(if negative <= 0.0 {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + positive / negative)
        });
    }
    output
}

pub fn stoch_rsi(values: &[f64], rsi_period: usize, stoch_period: usize) -> Vec<Option<f64>> {
    let rsi_values = rsi(values, rsi_period);
    let mut output = vec![None; values.len()];
    if stoch_period == 0 || values.len() < stoch_period {
        return output;
    }
    for index in stoch_period - 1..values.len() {
        let window = &rsi_values[index + 1 - stoch_period..=index];
        if window.iter().any(Option::is_none) {
            continue;
        }
        let min = window
            .iter()
            .flatten()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let max = window
            .iter()
            .flatten()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let Some(current) = rsi_values[index] else {
            continue;
        };
        output[index] = Some(if (max - min).abs() <= f64::EPSILON {
            50.0
        } else {
            (current - min) / (max - min) * 100.0
        });
    }
    output
}

pub fn choppiness_index(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; candles.len()];
    if period <= 1 || candles.len() < period {
        return output;
    }
    let true_ranges = true_range_series(candles);
    for index in period - 1..candles.len() {
        let window = &candles[index + 1 - period..=index];
        let tr_sum = true_ranges[index + 1 - period..=index].iter().sum::<f64>();
        let high = window
            .iter()
            .map(|candle| candle.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let low = window
            .iter()
            .map(|candle| candle.low)
            .fold(f64::INFINITY, f64::min);
        let range = high - low;
        if range > 0.0 && tr_sum > 0.0 {
            output[index] = Some(100.0 * (tr_sum / range).log10() / (period as f64).log10());
        }
    }
    output
}

pub fn atr_percentile(
    candles: &[Kline],
    atr_period: usize,
    percentile_window: usize,
) -> Vec<Option<f64>> {
    let atr_values = atr(candles, atr_period);
    let mut output = vec![None; candles.len()];
    if percentile_window == 0 || candles.len() < percentile_window {
        return output;
    }
    for index in percentile_window - 1..candles.len() {
        let Some(current) = atr_values[index] else {
            continue;
        };
        let window = &atr_values[index + 1 - percentile_window..=index];
        if window.iter().any(Option::is_none) {
            continue;
        }
        let less_or_equal = window
            .iter()
            .flatten()
            .filter(|value| **value <= current)
            .count();
        output[index] = Some(less_or_equal as f64 / percentile_window as f64 * 100.0);
    }
    output
}

pub fn volume_zscore(candles: &[Kline], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; candles.len()];
    if period == 0 || candles.len() < period {
        return output;
    }
    let volumes = candles
        .iter()
        .map(|candle| candle.volume)
        .collect::<Vec<_>>();
    for index in period - 1..candles.len() {
        let window = &volumes[index + 1 - period..=index];
        let mean = window.iter().sum::<f64>() / period as f64;
        let variance = window
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / period as f64;
        let std = variance.sqrt();
        if std > f64::EPSILON {
            output[index] = Some((volumes[index] - mean) / std);
        }
    }
    output
}

fn rsi_from_avgs(avg_gain: f64, avg_loss: f64) -> f64 {
    if avg_loss == 0.0 {
        100.0
    } else {
        100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    }
}

fn typical_price(candle: &Kline) -> f64 {
    (candle.high + candle.low + candle.close) / 3.0
}

fn true_range_series(candles: &[Kline]) -> Vec<f64> {
    let mut output = vec![0.0; candles.len()];
    for index in 1..candles.len() {
        let candle = &candles[index];
        let previous = &candles[index - 1];
        output[index] = (candle.high - candle.low)
            .max((candle.high - previous.close).abs())
            .max((candle.low - previous.close).abs());
    }
    output
}

fn atr_adx(candles: &[Kline], period: usize) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    let mut atr_values = vec![None; candles.len()];
    let mut adx_values = vec![None; candles.len()];
    if period == 0 || candles.len() <= period * 2 {
        return (atr_values, adx_values);
    }
    let mut tr = vec![0.0; candles.len()];
    let mut plus_dm = vec![0.0; candles.len()];
    let mut minus_dm = vec![0.0; candles.len()];
    for index in 1..candles.len() {
        let candle = &candles[index];
        let previous = &candles[index - 1];
        tr[index] = (candle.high - candle.low)
            .max((candle.high - previous.close).abs())
            .max((candle.low - previous.close).abs());
        let up_move = candle.high - previous.high;
        let down_move = previous.low - candle.low;
        plus_dm[index] = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        minus_dm[index] = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
    }
    let mut atr_smoothed = tr[1..=period].iter().sum::<f64>();
    let mut plus_smoothed = plus_dm[1..=period].iter().sum::<f64>();
    let mut minus_smoothed = minus_dm[1..=period].iter().sum::<f64>();
    let mut dx_values = vec![None; candles.len()];
    for index in period..candles.len() {
        if index > period {
            atr_smoothed = atr_smoothed - atr_smoothed / period as f64 + tr[index];
            plus_smoothed = plus_smoothed - plus_smoothed / period as f64 + plus_dm[index];
            minus_smoothed = minus_smoothed - minus_smoothed / period as f64 + minus_dm[index];
        }
        atr_values[index] = Some(atr_smoothed / period as f64);
        if atr_smoothed <= 0.0 {
            continue;
        }
        let plus_di = 100.0 * plus_smoothed / atr_smoothed;
        let minus_di = 100.0 * minus_smoothed / atr_smoothed;
        let denominator = plus_di + minus_di;
        dx_values[index] = Some(if denominator == 0.0 {
            0.0
        } else {
            100.0 * (plus_di - minus_di).abs() / denominator
        });
    }
    let first_adx = period * 2 - 1;
    let initial = dx_values[period..=first_adx]
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    if initial.len() == period {
        let mut current = initial.iter().sum::<f64>() / period as f64;
        adx_values[first_adx] = Some(current);
        for index in first_adx + 1..candles.len() {
            if let Some(dx) = dx_values[index] {
                current = (current * (period as f64 - 1.0) + dx) / period as f64;
                adx_values[index] = Some(current);
            }
        }
    }
    (atr_values, adx_values)
}

fn rolling_extreme(candles: &[Kline], period: usize, high: bool) -> Vec<Option<f64>> {
    let mut output = vec![None; candles.len()];
    if period == 0 || candles.len() < period {
        return output;
    }
    for index in period - 1..candles.len() {
        let window = &candles[index + 1 - period..=index];
        output[index] = Some(if high {
            window
                .iter()
                .map(|candle| candle.high)
                .fold(f64::NEG_INFINITY, f64::max)
        } else {
            window
                .iter()
                .map(|candle| candle.low)
                .fold(f64::INFINITY, f64::min)
        });
    }
    output
}
