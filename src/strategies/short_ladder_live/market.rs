use anyhow::{anyhow, Result};

use crate::core::types::{Interval, Kline};
use crate::utils::indicators::{calculate_atr, calculate_ema, calculate_rsi};

use super::config::ShortLadderLiveConfig;

#[derive(Debug, Clone)]
pub struct ShortLadderSignalSnapshot {
    pub current_price: f64,
    pub atr_5m: f64,
    pub rsi_5m: f64,
    pub close_time: chrono::DateTime<chrono::Utc>,
    pub short_regime: bool,
    pub strong_1h_short_regime: bool,
    pub short_entry_trigger: bool,
}

pub fn parse_interval(interval: &str) -> Interval {
    Interval::from_string(interval).unwrap_or(Interval::FiveMinutes)
}

pub fn build_signal_snapshot(
    symbol: &str,
    klines_5m: &[Kline],
    config: &ShortLadderLiveConfig,
) -> Result<ShortLadderSignalSnapshot> {
    if klines_5m.len() < 120 {
        return Err(anyhow!("{} K线数量不足，无法计算短梯信号", symbol));
    }
    let closes_5m = klines_5m.iter().map(|item| item.close).collect::<Vec<_>>();
    let highs_5m = klines_5m.iter().map(|item| item.high).collect::<Vec<_>>();
    let lows_5m = klines_5m.iter().map(|item| item.low).collect::<Vec<_>>();
    let last = klines_5m
        .last()
        .ok_or_else(|| anyhow!("{} K线为空", symbol))?;

    let atr_5m = calculate_atr(&highs_5m, &lows_5m, &closes_5m, config.signal.atr_period)
        .ok_or_else(|| anyhow!("{} ATR不可用", symbol))?;
    let rsi_5m = calculate_rsi(&closes_5m, config.signal.rsi_period)
        .ok_or_else(|| anyhow!("{} RSI不可用", symbol))?;

    let candles_15m = aggregate_klines(klines_5m, 3, "15m");
    let candles_1h = aggregate_klines(klines_5m, 12, "1h");
    let closes_15m = candles_15m
        .iter()
        .map(|item| item.close)
        .collect::<Vec<_>>();
    let closes_1h = candles_1h.iter().map(|item| item.close).collect::<Vec<_>>();
    let ema_15m = calculate_ema(&closes_15m, config.signal.filter_15m_ema)
        .ok_or_else(|| anyhow!("{} 15m EMA不可用", symbol))?;
    let ema_1h = calculate_ema(&closes_1h, config.signal.filter_1h_ema)
        .ok_or_else(|| anyhow!("{} 1h EMA不可用", symbol))?;

    let short_regime = candles_15m
        .last()
        .zip(candles_1h.last())
        .map(|(bar15, bar1h)| bar15.close < ema_15m && bar1h.close < ema_1h)
        .unwrap_or(false);

    let strong_1h_short_regime = build_strong_1h_short_regime(&candles_1h, config);
    let short_entry_trigger = short_entry_trigger(klines_5m, rsi_5m, atr_5m, config);

    Ok(ShortLadderSignalSnapshot {
        current_price: last.close,
        atr_5m,
        rsi_5m,
        close_time: last.close_time,
        short_regime,
        strong_1h_short_regime,
        short_entry_trigger,
    })
}

pub fn is_in_session(
    timestamp: chrono::DateTime<chrono::Utc>,
    config: &ShortLadderLiveConfig,
) -> bool {
    use chrono::Timelike;

    let hour = timestamp.hour();
    let start = config.data.session_start_hour_utc;
    let end = config.data.session_end_hour_utc;
    if start <= end {
        hour >= start && hour < end
    } else {
        hour >= start || hour < end
    }
}

fn short_entry_trigger(
    candles: &[Kline],
    rsi_now: f64,
    atr_now: f64,
    config: &ShortLadderLiveConfig,
) -> bool {
    if candles.len() < 2 || atr_now <= 0.0 || rsi_now < config.signal.entry_rsi_min {
        return false;
    }
    let previous = &candles[candles.len() - 2];
    let current = &candles[candles.len() - 1];
    if config.signal.entry_requires_lower_close && current.close >= previous.close {
        return false;
    }
    if config.signal.entry_requires_prior_high_sweep && current.high < previous.high {
        return false;
    }
    true
}

fn build_strong_1h_short_regime(candles_1h: &[Kline], config: &ShortLadderLiveConfig) -> bool {
    let lookback = config.signal.strong_1h_ema_slope_lookback;
    if lookback == 0 || candles_1h.len() <= lookback + config.signal.filter_1h_ema {
        return false;
    }
    let closes = candles_1h.iter().map(|item| item.close).collect::<Vec<_>>();
    let ema_now = calculate_ema(&closes, config.signal.filter_1h_ema).unwrap_or(0.0);
    let then_end = closes.len().saturating_sub(lookback);
    let ema_then = calculate_ema(&closes[..then_end], config.signal.filter_1h_ema).unwrap_or(0.0);
    if ema_now <= 0.0 || ema_then <= 0.0 {
        return false;
    }
    let slope_pct = (ema_now / ema_then - 1.0) * 100.0;
    candles_1h
        .last()
        .map(|bar| bar.close < ema_now && slope_pct <= config.signal.strong_1h_ema_slope_max_pct)
        .unwrap_or(false)
}

fn aggregate_klines(candles: &[Kline], chunk_size: usize, interval: &str) -> Vec<Kline> {
    candles
        .chunks_exact(chunk_size)
        .map(|chunk| {
            let first = chunk.first().expect("chunk has first candle");
            let last = chunk.last().expect("chunk has last candle");
            Kline {
                symbol: first.symbol.clone(),
                interval: interval.to_string(),
                open_time: first.open_time,
                close_time: last.close_time,
                open: first.open,
                high: chunk.iter().map(|bar| bar.high).fold(f64::MIN, f64::max),
                low: chunk.iter().map(|bar| bar.low).fold(f64::MAX, f64::min),
                close: last.close,
                volume: chunk.iter().map(|bar| bar.volume).sum(),
                quote_volume: chunk.iter().map(|bar| bar.quote_volume).sum(),
                trade_count: chunk.iter().map(|bar| bar.trade_count).sum(),
            }
        })
        .collect()
}
