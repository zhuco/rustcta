use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::utils::indicators::functions;

use super::config::{IndicatorConfig, VolumeConfig};
use super::model::SymbolSnapshot;

#[derive(Debug, Clone)]
pub struct BollingerSnapshot {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub sigma: f64,
    pub band_percent: f64,
    pub z_score: f64,
}

#[derive(Debug, Clone)]
pub struct IndicatorOutputs {
    pub timestamp: DateTime<Utc>,
    pub last_price: f64,
    pub bollinger_5m: BollingerSnapshot,
    pub bollinger_15m: BollingerSnapshot,
    pub rsi: f64,
    pub atr: f64,
    pub adx: f64,
    pub bbw: f64,
    pub bbw_percentile: f64,
    pub slope_metric: f64,
    pub choppiness: Option<f64>,
    pub recent_volume_quote: f64,
    pub volume_window_minutes: u64,
}

pub fn compute_indicators(
    snapshot: &SymbolSnapshot,
    cfg: &IndicatorConfig,
    volume_cfg: &VolumeConfig,
) -> Result<IndicatorOutputs> {
    if snapshot.five_minute.len() < cfg.bollinger.period + 2 {
        return Err(anyhow!("not enough 5m data"));
    }
    if snapshot.fifteen_minute.len() < cfg.bollinger.period + 2 {
        return Err(anyhow!("not enough 15m data"));
    }

    let closes_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.close).collect();
    let highs_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.high).collect();
    let lows_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.low).collect();

    let boll_5m =
        functions::bollinger_bands(&closes_5m, cfg.bollinger.period, cfg.bollinger.std_dev)
            .ok_or_else(|| anyhow!("bollinger calculation failed"))?;
    let sigma_5m = (boll_5m.0 - boll_5m.1) / cfg.bollinger.std_dev.max(1e-9);
    let last_price = closes_5m.last().copied().unwrap_or_default();
    let denom_5m = (boll_5m.0 - boll_5m.2).max(1e-9);
    let band_percent = (last_price - boll_5m.2) / denom_5m;
    let z_score = if sigma_5m.abs() < 1e-9 {
        0.0
    } else {
        (last_price - boll_5m.1) / sigma_5m
    };

    let bollinger_5m = BollingerSnapshot {
        upper: boll_5m.0,
        middle: boll_5m.1,
        lower: boll_5m.2,
        sigma: sigma_5m,
        band_percent,
        z_score,
    };

    let atr = functions::atr(&highs_5m, &lows_5m, &closes_5m, cfg.atr.period)
        .ok_or_else(|| anyhow!("ATR calculation failed"))?;
    let rsi = functions::rsi(&closes_5m, cfg.rsi.period)
        .ok_or_else(|| anyhow!("RSI calculation failed"))?;

    let closes_15m: Vec<f64> = snapshot.fifteen_minute.iter().map(|k| k.close).collect();
    let highs_15m: Vec<f64> = snapshot.fifteen_minute.iter().map(|k| k.high).collect();
    let lows_15m: Vec<f64> = snapshot.fifteen_minute.iter().map(|k| k.low).collect();

    let boll_15m =
        functions::bollinger_bands(&closes_15m, cfg.bollinger.period, cfg.bollinger.std_dev)
            .ok_or_else(|| anyhow!("15m bollinger calculation failed"))?;
    let sigma_15m = (boll_15m.0 - boll_15m.1) / cfg.bollinger.std_dev.max(1e-9);
    let denom_15m = (boll_15m.0 - boll_15m.2).max(1e-9);
    let last_price_15m = closes_15m.last().copied().unwrap_or(last_price);
    let band_percent_15m = (last_price_15m - boll_15m.2) / denom_15m;
    let z_score_15m = if sigma_15m.abs() < 1e-9 {
        0.0
    } else {
        (last_price_15m - boll_15m.1) / sigma_15m
    };

    let bollinger_15m = BollingerSnapshot {
        upper: boll_15m.0,
        middle: boll_15m.1,
        lower: boll_15m.2,
        sigma: sigma_15m,
        band_percent: band_percent_15m,
        z_score: z_score_15m,
    };

    let bbw = if boll_15m.1.abs() < 1e-9 {
        0.0
    } else {
        (boll_15m.0 - boll_15m.2) / boll_15m.1
    };
    let bbw_percentile = percentile_rank(bbw, &VecDeque::from(snapshot.bbw_history.clone()));

    let slope_metric = compute_slope_metric(
        &VecDeque::from(snapshot.mid_history.clone()),
        &VecDeque::from(snapshot.sigma_history.clone()),
        cfg,
    );

    let adx =
        functions::adx(&highs_15m, &lows_15m, &closes_15m, cfg.adx.period).unwrap_or_default();

    let choppiness = cfg
        .choppiness
        .as_ref()
        .filter(|c| c.enabled)
        .and_then(|c| compute_choppiness(&highs_15m, &lows_15m, &closes_15m, c.lookback))
        .filter(|ci| !ci.is_nan());

    let volume_window_minutes = volume_cfg.lookback_minutes.max(5);
    let volume_bars = ((volume_window_minutes as f64) / 5.0).ceil() as usize;
    let recent_volume_quote = snapshot
        .five_minute
        .iter()
        .rev()
        .take(volume_bars)
        .map(|k| k.quote_volume)
        .sum();

    Ok(IndicatorOutputs {
        timestamp: snapshot
            .five_minute
            .last()
            .map(|k| k.close_time)
            .unwrap_or_else(Utc::now),
        last_price,
        bollinger_5m,
        bollinger_15m,
        rsi,
        atr,
        adx,
        bbw,
        bbw_percentile,
        slope_metric,
        choppiness,
        recent_volume_quote,
        volume_window_minutes,
    })
}

fn compute_slope_metric(
    mid_history: &VecDeque<f64>,
    sigma_history: &VecDeque<f64>,
    cfg: &IndicatorConfig,
) -> f64 {
    if mid_history.len() < 2 || sigma_history.is_empty() {
        return 1.0;
    }
    let latest = mid_history.back().copied().unwrap_or_default();
    let prev_idx = mid_history.len().saturating_sub(cfg.slope.lookback.max(1));
    let prev = mid_history.get(prev_idx).copied().unwrap_or(latest);
    let sigma = sigma_history.back().copied().unwrap_or(1.0).abs().max(1e-9);
    ((latest - prev).abs() / sigma).abs()
}

fn compute_choppiness(highs: &[f64], lows: &[f64], closes: &[f64], lookback: usize) -> Option<f64> {
    if highs.len() < lookback + 1 || lookback < 2 {
        return None;
    }
    let start = highs.len() - lookback;
    let mut tr_sum = 0.0;
    for i in start..highs.len() {
        tr_sum += true_range(highs, lows, closes, i);
    }
    let slice_high = highs[start..].iter().fold(f64::MIN, |a, &b| a.max(b));
    let slice_low = lows[start..].iter().fold(f64::MAX, |a, &b| a.min(b));
    let denom = (slice_high - slice_low).abs().max(1e-9);
    let ratio = tr_sum / denom;
    Some(100.0 * (ratio.log10() / (lookback as f64).log10()))
}

fn true_range(highs: &[f64], lows: &[f64], closes: &[f64], idx: usize) -> f64 {
    if idx == 0 {
        return highs[0] - lows[0];
    }
    let high_low = highs[idx] - lows[idx];
    let high_close = (highs[idx] - closes[idx - 1]).abs();
    let low_close = (lows[idx] - closes[idx - 1]).abs();
    high_low.max(high_close).max(low_close)
}

fn percentile_rank(value: f64, history: &VecDeque<f64>) -> f64 {
    if history.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = history.iter().copied().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = sorted.iter().filter(|&&val| val <= value).count() as f64 / sorted.len() as f64;
    rank.clamp(0.0, 1.0)
}
