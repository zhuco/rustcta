use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use log::{debug, info, warn};
use tokio::sync::Mutex;

use crate::core::types::Kline;
use crate::strategies::common::{
    publish_trend_inputs_with_timestamp, MarketRegime, TrendFibAnchors, TrendIndicatorSnapshot,
};
use crate::strategies::mean_reversion::config::SymbolConfig;
use crate::strategies::mean_reversion::indicators::{BollingerSnapshot, IndicatorOutputs};
use crate::strategies::mean_reversion::model::{SymbolSnapshot, VolumeSnapshot};
use crate::strategies::trend::config::{IndicatorConfig, MarketDataConfig};
use crate::utils::indicators::functions;
use crate::utils::indicators::{calculate_adx, calculate_atr, calculate_ema, calculate_rsi};

use super::market_feed::{CandleCache, CandleEvent, CandleInterval, CandleSnapshot};

const MIN_EVENT_INTERVAL: Duration = Duration::seconds(5);

#[derive(Clone)]
pub struct TrendIndicatorService {
    symbols: Arc<HashSet<String>>,
    candle_cache: Arc<CandleCache>,
    indicator_config: IndicatorConfig,
    market_data: MarketDataConfig,
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    running: Arc<AtomicBool>,
}

impl TrendIndicatorService {
    pub fn new(
        symbols: Vec<String>,
        candle_cache: Arc<CandleCache>,
        indicator_config: IndicatorConfig,
        market_data: MarketDataConfig,
    ) -> Self {
        Self {
            symbols: Arc::new(symbols.into_iter().collect()),
            candle_cache,
            indicator_config,
            market_data,
            handles: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let mut receiver = self.candle_cache.subscribe();
        let service = self.clone();
        let handle = tokio::spawn(async move {
            while service.running.load(Ordering::SeqCst) {
                match receiver.recv().await {
                    Ok(event) => {
                        if let Err(err) = service.handle_event(event).await {
                            warn!("IndicatorService: 处理事件失败: {}", err);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        warn!("IndicatorService: 丢失 {} 条行情事件", count);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        self.handles.lock().await.push(handle);
        info!("IndicatorService: 已启动");
        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
    }

    async fn handle_event(&self, event: CandleEvent) -> Result<()> {
        if !self.symbols.contains(&event.symbol) {
            return Ok(());
        }

        if !event.is_final && event.interval != CandleInterval::OneMinute {
            return Ok(());
        }

        let Some(snapshot) = self.candle_cache.snapshot(&event.symbol).await else {
            return Ok(());
        };

        if !self.has_minimum_history(&snapshot) {
            debug!(
                "IndicatorService: {} 数据深度不足，等待更多K线",
                event.symbol
            );
            return Ok(());
        }

        let symbol_snapshot = self.build_symbol_snapshot(&event.symbol, &snapshot);
        let indicator_outputs = self.compute_indicator_outputs(&snapshot)?;
        let trend_snapshot = self.compute_trend_snapshot(&snapshot, &indicator_outputs, &event);

        publish_trend_inputs_with_timestamp(
            &event.symbol,
            &symbol_snapshot,
            &indicator_outputs,
            Some(trend_snapshot),
            event.kline.close_time,
        );

        Ok(())
    }

    fn has_minimum_history(&self, snapshot: &CandleSnapshot) -> bool {
        snapshot.one_minute.len() >= self.market_data.min_one_minute_bars
            && snapshot.five_minutes.len() >= self.market_data.min_five_minute_bars
            && snapshot.fifteen_minutes.len() >= self.market_data.min_fifteen_minute_bars
            && (!self.requires_one_hour_history()
                || snapshot.one_hour.len() >= self.market_data.min_one_hour_bars)
    }

    fn build_symbol_snapshot(&self, symbol: &str, candles: &CandleSnapshot) -> SymbolSnapshot {
        let last_price = candles
            .one_minute
            .last()
            .or_else(|| candles.five_minutes.last())
            .or_else(|| candles.fifteen_minutes.last())
            .or_else(|| candles.one_hour.last())
            .map(|k| k.close)
            .unwrap_or(0.0);

        let config = SymbolConfig {
            symbol: symbol.to_string(),
            enabled: true,
            min_quote_volume_5m: 0.0,
            depth_multiplier: 1.0,
            depth_levels: 5,
            enforce_spread_rule: false,
            blackout_windows: Vec::new(),
            overrides: HashMap::new(),
            allow_short: Some(true),
        };

        SymbolSnapshot {
            config,
            one_minute: candles.one_minute.clone(),
            five_minute: candles.five_minutes.clone(),
            fifteen_minute: candles.fifteen_minutes.clone(),
            one_hour: candles.one_hour.clone(),
            bbw_history: vec![0.0; 64],
            mid_history: vec![last_price; 64],
            sigma_history: vec![1.0; 64],
            long_position: None,
            short_position: None,
            last_depth: None,
            last_volume: Some(VolumeSnapshot {
                window_minutes: 5,
                quote_volume: candles
                    .five_minutes
                    .iter()
                    .rev()
                    .take(12)
                    .map(|k| k.quote_volume)
                    .sum(),
                timestamp: Utc::now(),
            }),
            frozen: false,
        }
    }

    fn requires_one_hour_history(&self) -> bool {
        let uses_1h = |tf: &str| tf.eq_ignore_ascii_case("1h");
        uses_1h(&self.indicator_config.primary_timeframe)
            || uses_1h(&self.indicator_config.secondary_timeframe)
            || uses_1h(&self.indicator_config.trigger_timeframe)
    }

    fn compute_indicator_outputs(&self, snapshot: &CandleSnapshot) -> Result<IndicatorOutputs> {
        let closes_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.close).collect();
        let highs_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.high).collect();
        let lows_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.low).collect();
        let closes_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.close).collect();
        let highs_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.high).collect();
        let lows_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.low).collect();
        let last_price = closes_5m.last().copied().unwrap_or_default();

        let boll_5m = self.compute_bollinger(&closes_5m, last_price)?;
        let boll_15m = self.compute_bollinger(&closes_15m, last_price)?;

        let atr = calculate_atr(
            &highs_5m,
            &lows_5m,
            &closes_5m,
            self.indicator_config.atr_period,
        )
        .unwrap_or(0.0);
        let rsi = calculate_rsi(&closes_5m, self.indicator_config.rsi_period).unwrap_or(50.0);
        let adx = calculate_adx(
            &highs_15m,
            &lows_15m,
            &closes_15m,
            self.indicator_config.adx_period,
        )
        .unwrap_or(20.0);

        let bbw = if boll_15m.middle.abs() > f64::EPSILON {
            (boll_15m.upper - boll_15m.lower).abs() / boll_15m.middle.abs()
        } else {
            0.0
        };

        Ok(IndicatorOutputs {
            timestamp: snapshot
                .fifteen_minutes
                .last()
                .map(|k| k.close_time)
                .unwrap_or_else(Utc::now),
            last_price,
            bollinger_5m: boll_5m,
            bollinger_15m: boll_15m,
            rsi,
            atr,
            adx,
            bbw,
            bbw_percentile: 0.5,
            slope_metric: self.compute_slope_metric(&closes_15m),
            choppiness: None,
            recent_volume_quote: snapshot
                .five_minutes
                .iter()
                .rev()
                .take(12)
                .map(|k| k.quote_volume)
                .sum(),
            volume_window_minutes: 60,
        })
    }

    fn compute_trend_snapshot(
        &self,
        snapshot: &CandleSnapshot,
        indicators: &IndicatorOutputs,
        event: &CandleEvent,
    ) -> TrendIndicatorSnapshot {
        let closes_1m: Vec<f64> = snapshot.one_minute.iter().map(|k| k.close).collect();
        let closes_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.close).collect();

        let ema_fast = calculate_ema(&closes_1m, self.indicator_config.fast_ema)
            .unwrap_or(indicators.last_price);
        let ema_slow = calculate_ema(&closes_1m, self.indicator_config.slow_ema)
            .unwrap_or(indicators.last_price);

        let ema_slope_1m = self.compute_slope_metric(&closes_1m);
        let ema_slope_5m = self.compute_slope_metric(&closes_5m);

        let highs_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.high).collect();
        let lows_5m: Vec<f64> = snapshot.five_minutes.iter().map(|k| k.low).collect();
        let atr_5m = calculate_atr(
            &highs_5m,
            &lows_5m,
            &closes_5m,
            self.indicator_config.atr_period,
        )
        .unwrap_or(0.0);

        let highs_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.high).collect();
        let lows_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.low).collect();
        let closes_15m: Vec<f64> = snapshot.fifteen_minutes.iter().map(|k| k.close).collect();
        let atr_15m = calculate_atr(
            &highs_15m,
            &lows_15m,
            &closes_15m,
            self.indicator_config.atr_period,
        )
        .unwrap_or(0.0);

        let fib_anchors = self.compute_fib_anchors(&snapshot.five_minutes);
        let ofi = self.compute_ofi(&snapshot.one_minute);
        let orderbook_tilt = self.compute_orderbook_tilt(&snapshot.one_minute);
        let data_quality = self.compute_data_quality(snapshot, event);
        let regime = self.classify_regime(ema_slope_5m, atr_5m, indicators.bbw);

        TrendIndicatorSnapshot {
            ema_fast,
            ema_slow,
            ema_slope_1m,
            ema_slope_5m,
            atr_5m,
            atr_15m,
            boll_mid: indicators.bollinger_5m.middle,
            boll_bandwidth: indicators.bbw,
            fib_anchors,
            ofi,
            orderbook_imbalance: orderbook_tilt,
            data_quality_score: data_quality,
            regime,
            updated_at: event.kline.close_time,
        }
    }

    fn compute_bollinger(&self, closes: &[f64], fallback: f64) -> Result<BollingerSnapshot> {
        if closes.len() < 5 {
            return Ok(BollingerSnapshot {
                upper: fallback,
                middle: fallback,
                lower: fallback,
                sigma: 0.0,
                band_percent: 0.5,
                z_score: 0.0,
            });
        }

        let (upper, middle, lower) = functions::bollinger_bands(
            closes,
            self.indicator_config.bb_period,
            self.indicator_config.bb_std,
        )
        .ok_or_else(|| anyhow!("bollinger calculation failed"))?;

        let sigma = (upper - middle).abs() / self.indicator_config.bb_std.max(1e-6);
        let denom = (upper - lower).abs().max(1e-6);
        let band_percent = ((fallout(closes) - lower) / denom).clamp(0.0, 1.0);
        let z_score = if sigma.abs() < 1e-6 {
            0.0
        } else {
            (fallout(closes) - middle) / sigma
        };

        Ok(BollingerSnapshot {
            upper,
            middle,
            lower,
            sigma,
            band_percent,
            z_score,
        })
    }

    fn compute_slope_metric(&self, closes: &[f64]) -> f64 {
        if closes.len() < 2 {
            return 0.0;
        }
        let latest = closes.last().copied().unwrap_or(0.0);
        let earliest = closes
            .iter()
            .rev()
            .nth(self.indicator_config.fast_ema.min(closes.len()) - 1)
            .copied()
            .unwrap_or(latest);
        if earliest.abs() < f64::EPSILON {
            return 0.0;
        }
        ((latest - earliest) / earliest).clamp(-1.0, 1.0)
    }

    fn compute_fib_anchors(&self, candles: &[Kline]) -> TrendFibAnchors {
        let lookback = candles.len().min(120);
        if lookback == 0 {
            return TrendFibAnchors::default();
        }
        let slice = &candles[candles.len() - lookback..];
        let swing_high = slice.iter().map(|c| c.high).fold(f64::MIN, f64::max);
        let swing_low = slice.iter().map(|c| c.low).fold(f64::MAX, f64::min);
        let range = (swing_high - swing_low).max(1e-6);

        TrendFibAnchors {
            swing_high,
            swing_low,
            level_382: swing_high - range * 0.382,
            level_500: (swing_high + swing_low) * 0.5,
            level_618: swing_high - range * 0.618,
        }
    }

    fn compute_ofi(&self, candles: &[Kline]) -> f64 {
        let window = candles.iter().rev().take(5);
        let mut imbalance = 0.0;
        for candle in window {
            let range = (candle.high - candle.low).max(1e-6);
            let direction = (candle.close - candle.open) / range;
            imbalance += direction * candle.volume;
        }
        imbalance
    }

    fn compute_orderbook_tilt(&self, candles: &[Kline]) -> f64 {
        if candles.len() < 3 {
            return 0.0;
        }
        let up_moves = candles
            .iter()
            .rev()
            .take(10)
            .filter(|c| c.close >= c.open)
            .count() as f64;
        let down_moves = candles
            .iter()
            .rev()
            .take(10)
            .filter(|c| c.close < c.open)
            .count() as f64;
        if up_moves + down_moves == 0.0 {
            return 0.0;
        }
        (up_moves - down_moves) / (up_moves + down_moves)
    }

    fn compute_data_quality(&self, snapshot: &CandleSnapshot, event: &CandleEvent) -> f64 {
        let mut score: f64 = 100.0;
        if event.latency_ms > self.market_data.max_data_lag_ms as i64 {
            score -= 15.0;
        }
        if snapshot.one_minute.len() < self.market_data.min_one_minute_bars {
            score -= 25.0;
        }
        if snapshot.five_minutes.len() < self.market_data.min_five_minute_bars {
            score -= 15.0;
        }
        if snapshot.fifteen_minutes.len() < self.market_data.min_fifteen_minute_bars {
            score -= 10.0;
        }
        if self.requires_one_hour_history()
            && snapshot.one_hour.len() < self.market_data.min_one_hour_bars
        {
            score -= 15.0;
        }

        if let Some(last) = snapshot.one_minute.last() {
            if Utc::now() - last.close_time > MIN_EVENT_INTERVAL {
                score -= 20.0;
            }
        }

        score.clamp(0.0, 100.0)
    }

    fn classify_regime(&self, slope: f64, atr: f64, bbw: f64) -> MarketRegime {
        let slope_abs = slope.abs();
        let normalized_atr = atr.max(1e-6);

        if slope_abs > 0.015 && bbw < 0.08 {
            MarketRegime::Trending
        } else if normalized_atr > 0.02 {
            MarketRegime::Extreme
        } else {
            MarketRegime::Ranging
        }
    }
}

fn fallout(values: &[f64]) -> f64 {
    values.last().copied().unwrap_or_default()
}
