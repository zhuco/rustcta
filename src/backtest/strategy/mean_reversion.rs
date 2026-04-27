use std::collections::{BTreeMap, VecDeque};

use anyhow::Result;
use chrono::{DateTime, NaiveTime, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::schema::{BacktestEvent, KlineEvent};
use crate::backtest::strategy::{BacktestStrategy, StrategySignal};
use crate::core::types::{Kline, OrderSide};
use crate::strategies::mean_reversion::config::{SymbolConfig, TradingSession};
use crate::strategies::mean_reversion::indicators::{compute_indicators, IndicatorOutputs};
use crate::strategies::mean_reversion::model::SymbolState;
use crate::strategies::mean_reversion::MeanReversionConfig;
use crate::utils::indicators::functions;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MeanReversionSymbolSummary {
    pub processed_1m_bars: usize,
    pub derived_5m_bars: usize,
    pub derived_15m_bars: usize,
    pub derived_1h_bars: usize,
    pub emitted_signals: usize,
    pub last_signal_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MeanReversionBacktestSummary {
    pub strategy: String,
    pub processed_events: usize,
    pub ignored_events: usize,
    pub emitted_signals: usize,
    pub by_symbol: BTreeMap<String, MeanReversionSymbolSummary>,
    pub signals: Vec<StrategySignal>,
}

#[derive(Debug)]
struct OfflineSymbolContext {
    state: SymbolState,
    summary: MeanReversionSymbolSummary,
}

#[derive(Debug)]
pub struct MeanReversionBacktestStrategy {
    config: MeanReversionConfig,
    symbols: BTreeMap<String, OfflineSymbolContext>,
    processed_events: usize,
    ignored_events: usize,
    signals: Vec<StrategySignal>,
}

impl MeanReversionBacktestStrategy {
    pub fn new(config: MeanReversionConfig) -> Result<Self> {
        let mut symbols = BTreeMap::new();
        for symbol in &config.symbols {
            if !symbol.enabled {
                continue;
            }

            symbols.insert(
                symbol.symbol.clone(),
                OfflineSymbolContext {
                    state: SymbolState::new(symbol.clone(), &config.cache),
                    summary: MeanReversionSymbolSummary::default(),
                },
            );
        }

        Ok(Self {
            config,
            symbols,
            processed_events: 0,
            ignored_events: 0,
            signals: Vec::new(),
        })
    }

    pub fn on_kline(&mut self, kline: &Kline) -> Result<Vec<StrategySignal>> {
        self.on_kline_event(&KlineEvent {
            exchange: "offline".to_string(),
            market: "futures".to_string(),
            symbol: kline.symbol.clone(),
            interval: kline.interval.clone(),
            exchange_ts: kline.close_time,
            logical_ts: kline.close_time,
            kline: kline.clone(),
        })
    }

    fn on_kline_event(&mut self, event: &KlineEvent) -> Result<Vec<StrategySignal>> {
        let Some(context) = self.symbols.get_mut(&event.symbol) else {
            self.ignored_events += 1;
            return Ok(Vec::new());
        };

        self.processed_events += 1;

        let should_evaluate = apply_kline_event(&self.config, context, &event.kline)?;
        if !should_evaluate {
            return Ok(Vec::new());
        }

        let signals = evaluate_symbol(&self.config, &event.symbol, context)?;
        if signals.is_empty() {
            return Ok(Vec::new());
        }

        context.summary.emitted_signals += signals.len();
        context.summary.last_signal_ts = signals.last().map(|signal| signal.logical_ts);
        self.signals.extend(signals.clone());
        Ok(signals)
    }
}

impl BacktestStrategy for MeanReversionBacktestStrategy {
    type Summary = MeanReversionBacktestSummary;

    fn on_event(&mut self, event: &BacktestEvent) -> Result<Vec<StrategySignal>> {
        match event {
            BacktestEvent::Kline(kline_event) => self.on_kline_event(kline_event),
            _ => {
                self.ignored_events += 1;
                Ok(Vec::new())
            }
        }
    }

    fn summary(&self) -> Self::Summary {
        let by_symbol = self
            .symbols
            .iter()
            .map(|(symbol, context)| (symbol.clone(), context.summary.clone()))
            .collect();

        MeanReversionBacktestSummary {
            strategy: self.config.strategy.name.clone(),
            processed_events: self.processed_events,
            ignored_events: self.ignored_events,
            emitted_signals: self.signals.len(),
            by_symbol,
            signals: self.signals.clone(),
        }
    }
}

fn apply_kline_event(
    config: &MeanReversionConfig,
    context: &mut OfflineSymbolContext,
    kline: &Kline,
) -> Result<bool> {
    let cache = &config.cache;
    match kline.interval.as_str() {
        "1m" => {
            context.summary.processed_1m_bars += 1;
            context.state.push_one_minute(kline.clone(), cache);

            let new_five_minute = maybe_rollup_bar(
                &mut context.state,
                &mut context.summary.derived_5m_bars,
                5,
                "5m",
                push_five_minute,
                cache,
            )?;
            let evaluate = maybe_rollup_bar(
                &mut context.state,
                &mut context.summary.derived_15m_bars,
                15,
                "15m",
                push_fifteen_minute,
                cache,
            )?;
            let new_hour = maybe_rollup_bar(
                &mut context.state,
                &mut context.summary.derived_1h_bars,
                60,
                "1h",
                push_one_hour,
                cache,
            )?;

            if new_hour {
                update_bbw_history(&mut context.state, config);
            }

            Ok(new_five_minute || evaluate)
        }
        "5m" => {
            let inserted = push_five_minute(&mut context.state, kline.clone(), cache);
            context.summary.derived_5m_bars += inserted as usize;
            if inserted {
                rollup_from_five_minute(config, context, cache)?;
            }
            Ok(inserted)
        }
        "15m" => {
            let inserted = push_fifteen_minute(&mut context.state, kline.clone(), cache);
            context.summary.derived_15m_bars += inserted as usize;
            Ok(inserted)
        }
        "1h" => {
            let inserted = push_one_hour(&mut context.state, kline.clone(), cache);
            context.summary.derived_1h_bars += inserted as usize;
            if inserted {
                update_bbw_history(&mut context.state, config);
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn rollup_from_five_minute(
    config: &MeanReversionConfig,
    context: &mut OfflineSymbolContext,
    cache: &crate::strategies::mean_reversion::config::CacheConfig,
) -> Result<()> {
    if let Some(bar) = aggregate_recent_bar(&context.state.five_minute, 3, "15m", 300) {
        if push_fifteen_minute(&mut context.state, bar, cache) {
            context.summary.derived_15m_bars += 1;
        }
    }

    if let Some(bar) = aggregate_recent_bar(&context.state.five_minute, 12, "1h", 300) {
        if push_one_hour(&mut context.state, bar, cache) {
            context.summary.derived_1h_bars += 1;
            update_bbw_history(&mut context.state, config);
        }
    }

    Ok(())
}

fn maybe_rollup_bar(
    state: &mut SymbolState,
    counter: &mut usize,
    minutes: usize,
    interval: &str,
    push: fn(
        &mut SymbolState,
        Kline,
        &crate::strategies::mean_reversion::config::CacheConfig,
    ) -> bool,
    cache: &crate::strategies::mean_reversion::config::CacheConfig,
) -> Result<bool> {
    let Some(bar) = aggregate_recent_one_minute_bar(&state.one_minute, minutes, interval) else {
        return Ok(false);
    };

    let inserted = push(state, bar, cache);
    if inserted {
        *counter += 1;
    }

    Ok(inserted)
}

fn aggregate_recent_one_minute_bar(
    bars: &VecDeque<Kline>,
    minutes: usize,
    interval: &str,
) -> Option<Kline> {
    aggregate_recent_bar(bars, minutes, interval, 60)
}

fn aggregate_recent_bar(
    bars: &VecDeque<Kline>,
    window_size: usize,
    interval: &str,
    source_seconds: i64,
) -> Option<Kline> {
    if bars.len() < window_size || window_size == 0 || source_seconds <= 0 {
        return None;
    }

    let mut window = bars
        .iter()
        .rev()
        .take(window_size)
        .cloned()
        .collect::<Vec<_>>();
    window.reverse();

    let last = window.last()?;
    let period_seconds = (window_size as i64) * source_seconds;
    if (last.close_time.timestamp() + 1) % period_seconds != 0 {
        return None;
    }

    if !window.windows(2).all(|pair| {
        (pair[1].close_time - pair[0].close_time).num_seconds() == source_seconds
            && pair[1].open_time > pair[0].open_time
    }) {
        return None;
    }

    let first = window.first()?;
    Some(Kline {
        symbol: first.symbol.clone(),
        interval: interval.to_string(),
        open_time: first.open_time,
        close_time: last.close_time,
        open: first.open,
        high: window
            .iter()
            .map(|bar| bar.high)
            .fold(f64::MIN, |acc, value| acc.max(value)),
        low: window
            .iter()
            .map(|bar| bar.low)
            .fold(f64::MAX, |acc, value| acc.min(value)),
        close: last.close,
        volume: window.iter().map(|bar| bar.volume).sum(),
        quote_volume: window.iter().map(|bar| bar.quote_volume).sum(),
        trade_count: window.iter().map(|bar| bar.trade_count).sum(),
    })
}

fn push_five_minute(
    state: &mut SymbolState,
    bar: Kline,
    cache: &crate::strategies::mean_reversion::config::CacheConfig,
) -> bool {
    let is_new = state
        .last_five_minute_close
        .map(|close_time| close_time < bar.close_time)
        .unwrap_or(true);
    state.push_five_minute(bar, cache);
    is_new
}

fn push_fifteen_minute(
    state: &mut SymbolState,
    bar: Kline,
    cache: &crate::strategies::mean_reversion::config::CacheConfig,
) -> bool {
    let is_new = state
        .last_fifteen_minute_close
        .map(|close_time| close_time < bar.close_time)
        .unwrap_or(true);
    state.push_fifteen_minute(bar, cache);
    is_new
}

fn push_one_hour(
    state: &mut SymbolState,
    bar: Kline,
    cache: &crate::strategies::mean_reversion::config::CacheConfig,
) -> bool {
    let is_new = state
        .last_one_hour_close
        .map(|close_time| close_time < bar.close_time)
        .unwrap_or(true);
    state.push_one_hour(bar, cache);
    is_new
}

fn update_bbw_history(state: &mut SymbolState, config: &MeanReversionConfig) {
    let period = config.indicators.bollinger.period;
    if state.one_hour.len() < period {
        return;
    }

    let closes = state
        .one_hour
        .iter()
        .rev()
        .take(period)
        .map(|bar| bar.close)
        .collect::<Vec<_>>();
    let mut closes = closes;
    closes.reverse();

    if let Some((upper, middle, lower)) =
        functions::bollinger_bands(&closes, period, config.indicators.bollinger.std_dev)
    {
        let sigma = (upper - middle) / config.indicators.bollinger.std_dev.max(1e-9);
        let bbw = if middle.abs() < 1e-9 {
            0.0
        } else {
            (upper - lower) / middle
        };
        state.update_bbw_series(bbw, middle, sigma);
    }
}

fn evaluate_symbol(
    config: &MeanReversionConfig,
    symbol: &str,
    context: &OfflineSymbolContext,
) -> Result<Vec<StrategySignal>> {
    let snapshot = context.state.snapshot();
    let indicators =
        match compute_indicators(&snapshot, &config.indicators, &config.indicators.volume) {
            Ok(outputs) => outputs,
            Err(_) => return Ok(Vec::new()),
        };

    if !is_in_trading_sessions(&snapshot.config.trading_sessions, indicators.timestamp) {
        return Ok(Vec::new());
    }

    if evaluate_range_conditions(config, &indicators) < 2 {
        return Ok(Vec::new());
    }

    let mut signals = Vec::new();
    let boll = &indicators.bollinger_5m;
    let boll_cfg = &config.indicators.bollinger;
    let rsi_cfg = &config.indicators.rsi;

    let long_entry =
        boll.band_percent <= boll_cfg.entry_band_pct_long || boll.z_score <= boll_cfg.entry_z_long;
    if long_entry && indicators.rsi <= rsi_cfg.long_threshold {
        signals.push(build_signal(
            config,
            symbol,
            OrderSide::Buy,
            &indicators,
            format!(
                "range-score={} band_percent={:.4} z_score={:.4} rsi={:.2}",
                evaluate_range_conditions(config, &indicators),
                boll.band_percent,
                boll.z_score,
                indicators.rsi
            ),
        ));
    }

    let allow_short = snapshot
        .config
        .allow_short
        .unwrap_or(config.account.allow_short)
        && config.account.allow_short;
    let short_entry = boll.band_percent >= boll_cfg.entry_band_pct_short
        || boll.z_score >= boll_cfg.entry_z_short;
    if allow_short && short_entry && indicators.rsi >= rsi_cfg.short_threshold {
        signals.push(build_signal(
            config,
            symbol,
            OrderSide::Sell,
            &indicators,
            format!(
                "range-score={} band_percent={:.4} z_score={:.4} rsi={:.2}",
                evaluate_range_conditions(config, &indicators),
                boll.band_percent,
                boll.z_score,
                indicators.rsi
            ),
        ));
    }

    Ok(signals)
}

fn build_signal(
    config: &MeanReversionConfig,
    symbol: &str,
    side: OrderSide,
    indicators: &IndicatorOutputs,
    reason: String,
) -> StrategySignal {
    StrategySignal {
        strategy: config.strategy.name.clone(),
        symbol: symbol.to_string(),
        side,
        logical_ts: indicators.timestamp,
        price: indicators.last_price,
        band_percent: indicators.bollinger_5m.band_percent,
        z_score: indicators.bollinger_5m.z_score,
        rsi: indicators.rsi,
        atr: indicators.atr,
        reason,
    }
}

fn evaluate_range_conditions(config: &MeanReversionConfig, indicators: &IndicatorOutputs) -> u8 {
    let mut passes = 0;

    if indicators.adx < config.indicators.adx.threshold {
        passes += 1;
    }
    if indicators.bbw_percentile < config.indicators.bbw.percentile {
        passes += 1;
    }
    if indicators.slope_metric < config.indicators.slope.threshold {
        passes += 1;
    }
    if let Some(choppiness) = indicators.choppiness {
        if let Some(choppiness_cfg) = &config.indicators.choppiness {
            if choppiness_cfg.enabled && choppiness >= choppiness_cfg.threshold {
                passes += 1;
            }
        }
    }

    passes
}

fn is_in_trading_sessions(sessions: &[TradingSession], timestamp: DateTime<Utc>) -> bool {
    if sessions.is_empty() {
        return true;
    }

    sessions
        .iter()
        .any(|session| trading_session_contains(session, timestamp))
}

fn trading_session_contains(session: &TradingSession, timestamp: DateTime<Utc>) -> bool {
    if session
        .timezone
        .as_deref()
        .is_some_and(|timezone| !timezone.eq_ignore_ascii_case("UTC"))
    {
        return false;
    }

    let Ok(start) = NaiveTime::parse_from_str(&session.start, "%H:%M") else {
        return false;
    };
    let Ok(end) = NaiveTime::parse_from_str(&session.end, "%H:%M") else {
        return false;
    };

    let Some(current) =
        NaiveTime::from_hms_opt(timestamp.hour(), timestamp.minute(), timestamp.second())
    else {
        return false;
    };

    if start <= end {
        current >= start && current < end
    } else {
        current >= start || current < end
    }
}
