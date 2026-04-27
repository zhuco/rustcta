use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::factors::{TrendFactorRunDefinition, TrendFactorScanSpec};
use crate::backtest::indicators::{
    adx, atr, atr_percentile, bollinger_width, choppiness_index, donchian_high, donchian_low, ema,
    ema_slope, keltner_channel, macd_histogram, mfi, roc, rsi, stoch_rsi, supertrend_direction,
    volume_ratio, volume_zscore, vwap_deviation, KeltnerChannelSeries,
};
use crate::backtest::scoring::{score_trend_run, TrendScore, TrendScoreInput};
use crate::core::types::{Kline, OrderSide};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorRunSummary {
    pub run_id: usize,
    pub symbol: String,
    pub strategy_name: String,
    pub parameters: BTreeMap<String, f64>,
    pub flags: BTreeMap<String, bool>,
    pub candles: usize,
    pub trades: usize,
    pub wins: usize,
    pub win_rate_pct: f64,
    pub final_equity: f64,
    pub net_pnl: f64,
    pub roi_pct: f64,
    pub max_drawdown_pct: f64,
    pub profit_factor: f64,
    pub avg_trade_pnl: f64,
    pub exposure_pct: f64,
    pub buy_hold_pct: f64,
    pub score: TrendScore,
    pub cost_scenarios: Vec<TrendFactorCostScenarioSummary>,
    pub live_candidate: bool,
    pub candidate_rejections: Vec<String>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorCostScenarioSummary {
    pub name: String,
    pub fee_rate: f64,
    pub slippage_rate: f64,
    pub final_equity: f64,
    pub net_pnl: f64,
    pub roi_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorTrade {
    pub side: OrderSide,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub gross_pnl: f64,
    pub fees_paid: f64,
    pub net_pnl: f64,
    pub exit_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorRunReport {
    pub summary: TrendFactorRunSummary,
    pub trades: Vec<TrendFactorTrade>,
}

#[derive(Debug, Clone)]
struct PreparedIndicators {
    closes: Vec<f64>,
    ema_cache: BTreeMap<usize, Vec<Option<f64>>>,
    ema_slope_cache: BTreeMap<String, Vec<Option<f64>>>,
    atr_cache: BTreeMap<usize, Vec<Option<f64>>>,
    adx_cache: BTreeMap<usize, Vec<Option<f64>>>,
    rsi_14: Vec<Option<f64>>,
    macd_histogram: Vec<Option<f64>>,
    donchian_high_cache: BTreeMap<usize, Vec<Option<f64>>>,
    donchian_low_cache: BTreeMap<usize, Vec<Option<f64>>>,
    keltner_cache: BTreeMap<String, KeltnerChannelSeries>,
    bb_width_cache: BTreeMap<usize, Vec<Option<f64>>>,
    volume_ratio_cache: BTreeMap<usize, Vec<Option<f64>>>,
    volume_zscore_cache: BTreeMap<usize, Vec<Option<f64>>>,
    vwap_deviation_cache: BTreeMap<usize, Vec<Option<f64>>>,
    mfi_cache: BTreeMap<usize, Vec<Option<f64>>>,
    stoch_rsi_cache: BTreeMap<String, Vec<Option<f64>>>,
    choppiness_cache: BTreeMap<usize, Vec<Option<f64>>>,
    atr_percentile_cache: BTreeMap<String, Vec<Option<f64>>>,
    roc_cache: BTreeMap<usize, Vec<Option<f64>>>,
    supertrend_cache: BTreeMap<String, Vec<Option<i8>>>,
}

#[derive(Debug, Clone)]
struct TrendPosition {
    side: OrderSide,
    entry_time: DateTime<Utc>,
    entry_index: usize,
    entry_price: f64,
    quantity: f64,
    stop_price: f64,
    target_price: f64,
    entry_fee: f64,
}

pub fn run_trend_factor_definition(
    symbol: &str,
    candles: &[Kline],
    definition: &TrendFactorRunDefinition,
    spec: &TrendFactorScanSpec,
) -> Result<TrendFactorRunReport> {
    if candles.is_empty() {
        return Err(anyhow!("trend factor dataset is empty"));
    }
    let indicators = prepare_indicators(candles, definition);
    let mut cash = spec.initial_equity;
    let mut position: Option<TrendPosition> = None;
    let mut trades = Vec::new();
    let mut equity_curve = vec![spec.initial_equity];
    let mut wins = 0usize;
    let mut gross_profit = 0.0;
    let mut gross_loss = 0.0;
    let mut exposure_bars = 0usize;
    let warmup = warmup_bars(definition).max(40) + 2;

    for index in warmup..candles.len() {
        let candle = &candles[index];
        let long_signal = signal_passes(definition, &indicators, candles, index, OrderSide::Buy);
        let short_signal = signal_passes(definition, &indicators, candles, index, OrderSide::Sell);

        if let Some(open_position) = position.as_ref() {
            exposure_bars += 1;
            if let Some((exit_price, exit_reason)) = exit_decision(
                open_position,
                candle,
                long_signal,
                short_signal,
                index,
                parameter_usize(definition, "exit.time_stop_bars", 288),
                spec.slippage_rate,
                flag(definition, "exit.reverse_signal_exit"),
            ) {
                let current = position.take().expect("position should exist");
                let gross_pnl = match current.side {
                    OrderSide::Buy => (exit_price - current.entry_price) * current.quantity,
                    OrderSide::Sell => (current.entry_price - exit_price) * current.quantity,
                };
                let exit_fee = (exit_price * current.quantity).abs() * spec.fee_rate;
                let fees_paid = current.entry_fee + exit_fee;
                let net_pnl = gross_pnl - exit_fee;
                cash += gross_pnl - exit_fee;
                if net_pnl > 0.0 {
                    wins += 1;
                    gross_profit += net_pnl;
                } else {
                    gross_loss += net_pnl.abs();
                }
                trades.push(TrendFactorTrade {
                    side: current.side,
                    entry_time: current.entry_time,
                    exit_time: candle.close_time,
                    entry_price: current.entry_price,
                    exit_price,
                    quantity: current.quantity,
                    gross_pnl,
                    fees_paid,
                    net_pnl,
                    exit_reason,
                });
            }
        }

        if position.is_none() && (long_signal || short_signal) {
            let side = if long_signal {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let entry_price = match side {
                OrderSide::Buy => candle.close * (1.0 + spec.slippage_rate),
                OrderSide::Sell => candle.close * (1.0 - spec.slippage_rate),
            };
            let atr_period = parameter_usize(definition, "filter.atr_pct.period", 14)
                .max(parameter_usize(definition, "entry.supertrend.period", 10))
                .max(14);
            let atr_now = indicators
                .atr_cache
                .get(&atr_period)
                .and_then(|series| series[index])
                .unwrap_or(entry_price * 0.01);
            let atr_stop = parameter(definition, "exit.atr_stop", 2.0);
            let reward_r = parameter(definition, "exit.reward_r", 2.5);
            let risk = (atr_now * atr_stop).max(entry_price * 0.003);
            let stop_price = match side {
                OrderSide::Buy => entry_price - risk,
                OrderSide::Sell => entry_price + risk,
            };
            let target_price = match side {
                OrderSide::Buy => entry_price + risk * reward_r,
                OrderSide::Sell => entry_price - risk * reward_r,
            };
            let quantity = spec.trade_notional / entry_price;
            let entry_fee = spec.trade_notional * spec.fee_rate;
            cash -= entry_fee;
            position = Some(TrendPosition {
                side,
                entry_time: candle.close_time,
                entry_index: index,
                entry_price,
                quantity,
                stop_price,
                target_price,
                entry_fee,
            });
        }

        let mut mark_equity = cash;
        if let Some(open_position) = position.as_ref() {
            mark_equity += match open_position.side {
                OrderSide::Buy => {
                    (candle.close - open_position.entry_price) * open_position.quantity
                }
                OrderSide::Sell => {
                    (open_position.entry_price - candle.close) * open_position.quantity
                }
            };
        }
        equity_curve.push(mark_equity);
    }

    if let Some(open_position) = position.take() {
        let candle = candles.last().expect("candles should not be empty");
        let exit_price = match open_position.side {
            OrderSide::Buy => candle.close * (1.0 - spec.slippage_rate),
            OrderSide::Sell => candle.close * (1.0 + spec.slippage_rate),
        };
        let gross_pnl = match open_position.side {
            OrderSide::Buy => (exit_price - open_position.entry_price) * open_position.quantity,
            OrderSide::Sell => (open_position.entry_price - exit_price) * open_position.quantity,
        };
        let exit_fee = (exit_price * open_position.quantity).abs() * spec.fee_rate;
        let fees_paid = open_position.entry_fee + exit_fee;
        let net_pnl = gross_pnl - exit_fee;
        cash += gross_pnl - exit_fee;
        if net_pnl > 0.0 {
            wins += 1;
            gross_profit += net_pnl;
        } else {
            gross_loss += net_pnl.abs();
        }
        trades.push(TrendFactorTrade {
            side: open_position.side,
            entry_time: open_position.entry_time,
            exit_time: candle.close_time,
            entry_price: open_position.entry_price,
            exit_price,
            quantity: open_position.quantity,
            gross_pnl,
            fees_paid,
            net_pnl,
            exit_reason: "final".to_string(),
        });
        equity_curve.push(cash);
    }

    let final_equity = *equity_curve.last().unwrap_or(&spec.initial_equity);
    let trade_count = trades.len();
    let roi_pct = (final_equity / spec.initial_equity - 1.0) * 100.0;
    let max_drawdown_pct = max_drawdown_pct(&equity_curve);
    let profit_factor = if gross_loss > 0.0 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        999.0
    } else {
        0.0
    };
    let win_rate_pct = if trade_count == 0 {
        0.0
    } else {
        wins as f64 / trade_count as f64 * 100.0
    };
    let score = score_trend_run(TrendScoreInput {
        roi_pct,
        max_drawdown_pct,
        profit_factor,
        trades: trade_count,
        win_rate_pct,
        min_trades: spec.search.min_trades,
    });
    let cost_scenarios = spec
        .cost_scenarios
        .iter()
        .map(|scenario| TrendFactorCostScenarioSummary {
            name: scenario.name.clone(),
            fee_rate: scenario.fee_rate,
            slippage_rate: scenario.slippage_rate,
            final_equity: final_equity_under_costs(
                spec.initial_equity,
                &trades,
                scenario.fee_rate,
                scenario.slippage_rate,
            ),
            net_pnl: 0.0,
            roi_pct: 0.0,
        })
        .map(|mut summary| {
            summary.net_pnl = summary.final_equity - spec.initial_equity;
            summary.roi_pct = (summary.final_equity / spec.initial_equity - 1.0) * 100.0;
            summary
        })
        .collect::<Vec<_>>();
    let candidate_rejections = candidate_rejections(
        trade_count,
        roi_pct,
        max_drawdown_pct,
        profit_factor,
        &cost_scenarios,
        spec,
    );
    let live_candidate = candidate_rejections.is_empty();

    Ok(TrendFactorRunReport {
        summary: TrendFactorRunSummary {
            run_id: definition.run_id,
            symbol: symbol.to_string(),
            strategy_name: definition.strategy_name.clone(),
            parameters: definition.parameters.clone(),
            flags: definition.flags.clone(),
            candles: candles.len(),
            trades: trade_count,
            wins,
            win_rate_pct,
            final_equity,
            net_pnl: final_equity - spec.initial_equity,
            roi_pct,
            max_drawdown_pct,
            profit_factor,
            avg_trade_pnl: if trade_count == 0 {
                0.0
            } else {
                (final_equity - spec.initial_equity) / trade_count as f64
            },
            exposure_pct: exposure_bars as f64 / candles.len().max(1) as f64 * 100.0,
            buy_hold_pct: (candles.last().unwrap().close / candles.first().unwrap().close - 1.0)
                * 100.0,
            score,
            cost_scenarios,
            live_candidate,
            candidate_rejections,
            start: candles.first().unwrap().close_time,
            end: candles.last().unwrap().close_time,
        },
        trades,
    })
}

fn final_equity_under_costs(
    initial_equity: f64,
    trades: &[TrendFactorTrade],
    fee_rate: f64,
    slippage_rate: f64,
) -> f64 {
    let mut equity = initial_equity;
    for trade in trades {
        let adjusted_entry = match trade.side {
            OrderSide::Buy => trade.entry_price * (1.0 + slippage_rate),
            OrderSide::Sell => trade.entry_price * (1.0 - slippage_rate),
        };
        let adjusted_exit = match trade.side {
            OrderSide::Buy => trade.exit_price * (1.0 - slippage_rate),
            OrderSide::Sell => trade.exit_price * (1.0 + slippage_rate),
        };
        let gross = match trade.side {
            OrderSide::Buy => (adjusted_exit - adjusted_entry) * trade.quantity,
            OrderSide::Sell => (adjusted_entry - adjusted_exit) * trade.quantity,
        };
        let fees = (adjusted_entry * trade.quantity).abs() * fee_rate
            + (adjusted_exit * trade.quantity).abs() * fee_rate;
        equity += gross - fees;
    }
    equity
}

fn candidate_rejections(
    trades: usize,
    roi_pct: f64,
    max_drawdown_pct: f64,
    profit_factor: f64,
    cost_scenarios: &[TrendFactorCostScenarioSummary],
    spec: &TrendFactorScanSpec,
) -> Vec<String> {
    let mut rejections = Vec::new();
    let filter = &spec.candidate_filter;
    if trades < filter.min_trades {
        rejections.push("min_trades".to_string());
    }
    if profit_factor < filter.min_profit_factor {
        rejections.push("min_profit_factor".to_string());
    }
    if roi_pct < filter.min_roi_pct {
        rejections.push("min_roi_pct".to_string());
    }
    if max_drawdown_pct < filter.max_drawdown_pct {
        rejections.push("max_drawdown_pct".to_string());
    }
    let profitable_cost_scenarios = cost_scenarios
        .iter()
        .filter(|scenario| scenario.net_pnl > 0.0)
        .count();
    if profitable_cost_scenarios < filter.require_profitable_cost_scenarios {
        rejections.push("cost_scenarios".to_string());
    }
    rejections
}

fn prepare_indicators(
    candles: &[Kline],
    definition: &TrendFactorRunDefinition,
) -> PreparedIndicators {
    let closes = candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let mut ema_cache = BTreeMap::new();
    for key in [
        "entry.ema_trend.fast",
        "entry.ema_trend.slow",
        "entry.ema_slope.period",
        "entry.keltner.ema_period",
    ] {
        if let Some(period) = definition.parameters.get(key) {
            ema_cache.insert(*period as usize, ema(&closes, *period as usize));
        }
    }
    let mut ema_slope_cache = BTreeMap::new();
    if let (Some(period), Some(lookback)) = (
        definition.parameters.get("entry.ema_slope.period"),
        definition.parameters.get("entry.ema_slope.lookback"),
    ) {
        ema_slope_cache.insert(
            format!("{}:{}", *period as usize, *lookback as usize),
            ema_slope(&closes, *period as usize, *lookback as usize),
        );
    }
    let mut atr_cache = BTreeMap::new();
    for period in [
        14,
        parameter_usize(definition, "filter.atr_pct.period", 14),
        parameter_usize(definition, "filter.atr_percentile.atr_period", 14),
        parameter_usize(definition, "entry.supertrend.period", 10),
        parameter_usize(definition, "entry.keltner.atr_period", 10),
    ] {
        atr_cache.insert(period, atr(candles, period));
    }
    let mut adx_cache = BTreeMap::new();
    adx_cache.insert(14, adx(candles, 14));
    let mut donchian_high_cache = BTreeMap::new();
    let mut donchian_low_cache = BTreeMap::new();
    if let Some(period) = definition
        .parameters
        .get("entry.donchian_breakout.lookback")
    {
        donchian_high_cache.insert(*period as usize, donchian_high(candles, *period as usize));
        donchian_low_cache.insert(*period as usize, donchian_low(candles, *period as usize));
    }
    let mut keltner_cache = BTreeMap::new();
    if let (Some(ema_period), Some(atr_period), Some(multiplier)) = (
        definition.parameters.get("entry.keltner.ema_period"),
        definition.parameters.get("entry.keltner.atr_period"),
        definition.parameters.get("entry.keltner.multiplier"),
    ) {
        keltner_cache.insert(
            format!(
                "{}:{}:{:.6}",
                *ema_period as usize, *atr_period as usize, multiplier
            ),
            keltner_channel(
                candles,
                *ema_period as usize,
                *atr_period as usize,
                *multiplier,
            ),
        );
    }
    let mut bb_width_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.bb_width.period") {
        bb_width_cache.insert(
            *period as usize,
            bollinger_width(&closes, *period as usize, 2.0),
        );
    }
    let mut volume_ratio_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.volume_ratio.period") {
        volume_ratio_cache.insert(*period as usize, volume_ratio(candles, *period as usize));
    }
    let mut volume_zscore_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.volume_zscore.period") {
        volume_zscore_cache.insert(*period as usize, volume_zscore(candles, *period as usize));
    }
    let mut vwap_deviation_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.vwap_deviation.period") {
        vwap_deviation_cache.insert(*period as usize, vwap_deviation(candles, *period as usize));
    }
    let mut mfi_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.mfi.period") {
        mfi_cache.insert(*period as usize, mfi(candles, *period as usize));
    }
    let mut stoch_rsi_cache = BTreeMap::new();
    if let (Some(rsi_period), Some(stoch_period)) = (
        definition.parameters.get("filter.stoch_rsi.rsi_period"),
        definition.parameters.get("filter.stoch_rsi.stoch_period"),
    ) {
        stoch_rsi_cache.insert(
            format!("{}:{}", *rsi_period as usize, *stoch_period as usize),
            stoch_rsi(&closes, *rsi_period as usize, *stoch_period as usize),
        );
    }
    let mut choppiness_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("filter.choppiness.period") {
        choppiness_cache.insert(
            *period as usize,
            choppiness_index(candles, *period as usize),
        );
    }
    let mut atr_percentile_cache = BTreeMap::new();
    if let (Some(atr_period), Some(window)) = (
        definition
            .parameters
            .get("filter.atr_percentile.atr_period"),
        definition.parameters.get("filter.atr_percentile.window"),
    ) {
        atr_percentile_cache.insert(
            format!("{}:{}", *atr_period as usize, *window as usize),
            atr_percentile(candles, *atr_period as usize, *window as usize),
        );
    }
    let mut roc_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("entry.roc.period") {
        roc_cache.insert(*period as usize, roc(&closes, *period as usize));
    }
    let mut supertrend_cache = BTreeMap::new();
    if let (Some(period), Some(multiplier)) = (
        definition.parameters.get("entry.supertrend.period"),
        definition.parameters.get("entry.supertrend.multiplier"),
    ) {
        supertrend_cache.insert(
            format!("{}:{:.6}", *period as usize, multiplier),
            supertrend_direction(candles, *period as usize, *multiplier),
        );
    }
    PreparedIndicators {
        closes,
        ema_cache,
        ema_slope_cache,
        atr_cache,
        adx_cache,
        rsi_14: rsi(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            14,
        ),
        macd_histogram: macd_histogram(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            12,
            26,
            9,
        ),
        donchian_high_cache,
        donchian_low_cache,
        keltner_cache,
        bb_width_cache,
        volume_ratio_cache,
        volume_zscore_cache,
        vwap_deviation_cache,
        mfi_cache,
        stoch_rsi_cache,
        choppiness_cache,
        atr_percentile_cache,
        roc_cache,
        supertrend_cache,
    }
}

fn signal_passes(
    definition: &TrendFactorRunDefinition,
    indicators: &PreparedIndicators,
    candles: &[Kline],
    index: usize,
    side: OrderSide,
) -> bool {
    let mut has_entry_rule = false;
    if let (Some(fast_period), Some(slow_period)) = (
        definition.parameters.get("entry.ema_trend.fast"),
        definition.parameters.get("entry.ema_trend.slow"),
    ) {
        has_entry_rule = true;
        let Some(fast) = indicators
            .ema_cache
            .get(&(*fast_period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        let Some(slow) = indicators
            .ema_cache
            .get(&(*slow_period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if !matches_direction(fast - slow, side) {
            return false;
        }
    }
    if let (Some(period), Some(lookback), Some(threshold)) = (
        definition.parameters.get("entry.ema_slope.period"),
        definition.parameters.get("entry.ema_slope.lookback"),
        definition.parameters.get("entry.ema_slope.threshold"),
    ) {
        has_entry_rule = true;
        let key = format!("{}:{}", *period as usize, *lookback as usize);
        let Some(value) = indicators
            .ema_slope_cache
            .get(&key)
            .and_then(|series| series[index])
        else {
            return false;
        };
        if side == OrderSide::Buy && value < *threshold {
            return false;
        }
        if side == OrderSide::Sell && value > -*threshold {
            return false;
        }
    }
    if let (Some(ema_period), Some(atr_period), Some(multiplier)) = (
        definition.parameters.get("entry.keltner.ema_period"),
        definition.parameters.get("entry.keltner.atr_period"),
        definition.parameters.get("entry.keltner.multiplier"),
    ) {
        has_entry_rule = true;
        let key = format!(
            "{}:{}:{:.6}",
            *ema_period as usize, *atr_period as usize, multiplier
        );
        let Some(channel) = indicators.keltner_cache.get(&key) else {
            return false;
        };
        match side {
            OrderSide::Buy => {
                let Some(upper) = channel.upper[index] else {
                    return false;
                };
                if candles[index].close < upper {
                    return false;
                }
            }
            OrderSide::Sell => {
                let Some(lower) = channel.lower[index] else {
                    return false;
                };
                if candles[index].close > lower {
                    return false;
                }
            }
        }
    }
    if let Some(period) = definition
        .parameters
        .get("entry.donchian_breakout.lookback")
    {
        has_entry_rule = true;
        match side {
            OrderSide::Buy => {
                let Some(high) = indicators
                    .donchian_high_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index.saturating_sub(1)])
                else {
                    return false;
                };
                if candles[index].close < high {
                    return false;
                }
            }
            OrderSide::Sell => {
                let Some(low) = indicators
                    .donchian_low_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index.saturating_sub(1)])
                else {
                    return false;
                };
                if candles[index].close > low {
                    return false;
                }
            }
        }
    }
    if let (Some(period), Some(multiplier)) = (
        definition.parameters.get("entry.supertrend.period"),
        definition.parameters.get("entry.supertrend.multiplier"),
    ) {
        has_entry_rule = true;
        let key = format!("{}:{:.6}", *period as usize, multiplier);
        let Some(direction) = indicators
            .supertrend_cache
            .get(&key)
            .and_then(|series| series[index])
        else {
            return false;
        };
        if (side == OrderSide::Buy && direction < 0) || (side == OrderSide::Sell && direction > 0) {
            return false;
        }
    }
    if flag(definition, "entry.macd_histogram.enabled") {
        has_entry_rule = true;
        let Some(value) = indicators.macd_histogram[index] else {
            return false;
        };
        if !matches_direction(value, side) {
            return false;
        }
    }
    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("entry.roc.period"),
        definition.parameters.get("entry.roc.threshold"),
    ) {
        has_entry_rule = true;
        let Some(value) = indicators
            .roc_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if side == OrderSide::Buy && value < *threshold {
            return false;
        }
        if side == OrderSide::Sell && value > -*threshold {
            return false;
        }
    }
    if !has_entry_rule {
        return false;
    }
    filters_pass(definition, indicators, candles, index)
}

fn filters_pass(
    definition: &TrendFactorRunDefinition,
    indicators: &PreparedIndicators,
    candles: &[Kline],
    index: usize,
) -> bool {
    if let Some(threshold) = definition.parameters.get("filter.adx_min") {
        let Some(value) = indicators
            .adx_cache
            .get(&14)
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *threshold {
            return false;
        }
    }
    if let (Some(min), Some(max)) = (
        definition.parameters.get("filter.rsi_min"),
        definition.parameters.get("filter.rsi_max"),
    ) {
        let Some(value) = indicators.rsi_14[index] else {
            return false;
        };
        if value < *min || value > *max {
            return false;
        }
    }
    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("filter.bb_width.period"),
        definition.parameters.get("filter.bb_width.threshold"),
    ) {
        let Some(value) = indicators
            .bb_width_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *threshold {
            return false;
        }
    }
    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("filter.volume_ratio.period"),
        definition.parameters.get("filter.volume_ratio.threshold"),
    ) {
        let Some(value) = indicators
            .volume_ratio_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *threshold {
            return false;
        }
    }
    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("filter.volume_zscore.period"),
        definition.parameters.get("filter.volume_zscore.threshold"),
    ) {
        let Some(value) = indicators
            .volume_zscore_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *threshold {
            return false;
        }
    }
    if let (Some(period), Some(max_abs)) = (
        definition.parameters.get("filter.vwap_deviation.period"),
        definition.parameters.get("filter.vwap_deviation.max_abs"),
    ) {
        let Some(value) = indicators
            .vwap_deviation_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value.abs() > *max_abs {
            return false;
        }
    }
    if let (Some(period), Some(min), Some(max)) = (
        definition.parameters.get("filter.mfi.period"),
        definition.parameters.get("filter.mfi.min"),
        definition.parameters.get("filter.mfi.max"),
    ) {
        let Some(value) = indicators
            .mfi_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *min || value > *max {
            return false;
        }
    }
    if let (Some(rsi_period), Some(stoch_period), Some(min), Some(max)) = (
        definition.parameters.get("filter.stoch_rsi.rsi_period"),
        definition.parameters.get("filter.stoch_rsi.stoch_period"),
        definition.parameters.get("filter.stoch_rsi.min"),
        definition.parameters.get("filter.stoch_rsi.max"),
    ) {
        let key = format!("{}:{}", *rsi_period as usize, *stoch_period as usize);
        let Some(value) = indicators
            .stoch_rsi_cache
            .get(&key)
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *min || value > *max {
            return false;
        }
    }
    if let (Some(period), Some(max)) = (
        definition.parameters.get("filter.choppiness.period"),
        definition.parameters.get("filter.choppiness.max"),
    ) {
        let Some(value) = indicators
            .choppiness_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value > *max {
            return false;
        }
    }
    if let (Some(atr_period), Some(window), Some(min), Some(max)) = (
        definition
            .parameters
            .get("filter.atr_percentile.atr_period"),
        definition.parameters.get("filter.atr_percentile.window"),
        definition.parameters.get("filter.atr_percentile.min"),
        definition.parameters.get("filter.atr_percentile.max"),
    ) {
        let key = format!("{}:{}", *atr_period as usize, *window as usize);
        let Some(value) = indicators
            .atr_percentile_cache
            .get(&key)
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *min || value > *max {
            return false;
        }
    }
    if let (Some(period), Some(min), Some(max)) = (
        definition.parameters.get("filter.atr_pct.period"),
        definition.parameters.get("filter.atr_pct.min"),
        definition.parameters.get("filter.atr_pct.max"),
    ) {
        let Some(atr_value) = indicators
            .atr_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        let atr_pct = atr_value / candles[index].close.max(1e-9);
        if atr_pct < *min || atr_pct > *max {
            return false;
        }
    }
    true
}

fn exit_decision(
    position: &TrendPosition,
    candle: &Kline,
    long_signal: bool,
    short_signal: bool,
    index: usize,
    max_hold_bars: usize,
    slippage_rate: f64,
    reverse_signal_exit: bool,
) -> Option<(f64, String)> {
    match position.side {
        OrderSide::Buy => {
            if candle.low <= position.stop_price {
                return Some((
                    position.stop_price * (1.0 - slippage_rate),
                    "stop".to_string(),
                ));
            }
            if candle.high >= position.target_price {
                return Some((
                    position.target_price * (1.0 - slippage_rate),
                    "target".to_string(),
                ));
            }
            if reverse_signal_exit && short_signal {
                return Some((candle.close * (1.0 - slippage_rate), "reverse".to_string()));
            }
        }
        OrderSide::Sell => {
            if candle.high >= position.stop_price {
                return Some((
                    position.stop_price * (1.0 + slippage_rate),
                    "stop".to_string(),
                ));
            }
            if candle.low <= position.target_price {
                return Some((
                    position.target_price * (1.0 + slippage_rate),
                    "target".to_string(),
                ));
            }
            if reverse_signal_exit && long_signal {
                return Some((candle.close * (1.0 + slippage_rate), "reverse".to_string()));
            }
        }
    }
    if index.saturating_sub(position.entry_index) >= max_hold_bars {
        let exit_price = match position.side {
            OrderSide::Buy => candle.close * (1.0 - slippage_rate),
            OrderSide::Sell => candle.close * (1.0 + slippage_rate),
        };
        return Some((exit_price, "time".to_string()));
    }
    None
}

fn warmup_bars(definition: &TrendFactorRunDefinition) -> usize {
    definition
        .parameters
        .iter()
        .filter(|(key, _)| {
            key.contains("period") || key.contains("lookback") || key.ends_with("slow")
        })
        .map(|(_, value)| *value as usize)
        .max()
        .unwrap_or(40)
}

fn parameter(definition: &TrendFactorRunDefinition, key: &str, default: f64) -> f64 {
    definition.parameters.get(key).copied().unwrap_or(default)
}

fn parameter_usize(definition: &TrendFactorRunDefinition, key: &str, default: usize) -> usize {
    parameter(definition, key, default as f64).round() as usize
}

fn flag(definition: &TrendFactorRunDefinition, key: &str) -> bool {
    definition.flags.get(key).copied().unwrap_or(false)
}

fn matches_direction(value: f64, side: OrderSide) -> bool {
    match side {
        OrderSide::Buy => value > 0.0,
        OrderSide::Sell => value < 0.0,
    }
}

fn max_drawdown_pct(equity_curve: &[f64]) -> f64 {
    let mut peak = f64::NEG_INFINITY;
    let mut max_drawdown = 0.0;
    for equity in equity_curve {
        peak = peak.max(*equity);
        if peak > 0.0 {
            let drawdown = (*equity - peak) / peak;
            if drawdown < max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }
    max_drawdown * 100.0
}
