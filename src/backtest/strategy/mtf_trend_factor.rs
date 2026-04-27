use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::backtest::factors::{MtfTrendFactorRunDefinition, MtfTrendFactorScanSpec};
use crate::backtest::indicators::{
    adx, atr, bollinger_width, choppiness_index, donchian_high, donchian_low, ema, keltner_channel,
    mfi, roc, supertrend_direction, volume_zscore, KeltnerChannelSeries,
};
use crate::backtest::scoring::{score_trend_run, TrendScoreInput};
use crate::backtest::strategy::trend_factor::{
    TrendFactorCostScenarioSummary, TrendFactorRunReport, TrendFactorRunSummary, TrendFactorTrade,
};
use crate::core::types::{Kline, OrderSide};

#[derive(Debug, Clone)]
struct MtfPreparedIndicators {
    trend_closes: Vec<f64>,
    trigger_closes: Vec<f64>,
    trend_ema_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trend_adx_14: Vec<Option<f64>>,
    trend_choppiness_cache: BTreeMap<usize, Vec<Option<f64>>>,
    structure_donchian_high_cache: BTreeMap<usize, Vec<Option<f64>>>,
    structure_donchian_low_cache: BTreeMap<usize, Vec<Option<f64>>>,
    structure_bb_width_cache: BTreeMap<usize, Vec<Option<f64>>>,
    structure_keltner_cache: BTreeMap<String, KeltnerChannelSeries>,
    trigger_roc_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trigger_volume_zscore_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trigger_supertrend_cache: BTreeMap<String, Vec<Option<i8>>>,
    trigger_mfi_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trigger_atr_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trigger_donchian_high_cache: BTreeMap<usize, Vec<Option<f64>>>,
    trigger_donchian_low_cache: BTreeMap<usize, Vec<Option<f64>>>,
}

#[derive(Debug, Clone)]
struct MtfPosition {
    side: OrderSide,
    entry_time: chrono::DateTime<chrono::Utc>,
    entry_index: usize,
    entry_price: f64,
    quantity: f64,
    stop_price: f64,
    target_price: f64,
    entry_fee: f64,
    initial_risk: f64,
    high_water: f64,
    low_water: f64,
}

#[derive(Debug, Clone)]
struct StructureBreakoutEvent {
    side: OrderSide,
    level: f64,
    expires_at_trigger_index: usize,
}

pub fn run_mtf_trend_factor_definition(
    symbol: &str,
    trend_candles: &[Kline],
    structure_candles: &[Kline],
    trigger_candles: &[Kline],
    definition: &MtfTrendFactorRunDefinition,
    spec: &MtfTrendFactorScanSpec,
) -> Result<TrendFactorRunReport> {
    if trend_candles.is_empty() || structure_candles.is_empty() || trigger_candles.is_empty() {
        return Err(anyhow!(
            "mtf trend factor requires non-empty trend/structure/trigger datasets"
        ));
    }

    let indicators = prepare_mtf_indicators(
        trend_candles,
        structure_candles,
        trigger_candles,
        definition,
    );
    let mut cash = spec.initial_equity;
    let mut position: Option<MtfPosition> = None;
    let mut trades = Vec::new();
    let mut equity_curve = vec![spec.initial_equity];
    let mut wins = 0usize;
    let mut gross_profit = 0.0;
    let mut gross_loss = 0.0;
    let mut exposure_bars = 0usize;
    let warmup = warmup_bars(definition).max(40) + 2;
    let mut trend_index = 0usize;
    let mut structure_index = 0usize;
    let mut last_processed_structure_index = 0usize;
    if let Some(first_trigger_candle) = trigger_candles.get(warmup) {
        structure_index = advance_to_close_time(
            structure_candles,
            structure_index,
            first_trigger_candle.close_time,
        );
        last_processed_structure_index = structure_index;
    }
    let mut active_structure_event: Option<StructureBreakoutEvent> = None;

    for trigger_index in warmup..trigger_candles.len() {
        let trigger_candle = &trigger_candles[trigger_index];
        trend_index = advance_to_close_time(trend_candles, trend_index, trigger_candle.close_time);
        structure_index = advance_to_close_time(
            structure_candles,
            structure_index,
            trigger_candle.close_time,
        );

        while last_processed_structure_index < structure_index {
            last_processed_structure_index += 1;
            for side in [OrderSide::Buy, OrderSide::Sell] {
                if trend_layer_passes(definition, &indicators, trend_index, side) {
                    if let Some(level) = breakout_event_level(
                        definition,
                        &indicators,
                        structure_candles,
                        last_processed_structure_index,
                        side,
                    ) {
                        active_structure_event = Some(StructureBreakoutEvent {
                            side,
                            level,
                            expires_at_trigger_index: trigger_index
                                + parameter_usize(definition, "structure.breakout_valid_bars", 1),
                        });
                    }
                }
            }
        }

        if active_structure_event
            .as_ref()
            .is_some_and(|event| trigger_index > event.expires_at_trigger_index)
        {
            active_structure_event = None;
        }

        let long_signal = event_entry_signal_passes(
            definition,
            &indicators,
            trend_candles,
            trigger_candles,
            trend_index,
            trigger_index,
            active_structure_event.as_ref(),
            OrderSide::Buy,
        );
        let short_signal = event_entry_signal_passes(
            definition,
            &indicators,
            trend_candles,
            trigger_candles,
            trend_index,
            trigger_index,
            active_structure_event.as_ref(),
            OrderSide::Sell,
        );

        if let Some(open_position) = position.as_mut() {
            exposure_bars += 1;
            update_trailing_stop(
                definition,
                &indicators,
                open_position,
                trigger_index,
                trigger_candle,
            );
        }

        if let Some(open_position) = position.as_ref() {
            if let Some((exit_price, exit_reason)) = mtf_exit_decision(
                definition,
                &indicators,
                open_position,
                trigger_candle,
                long_signal,
                short_signal,
                trigger_index,
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
                    exit_time: trigger_candle.close_time,
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
                OrderSide::Buy => trigger_candle.close * (1.0 + spec.slippage_rate),
                OrderSide::Sell => trigger_candle.close * (1.0 - spec.slippage_rate),
            };
            let atr_period = parameter_usize(definition, "trigger.supertrend.period", 10)
                .max(parameter_usize(definition, "exit.trailing_atr.period", 14))
                .max(14);
            let atr_now = indicators
                .trigger_atr_cache
                .get(&atr_period)
                .and_then(|series| series[trigger_index])
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
            position = Some(MtfPosition {
                side,
                entry_time: trigger_candle.close_time,
                entry_index: trigger_index,
                entry_price,
                quantity,
                stop_price,
                target_price,
                entry_fee,
                initial_risk: risk,
                high_water: trigger_candle.high,
                low_water: trigger_candle.low,
            });
            active_structure_event = None;
        }

        let mut mark_equity = cash;
        if let Some(open_position) = position.as_ref() {
            mark_equity += match open_position.side {
                OrderSide::Buy => {
                    (trigger_candle.close - open_position.entry_price) * open_position.quantity
                }
                OrderSide::Sell => {
                    (open_position.entry_price - trigger_candle.close) * open_position.quantity
                }
            };
        }
        equity_curve.push(mark_equity);
    }

    if let Some(open_position) = position.take() {
        let candle = trigger_candles
            .last()
            .expect("trigger candles should not be empty");
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
    let candidate_rejections = mtf_candidate_rejections(
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
            candles: trigger_candles.len(),
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
            exposure_pct: exposure_bars as f64 / trigger_candles.len().max(1) as f64 * 100.0,
            buy_hold_pct: (trigger_candles.last().unwrap().close
                / trigger_candles.first().unwrap().close
                - 1.0)
                * 100.0,
            score,
            cost_scenarios,
            live_candidate,
            candidate_rejections,
            start: trigger_candles.first().unwrap().close_time,
            end: trigger_candles.last().unwrap().close_time,
        },
        trades,
    })
}

fn prepare_mtf_indicators(
    trend_candles: &[Kline],
    structure_candles: &[Kline],
    trigger_candles: &[Kline],
    definition: &MtfTrendFactorRunDefinition,
) -> MtfPreparedIndicators {
    let trend_closes = trend_candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let structure_closes = structure_candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let trigger_closes = trigger_candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();

    let mut trend_ema_cache = BTreeMap::new();
    for key in ["trend.ema.fast", "trend.ema.slow"] {
        if let Some(period) = definition.parameters.get(key) {
            trend_ema_cache.insert(*period as usize, ema(&trend_closes, *period as usize));
        }
    }

    let mut trend_choppiness_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("trend.choppiness.period") {
        trend_choppiness_cache.insert(
            *period as usize,
            choppiness_index(trend_candles, *period as usize),
        );
    }

    let mut structure_donchian_high_cache = BTreeMap::new();
    let mut structure_donchian_low_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("structure.donchian.lookback") {
        structure_donchian_high_cache.insert(
            *period as usize,
            donchian_high(structure_candles, *period as usize),
        );
        structure_donchian_low_cache.insert(
            *period as usize,
            donchian_low(structure_candles, *period as usize),
        );
    }

    let mut structure_bb_width_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("structure.bb_width.period") {
        structure_bb_width_cache.insert(
            *period as usize,
            bollinger_width(&structure_closes, *period as usize, 2.0),
        );
    }

    let mut structure_keltner_cache = BTreeMap::new();
    if let (Some(ema_period), Some(atr_period), Some(multiplier)) = (
        definition.parameters.get("structure.keltner.ema_period"),
        definition.parameters.get("structure.keltner.atr_period"),
        definition.parameters.get("structure.keltner.multiplier"),
    ) {
        structure_keltner_cache.insert(
            format!(
                "{}:{}:{:.4}",
                *ema_period as usize, *atr_period as usize, multiplier
            ),
            keltner_channel(
                structure_candles,
                *ema_period as usize,
                *atr_period as usize,
                *multiplier,
            ),
        );
    }

    let mut trigger_roc_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("trigger.roc.period") {
        trigger_roc_cache.insert(*period as usize, roc(&trigger_closes, *period as usize));
    }

    let mut trigger_volume_zscore_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("trigger.volume_zscore.period") {
        trigger_volume_zscore_cache.insert(
            *period as usize,
            volume_zscore(trigger_candles, *period as usize),
        );
    }

    let mut trigger_supertrend_cache = BTreeMap::new();
    if let (Some(period), Some(multiplier)) = (
        definition.parameters.get("trigger.supertrend.period"),
        definition.parameters.get("trigger.supertrend.multiplier"),
    ) {
        trigger_supertrend_cache.insert(
            format!("{}:{:.4}", *period as usize, multiplier),
            supertrend_direction(trigger_candles, *period as usize, *multiplier),
        );
    }

    let mut trigger_mfi_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("trigger.mfi.period") {
        trigger_mfi_cache.insert(*period as usize, mfi(trigger_candles, *period as usize));
    }

    let mut trigger_atr_cache = BTreeMap::new();
    for period in [
        14,
        parameter_usize(definition, "trigger.supertrend.period", 10).max(14),
        parameter_usize(definition, "exit.trailing_atr.period", 14).max(1),
    ] {
        trigger_atr_cache.insert(period, atr(trigger_candles, period));
    }

    let mut trigger_donchian_high_cache = BTreeMap::new();
    let mut trigger_donchian_low_cache = BTreeMap::new();
    if let Some(period) = definition.parameters.get("exit.donchian.lookback") {
        trigger_donchian_high_cache.insert(
            *period as usize,
            donchian_high(trigger_candles, *period as usize),
        );
        trigger_donchian_low_cache.insert(
            *period as usize,
            donchian_low(trigger_candles, *period as usize),
        );
    }

    MtfPreparedIndicators {
        trend_closes,
        trigger_closes,
        trend_ema_cache,
        trend_adx_14: adx(trend_candles, 14),
        trend_choppiness_cache,
        structure_donchian_high_cache,
        structure_donchian_low_cache,
        structure_bb_width_cache,
        structure_keltner_cache,
        trigger_roc_cache,
        trigger_volume_zscore_cache,
        trigger_supertrend_cache,
        trigger_mfi_cache,
        trigger_atr_cache,
        trigger_donchian_high_cache,
        trigger_donchian_low_cache,
    }
}

fn event_entry_signal_passes(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    _trend_candles: &[Kline],
    trigger_candles: &[Kline],
    trend_index: usize,
    trigger_index: usize,
    event: Option<&StructureBreakoutEvent>,
    side: OrderSide,
) -> bool {
    let Some(event) = event else {
        return false;
    };
    event.side == side
        && trigger_index <= event.expires_at_trigger_index
        && trend_layer_passes(definition, indicators, trend_index, side)
        && pullback_entry_passes(definition, trigger_candles, trigger_index, event, side)
        && trigger_layer_passes(definition, indicators, trigger_candles, trigger_index, side)
}

fn trend_layer_passes(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    index: usize,
    side: OrderSide,
) -> bool {
    let Some(fast_period) = definition.parameters.get("trend.ema.fast") else {
        return false;
    };
    let Some(slow_period) = definition.parameters.get("trend.ema.slow") else {
        return false;
    };
    let Some(fast) = indicators
        .trend_ema_cache
        .get(&(*fast_period as usize))
        .and_then(|series| series[index])
    else {
        return false;
    };
    let Some(slow) = indicators
        .trend_ema_cache
        .get(&(*slow_period as usize))
        .and_then(|series| series[index])
    else {
        return false;
    };
    if !matches_direction(fast - slow, side) {
        return false;
    }

    if let Some(min_adx) = definition.parameters.get("trend.adx_min") {
        let Some(value) = indicators.trend_adx_14[index] else {
            return false;
        };
        if value < *min_adx {
            return false;
        }
    }

    if let (Some(period), Some(max_choppiness)) = (
        definition.parameters.get("trend.choppiness.period"),
        definition.parameters.get("trend.choppiness.max"),
    ) {
        let Some(value) = indicators
            .trend_choppiness_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value > *max_choppiness {
            return false;
        }
    }

    true
}

fn breakout_event_level(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    candles: &[Kline],
    index: usize,
    side: OrderSide,
) -> Option<f64> {
    if index == 0 {
        return None;
    }

    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("structure.bb_width.period"),
        definition.parameters.get("structure.bb_width.threshold"),
    ) {
        let width = indicators
            .structure_bb_width_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])?;
        if width < *threshold {
            return None;
        }
    }

    if let Some(period) = definition.parameters.get("structure.donchian.lookback") {
        match side {
            OrderSide::Buy => {
                let level = indicators
                    .structure_donchian_high_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index - 1])?;
                if candles[index].close > level {
                    return Some(level);
                }
            }
            OrderSide::Sell => {
                let level = indicators
                    .structure_donchian_low_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index - 1])?;
                if candles[index].close < level {
                    return Some(level);
                }
            }
        }
    }

    if let (Some(ema_period), Some(atr_period), Some(multiplier)) = (
        definition.parameters.get("structure.keltner.ema_period"),
        definition.parameters.get("structure.keltner.atr_period"),
        definition.parameters.get("structure.keltner.multiplier"),
    ) {
        let key = format!(
            "{}:{}:{:.4}",
            *ema_period as usize, *atr_period as usize, multiplier
        );
        let channel = indicators.structure_keltner_cache.get(&key)?;
        match side {
            OrderSide::Buy => {
                let level = channel.upper[index - 1]?;
                if candles[index].close > level {
                    return Some(level);
                }
            }
            OrderSide::Sell => {
                let level = channel.lower[index - 1]?;
                if candles[index].close < level {
                    return Some(level);
                }
            }
        }
    }

    None
}

fn pullback_entry_passes(
    definition: &MtfTrendFactorRunDefinition,
    trigger_candles: &[Kline],
    trigger_index: usize,
    event: &StructureBreakoutEvent,
    side: OrderSide,
) -> bool {
    let (Some(max_pullback_pct), Some(reclaim_pct)) = (
        definition.parameters.get("structure.pullback.max_pct"),
        definition.parameters.get("structure.pullback.reclaim_pct"),
    ) else {
        return true;
    };
    let candle = &trigger_candles[trigger_index];
    match side {
        OrderSide::Buy => {
            candle.low <= event.level * (1.0 + max_pullback_pct)
                && candle.close >= event.level * (1.0 + reclaim_pct)
        }
        OrderSide::Sell => {
            candle.high >= event.level * (1.0 - max_pullback_pct)
                && candle.close <= event.level * (1.0 - reclaim_pct)
        }
    }
}

fn trigger_layer_passes(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    _candles: &[Kline],
    index: usize,
    side: OrderSide,
) -> bool {
    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("trigger.roc.period"),
        definition.parameters.get("trigger.roc.threshold"),
    ) {
        let Some(value) = indicators
            .trigger_roc_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        match side {
            OrderSide::Buy if value < *threshold => return false,
            OrderSide::Sell if value > -*threshold => return false,
            _ => {}
        }
    }

    if let (Some(period), Some(threshold)) = (
        definition.parameters.get("trigger.volume_zscore.period"),
        definition.parameters.get("trigger.volume_zscore.threshold"),
    ) {
        let Some(value) = indicators
            .trigger_volume_zscore_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *threshold {
            return false;
        }
    }

    if let (Some(period), Some(multiplier)) = (
        definition.parameters.get("trigger.supertrend.period"),
        definition.parameters.get("trigger.supertrend.multiplier"),
    ) {
        let key = format!("{}:{:.4}", *period as usize, multiplier);
        let Some(direction) = indicators
            .trigger_supertrend_cache
            .get(&key)
            .and_then(|series| series[index])
        else {
            return false;
        };
        match side {
            OrderSide::Buy if direction < 0 => return false,
            OrderSide::Sell if direction > 0 => return false,
            _ => {}
        }
    }

    if let (Some(period), Some(min), Some(max)) = (
        definition.parameters.get("trigger.mfi.period"),
        definition.parameters.get("trigger.mfi.min"),
        definition.parameters.get("trigger.mfi.max"),
    ) {
        let Some(value) = indicators
            .trigger_mfi_cache
            .get(&(*period as usize))
            .and_then(|series| series[index])
        else {
            return false;
        };
        if value < *min || value > *max {
            return false;
        }
    }

    true
}

fn advance_to_close_time(
    candles: &[Kline],
    mut index: usize,
    close_time: chrono::DateTime<chrono::Utc>,
) -> usize {
    while index + 1 < candles.len() && candles[index + 1].close_time <= close_time {
        index += 1;
    }
    index
}

fn update_trailing_stop(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    position: &mut MtfPosition,
    trigger_index: usize,
    candle: &Kline,
) {
    let (Some(period), Some(multiplier), Some(activation_r)) = (
        definition.parameters.get("exit.trailing_atr.period"),
        definition.parameters.get("exit.trailing_atr.multiplier"),
        definition.parameters.get("exit.trailing_atr.activation_r"),
    ) else {
        return;
    };
    let period = *period as usize;
    let Some(atr_now) = indicators
        .trigger_atr_cache
        .get(&period)
        .and_then(|series| series[trigger_index])
    else {
        return;
    };
    match position.side {
        OrderSide::Buy => {
            position.high_water = position.high_water.max(candle.high);
            let favorable_move = position.high_water - position.entry_price;
            if favorable_move >= position.initial_risk * activation_r {
                let trailing_stop = position.high_water - atr_now * multiplier;
                position.stop_price = position.stop_price.max(trailing_stop);
            }
        }
        OrderSide::Sell => {
            position.low_water = position.low_water.min(candle.low);
            let favorable_move = position.entry_price - position.low_water;
            if favorable_move >= position.initial_risk * activation_r {
                let trailing_stop = position.low_water + atr_now * multiplier;
                position.stop_price = position.stop_price.min(trailing_stop);
            }
        }
    }
}

fn mtf_exit_decision(
    definition: &MtfTrendFactorRunDefinition,
    indicators: &MtfPreparedIndicators,
    position: &MtfPosition,
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
            if let Some(period) = definition.parameters.get("exit.donchian.lookback") {
                if let Some(exit_level) = indicators
                    .trigger_donchian_low_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index.saturating_sub(1)])
                {
                    if candle.close < exit_level {
                        return Some((
                            candle.close * (1.0 - slippage_rate),
                            "donchian_exit".to_string(),
                        ));
                    }
                }
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
            if let Some(period) = definition.parameters.get("exit.donchian.lookback") {
                if let Some(exit_level) = indicators
                    .trigger_donchian_high_cache
                    .get(&(*period as usize))
                    .and_then(|series| series[index.saturating_sub(1)])
                {
                    if candle.close > exit_level {
                        return Some((
                            candle.close * (1.0 + slippage_rate),
                            "donchian_exit".to_string(),
                        ));
                    }
                }
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

fn mtf_candidate_rejections(
    trades: usize,
    roi_pct: f64,
    max_drawdown_pct: f64,
    profit_factor: f64,
    cost_scenarios: &[TrendFactorCostScenarioSummary],
    spec: &MtfTrendFactorScanSpec,
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

fn warmup_bars(definition: &MtfTrendFactorRunDefinition) -> usize {
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

fn parameter(definition: &MtfTrendFactorRunDefinition, key: &str, default: f64) -> f64 {
    definition.parameters.get(key).copied().unwrap_or(default)
}

fn parameter_usize(definition: &MtfTrendFactorRunDefinition, key: &str, default: usize) -> usize {
    parameter(definition, key, default as f64).round() as usize
}

fn flag(definition: &MtfTrendFactorRunDefinition, key: &str) -> bool {
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
