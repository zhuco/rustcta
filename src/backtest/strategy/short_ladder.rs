use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::backtest::indicators::{atr, ema, rsi};
use crate::core::types::Kline;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShortLadderMode {
    AdverseAveraging,
    FavorablePyramiding,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntrabarPathMode {
    Current,
    Pessimistic,
}

impl Default for IntrabarPathMode {
    fn default() -> Self {
        Self::Current
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TakeProfitMode {
    FixedAtr,
    AtrTrailing,
}

impl Default for TakeProfitMode {
    fn default() -> Self {
        Self::FixedAtr
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LayerPriceMode {
    AtrSpacing,
    AtrResistanceLoss,
}

impl Default for LayerPriceMode {
    fn default() -> Self {
        Self::AtrSpacing
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntrySignalMode {
    LegacyRsi,
    ResistanceSweepRejection,
    BreakdownPullback,
    BearishVolatilityExpansion,
}

impl Default for EntrySignalMode {
    fn default() -> Self {
        Self::LegacyRsi
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderConfig {
    #[serde(default = "default_initial_equity")]
    pub initial_equity: f64,
    #[serde(default = "default_initial_notional")]
    pub initial_notional: f64,
    #[serde(default = "default_layer_weights")]
    pub layer_weights: Vec<f64>,
    #[serde(default = "default_session_start_hour")]
    pub session_start_hour_utc: u32,
    #[serde(default = "default_session_end_hour")]
    pub session_end_hour_utc: u32,
    #[serde(default = "default_maker_fee_bps")]
    pub maker_fee_bps: f64,
    #[serde(default = "default_taker_fee_bps")]
    pub taker_fee_bps: f64,
    #[serde(default = "default_maker_fill_rate")]
    pub maker_fill_rate: f64,
    #[serde(default)]
    pub maker_fill_seed: u64,
    #[serde(default)]
    pub initial_order_taker_fallback_secs: u64,
    #[serde(default)]
    pub intrabar_path_mode: IntrabarPathMode,
    #[serde(default)]
    pub breakeven_stop: bool,
    #[serde(default = "default_breakeven_trigger_atr")]
    pub breakeven_trigger_atr: f64,
    #[serde(default = "default_breakeven_buffer_bps")]
    pub breakeven_buffer_bps: f64,
    #[serde(default = "default_atr_period")]
    pub atr_period: usize,
    #[serde(default = "default_rsi_period")]
    pub rsi_period: usize,
    #[serde(default = "default_entry_rsi")]
    pub entry_rsi_min: f64,
    #[serde(default)]
    pub entry_signal_mode: EntrySignalMode,
    #[serde(default = "default_true")]
    pub entry_requires_lower_close: bool,
    #[serde(default = "default_true")]
    pub entry_requires_prior_high_sweep: bool,
    #[serde(default = "default_entry_resistance_lookback_bars")]
    pub entry_resistance_lookback_bars: usize,
    #[serde(default = "default_entry_upper_wick_min_ratio")]
    pub entry_upper_wick_min_ratio: f64,
    #[serde(default = "default_entry_breakdown_lookback_bars")]
    pub entry_breakdown_lookback_bars: usize,
    #[serde(default = "default_entry_pullback_max_bars")]
    pub entry_pullback_max_bars: usize,
    #[serde(default = "default_entry_pullback_tolerance_atr")]
    pub entry_pullback_tolerance_atr: f64,
    #[serde(default = "default_entry_body_atr_min")]
    pub entry_body_atr_min: f64,
    #[serde(default = "default_entry_range_atr_min")]
    pub entry_range_atr_min: f64,
    #[serde(default = "default_entry_close_near_low_ratio")]
    pub entry_close_near_low_ratio: f64,
    #[serde(default = "default_entry_volume_sma_bars")]
    pub entry_volume_sma_bars: usize,
    #[serde(default = "default_entry_volume_multiplier")]
    pub entry_volume_multiplier: f64,
    #[serde(default)]
    pub entry_range_position_lookback_bars: usize,
    #[serde(default)]
    pub entry_range_position_min: f64,
    #[serde(default = "default_entry_range_position_max")]
    pub entry_range_position_max: f64,
    #[serde(default = "default_layer_spacing_atr")]
    pub layer_spacing_atr: f64,
    #[serde(default)]
    pub layer_price_mode: LayerPriceMode,
    #[serde(default = "default_layer_resistance_lookback_bars")]
    pub layer_resistance_lookback_bars: usize,
    #[serde(default = "default_layer_resistance_buffer_atr")]
    pub layer_resistance_buffer_atr: f64,
    #[serde(default = "default_layer_min_loss_bps")]
    pub layer_min_loss_bps: Vec<f64>,
    #[serde(default)]
    pub final_layer_spacing_atr: Option<f64>,
    #[serde(default)]
    pub final_layer_resistance_lookback_bars: Option<usize>,
    #[serde(default)]
    pub final_layer_resistance_buffer_atr: Option<f64>,
    #[serde(default = "default_take_profit_atr")]
    pub take_profit_atr: f64,
    #[serde(default)]
    pub take_profit_mode: TakeProfitMode,
    #[serde(default = "default_trailing_take_profit_activation_atr")]
    pub trailing_take_profit_activation_atr: f64,
    #[serde(default = "default_trailing_take_profit_distance_atr")]
    pub trailing_take_profit_distance_atr: f64,
    #[serde(default = "default_stop_loss_atr")]
    pub stop_loss_atr: f64,
    #[serde(default = "default_stop_loss_min_layers")]
    pub stop_loss_min_layers: usize,
    #[serde(default = "default_max_hold_bars")]
    pub max_hold_bars: usize,
    #[serde(default = "default_filter_15m_ema")]
    pub filter_15m_ema: usize,
    #[serde(default = "default_filter_1h_ema")]
    pub filter_1h_ema: usize,
    #[serde(default)]
    pub final_layer_requires_strong_1h: bool,
    #[serde(default = "default_strong_1h_ema_slope_lookback")]
    pub strong_1h_ema_slope_lookback: usize,
    #[serde(default = "default_strong_1h_ema_slope_max_pct")]
    pub strong_1h_ema_slope_max_pct: f64,
}

impl Default for ShortLadderConfig {
    fn default() -> Self {
        Self {
            initial_equity: default_initial_equity(),
            initial_notional: default_initial_notional(),
            layer_weights: default_layer_weights(),
            session_start_hour_utc: default_session_start_hour(),
            session_end_hour_utc: default_session_end_hour(),
            maker_fee_bps: default_maker_fee_bps(),
            taker_fee_bps: default_taker_fee_bps(),
            maker_fill_rate: default_maker_fill_rate(),
            maker_fill_seed: 0,
            initial_order_taker_fallback_secs: 0,
            intrabar_path_mode: IntrabarPathMode::Current,
            breakeven_stop: false,
            breakeven_trigger_atr: default_breakeven_trigger_atr(),
            breakeven_buffer_bps: default_breakeven_buffer_bps(),
            atr_period: default_atr_period(),
            rsi_period: default_rsi_period(),
            entry_rsi_min: default_entry_rsi(),
            entry_signal_mode: EntrySignalMode::default(),
            entry_requires_lower_close: default_true(),
            entry_requires_prior_high_sweep: default_true(),
            entry_resistance_lookback_bars: default_entry_resistance_lookback_bars(),
            entry_upper_wick_min_ratio: default_entry_upper_wick_min_ratio(),
            entry_breakdown_lookback_bars: default_entry_breakdown_lookback_bars(),
            entry_pullback_max_bars: default_entry_pullback_max_bars(),
            entry_pullback_tolerance_atr: default_entry_pullback_tolerance_atr(),
            entry_body_atr_min: default_entry_body_atr_min(),
            entry_range_atr_min: default_entry_range_atr_min(),
            entry_close_near_low_ratio: default_entry_close_near_low_ratio(),
            entry_volume_sma_bars: default_entry_volume_sma_bars(),
            entry_volume_multiplier: default_entry_volume_multiplier(),
            entry_range_position_lookback_bars: 0,
            entry_range_position_min: 0.0,
            entry_range_position_max: default_entry_range_position_max(),
            layer_spacing_atr: default_layer_spacing_atr(),
            layer_price_mode: LayerPriceMode::default(),
            layer_resistance_lookback_bars: default_layer_resistance_lookback_bars(),
            layer_resistance_buffer_atr: default_layer_resistance_buffer_atr(),
            layer_min_loss_bps: default_layer_min_loss_bps(),
            final_layer_spacing_atr: None,
            final_layer_resistance_lookback_bars: None,
            final_layer_resistance_buffer_atr: None,
            take_profit_atr: default_take_profit_atr(),
            take_profit_mode: TakeProfitMode::default(),
            trailing_take_profit_activation_atr: default_trailing_take_profit_activation_atr(),
            trailing_take_profit_distance_atr: default_trailing_take_profit_distance_atr(),
            stop_loss_atr: default_stop_loss_atr(),
            stop_loss_min_layers: default_stop_loss_min_layers(),
            max_hold_bars: default_max_hold_bars(),
            filter_15m_ema: default_filter_15m_ema(),
            filter_1h_ema: default_filter_1h_ema(),
            final_layer_requires_strong_1h: false,
            strong_1h_ema_slope_lookback: default_strong_1h_ema_slope_lookback(),
            strong_1h_ema_slope_max_pct: default_strong_1h_ema_slope_max_pct(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderTrade {
    pub mode: ShortLadderMode,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub average_entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub layers_filled: usize,
    pub max_notional: f64,
    pub realized_pnl: f64,
    pub fees_paid: f64,
    pub net_pnl: f64,
    pub exit_reason: String,
    pub max_unrealized_loss: f64,
    pub entry_prices: Vec<f64>,
    pub entry_notionals: Vec<f64>,
    pub entry_times: Vec<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderRunSummary {
    pub mode: ShortLadderMode,
    pub bars: usize,
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate_pct: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub net_pnl: f64,
    pub roi_pct: f64,
    pub annualized_return_pct: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
    pub max_layers_filled: usize,
    pub max_position_notional: f64,
    pub total_fees_paid: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderRunReport {
    pub symbol: String,
    pub mode: ShortLadderMode,
    pub config: ShortLadderConfig,
    pub summary: ShortLadderRunSummary,
    pub trades: Vec<ShortLadderTrade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderMtfExecutionConfig {
    pub signal_interval: String,
    pub execution_interval: String,
    pub order_cooldown_secs: u64,
    pub min_minutes_to_l4: u64,
    pub max_layers_per_hour: usize,
}

impl Default for ShortLadderMtfExecutionConfig {
    fn default() -> Self {
        Self {
            signal_interval: "5m".to_string(),
            execution_interval: "1m".to_string(),
            order_cooldown_secs: 300,
            min_minutes_to_l4: 15,
            max_layers_per_hour: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderMtfExecutionRunReport {
    pub symbol: String,
    pub mode: ShortLadderMode,
    pub config: ShortLadderConfig,
    pub execution_config: ShortLadderMtfExecutionConfig,
    pub summary: ShortLadderRunSummary,
    pub trades: Vec<ShortLadderTrade>,
}

#[derive(Debug, Clone)]
struct OpenShortPosition {
    mode: ShortLadderMode,
    entry_time: DateTime<Utc>,
    average_entry_price: f64,
    quantity: f64,
    next_layer_index: usize,
    last_layer_price: f64,
    atr_at_entry: f64,
    opened_bar_index: usize,
    entry_prices: Vec<f64>,
    entry_notionals: Vec<f64>,
    entry_times: Vec<DateTime<Utc>>,
    fees_paid: f64,
    max_unrealized_loss: f64,
    breakeven_armed: bool,
    best_favorable_price: f64,
    trailing_take_profit_armed: bool,
}

pub fn run_short_ladder_backtest(
    symbol: &str,
    klines_5m: &[Kline],
    mode: ShortLadderMode,
    config: ShortLadderConfig,
) -> ShortLadderRunReport {
    let candles_15m = aggregate_klines(klines_5m, 3, "15m");
    let candles_1h = aggregate_klines(klines_5m, 12, "1h");
    let closes_5m = klines_5m.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let closes_15m = candles_15m.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let closes_1h = candles_1h.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let atr_5m = atr(klines_5m, config.atr_period);
    let rsi_5m = rsi(&closes_5m, config.rsi_period);
    let ema_15m = ema(&closes_15m, config.filter_15m_ema);
    let ema_1h = ema(&closes_1h, config.filter_1h_ema);

    let mut cash = config.initial_equity;
    let mut equity_curve = vec![cash];
    let mut position: Option<OpenShortPosition> = None;
    let mut trades = Vec::new();

    for (index, bar) in klines_5m.iter().enumerate() {
        if let Some(open) = position.as_mut() {
            update_unrealized_loss(open, bar.high);
            if config.breakeven_stop && !open.breakeven_armed {
                let trigger_price =
                    open.average_entry_price - config.breakeven_trigger_atr * open.atr_at_entry;
                if bar.low <= trigger_price {
                    open.breakeven_armed = true;
                }
            }
        }

        let mut filled_layer_this_bar = false;
        if let Some(open) = position.as_ref() {
            if matches!(config.intrabar_path_mode, IntrabarPathMode::Pessimistic) {
                if let Some((exit_price, reason)) =
                    pre_layer_adverse_exit_decision(open, bar, index, &config)
                {
                    let trade = close_position(open, bar.close_time, exit_price, reason, &config);
                    cash += trade.net_pnl;
                    equity_curve.push(cash);
                    trades.push(trade);
                    position = None;
                    continue;
                }
            }
        }

        if let Some(open) = position.as_mut() {
            let allow_final_layer = strong_1h_short_regime(index, &candles_1h, &ema_1h, &config);
            filled_layer_this_bar = fill_next_layer_if_triggered_with_history(
                symbol,
                open,
                bar,
                mode,
                &config,
                allow_final_layer,
                klines_5m,
                index,
            );
        }

        if let Some(open) = position.as_mut() {
            update_take_profit_state(open, bar, &config);
            if let Some((exit_price, reason)) = exit_decision_after_intrabar_updates(
                open,
                bar,
                index,
                &config,
                filled_layer_this_bar,
            ) {
                let trade = close_position(open, bar.close_time, exit_price, reason, &config);
                cash += trade.net_pnl;
                equity_curve.push(cash);
                trades.push(trade);
                position = None;
                continue;
            }
        }

        if position.is_none()
            && is_in_session(bar.close_time, &config)
            && short_regime(index, &candles_15m, &candles_1h, &ema_15m, &ema_1h)
            && short_entry_trigger(index, klines_5m, &rsi_5m, &atr_5m, &config)
        {
            if let (Some(atr_now), Some(entry_fee_bps)) = (
                atr_5m[index],
                initial_entry_fee_bps(symbol, bar.close_time.timestamp_millis(), &config),
            ) {
                position = Some(open_initial_short(
                    mode,
                    bar,
                    index,
                    atr_now,
                    entry_fee_bps,
                    &config,
                ));
            }
        }
    }

    if let (Some(open), Some(last_bar)) = (position.as_ref(), klines_5m.last()) {
        let trade = close_position(
            open,
            last_bar.close_time,
            last_bar.close,
            "end_of_data".to_string(),
            &config,
        );
        cash += trade.net_pnl;
        equity_curve.push(cash);
        trades.push(trade);
    }

    let summary = summarize_run(mode, klines_5m.len(), &trades, &equity_curve, &config);
    ShortLadderRunReport {
        symbol: symbol.to_string(),
        mode,
        config,
        summary,
        trades,
    }
}

pub fn run_short_ladder_mtf_execution_backtest(
    symbol: &str,
    klines_1m: &[Kline],
    mode: ShortLadderMode,
    config: ShortLadderConfig,
    execution_config: ShortLadderMtfExecutionConfig,
) -> ShortLadderMtfExecutionRunReport {
    let mut execution_candles = klines_1m.to_vec();
    execution_candles.sort_by_key(|bar| bar.close_time);
    execution_candles.dedup_by_key(|bar| bar.close_time);

    let candles_5m = aggregate_klines(&execution_candles, 5, "5m");
    let candles_15m = aggregate_klines(&candles_5m, 3, "15m");
    let candles_1h = aggregate_klines(&candles_5m, 12, "1h");
    let closes_5m = candles_5m.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let closes_15m = candles_15m.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let closes_1h = candles_1h.iter().map(|bar| bar.close).collect::<Vec<_>>();
    let atr_5m = atr(&candles_5m, config.atr_period);
    let rsi_5m = rsi(&closes_5m, config.rsi_period);
    let ema_15m = ema(&closes_15m, config.filter_15m_ema);
    let ema_1h = ema(&closes_1h, config.filter_1h_ema);

    let mut cash = config.initial_equity;
    let mut equity_curve = vec![cash];
    let mut position: Option<OpenShortPosition> = None;
    let mut trades = Vec::new();
    let mut last_order_at: Option<DateTime<Utc>> = None;
    let mut layer_fill_times: Vec<DateTime<Utc>> = Vec::new();

    for (signal_index, signal_bar) in candles_5m.iter().enumerate() {
        let execution_start = signal_index * 5;
        let execution_end = (execution_start + 5).min(execution_candles.len());
        let execution_slice = &execution_candles[execution_start..execution_end];
        let mut closed_during_signal_bar = false;

        if position.is_some() {
            for execution_bar in execution_slice {
                let allow_final_layer =
                    strong_1h_short_regime(signal_index, &candles_1h, &ema_1h, &config);
                let mut closing_trade = None;

                if let Some(open) = position.as_mut() {
                    update_unrealized_loss(open, execution_bar.high);
                    if config.breakeven_stop && !open.breakeven_armed {
                        let trigger_price = open.average_entry_price
                            - config.breakeven_trigger_atr * open.atr_at_entry;
                        if execution_bar.low <= trigger_price {
                            open.breakeven_armed = true;
                        }
                    }

                    if matches!(config.intrabar_path_mode, IntrabarPathMode::Pessimistic) {
                        if let Some((exit_price, reason)) = pre_layer_adverse_exit_decision(
                            open,
                            execution_bar,
                            signal_index,
                            &config,
                        ) {
                            closing_trade = Some(close_position(
                                open,
                                execution_bar.close_time,
                                exit_price,
                                reason,
                                &config,
                            ));
                        }
                    }

                    if closing_trade.is_none()
                        && is_in_session(execution_bar.close_time, &config)
                        && can_add_layer_on_execution_bar(
                            open,
                            execution_bar.close_time,
                            last_order_at,
                            &layer_fill_times,
                            &config,
                            &execution_config,
                        )
                    {
                        let filled_layer_this_bar = fill_next_layer_once_if_triggered_with_history(
                            symbol,
                            open,
                            execution_bar,
                            mode,
                            &config,
                            allow_final_layer,
                            &candles_5m,
                            signal_index,
                        );
                        if filled_layer_this_bar {
                            last_order_at = Some(execution_bar.close_time);
                            layer_fill_times.push(execution_bar.close_time);
                        }

                        update_take_profit_state(open, execution_bar, &config);
                        if let Some((exit_price, reason)) = exit_decision_after_intrabar_updates(
                            open,
                            execution_bar,
                            signal_index,
                            &config,
                            filled_layer_this_bar,
                        ) {
                            closing_trade = Some(close_position(
                                open,
                                execution_bar.close_time,
                                exit_price,
                                reason,
                                &config,
                            ));
                        }
                    } else if closing_trade.is_none() {
                        update_take_profit_state(open, execution_bar, &config);
                        if let Some((exit_price, reason)) =
                            exit_decision(open, execution_bar, signal_index, &config)
                        {
                            closing_trade = Some(close_position(
                                open,
                                execution_bar.close_time,
                                exit_price,
                                reason,
                                &config,
                            ));
                        }
                    }
                }

                if let Some(trade) = closing_trade {
                    cash += trade.net_pnl;
                    equity_curve.push(cash);
                    trades.push(trade);
                    position = None;
                    last_order_at = None;
                    layer_fill_times.clear();
                    closed_during_signal_bar = true;
                    break;
                }
            }
        }

        if position.is_none()
            && !closed_during_signal_bar
            && is_in_session(signal_bar.close_time, &config)
            && short_regime(signal_index, &candles_15m, &candles_1h, &ema_15m, &ema_1h)
            && short_entry_trigger(signal_index, &candles_5m, &rsi_5m, &atr_5m, &config)
        {
            if let (Some(atr_now), Some(entry_fee_bps)) = (
                atr_5m[signal_index],
                initial_entry_fee_bps(symbol, signal_bar.close_time.timestamp_millis(), &config),
            ) {
                position = Some(open_initial_short(
                    mode,
                    signal_bar,
                    signal_index,
                    atr_now,
                    entry_fee_bps,
                    &config,
                ));
                last_order_at = Some(signal_bar.close_time);
                layer_fill_times.clear();
                layer_fill_times.push(signal_bar.close_time);
            }
        }
    }

    if let (Some(open), Some(last_bar)) = (position.as_ref(), execution_candles.last()) {
        let trade = close_position(
            open,
            last_bar.close_time,
            last_bar.close,
            "end_of_data".to_string(),
            &config,
        );
        cash += trade.net_pnl;
        equity_curve.push(cash);
        trades.push(trade);
    }

    let summary = summarize_run(
        mode,
        execution_candles.len(),
        &trades,
        &equity_curve,
        &config,
    );
    ShortLadderMtfExecutionRunReport {
        symbol: symbol.to_string(),
        mode,
        config,
        execution_config,
        summary,
        trades,
    }
}

fn open_initial_short(
    mode: ShortLadderMode,
    bar: &Kline,
    index: usize,
    atr_now: f64,
    fee_bps: f64,
    config: &ShortLadderConfig,
) -> OpenShortPosition {
    let notional = config.initial_notional * config.layer_weights.first().copied().unwrap_or(1.0);
    let quantity = notional / bar.close;
    let fee = notional * fee_bps / 10_000.0;
    OpenShortPosition {
        mode,
        entry_time: bar.close_time,
        average_entry_price: bar.close,
        quantity,
        next_layer_index: 1,
        last_layer_price: bar.close,
        atr_at_entry: atr_now,
        opened_bar_index: index,
        entry_prices: vec![bar.close],
        entry_notionals: vec![notional],
        entry_times: vec![bar.close_time],
        fees_paid: fee,
        max_unrealized_loss: 0.0,
        breakeven_armed: false,
        best_favorable_price: bar.close,
        trailing_take_profit_armed: false,
    }
}

fn fill_next_layer_if_triggered(
    symbol: &str,
    position: &mut OpenShortPosition,
    bar: &Kline,
    mode: ShortLadderMode,
    config: &ShortLadderConfig,
    allow_final_layer: bool,
) -> bool {
    fill_next_layer_if_triggered_with_history(
        symbol,
        position,
        bar,
        mode,
        config,
        allow_final_layer,
        &[],
        0,
    )
}

fn fill_next_layer_if_triggered_with_history(
    symbol: &str,
    position: &mut OpenShortPosition,
    bar: &Kline,
    mode: ShortLadderMode,
    config: &ShortLadderConfig,
    allow_final_layer: bool,
    signal_history: &[Kline],
    signal_index: usize,
) -> bool {
    let mut filled_any = false;
    while position.next_layer_index < config.layer_weights.len() {
        let is_final_layer = position.next_layer_index + 1 == config.layer_weights.len();
        if is_final_layer && config.final_layer_requires_strong_1h && !allow_final_layer {
            break;
        }

        let trigger_price = next_layer_trigger_price(
            position,
            mode,
            config,
            signal_history,
            signal_index,
            is_final_layer,
        );
        let touched = match mode {
            ShortLadderMode::AdverseAveraging => bar.high >= trigger_price,
            ShortLadderMode::FavorablePyramiding => bar.low <= trigger_price,
        };
        if !touched {
            break;
        }
        if !maker_fill_allowed(
            symbol,
            bar.close_time.timestamp_millis(),
            position.next_layer_index,
            config,
        ) {
            break;
        }

        let weight = config.layer_weights[position.next_layer_index];
        let notional = config.initial_notional * weight;
        let quantity = notional / trigger_price;
        let old_notional = position.average_entry_price * position.quantity;
        position.quantity += quantity;
        position.average_entry_price =
            (old_notional + trigger_price * quantity) / position.quantity;
        position.last_layer_price = trigger_price;
        position.next_layer_index += 1;
        position.entry_prices.push(trigger_price);
        position.entry_notionals.push(notional);
        position.entry_times.push(bar.close_time);
        reset_take_profit_state(position, trigger_price);
        position.fees_paid += notional * config.maker_fee_bps / 10_000.0;
        filled_any = true;
    }
    filled_any
}

fn fill_next_layer_once_if_triggered(
    symbol: &str,
    position: &mut OpenShortPosition,
    bar: &Kline,
    mode: ShortLadderMode,
    config: &ShortLadderConfig,
    allow_final_layer: bool,
) -> bool {
    fill_next_layer_once_if_triggered_with_history(
        symbol,
        position,
        bar,
        mode,
        config,
        allow_final_layer,
        &[],
        0,
    )
}

fn fill_next_layer_once_if_triggered_with_history(
    symbol: &str,
    position: &mut OpenShortPosition,
    bar: &Kline,
    mode: ShortLadderMode,
    config: &ShortLadderConfig,
    allow_final_layer: bool,
    signal_history: &[Kline],
    signal_index: usize,
) -> bool {
    if position.next_layer_index >= config.layer_weights.len() {
        return false;
    }
    let is_final_layer = position.next_layer_index + 1 == config.layer_weights.len();
    if is_final_layer && config.final_layer_requires_strong_1h && !allow_final_layer {
        return false;
    }

    let trigger_price = next_layer_trigger_price(
        position,
        mode,
        config,
        signal_history,
        signal_index,
        is_final_layer,
    );
    let touched = match mode {
        ShortLadderMode::AdverseAveraging => bar.high >= trigger_price,
        ShortLadderMode::FavorablePyramiding => bar.low <= trigger_price,
    };
    if !touched {
        return false;
    }
    if !maker_fill_allowed(
        symbol,
        bar.close_time.timestamp_millis(),
        position.next_layer_index,
        config,
    ) {
        return false;
    }

    let weight = config.layer_weights[position.next_layer_index];
    let notional = config.initial_notional * weight;
    let quantity = notional / trigger_price;
    let old_notional = position.average_entry_price * position.quantity;
    position.quantity += quantity;
    position.average_entry_price = (old_notional + trigger_price * quantity) / position.quantity;
    position.last_layer_price = trigger_price;
    position.next_layer_index += 1;
    position.entry_prices.push(trigger_price);
    position.entry_notionals.push(notional);
    position.entry_times.push(bar.close_time);
    reset_take_profit_state(position, trigger_price);
    position.fees_paid += notional * config.maker_fee_bps / 10_000.0;
    true
}

fn can_add_layer_on_execution_bar(
    position: &OpenShortPosition,
    timestamp: DateTime<Utc>,
    last_order_at: Option<DateTime<Utc>>,
    layer_fill_times: &[DateTime<Utc>],
    config: &ShortLadderConfig,
    execution_config: &ShortLadderMtfExecutionConfig,
) -> bool {
    if position.next_layer_index >= config.layer_weights.len() {
        return false;
    }
    if let Some(last_order_at) = last_order_at {
        let elapsed = timestamp
            .signed_duration_since(last_order_at)
            .num_seconds()
            .max(0) as u64;
        if elapsed < execution_config.order_cooldown_secs {
            return false;
        }
    }

    let next_filled_layers = position.next_layer_index + 1;
    if next_filled_layers >= config.layer_weights.len() {
        let elapsed = timestamp
            .signed_duration_since(position.entry_time)
            .num_minutes()
            .max(0) as u64;
        if elapsed < execution_config.min_minutes_to_l4 {
            return false;
        }
    }

    if execution_config.max_layers_per_hour > 0 {
        let recent_layers = layer_fill_times
            .iter()
            .filter(|fill_time| {
                timestamp
                    .signed_duration_since(**fill_time)
                    .num_seconds()
                    .max(0)
                    < 3_600
            })
            .count();
        if recent_layers >= execution_config.max_layers_per_hour {
            return false;
        }
    }

    true
}

fn next_layer_trigger_price(
    position: &OpenShortPosition,
    mode: ShortLadderMode,
    config: &ShortLadderConfig,
    signal_history: &[Kline],
    signal_index: usize,
    is_final_layer: bool,
) -> f64 {
    let spacing_atr = if is_final_layer {
        config
            .final_layer_spacing_atr
            .unwrap_or(config.layer_spacing_atr)
            .max(0.0)
    } else {
        config.layer_spacing_atr.max(0.0)
    };
    let spacing = spacing_atr * position.atr_at_entry;
    let atr_spacing_price = match mode {
        ShortLadderMode::AdverseAveraging => position.last_layer_price + spacing,
        ShortLadderMode::FavorablePyramiding => position.last_layer_price - spacing,
    };

    if !matches!(config.layer_price_mode, LayerPriceMode::AtrResistanceLoss)
        || !matches!(mode, ShortLadderMode::AdverseAveraging)
    {
        return atr_spacing_price;
    }

    let resistance_lookback_bars = if is_final_layer {
        config
            .final_layer_resistance_lookback_bars
            .unwrap_or(config.layer_resistance_lookback_bars)
    } else {
        config.layer_resistance_lookback_bars
    };
    let resistance_buffer_atr = if is_final_layer {
        config
            .final_layer_resistance_buffer_atr
            .unwrap_or(config.layer_resistance_buffer_atr)
    } else {
        config.layer_resistance_buffer_atr
    };
    let resistance_price =
        recent_resistance_price(signal_history, signal_index, resistance_lookback_bars)
            .map(|price| price + resistance_buffer_atr.max(0.0) * position.atr_at_entry)
            .unwrap_or(atr_spacing_price);
    let min_loss_price = min_loss_trigger_price(position, config);

    atr_spacing_price.max(resistance_price).max(min_loss_price)
}

fn recent_resistance_price(
    signal_history: &[Kline],
    signal_index: usize,
    lookback_bars: usize,
) -> Option<f64> {
    if signal_history.is_empty() || lookback_bars == 0 {
        return None;
    }
    let end = signal_index.min(signal_history.len());
    let start = end.saturating_sub(lookback_bars);
    signal_history
        .get(start..end)
        .and_then(|bars| bars.iter().map(|bar| bar.high).reduce(f64::max))
}

fn recent_support_price(
    signal_history: &[Kline],
    signal_index: usize,
    lookback_bars: usize,
) -> Option<f64> {
    if signal_history.is_empty() || lookback_bars == 0 {
        return None;
    }
    let end = signal_index.min(signal_history.len());
    let start = end.saturating_sub(lookback_bars);
    signal_history
        .get(start..end)
        .and_then(|bars| bars.iter().map(|bar| bar.low).reduce(f64::min))
}

fn recent_average_volume(
    signal_history: &[Kline],
    signal_index: usize,
    lookback_bars: usize,
) -> Option<f64> {
    if signal_history.is_empty() || lookback_bars == 0 {
        return None;
    }
    let end = signal_index.min(signal_history.len());
    let start = end.saturating_sub(lookback_bars);
    let bars = signal_history.get(start..end)?;
    if bars.is_empty() {
        return None;
    }
    Some(bars.iter().map(|bar| bar.volume).sum::<f64>() / bars.len() as f64)
}

fn candle_range(bar: &Kline) -> f64 {
    (bar.high - bar.low).max(0.0)
}

fn candle_upper_wick_ratio(bar: &Kline) -> f64 {
    let range = candle_range(bar);
    if range <= f64::EPSILON {
        return 0.0;
    }
    let body_top = bar.open.max(bar.close);
    (bar.high - body_top).max(0.0) / range
}

fn min_loss_trigger_price(position: &OpenShortPosition, config: &ShortLadderConfig) -> f64 {
    let min_loss_bps = config
        .layer_min_loss_bps
        .get(position.next_layer_index)
        .copied()
        .unwrap_or(0.0)
        .max(0.0);
    position.average_entry_price * (1.0 + min_loss_bps / 10_000.0)
}

fn maker_fill_allowed(
    symbol: &str,
    close_time_millis: i64,
    layer_index: usize,
    config: &ShortLadderConfig,
) -> bool {
    let fill_rate = config.maker_fill_rate.clamp(0.0, 1.0);
    if fill_rate >= 1.0 {
        return true;
    }
    if fill_rate <= 0.0 {
        return false;
    }

    let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ config.maker_fill_seed;
    for byte in symbol.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= close_time_millis as u64;
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    hash ^= layer_index as u64;
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;

    let sample = (hash as f64) / (u64::MAX as f64);
    sample < fill_rate
}

fn initial_entry_fee_bps(
    symbol: &str,
    close_time_millis: i64,
    config: &ShortLadderConfig,
) -> Option<f64> {
    if maker_fill_allowed(symbol, close_time_millis, 0, config) {
        return Some(config.maker_fee_bps);
    }
    if config.initial_order_taker_fallback_secs > 0 {
        return Some(config.taker_fee_bps);
    }
    None
}

fn reset_take_profit_state(position: &mut OpenShortPosition, reference_price: f64) {
    position.best_favorable_price = reference_price;
    position.trailing_take_profit_armed = false;
}

fn update_take_profit_state(
    position: &mut OpenShortPosition,
    bar: &Kline,
    config: &ShortLadderConfig,
) {
    if !matches!(config.take_profit_mode, TakeProfitMode::AtrTrailing) {
        return;
    }
    position.best_favorable_price = position.best_favorable_price.min(bar.low);
    let activation_price = position.average_entry_price
        - config.trailing_take_profit_activation_atr.max(0.0) * position.atr_at_entry;
    if position.best_favorable_price <= activation_price {
        position.trailing_take_profit_armed = true;
    }
}

fn take_profit_exit_decision(
    position: &OpenShortPosition,
    bar: &Kline,
    config: &ShortLadderConfig,
) -> Option<(f64, String)> {
    match config.take_profit_mode {
        TakeProfitMode::FixedAtr => {
            let take_profit_price =
                position.average_entry_price - config.take_profit_atr * position.atr_at_entry;
            if bar.low <= take_profit_price {
                Some((take_profit_price, "take_profit".to_string()))
            } else {
                None
            }
        }
        TakeProfitMode::AtrTrailing => {
            if !position.trailing_take_profit_armed {
                return None;
            }
            let trailing_price = position.best_favorable_price
                + config.trailing_take_profit_distance_atr.max(0.0) * position.atr_at_entry;
            if bar.high >= trailing_price {
                Some((trailing_price, "trailing_take_profit".to_string()))
            } else {
                None
            }
        }
    }
}

fn exit_decision(
    position: &OpenShortPosition,
    bar: &Kline,
    index: usize,
    config: &ShortLadderConfig,
) -> Option<(f64, String)> {
    let stop_price = position.average_entry_price + config.stop_loss_atr * position.atr_at_entry;
    let breakeven_price =
        position.average_entry_price * (1.0 - config.breakeven_buffer_bps / 10_000.0);

    if stop_loss_enabled(position, config) && bar.high >= stop_price {
        return Some((stop_price, "stop_loss".to_string()));
    }
    if position.breakeven_armed && bar.high >= breakeven_price {
        return Some((breakeven_price, "breakeven_stop".to_string()));
    }
    if let Some(exit) = take_profit_exit_decision(position, bar, config) {
        return Some(exit);
    }
    if index.saturating_sub(position.opened_bar_index) >= config.max_hold_bars {
        return Some((bar.close, "time_stop".to_string()));
    }
    if !is_in_session(bar.close_time, config) {
        return Some((bar.close, "session_end".to_string()));
    }
    None
}

fn pre_layer_adverse_exit_decision(
    position: &OpenShortPosition,
    bar: &Kline,
    index: usize,
    config: &ShortLadderConfig,
) -> Option<(f64, String)> {
    let stop_price = position.average_entry_price + config.stop_loss_atr * position.atr_at_entry;
    let breakeven_price =
        position.average_entry_price * (1.0 - config.breakeven_buffer_bps / 10_000.0);

    if stop_loss_enabled(position, config) && bar.high >= stop_price {
        return Some((stop_price, "stop_loss".to_string()));
    }
    if position.breakeven_armed && bar.high >= breakeven_price {
        return Some((breakeven_price, "breakeven_stop".to_string()));
    }
    if index.saturating_sub(position.opened_bar_index) >= config.max_hold_bars {
        return Some((bar.close, "time_stop".to_string()));
    }
    if !is_in_session(bar.close_time, config) {
        return Some((bar.close, "session_end".to_string()));
    }
    None
}

fn stop_loss_enabled(position: &OpenShortPosition, config: &ShortLadderConfig) -> bool {
    let min_layers = config.stop_loss_min_layers.max(1);
    position.entry_prices.len() >= min_layers
}

fn exit_decision_after_intrabar_updates(
    position: &OpenShortPosition,
    bar: &Kline,
    index: usize,
    config: &ShortLadderConfig,
    filled_layer_this_bar: bool,
) -> Option<(f64, String)> {
    if matches!(config.intrabar_path_mode, IntrabarPathMode::Pessimistic) {
        if filled_layer_this_bar {
            return pre_layer_adverse_exit_decision(position, bar, index, config);
        }
    }
    exit_decision(position, bar, index, config)
}

fn close_position(
    position: &OpenShortPosition,
    exit_time: DateTime<Utc>,
    exit_price: f64,
    reason: String,
    config: &ShortLadderConfig,
) -> ShortLadderTrade {
    let exit_notional = exit_price * position.quantity;
    let exit_fee = exit_notional * config.taker_fee_bps / 10_000.0;
    let fees_paid = position.fees_paid + exit_fee;
    let realized_pnl = (position.average_entry_price - exit_price) * position.quantity;
    ShortLadderTrade {
        mode: position.mode,
        entry_time: position.entry_time,
        exit_time,
        average_entry_price: position.average_entry_price,
        exit_price,
        quantity: position.quantity,
        layers_filled: position.entry_prices.len(),
        max_notional: position.entry_notionals.iter().sum(),
        realized_pnl,
        fees_paid,
        net_pnl: realized_pnl - fees_paid,
        exit_reason: reason,
        max_unrealized_loss: position.max_unrealized_loss,
        entry_prices: position.entry_prices.clone(),
        entry_notionals: position.entry_notionals.clone(),
        entry_times: position.entry_times.clone(),
    }
}

fn short_entry_trigger(
    index: usize,
    candles: &[Kline],
    rsi_values: &[Option<f64>],
    atr_values: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    if index < 2 || atr_values[index].is_none() {
        return false;
    }
    if !entry_range_position_filter(index, candles, config) {
        return false;
    }
    match config.entry_signal_mode {
        EntrySignalMode::LegacyRsi => legacy_rsi_entry(index, candles, rsi_values, config),
        EntrySignalMode::ResistanceSweepRejection => {
            resistance_sweep_rejection_entry(index, candles, rsi_values, config)
        }
        EntrySignalMode::BreakdownPullback => {
            breakdown_pullback_entry(index, candles, atr_values, config)
        }
        EntrySignalMode::BearishVolatilityExpansion => {
            bearish_volatility_expansion_entry(index, candles, atr_values, config)
        }
    }
}

fn entry_range_position_filter(
    index: usize,
    candles: &[Kline],
    config: &ShortLadderConfig,
) -> bool {
    if config.entry_range_position_lookback_bars == 0 {
        return true;
    }
    let end = index.min(candles.len().saturating_sub(1));
    let start = end.saturating_sub(config.entry_range_position_lookback_bars);
    let Some(history) = candles.get(start..end) else {
        return false;
    };
    if history.is_empty() {
        return false;
    }
    let range_high = history.iter().map(|bar| bar.high).fold(f64::MIN, f64::max);
    let range_low = history.iter().map(|bar| bar.low).fold(f64::MAX, f64::min);
    let range = range_high - range_low;
    if range <= f64::EPSILON {
        return false;
    }
    let close = candles[index].close;
    let position = ((close - range_low) / range).clamp(0.0, 1.0);
    position >= config.entry_range_position_min.clamp(0.0, 1.0)
        && position <= config.entry_range_position_max.clamp(0.0, 1.0)
}

fn legacy_rsi_entry(
    index: usize,
    candles: &[Kline],
    rsi_values: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    let Some(rsi_now) = rsi_values[index] else {
        return false;
    };
    let previous = &candles[index - 1];
    let current = &candles[index];
    if rsi_now < config.entry_rsi_min {
        return false;
    }
    if config.entry_requires_lower_close && current.close >= previous.close {
        return false;
    }
    !config.entry_requires_prior_high_sweep || current.high >= previous.high
}

fn resistance_sweep_rejection_entry(
    index: usize,
    candles: &[Kline],
    rsi_values: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    let Some(rsi_now) = rsi_values[index] else {
        return false;
    };
    if rsi_now < config.entry_rsi_min {
        return false;
    }
    let Some(resistance) =
        recent_resistance_price(candles, index, config.entry_resistance_lookback_bars)
    else {
        return false;
    };
    let current = &candles[index];
    if current.high <= resistance || current.close >= resistance {
        return false;
    }
    candle_upper_wick_ratio(current) >= config.entry_upper_wick_min_ratio
}

fn breakdown_pullback_entry(
    index: usize,
    candles: &[Kline],
    atr_values: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    let Some(atr_now) = atr_values[index] else {
        return false;
    };
    let max_bars = config.entry_pullback_max_bars.max(1);
    let earliest = index.saturating_sub(max_bars);
    for breakdown_index in earliest..index {
        let Some(support) = recent_support_price(
            candles,
            breakdown_index,
            config.entry_breakdown_lookback_bars,
        ) else {
            continue;
        };
        let breakdown_bar = &candles[breakdown_index];
        if breakdown_bar.close >= support {
            continue;
        }
        let current = &candles[index];
        let tolerance = config.entry_pullback_tolerance_atr.max(0.0) * atr_now;
        if current.high >= support - tolerance
            && current.close < support
            && current.close < current.open
        {
            return true;
        }
    }
    false
}

fn bearish_volatility_expansion_entry(
    index: usize,
    candles: &[Kline],
    atr_values: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    let Some(atr_now) = atr_values[index] else {
        return false;
    };
    let current = &candles[index];
    let range = candle_range(current);
    if range <= f64::EPSILON || current.close >= current.open {
        return false;
    }
    let body = (current.open - current.close).abs();
    if body < config.entry_body_atr_min.max(0.0) * atr_now {
        return false;
    }
    if range < config.entry_range_atr_min.max(0.0) * atr_now {
        return false;
    }
    let close_near_low = (current.close - current.low) / range;
    if close_near_low > config.entry_close_near_low_ratio {
        return false;
    }
    if config.entry_volume_sma_bars > 0 && config.entry_volume_multiplier > 0.0 {
        let Some(volume_sma) = recent_average_volume(candles, index, config.entry_volume_sma_bars)
        else {
            return false;
        };
        if current.volume < volume_sma * config.entry_volume_multiplier {
            return false;
        }
    }
    true
}

fn short_regime(
    index_5m: usize,
    candles_15m: &[Kline],
    candles_1h: &[Kline],
    ema_15m: &[Option<f64>],
    ema_1h: &[Option<f64>],
) -> bool {
    let index_15m = index_5m / 3;
    let index_1h = index_5m / 12;
    if index_15m >= candles_15m.len() || index_1h >= candles_1h.len() {
        return false;
    }
    let Some(ema15) = ema_15m[index_15m] else {
        return false;
    };
    let Some(ema1h) = ema_1h[index_1h] else {
        return false;
    };
    candles_15m[index_15m].close < ema15 && candles_1h[index_1h].close < ema1h
}

fn strong_1h_short_regime(
    index_5m: usize,
    candles_1h: &[Kline],
    ema_1h: &[Option<f64>],
    config: &ShortLadderConfig,
) -> bool {
    let index_1h = index_5m / 12;
    let lookback = config.strong_1h_ema_slope_lookback;
    if lookback == 0 || index_1h < lookback || index_1h >= candles_1h.len() {
        return false;
    }
    let Some(ema_now) = ema_1h[index_1h] else {
        return false;
    };
    let Some(ema_then) = ema_1h[index_1h - lookback] else {
        return false;
    };
    if ema_then.abs() <= f64::EPSILON {
        return false;
    }
    let ema_slope_pct = (ema_now / ema_then - 1.0) * 100.0;
    candles_1h[index_1h].close < ema_now && ema_slope_pct <= config.strong_1h_ema_slope_max_pct
}

fn update_unrealized_loss(position: &mut OpenShortPosition, high_price: f64) {
    let unrealized = (high_price - position.average_entry_price) * position.quantity;
    if unrealized > position.max_unrealized_loss {
        position.max_unrealized_loss = unrealized;
    }
}

fn summarize_run(
    mode: ShortLadderMode,
    bars: usize,
    trades: &[ShortLadderTrade],
    equity_curve: &[f64],
    config: &ShortLadderConfig,
) -> ShortLadderRunSummary {
    let wins = trades.iter().filter(|trade| trade.net_pnl > 0.0).count();
    let losses = trades.iter().filter(|trade| trade.net_pnl < 0.0).count();
    let gross_profit = trades
        .iter()
        .filter(|trade| trade.net_pnl > 0.0)
        .map(|trade| trade.net_pnl)
        .sum::<f64>();
    let gross_loss = trades
        .iter()
        .filter(|trade| trade.net_pnl < 0.0)
        .map(|trade| -trade.net_pnl)
        .sum::<f64>();
    let net_pnl = trades.iter().map(|trade| trade.net_pnl).sum::<f64>();
    let roi_pct = net_pnl / config.initial_equity * 100.0;
    let annualized_return_pct = annualized_return_pct(roi_pct, trades);
    ShortLadderRunSummary {
        mode,
        bars,
        trades: trades.len(),
        wins,
        losses,
        win_rate_pct: if trades.is_empty() {
            0.0
        } else {
            wins as f64 / trades.len() as f64 * 100.0
        },
        gross_profit,
        gross_loss,
        net_pnl,
        roi_pct,
        annualized_return_pct,
        profit_factor: if gross_loss > f64::EPSILON {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        },
        max_drawdown_pct: max_drawdown_pct(equity_curve),
        max_layers_filled: trades
            .iter()
            .map(|trade| trade.layers_filled)
            .max()
            .unwrap_or(0),
        max_position_notional: trades
            .iter()
            .map(|trade| trade.max_notional)
            .fold(0.0, f64::max),
        total_fees_paid: trades.iter().map(|trade| trade.fees_paid).sum(),
    }
}

fn annualized_return_pct(roi_pct: f64, trades: &[ShortLadderTrade]) -> f64 {
    let Some(first_trade) = trades.first() else {
        return 0.0;
    };
    let Some(last_trade) = trades.last() else {
        return 0.0;
    };
    let elapsed_seconds = last_trade
        .exit_time
        .signed_duration_since(first_trade.entry_time)
        .num_seconds()
        .max(1) as f64;
    let years = elapsed_seconds / (365.25 * 24.0 * 60.0 * 60.0);
    if years <= f64::EPSILON {
        return roi_pct;
    }
    let growth = 1.0 + roi_pct / 100.0;
    if growth <= 0.0 {
        return -100.0;
    }
    (growth.powf(1.0 / years) - 1.0) * 100.0
}

fn max_drawdown_pct(equity_curve: &[f64]) -> f64 {
    let mut peak = f64::MIN;
    let mut max_drawdown = 0.0;
    for equity in equity_curve {
        peak = peak.max(*equity);
        if peak > f64::EPSILON {
            let drawdown = equity / peak - 1.0;
            if drawdown < max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }
    max_drawdown * 100.0
}

fn is_in_session(timestamp: DateTime<Utc>, config: &ShortLadderConfig) -> bool {
    let hour = timestamp.hour();
    if config.session_start_hour_utc <= config.session_end_hour_utc {
        hour >= config.session_start_hour_utc && hour < config.session_end_hour_utc
    } else {
        hour >= config.session_start_hour_utc || hour < config.session_end_hour_utc
    }
}

fn aggregate_klines(candles: &[Kline], chunk_size: usize, interval: &str) -> Vec<Kline> {
    if candles.is_empty() || chunk_size == 0 {
        return Vec::new();
    }

    let source_duration_ms =
        interval_label_duration_ms(&candles[0].interval).unwrap_or_else(|| {
            candles[0]
                .close_time
                .signed_duration_since(candles[0].open_time)
                .num_milliseconds()
                + 1
        });
    if source_duration_ms <= 0 {
        return Vec::new();
    }
    let target_duration_ms = source_duration_ms * chunk_size as i64;

    let mut sorted = candles.to_vec();
    sorted.sort_by_key(|bar| bar.open_time);
    sorted.dedup_by_key(|bar| bar.open_time);

    let mut aggregated = Vec::new();
    let mut bucket = Vec::new();
    let mut current_bucket_id = None;

    for candle in sorted {
        let bucket_id = candle
            .open_time
            .timestamp_millis()
            .div_euclid(target_duration_ms);
        if current_bucket_id.is_some_and(|current| current != bucket_id) {
            if let Some(aggregated_bar) = aggregate_complete_bucket(&bucket, chunk_size, interval) {
                aggregated.push(aggregated_bar);
            }
            bucket.clear();
        }
        current_bucket_id = Some(bucket_id);
        bucket.push(candle);
    }

    if let Some(aggregated_bar) = aggregate_complete_bucket(&bucket, chunk_size, interval) {
        aggregated.push(aggregated_bar);
    }

    aggregated
}

fn interval_label_duration_ms(interval: &str) -> Option<i64> {
    let (value, unit) = interval.split_at(interval.len().saturating_sub(1));
    let value = value.parse::<i64>().ok()?;
    let unit_ms = match unit {
        "m" => 60_000,
        "h" => 60 * 60_000,
        "d" => 24 * 60 * 60_000,
        "w" => 7 * 24 * 60 * 60_000,
        _ => return None,
    };
    Some(value * unit_ms)
}

fn aggregate_complete_bucket(bucket: &[Kline], chunk_size: usize, interval: &str) -> Option<Kline> {
    if bucket.len() != chunk_size {
        return None;
    }

    let first = bucket.first().expect("bucket has first candle");
    let last = bucket.last().expect("bucket has last candle");
    Some(Kline {
        symbol: first.symbol.clone(),
        interval: interval.to_string(),
        open_time: first.open_time,
        close_time: last.close_time,
        open: first.open,
        high: bucket.iter().map(|bar| bar.high).fold(f64::MIN, f64::max),
        low: bucket.iter().map(|bar| bar.low).fold(f64::MAX, f64::min),
        close: last.close,
        volume: bucket.iter().map(|bar| bar.volume).sum(),
        quote_volume: bucket.iter().map(|bar| bar.quote_volume).sum(),
        trade_count: bucket.iter().map(|bar| bar.trade_count).sum(),
    })
}

fn default_initial_equity() -> f64 {
    1_000.0
}

fn default_initial_notional() -> f64 {
    50.0
}

fn default_layer_weights() -> Vec<f64> {
    vec![1.0, 1.0, 3.0, 5.0]
}

fn default_session_start_hour() -> u32 {
    11
}

fn default_session_end_hour() -> u32 {
    22
}

fn default_maker_fee_bps() -> f64 {
    0.0
}

fn default_taker_fee_bps() -> f64 {
    4.0
}

fn default_maker_fill_rate() -> f64 {
    1.0
}

fn default_breakeven_trigger_atr() -> f64 {
    0.8
}

fn default_breakeven_buffer_bps() -> f64 {
    2.0
}

fn default_atr_period() -> usize {
    14
}

fn default_rsi_period() -> usize {
    14
}

fn default_entry_rsi() -> f64 {
    55.0
}

fn default_entry_resistance_lookback_bars() -> usize {
    24
}

fn default_entry_upper_wick_min_ratio() -> f64 {
    0.35
}

fn default_entry_breakdown_lookback_bars() -> usize {
    24
}

fn default_entry_pullback_max_bars() -> usize {
    6
}

fn default_entry_pullback_tolerance_atr() -> f64 {
    0.3
}

fn default_entry_body_atr_min() -> f64 {
    0.6
}

fn default_entry_range_atr_min() -> f64 {
    1.2
}

fn default_entry_close_near_low_ratio() -> f64 {
    0.25
}

fn default_entry_volume_sma_bars() -> usize {
    24
}

fn default_entry_volume_multiplier() -> f64 {
    1.2
}

fn default_entry_range_position_max() -> f64 {
    1.0
}

fn default_true() -> bool {
    true
}

fn default_layer_spacing_atr() -> f64 {
    0.55
}

fn default_layer_resistance_lookback_bars() -> usize {
    24
}

fn default_layer_resistance_buffer_atr() -> f64 {
    0.1
}

fn default_layer_min_loss_bps() -> Vec<f64> {
    vec![0.0, 35.0, 80.0, 140.0]
}

fn default_take_profit_atr() -> f64 {
    1.2
}

fn default_trailing_take_profit_activation_atr() -> f64 {
    1.0
}

fn default_trailing_take_profit_distance_atr() -> f64 {
    0.8
}

fn default_stop_loss_atr() -> f64 {
    1.7
}

fn default_stop_loss_min_layers() -> usize {
    1
}

fn default_max_hold_bars() -> usize {
    48
}

fn default_filter_15m_ema() -> usize {
    50
}

fn default_filter_1h_ema() -> usize {
    50
}

fn default_strong_1h_ema_slope_lookback() -> usize {
    3
}

fn default_strong_1h_ema_slope_max_pct() -> f64 {
    -0.03
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn bar(index: i64, open: f64, high: f64, low: f64, close: f64) -> Kline {
        let open_time =
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap() + Duration::minutes(index * 5);
        Kline {
            symbol: "SOL/USDT".to_string(),
            interval: "5m".to_string(),
            open_time,
            close_time: open_time + Duration::minutes(5) - Duration::seconds(1),
            open,
            high,
            low,
            close,
            volume: 1_000.0,
            quote_volume: 100_000.0,
            trade_count: 100,
        }
    }

    fn one_minute_bar(minute: i64, close: f64) -> Kline {
        let open_time =
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap() + Duration::minutes(minute);
        Kline {
            symbol: "SOL/USDT".to_string(),
            interval: "1m".to_string(),
            open_time,
            close_time: open_time + Duration::minutes(1) - Duration::seconds(1),
            open: close,
            high: close + 0.1,
            low: close - 0.1,
            close,
            volume: 100.0,
            quote_volume: 10_000.0,
            trade_count: 10,
        }
    }

    #[test]
    fn aggregate_klines_uses_clock_aligned_complete_buckets() {
        let candles = (2..10)
            .map(|minute| one_minute_bar(minute, 100.0 + minute as f64))
            .collect::<Vec<_>>();

        let aggregated = aggregate_klines(&candles, 5, "5m");

        assert_eq!(aggregated.len(), 1);
        assert_eq!(
            aggregated[0].open_time,
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 5, 0).unwrap()
        );
        assert_eq!(
            aggregated[0].close_time,
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 9, 59).unwrap()
        );
        assert_eq!(aggregated[0].open, 105.0);
        assert_eq!(aggregated[0].close, 109.0);
    }

    #[test]
    fn initial_entry_uses_taker_fee_when_maker_times_out_with_fallback() {
        let config = ShortLadderConfig {
            maker_fill_rate: 0.0,
            maker_fee_bps: 0.0,
            taker_fee_bps: 3.2,
            initial_order_taker_fallback_secs: 45,
            ..Default::default()
        };

        let fee_bps = initial_entry_fee_bps("SOLUSDC", 1_700_000_000_000, &config)
            .expect("fallback should ensure initial fill");

        assert_eq!(fee_bps, 3.2);
    }

    #[test]
    fn initial_entry_without_fallback_skips_when_maker_order_not_filled() {
        let config = ShortLadderConfig {
            maker_fill_rate: 0.0,
            initial_order_taker_fallback_secs: 0,
            ..Default::default()
        };

        assert!(initial_entry_fee_bps("SOLUSDC", 1_700_000_000_000, &config).is_none());
    }

    #[test]
    fn entry_range_position_filter_requires_golden_ratio_zone() {
        let candles = vec![
            bar(0, 95.0, 100.0, 90.0, 95.0),
            bar(1, 95.0, 99.0, 91.0, 96.0),
            bar(2, 96.0, 98.0, 92.0, 96.5),
            bar(3, 96.5, 97.0, 94.0, 95.5),
            bar(4, 96.5, 99.0, 94.0, 97.0),
        ];
        let config = ShortLadderConfig {
            entry_range_position_lookback_bars: 3,
            entry_range_position_min: 0.618,
            ..Default::default()
        };

        assert!(!entry_range_position_filter(3, &candles, &config));
        assert!(entry_range_position_filter(4, &candles, &config));
    }

    #[test]
    fn adverse_averaging_fills_layers_when_price_moves_against_short() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_spacing_atr: 0.5,
            ..Default::default()
        };

        fill_next_layer_if_triggered(
            "SOLUSDC",
            &mut position,
            &bar(1, 100.0, 101.1, 99.0, 100.5),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
        );

        assert_eq!(position.entry_prices, vec![100.0, 101.0]);
        assert_eq!(position.entry_notionals, vec![50.0, 50.0]);
    }

    #[test]
    fn structured_layering_waits_until_min_loss_price_is_touched() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_price_mode: LayerPriceMode::AtrResistanceLoss,
            layer_spacing_atr: 0.5,
            layer_min_loss_bps: vec![0.0, 200.0, 400.0, 800.0],
            ..Default::default()
        };

        let history = vec![bar(0, 100.0, 100.2, 99.0, 100.0)];
        let filled = fill_next_layer_once_if_triggered_with_history(
            "SOLUSDC",
            &mut position,
            &bar(1, 100.0, 101.9, 99.0, 101.5),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
            &history,
            history.len(),
        );

        assert!(!filled);
        assert_eq!(position.entry_prices, vec![100.0]);

        let filled = fill_next_layer_once_if_triggered_with_history(
            "SOLUSDC",
            &mut position,
            &bar(2, 101.5, 102.1, 100.0, 101.8),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
            &history,
            history.len(),
        );

        assert!(filled);
        assert_eq!(position.entry_prices, vec![100.0, 102.0]);
    }

    #[test]
    fn structured_layering_uses_recent_resistance_plus_buffer() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_price_mode: LayerPriceMode::AtrResistanceLoss,
            layer_spacing_atr: 0.5,
            layer_resistance_lookback_bars: 3,
            layer_resistance_buffer_atr: 0.25,
            layer_min_loss_bps: vec![0.0, 0.0, 0.0, 0.0],
            ..Default::default()
        };
        let history = vec![
            bar(0, 100.0, 101.0, 99.0, 100.0),
            bar(1, 100.0, 103.0, 99.0, 101.0),
            bar(2, 101.0, 102.0, 100.0, 101.5),
        ];

        let filled = fill_next_layer_once_if_triggered_with_history(
            "SOLUSDC",
            &mut position,
            &bar(3, 101.5, 103.4, 100.0, 102.0),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
            &history,
            history.len(),
        );

        assert!(!filled);

        let filled = fill_next_layer_once_if_triggered_with_history(
            "SOLUSDC",
            &mut position,
            &bar(4, 102.0, 103.6, 101.0, 103.0),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
            &history,
            history.len(),
        );

        assert!(filled);
        assert_eq!(position.entry_prices, vec![100.0, 103.5]);
    }

    #[test]
    fn favorable_pyramiding_fills_layers_when_price_moves_with_short() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::FavorablePyramiding,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_spacing_atr: 0.5,
            ..Default::default()
        };

        fill_next_layer_if_triggered(
            "SOLUSDC",
            &mut position,
            &bar(1, 100.0, 100.2, 98.9, 99.1),
            ShortLadderMode::FavorablePyramiding,
            &config,
            true,
        );

        assert_eq!(position.entry_prices, vec![100.0, 99.0]);
        assert_eq!(position.entry_notionals, vec![50.0, 50.0]);
    }

    #[test]
    fn final_layer_is_blocked_when_strong_1h_filter_is_not_met() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 101.0,
            quantity: 1.5,
            next_layer_index: 3,
            last_layer_price: 102.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0, 102.0],
            entry_notionals: vec![50.0, 50.0, 150.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_spacing_atr: 0.5,
            final_layer_requires_strong_1h: true,
            ..Default::default()
        };

        fill_next_layer_if_triggered(
            "SOLUSDC",
            &mut position,
            &bar(1, 102.0, 103.2, 101.0, 102.8),
            ShortLadderMode::AdverseAveraging,
            &config,
            false,
        );

        assert_eq!(position.entry_prices, vec![100.0, 101.0, 102.0]);
        assert_eq!(position.next_layer_index, 3);
    }

    #[test]
    fn maker_fill_rate_zero_blocks_layer_fill() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_spacing_atr: 0.5,
            maker_fill_rate: 0.0,
            ..Default::default()
        };

        fill_next_layer_if_triggered(
            "SOLUSDC",
            &mut position,
            &bar(1, 100.0, 101.1, 99.0, 100.5),
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
        );

        assert_eq!(position.entry_prices, vec![100.0]);
        assert_eq!(position.next_layer_index, 1);
    }

    #[test]
    fn maker_fill_rate_one_always_allows_fill() {
        let config = ShortLadderConfig {
            maker_fill_rate: 1.0,
            maker_fill_seed: 7,
            ..Default::default()
        };

        assert!(maker_fill_allowed("SOLUSDC", 1_767_225_600_000, 0, &config));
        assert!(maker_fill_allowed("SOLUSDC", 1_767_225_900_000, 3, &config));
    }

    #[test]
    fn mtf_execution_records_layer_fill_time() {
        let entry_time = Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap();
        let fill_bar = bar(1, 100.0, 101.1, 99.0, 100.5);
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time,
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: vec![entry_time],
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            layer_spacing_atr: 0.5,
            ..Default::default()
        };

        assert!(fill_next_layer_once_if_triggered(
            "SOLUSDC",
            &mut position,
            &fill_bar,
            ShortLadderMode::AdverseAveraging,
            &config,
            true,
        ));

        assert_eq!(position.entry_times, vec![entry_time, fill_bar.close_time]);
    }

    #[test]
    fn mtf_execution_respects_layer_cooldown() {
        let entry_time = Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap();
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time,
            average_entry_price: 100.0,
            quantity: 2.0,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![200.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig::default();
        let execution_config = ShortLadderMtfExecutionConfig {
            order_cooldown_secs: 600,
            max_layers_per_hour: 10,
            ..Default::default()
        };

        assert!(!can_add_layer_on_execution_bar(
            &position,
            entry_time + Duration::minutes(5),
            Some(entry_time),
            &[entry_time],
            &config,
            &execution_config,
        ));
        assert!(can_add_layer_on_execution_bar(
            &position,
            entry_time + Duration::minutes(10),
            Some(entry_time),
            &[entry_time],
            &config,
            &execution_config,
        ));
    }

    #[test]
    fn mtf_execution_limits_layers_per_hour() {
        let entry_time = Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap();
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time,
            average_entry_price: 100.0,
            quantity: 4.0,
            next_layer_index: 2,
            last_layer_price: 101.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0],
            entry_notionals: vec![200.0, 200.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig::default();
        let execution_config = ShortLadderMtfExecutionConfig {
            order_cooldown_secs: 300,
            max_layers_per_hour: 2,
            ..Default::default()
        };
        let layer_fill_times = vec![entry_time, entry_time + Duration::minutes(10)];

        assert!(!can_add_layer_on_execution_bar(
            &position,
            entry_time + Duration::minutes(30),
            Some(entry_time + Duration::minutes(10)),
            &layer_fill_times,
            &config,
            &execution_config,
        ));
        assert!(can_add_layer_on_execution_bar(
            &position,
            entry_time + Duration::minutes(61),
            Some(entry_time + Duration::minutes(10)),
            &layer_fill_times,
            &config,
            &execution_config,
        ));
    }

    #[test]
    fn maker_fill_rate_zero_blocks_initial_entries() {
        let candles = (0..720)
            .map(|index| {
                let close = 200.0 - index as f64 * 0.01;
                bar(index, close + 0.02, close + 0.05, close - 0.05, close)
            })
            .collect::<Vec<_>>();
        let config = ShortLadderConfig {
            maker_fill_rate: 0.0,
            entry_rsi_min: 0.0,
            entry_requires_lower_close: false,
            entry_requires_prior_high_sweep: false,
            session_start_hour_utc: 0,
            session_end_hour_utc: 24,
            ..Default::default()
        };

        let report = run_short_ladder_backtest(
            "SOLUSDC",
            &candles,
            ShortLadderMode::AdverseAveraging,
            config,
        );

        assert_eq!(report.summary.trades, 0);
    }

    #[test]
    fn pessimistic_path_stops_before_same_bar_layer_can_move_stop() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            intrabar_path_mode: IntrabarPathMode::Pessimistic,
            stop_loss_atr: 1.7,
            layer_spacing_atr: 0.5,
            ..Default::default()
        };

        let exit = pre_layer_adverse_exit_decision(
            &position,
            &bar(1, 100.0, 103.5, 97.0, 100.0),
            1,
            &config,
        )
        .expect("same bar should stop before layer changes average entry");

        assert_eq!(exit.1, "stop_loss");
        assert!((exit.0 - 103.4).abs() < 1e-9);
    }

    #[test]
    fn stop_loss_min_layers_defers_stop_before_l4() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 1.5,
            next_layer_index: 3,
            last_layer_price: 102.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0, 102.0],
            entry_notionals: vec![50.0, 50.0, 150.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            stop_loss_min_layers: 4,
            stop_loss_atr: 1.0,
            ..Default::default()
        };

        assert!(exit_decision(&position, &bar(1, 100.0, 103.0, 99.0, 101.0), 1, &config).is_none());
    }

    #[test]
    fn stop_loss_min_layers_allows_stop_after_l4() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 2.5,
            next_layer_index: 4,
            last_layer_price: 103.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0, 102.0, 103.0],
            entry_notionals: vec![50.0, 50.0, 150.0, 250.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            stop_loss_min_layers: 4,
            stop_loss_atr: 1.0,
            ..Default::default()
        };

        let exit = exit_decision(&position, &bar(1, 100.0, 103.0, 99.0, 101.0), 1, &config)
            .expect("L4 鍚庡簲鍏佽姝㈡崯");
        assert_eq!(exit.1, "stop_loss");
        assert!((exit.0 - 102.0).abs() < 1e-9);
    }

    #[test]
    fn pessimistic_path_defers_take_profit_after_same_bar_layer_fill() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.8,
            quantity: 1.0,
            next_layer_index: 2,
            last_layer_price: 101.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0],
            entry_notionals: vec![50.0, 50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            intrabar_path_mode: IntrabarPathMode::Pessimistic,
            take_profit_atr: 1.0,
            stop_loss_atr: 3.0,
            ..Default::default()
        };
        let candle = bar(1, 100.0, 101.2, 98.7, 100.0);

        assert!(
            exit_decision_after_intrabar_updates(&position, &candle, 1, &config, true).is_none()
        );
        assert_eq!(
            exit_decision_after_intrabar_updates(&position, &candle, 1, &config, false)
                .expect("take profit should be allowed without same-bar layer fill")
                .1,
            "take_profit"
        );
    }

    #[test]
    fn pessimistic_path_allows_stop_after_same_bar_l4_fill() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 2.5,
            next_layer_index: 4,
            last_layer_price: 103.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0, 101.0, 102.0, 103.0],
            entry_notionals: vec![50.0, 50.0, 150.0, 250.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            intrabar_path_mode: IntrabarPathMode::Pessimistic,
            stop_loss_min_layers: 4,
            stop_loss_atr: 1.0,
            ..Default::default()
        };

        let exit = exit_decision_after_intrabar_updates(
            &position,
            &bar(1, 100.0, 103.0, 99.0, 101.0),
            1,
            &config,
            true,
        )
        .expect("same-bar L4 fill should still allow adverse stop");

        assert_eq!(exit.1, "stop_loss");
        assert!((exit.0 - 102.0).abs() < 1e-9);
    }

    #[test]
    fn relaxed_entry_can_disable_lower_close_and_prior_high_sweep() {
        let candles = vec![
            bar(0, 100.0, 101.0, 99.0, 100.0),
            bar(1, 100.0, 101.0, 99.0, 100.0),
            bar(2, 100.0, 100.8, 99.5, 100.4),
        ];
        let rsi_values = vec![None, Some(49.0), Some(52.0)];
        let atr_values = vec![None, Some(1.0), Some(1.0)];
        let config = ShortLadderConfig {
            entry_rsi_min: 52.0,
            entry_requires_lower_close: false,
            entry_requires_prior_high_sweep: false,
            ..Default::default()
        };

        assert!(short_entry_trigger(
            2,
            &candles,
            &rsi_values,
            &atr_values,
            &config
        ));
    }

    #[test]
    fn resistance_sweep_rejection_entry_requires_sweep_and_close_back_below_resistance() {
        let candles = vec![
            bar(0, 100.0, 101.0, 99.0, 100.0),
            bar(1, 100.0, 102.0, 99.0, 100.5),
            bar(2, 100.5, 101.5, 99.5, 100.7),
            bar(3, 100.7, 103.2, 99.8, 101.7),
        ];
        let rsi_values = vec![None, Some(55.0), Some(58.0), Some(61.0)];
        let atr_values = vec![None, Some(1.0), Some(1.0), Some(1.0)];
        let config = ShortLadderConfig {
            entry_signal_mode: EntrySignalMode::ResistanceSweepRejection,
            entry_resistance_lookback_bars: 3,
            entry_upper_wick_min_ratio: 0.35,
            entry_rsi_min: 60.0,
            ..Default::default()
        };

        assert!(short_entry_trigger(
            3,
            &candles,
            &rsi_values,
            &atr_values,
            &config
        ));
    }

    #[test]
    fn breakdown_pullback_entry_requires_recent_breakdown_and_failed_retest() {
        let candles = vec![
            bar(0, 102.0, 103.0, 100.0, 102.0),
            bar(1, 102.0, 102.5, 100.5, 101.0),
            bar(2, 101.0, 101.4, 99.8, 100.2),
            bar(3, 100.2, 100.8, 98.9, 99.2),
            bar(4, 100.1, 100.4, 98.8, 99.5),
        ];
        let rsi_values = vec![None, Some(55.0), Some(53.0), Some(49.0), Some(48.0)];
        let atr_values = vec![None, Some(1.0), Some(1.0), Some(1.0), Some(1.0)];
        let config = ShortLadderConfig {
            entry_signal_mode: EntrySignalMode::BreakdownPullback,
            entry_breakdown_lookback_bars: 3,
            entry_pullback_max_bars: 2,
            entry_pullback_tolerance_atr: 0.3,
            ..Default::default()
        };

        assert!(short_entry_trigger(
            4,
            &candles,
            &rsi_values,
            &atr_values,
            &config
        ));
    }

    #[test]
    fn bearish_volatility_expansion_entry_requires_large_bearish_close_and_volume() {
        let candles = vec![
            bar(0, 100.0, 100.5, 99.5, 100.0),
            bar(1, 100.0, 100.4, 99.6, 100.1),
            bar(2, 100.1, 100.6, 99.7, 100.2),
            bar(3, 100.2, 100.5, 99.8, 100.0),
            bar(4, 100.0, 100.2, 98.6, 98.75),
        ];
        let rsi_values = vec![None, Some(50.0), Some(50.0), Some(50.0), Some(45.0)];
        let atr_values = vec![None, Some(1.0), Some(1.0), Some(1.0), Some(1.0)];
        let config = ShortLadderConfig {
            entry_signal_mode: EntrySignalMode::BearishVolatilityExpansion,
            entry_body_atr_min: 1.0,
            entry_range_atr_min: 1.4,
            entry_close_near_low_ratio: 0.25,
            entry_volume_sma_bars: 3,
            entry_volume_multiplier: 1.2,
            ..Default::default()
        };
        let mut candles = candles;
        candles[4].volume = 2_000.0;

        assert!(short_entry_trigger(
            4,
            &candles,
            &rsi_values,
            &atr_values,
            &config
        ));
    }

    #[test]
    fn atr_trailing_take_profit_exits_on_rebound_after_activation() {
        let mut position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 1.0,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![100.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            take_profit_mode: TakeProfitMode::AtrTrailing,
            trailing_take_profit_activation_atr: 1.0,
            trailing_take_profit_distance_atr: 0.5,
            ..Default::default()
        };
        let candle = bar(1, 100.0, 100.0, 97.8, 99.0);

        update_take_profit_state(&mut position, &candle, &config);
        let exit = exit_decision(&position, &candle, 1, &config)
            .expect("trailing take profit should exit after rebound");

        assert_eq!(exit.1, "trailing_take_profit");
        assert!((exit.0 - 98.8).abs() < 1e-9);
    }
    #[test]
    fn breakeven_stop_exits_after_profit_trigger_is_armed() {
        let position = OpenShortPosition {
            mode: ShortLadderMode::AdverseAveraging,
            entry_time: Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap(),
            average_entry_price: 100.0,
            quantity: 0.5,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            opened_bar_index: 0,
            entry_prices: vec![100.0],
            entry_notionals: vec![50.0],
            entry_times: Vec::new(),
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: true,
            best_favorable_price: 100.0,
            trailing_take_profit_armed: false,
        };
        let config = ShortLadderConfig {
            breakeven_stop: true,
            breakeven_buffer_bps: 2.0,
            ..Default::default()
        };

        let exit = exit_decision(&position, &bar(2, 99.0, 99.99, 98.0, 99.8), 2, &config)
            .expect("breakeven stop should trigger");

        assert_eq!(exit.1, "breakeven_stop");
        assert!((exit.0 - 99.98).abs() < 1e-9);
    }
}
