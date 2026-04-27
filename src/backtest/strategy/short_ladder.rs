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
    #[serde(default = "default_true")]
    pub entry_requires_lower_close: bool,
    #[serde(default = "default_true")]
    pub entry_requires_prior_high_sweep: bool,
    #[serde(default = "default_layer_spacing_atr")]
    pub layer_spacing_atr: f64,
    #[serde(default = "default_take_profit_atr")]
    pub take_profit_atr: f64,
    #[serde(default = "default_stop_loss_atr")]
    pub stop_loss_atr: f64,
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
            intrabar_path_mode: IntrabarPathMode::Current,
            breakeven_stop: false,
            breakeven_trigger_atr: default_breakeven_trigger_atr(),
            breakeven_buffer_bps: default_breakeven_buffer_bps(),
            atr_period: default_atr_period(),
            rsi_period: default_rsi_period(),
            entry_rsi_min: default_entry_rsi(),
            entry_requires_lower_close: default_true(),
            entry_requires_prior_high_sweep: default_true(),
            layer_spacing_atr: default_layer_spacing_atr(),
            take_profit_atr: default_take_profit_atr(),
            stop_loss_atr: default_stop_loss_atr(),
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
    fees_paid: f64,
    max_unrealized_loss: f64,
    breakeven_armed: bool,
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
            filled_layer_this_bar =
                fill_next_layer_if_triggered(symbol, open, bar, mode, &config, allow_final_layer);
        }

        if let Some(open) = position.as_ref() {
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
            && maker_fill_allowed(symbol, bar.close_time.timestamp_millis(), 0, &config)
        {
            if let Some(atr_now) = atr_5m[index] {
                position = Some(open_initial_short(mode, bar, index, atr_now, &config));
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

fn open_initial_short(
    mode: ShortLadderMode,
    bar: &Kline,
    index: usize,
    atr_now: f64,
    config: &ShortLadderConfig,
) -> OpenShortPosition {
    let notional = config.initial_notional * config.layer_weights.first().copied().unwrap_or(1.0);
    let quantity = notional / bar.close;
    let fee = notional * config.maker_fee_bps / 10_000.0;
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
        fees_paid: fee,
        max_unrealized_loss: 0.0,
        breakeven_armed: false,
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
    let mut filled_any = false;
    while position.next_layer_index < config.layer_weights.len() {
        let is_final_layer = position.next_layer_index + 1 == config.layer_weights.len();
        if is_final_layer && config.final_layer_requires_strong_1h && !allow_final_layer {
            break;
        }

        let spacing = config.layer_spacing_atr * position.atr_at_entry;
        let trigger_price = match mode {
            ShortLadderMode::AdverseAveraging => position.last_layer_price + spacing,
            ShortLadderMode::FavorablePyramiding => position.last_layer_price - spacing,
        };
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
        position.fees_paid += notional * config.maker_fee_bps / 10_000.0;
        filled_any = true;
    }
    filled_any
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

fn exit_decision(
    position: &OpenShortPosition,
    bar: &Kline,
    index: usize,
    config: &ShortLadderConfig,
) -> Option<(f64, String)> {
    let stop_price = position.average_entry_price + config.stop_loss_atr * position.atr_at_entry;
    let take_profit_price =
        position.average_entry_price - config.take_profit_atr * position.atr_at_entry;
    let breakeven_price =
        position.average_entry_price * (1.0 - config.breakeven_buffer_bps / 10_000.0);

    if bar.high >= stop_price {
        return Some((stop_price, "stop_loss".to_string()));
    }
    if position.breakeven_armed && bar.high >= breakeven_price {
        return Some((breakeven_price, "breakeven_stop".to_string()));
    }
    if bar.low <= take_profit_price {
        return Some((take_profit_price, "take_profit".to_string()));
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

    if bar.high >= stop_price {
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

fn exit_decision_after_intrabar_updates(
    position: &OpenShortPosition,
    bar: &Kline,
    index: usize,
    config: &ShortLadderConfig,
    filled_layer_this_bar: bool,
) -> Option<(f64, String)> {
    if matches!(config.intrabar_path_mode, IntrabarPathMode::Pessimistic) {
        if filled_layer_this_bar {
            return None;
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
    if config.entry_requires_prior_high_sweep && current.high < previous.high {
        return false;
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
        roi_pct: net_pnl / config.initial_equity * 100.0,
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

fn default_true() -> bool {
    true
}

fn default_layer_spacing_atr() -> f64 {
    0.55
}

fn default_take_profit_atr() -> f64 {
    1.2
}

fn default_stop_loss_atr() -> f64 {
    1.7
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: false,
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
            fees_paid: 0.0,
            max_unrealized_loss: 0.0,
            breakeven_armed: true,
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
