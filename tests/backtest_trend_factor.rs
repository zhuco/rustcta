use chrono::{Duration, TimeZone, Utc};
use rustcta::backtest::factors::{
    expand_mtf_trend_factor_runs, expand_trend_factor_runs, MtfTrendFactorScanSpec,
    TrendFactorScanSpec,
};
use rustcta::backtest::indicators::{
    adx, atr, atr_percentile, bollinger_width, choppiness_index, donchian_high, donchian_low, ema,
    ema_slope, keltner_channel, macd_histogram, mfi, roc, rsi, stoch_rsi, supertrend_direction,
    volume_ratio, volume_zscore, vwap_deviation,
};
use rustcta::backtest::scoring::{score_trend_run, TrendScoreInput};
use rustcta::core::types::Kline;

fn kline(index: usize, close: f64, volume: f64) -> Kline {
    let open_time =
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::minutes(index as i64);
    Kline {
        symbol: "BTC/USDC".to_string(),
        interval: "1m".to_string(),
        open_time,
        close_time: open_time + Duration::seconds(59),
        open: close - 0.5,
        high: close + 1.0,
        low: close - 1.0,
        close,
        volume,
        quote_volume: volume * close,
        trade_count: 10,
    }
}

fn sample_klines() -> Vec<Kline> {
    (0..80)
        .map(|index| {
            kline(
                index,
                100.0 + index as f64 * 0.5,
                1_000.0 + index as f64 * 10.0,
            )
        })
        .collect()
}

#[test]
fn indicator_series_calculate_expected_values() {
    let candles = sample_klines();
    let closes = candles.iter().map(|item| item.close).collect::<Vec<_>>();

    let ema_values = ema(&closes, 3);
    assert_eq!(ema_values[0], None);
    assert_eq!(ema_values[2], Some(100.5));

    let donchian_highs = donchian_high(&candles, 5);
    let donchian_lows = donchian_low(&candles, 5);
    assert_eq!(donchian_highs[4], Some(103.0));
    assert_eq!(donchian_lows[4], Some(99.0));

    let width = bollinger_width(&closes, 20, 2.0);
    assert!(width[19].is_some_and(|value| value > 0.0));

    let volume = volume_ratio(&candles, 10);
    assert!(volume[10].is_some_and(|value| value > 1.0));

    let roc_values = roc(&closes, 10);
    assert!(roc_values[10].is_some_and(|value| value > 0.0));

    let atr_values = atr(&candles, 14);
    let adx_values = adx(&candles, 14);
    let rsi_values = rsi(&closes, 14);
    let macd_values = macd_histogram(&closes, 12, 26, 9);
    let supertrend = supertrend_direction(&candles, 10, 3.0);
    assert!(atr_values.iter().flatten().all(|value| *value > 0.0));
    assert!(adx_values.iter().flatten().all(|value| *value >= 0.0));
    assert!(rsi_values
        .iter()
        .flatten()
        .all(|value| *value >= 0.0 && *value <= 100.0));
    assert!(macd_values.iter().flatten().any(|value| *value != 0.0));
    assert!(supertrend
        .iter()
        .flatten()
        .any(|value| *value == 1 || *value == -1));
}

#[test]
fn trend_factor_grid_expands_parameter_sets() {
    let yaml = r#"
initial_equity: 10000.0
trade_notional: 1000.0
strategies:
  - name: donchian_adx_volume
    entry:
      donchian_breakout:
        lookback: [20, 30]
    filters:
      adx_min: [20, 25]
      volume_ratio_min:
        period: [20]
        threshold: [1.0, 1.2]
    exit:
      atr_stop: [2.0]
      reward_r: [2.5]
      time_stop_bars: [288]
"#;
    let spec: TrendFactorScanSpec = serde_yaml::from_str(yaml).expect("scan spec should parse");
    let runs = expand_trend_factor_runs(&spec).expect("runs should expand");
    assert_eq!(runs.len(), 8);
    assert_eq!(runs[0].strategy_name, "donchian_adx_volume");
    assert!(runs[0]
        .parameters
        .contains_key("entry.donchian_breakout.lookback"));
}

#[test]
fn extended_indicator_series_are_available_for_short_cycle_scans() {
    let candles = sample_klines();
    let closes = candles.iter().map(|item| item.close).collect::<Vec<_>>();

    let slope = ema_slope(&closes, 10, 5);
    assert!(slope.iter().flatten().any(|value| *value > 0.0));

    let keltner = keltner_channel(&candles, 20, 10, 1.5);
    assert!(keltner.upper.iter().flatten().any(|value| *value > 0.0));
    assert!(keltner.lower.iter().flatten().any(|value| *value > 0.0));

    let vwap = vwap_deviation(&candles, 20);
    assert!(vwap.iter().flatten().any(|value| value.is_finite()));

    let mfi_values = mfi(&candles, 14);
    assert!(mfi_values
        .iter()
        .flatten()
        .all(|value| *value >= 0.0 && *value <= 100.0));

    let stoch_values = stoch_rsi(&closes, 14, 14);
    assert!(stoch_values
        .iter()
        .flatten()
        .all(|value| *value >= 0.0 && *value <= 100.0));

    let chop = choppiness_index(&candles, 14);
    assert!(chop
        .iter()
        .flatten()
        .all(|value| *value >= 0.0 && *value <= 100.0));

    let atr_pct = atr_percentile(&candles, 14, 30);
    assert!(atr_pct
        .iter()
        .flatten()
        .all(|value| *value >= 0.0 && *value <= 100.0));

    let volume_z = volume_zscore(&candles, 20);
    assert!(volume_z.iter().flatten().any(|value| value.is_finite()));
}

#[test]
fn trend_factor_spec_accepts_cost_scenarios_and_candidate_filter() {
    let yaml = r#"
initial_equity: 10000.0
trade_notional: 1000.0
cost_scenarios:
  - name: normal
    fee_rate: 0.001
    slippage_rate: 0.0002
  - name: stressed
    fee_rate: 0.002
    slippage_rate: 0.001
candidate_filter:
  min_trades: 30
  min_profit_factor: 1.2
  min_roi_pct: 0.2
  max_drawdown_pct: -5.0
  require_profitable_cost_scenarios: 2
strategies:
  - name: ema_slope_keltner_volume
    entry:
      ema_slope:
        period: [20]
        lookback: [5]
        threshold: [0.01]
      keltner_breakout:
        ema_period: [20]
        atr_period: [10]
        multiplier: [1.5]
    filters:
      volume_zscore_min:
        period: [20]
        threshold: [0.5]
      choppiness_max:
        period: [14]
        threshold: [55]
    exit:
      atr_stop: [2.0]
      reward_r: [2.5]
      time_stop_bars: [288]
"#;
    let spec: TrendFactorScanSpec =
        serde_yaml::from_str(yaml).expect("extended scan spec should parse");
    assert_eq!(spec.cost_scenarios.len(), 2);
    assert_eq!(spec.candidate_filter.min_trades, 30);
    let runs = expand_trend_factor_runs(&spec).expect("runs should expand");
    assert_eq!(runs.len(), 1);
    assert!(runs[0].parameters.contains_key("entry.ema_slope.period"));
    assert!(runs[0]
        .parameters
        .contains_key("filter.volume_zscore.threshold"));
}

#[test]
fn mtf_trend_factor_spec_expands_timeframe_roles() {
    let yaml = r#"
initial_equity: 10000.0
trade_notional: 1000.0
timeframes:
  trend: "15m"
  structure: "5m"
  trigger: "3m"
cost_scenarios:
  - name: normal
    fee_rate: 0.001
    slippage_rate: 0.0002
candidate_filter:
  min_trades: 20
  min_profit_factor: 1.1
  min_roi_pct: 0.1
  max_drawdown_pct: -5.0
  require_profitable_cost_scenarios: 1
strategies:
  - name: mtf_donchian_trigger
    trend:
      ema_trend:
        fast: [20]
        slow: [50]
      adx_min: [20]
      choppiness_max:
        period: [14]
        threshold: [55]
    structure:
      donchian_breakout:
        lookback: [20]
      bb_width_min:
        period: [20]
        threshold: [0.01]
    trigger:
      roc_direction:
        period: [12]
        threshold: [0.1]
      volume_zscore_min:
        period: [20]
        threshold: [0.3]
    exit:
      atr_stop: [2.0]
      reward_r: [2.5]
      time_stop_bars: [288]
      reverse_signal_exit: true
"#;
    let spec: MtfTrendFactorScanSpec = serde_yaml::from_str(yaml).expect("mtf spec should parse");
    assert_eq!(spec.timeframes.trend, "15m");
    let runs = expand_mtf_trend_factor_runs(&spec).expect("mtf runs should expand");
    assert_eq!(runs.len(), 1);
    assert!(runs[0].parameters.contains_key("trend.ema.fast"));
    assert!(runs[0]
        .parameters
        .contains_key("structure.donchian.lookback"));
    assert!(runs[0].parameters.contains_key("trigger.roc.period"));
}

#[test]
fn mtf_trend_factor_spec_expands_event_pullback_and_trailing_exit() {
    let yaml = r#"
initial_equity: 10000.0
trade_notional: 1000.0
timeframes:
  trend: "15m"
  structure: "5m"
  trigger: "1m"
strategies:
  - name: mtf_event_pullback
    trend:
      ema_trend:
        fast: [20]
        slow: [50]
      adx_min: [20]
    structure:
      donchian_breakout:
        lookback: [20]
      breakout_valid_bars: [8]
      pullback_entry:
        max_pullback_pct: [0.003]
        reclaim_pct: [0.0005]
    trigger:
      roc_direction:
        period: [6]
        threshold: [0.03]
    exit:
      atr_stop: [2.0]
      reward_r: [2.5]
      time_stop_bars: [240]
      reverse_signal_exit: true
      trailing_atr:
        period: [14]
        multiplier: [2.2]
        activation_r: [0.8]
"#;
    let spec: MtfTrendFactorScanSpec = serde_yaml::from_str(yaml).expect("mtf spec should parse");
    let runs = expand_mtf_trend_factor_runs(&spec).expect("mtf runs should expand");
    assert_eq!(runs.len(), 1);
    let params = &runs[0].parameters;
    assert_eq!(params.get("structure.breakout_valid_bars"), Some(&8.0));
    assert_eq!(params.get("structure.pullback.max_pct"), Some(&0.003));
    assert_eq!(params.get("structure.pullback.reclaim_pct"), Some(&0.0005));
    assert_eq!(params.get("exit.trailing_atr.period"), Some(&14.0));
    assert_eq!(params.get("exit.trailing_atr.multiplier"), Some(&2.2));
    assert_eq!(params.get("exit.trailing_atr.activation_r"), Some(&0.8));
}

#[test]
fn mtf_trend_factor_spec_expands_network_donchian_exit() {
    let yaml = r#"
initial_equity: 10000.0
trade_notional: 1000.0
timeframes:
  trend: "15m"
  structure: "5m"
  trigger: "15m"
strategies:
  - name: web_turtle_20_10
    trend:
      ema_trend:
        fast: [20]
        slow: [50]
      adx_min: [25]
      choppiness_max:
        period: [14]
        threshold: [38.2]
    structure:
      donchian_breakout:
        lookback: [20]
      breakout_valid_bars: [1]
    trigger:
      supertrend_direction:
        period: [10]
        multiplier: [3.0]
    exit:
      atr_stop: [2.0]
      reward_r: [999.0]
      time_stop_bars: [240]
      reverse_signal_exit: true
      donchian_exit:
        lookback: [10]
"#;
    let spec: MtfTrendFactorScanSpec = serde_yaml::from_str(yaml).expect("mtf spec should parse");
    let runs = expand_mtf_trend_factor_runs(&spec).expect("mtf runs should expand");
    assert_eq!(runs.len(), 1);
    let params = &runs[0].parameters;
    assert_eq!(params.get("structure.donchian.lookback"), Some(&20.0));
    assert_eq!(params.get("exit.donchian.lookback"), Some(&10.0));
}

#[test]
fn scoring_penalizes_low_trade_count() {
    let low_trade = score_trend_run(TrendScoreInput {
        roi_pct: 10.0,
        max_drawdown_pct: -2.0,
        profit_factor: 2.0,
        trades: 3,
        win_rate_pct: 60.0,
        min_trades: 20,
    });
    let enough_trades = score_trend_run(TrendScoreInput {
        trades: 30,
        ..low_trade.input
    });

    assert!(enough_trades.composite_score > low_trade.composite_score);
    assert!(low_trade
        .risk_flags
        .iter()
        .any(|flag| flag == "low_trade_count"));
}
