use std::path::PathBuf;

use chrono::{Duration, TimeZone, Timelike, Utc};
use rustcta::backtest::data::depth_dataset::DepthDatasetWriter;
use rustcta::backtest::data::kline_dataset::KlineDatasetWriter;
use rustcta::backtest::data::trade_dataset::TradeDatasetWriter;
use rustcta::backtest::runtime::BacktestRuntimeConfig;
use rustcta::backtest::schema::DepthDeltaEvent;
use rustcta::backtest::strategy::mean_reversion::MeanReversionBacktestStrategy;
use rustcta::backtest::strategy::BacktestStrategy;
use rustcta::core::types::{Interval, Kline, OrderSide, Trade};
use rustcta::strategies::mean_reversion::MeanReversionConfig;
use tempfile::tempdir;

fn sample_config() -> MeanReversionConfig {
    serde_yaml::from_str(
        r#"
strategy:
  name: "offline_mean_reversion"
  version: "1.0.0"
account:
  account_id: "paper"
  market_type: "Futures"
  allow_short: true
  dual_position_mode: false
data:
  scan_interval_secs: 60
  websocket: {}
  rest: {}
symbols:
  - symbol: "BTC/USDT"
    enabled: true
    min_quote_volume_5m: 0
    depth_multiplier: 1.0
    depth_levels: 1
    enforce_spread_rule: false
indicators:
  bollinger:
    period: 20
    std_dev: 2.0
    entry_band_pct_long: 0.25
    entry_band_pct_short: 0.75
    entry_z_long: -0.2
    entry_z_short: 0.2
  rsi:
    period: 14
    long_threshold: 60.0
    short_threshold: 40.0
  atr:
    period: 14
    initial_stop_k: 1.4
    trailing_stop_k: 1.2
  adx:
    period: 14
    threshold: 100.0
  bbw:
    lookback: 400
    percentile: 1.0
  slope:
    lookback: 5
    threshold: 100.0
  choppiness:
    enabled: false
    lookback: 14
    threshold: 0.0
  volume:
    lookback_minutes: 60
    min_quote_volume: 0
execution:
  post_only: false
  base_improve: 0.0003
  alpha_atr: 1.0
  beta_sigma: 1.0
  take_profit_atr_k: 2.0
  ttl_secs: 60
  cancel_rate_limit_per_min: 20
  relist_delay_ms: 500
  max_relist_attempts: 1
  lock_profit_pct: 0.004
  lock_buffer_pct: 0.001
  auto_resolve_constraints: false
risk:
  enable: true
  max_positions: 4
  max_positions_per_symbol: 1
  max_net_exposure: 1000.0
  per_trade_notional: 10.0
  per_trade_risk: 5.0
  time_stop_bars: 10
  daily_drawdown: 0.05
  freeze_on_gap: false
liquidity:
  min_recent_quote_volume: 0
  volume_window_minutes: 60
  depth_multiplier: 1.0
  top_levels: 1
  maker_fee_threshold: 1.0
cache:
  max_1m_bars: 4000
  max_5m_bars: 1000
  max_15m_bars: 1000
  max_1h_bars: 1000
"#,
    )
    .expect("config should deserialize")
}

fn build_one_minute_bars(symbol: &str) -> Vec<Kline> {
    let start = Utc.with_ymd_and_hms(2024, 3, 31, 0, 0, 0).unwrap();
    let mut bars = Vec::new();

    for index in 0..1800 {
        let open_time = start + Duration::minutes(index as i64);
        let close_time = open_time + Duration::seconds(59);

        let base_price = if index < 1680 {
            100.0 + (index % 8) as f64 * 0.02
        } else {
            100.0 - (index - 1680) as f64 * 0.18
        };

        let close = if index < 1680 {
            base_price + if index % 2 == 0 { 0.03 } else { -0.03 }
        } else {
            base_price - 0.35
        };

        bars.push(Kline {
            symbol: symbol.to_string(),
            interval: "1m".to_string(),
            open_time,
            close_time,
            open: base_price,
            high: base_price + 0.15,
            low: close.min(base_price) - 0.2,
            close,
            volume: 50.0,
            quote_volume: 5_000.0,
            trade_count: 100,
        });
    }

    bars
}

fn build_five_minute_bars(symbol: &str) -> Vec<Kline> {
    build_one_minute_bars(symbol)
        .chunks_exact(5)
        .map(|window| {
            let first = window.first().expect("window should have first bar");
            let last = window.last().expect("window should have last bar");
            Kline {
                symbol: symbol.to_string(),
                interval: "5m".to_string(),
                open_time: first.open_time,
                close_time: last.close_time,
                open: first.open,
                high: window.iter().map(|bar| bar.high).fold(f64::MIN, f64::max),
                low: window.iter().map(|bar| bar.low).fold(f64::MAX, f64::min),
                close: last.close,
                volume: window.iter().map(|bar| bar.volume).sum(),
                quote_volume: window.iter().map(|bar| bar.quote_volume).sum(),
                trade_count: window.iter().map(|bar| bar.trade_count).sum(),
            }
        })
        .collect()
}

fn build_exit_cycle_bars(symbol: &str) -> Vec<Kline> {
    let start = Utc.with_ymd_and_hms(2024, 4, 1, 0, 0, 0).unwrap();
    let mut bars = Vec::new();

    for index in 0..1800 {
        let open_time = start + Duration::minutes(index as i64);
        let close_time = open_time + Duration::seconds(59);

        let close = if index < 1680 {
            100.0 + ((index % 6) as f64 - 3.0) * 0.03
        } else if index < 1710 {
            100.0 - (index - 1680) as f64 * 0.45 - 0.4
        } else if index < 1770 {
            86.0 + (index - 1710) as f64 * 0.52 + 0.3
        } else {
            117.0 + ((index % 4) as f64 - 2.0) * 0.04
        };
        let open = if index == 0 {
            close
        } else {
            bars.last().map(|bar: &Kline| bar.close).unwrap_or(close)
        };
        let high = open.max(close) + 0.2;
        let low = open.min(close) - 0.2;

        bars.push(Kline {
            symbol: symbol.to_string(),
            interval: "1m".to_string(),
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume: 60.0,
            quote_volume: 6_000.0,
            trade_count: 120,
        });
    }

    bars
}

fn build_depth_deltas_for_long_fills(
    symbol: &str,
    bars: &[Kline],
    improve: f64,
) -> Vec<DepthDeltaEvent> {
    let mut deltas = Vec::new();
    let mut update_id = 1u64;
    let mut previous_bid = None;
    let mut previous_ask = None;

    for bar in bars {
        let high_ask = bar.close * (1.0 + improve + 0.01);
        let crossing_ask = bar.close * (1.0 - improve);
        let bid = bar.close * (1.0 - improve - 0.01);

        let pre_ts = bar.close_time - Duration::milliseconds(1);
        let mut pre_bids = Vec::new();
        let mut pre_asks = Vec::new();
        if let Some(previous_bid) = previous_bid {
            pre_bids.push([previous_bid, 0.0]);
        }
        if let Some(previous_ask) = previous_ask {
            pre_asks.push([previous_ask, 0.0]);
        }
        pre_bids.push([bid, 5.0]);
        pre_asks.push([high_ask, 5.0]);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: pre_ts,
            logical_ts: pre_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: pre_bids,
            asks: pre_asks,
        });
        update_id += 1;

        let post_ts = bar.close_time + Duration::milliseconds(1);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: post_ts,
            logical_ts: post_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[high_ask, 0.0], [crossing_ask, 5.0]],
        });
        update_id += 1;

        previous_bid = Some(bid);
        previous_ask = Some(crossing_ask);
    }

    deltas
}

fn build_split_depth_deltas_for_long_fills(
    symbol: &str,
    bars: &[Kline],
    improve: f64,
    first_cross_qty: f64,
    second_cross_qty: f64,
) -> Vec<DepthDeltaEvent> {
    let mut deltas = Vec::new();
    let mut update_id = 1u64;
    let mut previous_bid = None;
    let mut previous_ask = None;

    for bar in bars {
        let high_ask = bar.close * (1.0 + improve + 0.01);
        let crossing_ask = bar.close * (1.0 - improve);
        let bid = bar.close * (1.0 - improve - 0.01);

        let pre_ts = bar.close_time - Duration::milliseconds(2);
        let mut pre_bids = Vec::new();
        let mut pre_asks = Vec::new();
        if let Some(previous_bid) = previous_bid {
            pre_bids.push([previous_bid, 0.0]);
        }
        if let Some(previous_ask) = previous_ask {
            pre_asks.push([previous_ask, 0.0]);
        }
        pre_bids.push([bid, 5.0]);
        pre_asks.push([high_ask, 5.0]);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: pre_ts,
            logical_ts: pre_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: pre_bids,
            asks: pre_asks,
        });
        update_id += 1;

        let first_fill_ts = bar.close_time + Duration::milliseconds(1);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: first_fill_ts,
            logical_ts: first_fill_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[high_ask, 0.0], [crossing_ask, first_cross_qty]],
        });
        update_id += 1;

        let second_fill_ts = bar.close_time + Duration::milliseconds(2);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: second_fill_ts,
            logical_ts: second_fill_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[crossing_ask, second_cross_qty]],
        });
        update_id += 1;

        previous_bid = Some(bid);
        previous_ask = Some(crossing_ask);
    }

    deltas
}

fn build_latency_depth_deltas_for_long_fills(
    symbol: &str,
    bars: &[Kline],
    improve: f64,
    early_cross_delay_ms: i64,
    late_cross_delay_ms: i64,
) -> Vec<DepthDeltaEvent> {
    let mut deltas = Vec::new();
    let mut update_id = 1u64;
    let mut previous_bid = None;
    let mut previous_ask = None;

    for bar in bars {
        let high_ask = bar.close * (1.0 + improve + 0.01);
        let crossing_ask = bar.close * (1.0 - improve);
        let bid = bar.close * (1.0 - improve - 0.01);

        let pre_ts = bar.close_time - Duration::milliseconds(1);
        let mut pre_bids = Vec::new();
        let mut pre_asks = Vec::new();
        if let Some(previous_bid) = previous_bid {
            pre_bids.push([previous_bid, 0.0]);
        }
        if let Some(previous_ask) = previous_ask {
            pre_asks.push([previous_ask, 0.0]);
        }
        pre_bids.push([bid, 5.0]);
        pre_asks.push([high_ask, 5.0]);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: pre_ts,
            logical_ts: pre_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: pre_bids,
            asks: pre_asks,
        });
        update_id += 1;

        let early_cross_ts = bar.close_time + Duration::milliseconds(early_cross_delay_ms);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: early_cross_ts,
            logical_ts: early_cross_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[high_ask, 0.0], [crossing_ask, 5.0]],
        });
        update_id += 1;

        let reset_ts = bar.close_time
            + Duration::milliseconds((early_cross_delay_ms + late_cross_delay_ms) / 2);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: reset_ts,
            logical_ts: reset_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[crossing_ask, 0.0], [high_ask, 5.0]],
        });
        update_id += 1;

        let late_cross_ts = bar.close_time + Duration::milliseconds(late_cross_delay_ms);
        deltas.push(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: symbol.to_string(),
            exchange_ts: late_cross_ts,
            logical_ts: late_cross_ts,
            first_update_id: update_id,
            final_update_id: update_id,
            bids: vec![],
            asks: vec![[crossing_ask, 5.0]],
        });
        update_id += 1;

        previous_bid = Some(bid);
        previous_ask = Some(crossing_ask);
    }

    deltas
}

fn build_sell_trades_for_long_fills(symbol: &str, bars: &[Kline], price_offset: f64) -> Vec<Trade> {
    bars.iter()
        .enumerate()
        .map(|(index, bar)| Trade {
            id: format!("trade-{index}"),
            symbol: symbol.to_string(),
            side: OrderSide::Sell,
            amount: 5.0,
            price: bar.close + price_offset,
            timestamp: bar.close_time + Duration::milliseconds(1),
            order_id: None,
            fee: None,
        })
        .collect()
}

#[test]
fn offline_mean_reversion_emits_long_signal_from_one_minute_replay() {
    let config = sample_config();
    let mut strategy = MeanReversionBacktestStrategy::new(config).expect("strategy should build");
    let mut emitted = Vec::new();

    for bar in build_one_minute_bars("BTC/USDT") {
        let signals = strategy.on_kline(&bar).expect("kline should apply");
        emitted.extend(signals);
    }

    assert!(!emitted.is_empty(), "expected at least one signal");
    assert!(emitted.iter().any(|signal| signal.side == OrderSide::Buy));

    let summary = strategy.summary();
    let btc = summary
        .by_symbol
        .get("BTC/USDT")
        .expect("summary should include BTC/USDT");

    assert_eq!(btc.processed_1m_bars, 1800);
    assert!(btc.derived_15m_bars >= 100);
    assert!(btc.derived_1h_bars >= 24);
    assert!(summary.emitted_signals >= emitted.len());
}

#[test]
fn offline_mean_reversion_emits_signal_from_five_minute_replay() {
    let config = sample_config();
    let mut strategy = MeanReversionBacktestStrategy::new(config).expect("strategy should build");
    let bars = build_five_minute_bars("BTC/USDT");
    let mut emitted = Vec::new();

    for bar in &bars {
        let signals = strategy.on_kline(bar).expect("kline should apply");
        emitted.extend(signals);
    }

    assert!(!emitted.is_empty(), "expected at least one 5m signal");
    assert!(emitted.iter().any(|signal| signal.side == OrderSide::Buy));

    let summary = strategy.summary();
    let btc = summary
        .by_symbol
        .get("BTC/USDT")
        .expect("summary should include BTC/USDT");

    assert_eq!(btc.processed_1m_bars, 0);
    assert_eq!(btc.derived_5m_bars, bars.len());
    assert!(btc.derived_15m_bars >= 100);
    assert!(btc.derived_1h_bars >= 24);
    assert!(summary.emitted_signals >= emitted.len());
}

#[test]
fn offline_mean_reversion_respects_symbol_trading_sessions() {
    let mut config = sample_config();
    config.symbols[0].trading_sessions = vec![serde_yaml::from_str(
        r#"
name: "US session"
start: "13:00"
end: "21:00"
timezone: "UTC"
"#,
    )
    .expect("trading session should deserialize")];
    let mut strategy = MeanReversionBacktestStrategy::new(config).expect("strategy should build");
    let mut bars = build_five_minute_bars("BTC/USDT");
    for bar in &mut bars {
        bar.open_time += Duration::hours(9);
        bar.close_time += Duration::hours(9);
    }
    let mut emitted = Vec::new();

    for bar in &bars {
        let signals = strategy.on_kline(bar).expect("kline should apply");
        emitted.extend(signals);
    }

    assert!(
        !emitted.is_empty(),
        "expected filtered session to retain some signals"
    );
    assert!(
        emitted.iter().all(|signal| {
            let hour = signal.logical_ts.hour();
            (13..21).contains(&hour)
        }),
        "all signals should be inside the configured UTC US session"
    );
}

#[test]
fn runtime_runs_mean_reversion_on_persisted_dataset() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_one_minute_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.execution.base_improve = 0.0;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute mean reversion");

    assert_eq!(report.signals.processed_events, 1800);
    assert!(report.signals.emitted_signals > 0);
    assert!(report.execution.executed_signals > 0);
    assert!(report.execution.limit_orders_placed <= report.signals.emitted_signals);
    assert!(report.execution.limit_orders_placed > 0);
    assert!(report.execution.limit_orders_filled > 0);
    assert!(!report.execution.depth_replay_enabled);
    assert_eq!(report.execution.depth_events_processed, 0);
    assert!(report.execution.total_fees_paid > 0.0);
    assert!(report.execution.final_equity.is_finite());
    assert!(report.execution.fills.iter().any(|fill| fill.is_maker));
    assert!(
        report.execution.positions.contains_key("BTC/USDT")
            || !report.execution.closed_trades.is_empty()
    );
}

#[test]
fn runtime_closes_mean_reversion_trade_and_reports_realized_pnl() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.execution.take_profit_atr_k = 0.4;
    config.indicators.atr.initial_stop_k = 0.8;
    config.risk.time_stop_bars = 6;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute mean reversion with exits");

    assert!(!report.execution.closed_trades.is_empty());
    assert!(report.execution.realized_pnl.is_finite());
    assert!(report
        .execution
        .closed_trades
        .iter()
        .any(|trade| trade.exit_reason == "take_profit"));
}

#[test]
fn runtime_uses_symbol_quote_asset_as_settlement_currency() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDC");
    writer
        .write_binance_futures_klines("BTC/USDC", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDC".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.symbols[0].symbol = "BTC/USDC".to_string();

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute USDC mean reversion");

    assert_eq!(report.execution.settlement_currency, "USDC");
}
#[test]
fn runtime_applies_configured_maker_and_taker_fee_bps() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.execution.maker_fee_bps = 0.0;
    config.execution.taker_fee_bps = 4.0;
    config.execution.take_profit_atr_k = 0.4;
    config.indicators.atr.initial_stop_k = 0.8;
    config.risk.time_stop_bars = 6;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute mean reversion with configured fees");
    let trade = report
        .execution
        .closed_trades
        .iter()
        .find(|trade| trade.entry_fill_count > 0 && trade.exit_fill_count > 0)
        .expect("expected at least one closed trade");

    assert_eq!(trade.entry_fees_paid, 0.0);
    let expected_exit_fee = trade.average_exit_price * trade.quantity * 0.0004;
    assert!((trade.exit_fees_paid - expected_exit_fee).abs() < 1e-9);
}

#[test]
fn runtime_applies_configured_market_slippage_bps_to_market_exits() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut base_config = sample_config();
    base_config.account.allow_short = false;
    base_config.execution.maker_fee_bps = 0.0;
    base_config.execution.taker_fee_bps = 4.0;
    base_config.execution.market_slippage_bps = 0.0;
    base_config.execution.take_profit_atr_k = 0.4;
    base_config.indicators.atr.initial_stop_k = 0.8;
    base_config.risk.time_stop_bars = 6;

    let baseline_report = runtime
        .run_mean_reversion_backtest(base_config.clone())
        .expect("runtime should execute baseline mean reversion");

    let mut slipped_config = base_config;
    slipped_config.execution.market_slippage_bps = 10.0;
    let slipped_report = runtime
        .run_mean_reversion_backtest(slipped_config)
        .expect("runtime should execute slipped mean reversion");

    let baseline_trade = baseline_report
        .execution
        .closed_trades
        .iter()
        .find(|trade| trade.side == OrderSide::Buy && trade.exit_fill_count > 0)
        .expect("expected at least one long closed trade");
    let slipped_trade = slipped_report
        .execution
        .closed_trades
        .iter()
        .find(|trade| trade.side == OrderSide::Buy && trade.exit_fill_count > 0)
        .expect("expected at least one slipped long closed trade");

    let expected_slipped_exit = baseline_trade.average_exit_price * (1.0 - 10.0 / 10_000.0);
    assert!((slipped_trade.average_exit_price - expected_slipped_exit).abs() < 1e-9);
    assert!(slipped_trade.net_realized_pnl < baseline_trade.net_realized_pnl);
}

#[test]
fn runtime_uses_depth_events_to_fill_posted_limit_orders() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let depth_writer = DepthDatasetWriter::new(temp_dir.path());
    let mut bars = build_one_minute_bars("BTC/USDT");
    for bar in &mut bars {
        bar.high = bar.close + 0.05;
        bar.low = bar.close - 0.05;
    }
    let improve = 0.01;
    let depth_deltas = build_depth_deltas_for_long_fills("BTC/USDT", &bars, improve);

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("kline dataset write should succeed");
    depth_writer
        .write_binance_futures_depth_deltas("BTC/USDT", &depth_deltas)
        .expect("depth dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time - Duration::milliseconds(1),
        end: bars.last().expect("bars").close_time + Duration::milliseconds(1),
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.account.allow_short = false;
    config.execution.base_improve = improve;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute depth-assisted mean reversion");

    assert!(report.signals.emitted_signals > 0);
    assert!(report.execution.limit_orders_placed > 0);
    assert!(report.execution.limit_orders_filled > 0);
    assert!(report.execution.depth_replay_enabled);
    assert!(report.execution.depth_events_processed > 0);
    assert!(report.execution.fills.iter().any(|fill| fill.is_maker));
    assert!(
        report.execution.fills.iter().any(|fill| fill.price < 99.0),
        "depth-driven maker fills should execute below the un-improved bar close range"
    );
}

#[test]
fn runtime_reports_partial_entry_legs_for_depth_driven_trades() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let depth_writer = DepthDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDT");
    let improve = 0.01;
    let depth_deltas =
        build_split_depth_deltas_for_long_fills("BTC/USDT", &bars, improve, 0.04, 0.08);

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("kline dataset write should succeed");
    depth_writer
        .write_binance_futures_depth_deltas("BTC/USDT", &depth_deltas)
        .expect("depth dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time - Duration::milliseconds(2),
        end: bars.last().expect("bars").close_time + Duration::milliseconds(2),
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.account.allow_short = false;
    config.execution.base_improve = improve;
    config.execution.take_profit_atr_k = 0.4;
    config.indicators.atr.initial_stop_k = 0.8;
    config.risk.time_stop_bars = 6;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute partial-fill depth-assisted mean reversion");

    let trade = report
        .execution
        .closed_trades
        .iter()
        .find(|trade| trade.entry_fill_count > 1)
        .expect("expected at least one partially built trade");

    assert!(trade.entry_fill_count >= 2);
    assert_eq!(trade.entry_legs.len(), trade.entry_fill_count);
    assert_eq!(trade.exit_legs.len(), trade.exit_fill_count);
    assert!(trade.average_entry_price > 0.0);
    assert!(trade.average_exit_price > 0.0);
    assert!(trade.entry_fees_paid > 0.0);
    assert!(trade.exit_fees_paid > 0.0);
    assert!(trade.net_realized_pnl.is_finite());
}

#[test]
fn runtime_delays_depth_entry_activation_until_submit_latency_expires() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let depth_writer = DepthDatasetWriter::new(temp_dir.path());
    let mut bars = build_one_minute_bars("BTC/USDT");
    for bar in &mut bars {
        bar.high = bar.close + 0.05;
        bar.low = bar.close - 0.05;
    }
    let improve = 0.01;
    let depth_deltas = build_latency_depth_deltas_for_long_fills("BTC/USDT", &bars, improve, 1, 20);

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("kline dataset write should succeed");
    depth_writer
        .write_binance_futures_depth_deltas("BTC/USDT", &depth_deltas)
        .expect("depth dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time - Duration::milliseconds(1),
        end: bars.last().expect("bars").close_time + Duration::milliseconds(20),
        output: PathBuf::from(temp_dir.path()),
    });

    let mut immediate_config = sample_config();
    immediate_config.account.allow_short = false;
    immediate_config.execution.base_improve = improve;

    let immediate_report = runtime
        .run_mean_reversion_backtest(immediate_config)
        .expect("runtime should execute immediate depth-assisted entries");

    let immediate_fill_ts = immediate_report
        .execution
        .fills
        .iter()
        .filter(|fill| fill.is_maker)
        .map(|fill| fill.timestamp)
        .min()
        .expect("expected maker fills without submit latency");

    let mut delayed_config = sample_config();
    delayed_config.account.allow_short = false;
    delayed_config.execution.base_improve = improve;
    delayed_config.execution.submit_latency_ms = 10;

    let delayed_report = runtime
        .run_mean_reversion_backtest(delayed_config)
        .expect("runtime should execute delayed depth-assisted entries");

    let delayed_fill_ts = delayed_report
        .execution
        .fills
        .iter()
        .filter(|fill| fill.is_maker)
        .map(|fill| fill.timestamp)
        .min()
        .expect("expected maker fills with submit latency");

    assert!(delayed_fill_ts > immediate_fill_ts);
    assert!(delayed_fill_ts >= immediate_fill_ts + Duration::milliseconds(10));
}

#[test]
fn runtime_rejects_entries_that_violate_venue_min_notional_constraints() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_one_minute_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.execution.base_improve = 0.0;
    config.execution.venue.tick_size = Some(0.5);
    config.execution.venue.step_size = Some(0.01);
    config.execution.venue.min_notional = Some(25.0);
    config.risk.per_trade_notional = 10.0;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should handle venue constraint rejection");

    assert!(report.signals.emitted_signals > 0);
    assert_eq!(report.execution.limit_orders_placed, 0);
    assert_eq!(report.execution.limit_orders_filled, 0);
    assert_eq!(report.execution.executed_signals, 0);
    assert!(report.execution.rejected_orders > 0);
    assert_eq!(
        report
            .execution
            .rejection_reasons
            .get("below_min_notional")
            .copied()
            .unwrap_or(0),
        report.execution.rejected_orders
    );
}

#[test]
fn runtime_delays_market_exit_until_market_latency_expires() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());
    let bars = build_exit_cycle_bars("BTC/USDT");
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time,
        output: PathBuf::from(temp_dir.path()),
    });

    let mut immediate_config = sample_config();
    immediate_config.execution.base_improve = 0.0;
    immediate_config.execution.take_profit_atr_k = 0.4;
    immediate_config.indicators.atr.initial_stop_k = 0.8;
    immediate_config.risk.time_stop_bars = 6;

    let immediate_report = runtime
        .run_mean_reversion_backtest(immediate_config)
        .expect("runtime should execute immediate market exits");

    let immediate_exit_ts = immediate_report
        .execution
        .closed_trades
        .iter()
        .map(|trade| trade.exit_timestamp)
        .min()
        .expect("expected at least one immediate closed trade");

    let mut delayed_config = sample_config();
    delayed_config.execution.base_improve = 0.0;
    delayed_config.execution.take_profit_atr_k = 0.4;
    delayed_config.indicators.atr.initial_stop_k = 0.8;
    delayed_config.risk.time_stop_bars = 6;
    delayed_config.execution.market_latency_ms = 1_000;

    let delayed_report = runtime
        .run_mean_reversion_backtest(delayed_config)
        .expect("runtime should execute delayed market exits");

    let delayed_exit_ts = delayed_report
        .execution
        .closed_trades
        .iter()
        .map(|trade| trade.exit_timestamp)
        .min()
        .expect("expected at least one delayed closed trade");

    assert!(delayed_exit_ts > immediate_exit_ts);
}

#[test]
fn runtime_uses_trade_events_to_fill_posted_limit_orders_without_depth() {
    let temp_dir = tempdir().expect("temp dir");
    let kline_writer = KlineDatasetWriter::new(temp_dir.path());
    let trade_writer = TradeDatasetWriter::new(temp_dir.path());
    let mut bars = build_one_minute_bars("BTC/USDT");
    for bar in &mut bars {
        bar.high = bar.close + 0.01;
        bar.low = bar.close - 0.01;
    }
    let trades = build_sell_trades_for_long_fills("BTC/USDT", &bars, 0.0);

    kline_writer
        .write_binance_futures_klines("BTC/USDT", "1m", &bars)
        .expect("kline dataset write should succeed");
    trade_writer
        .write_binance_futures_trades("BTC/USDT", &trades)
        .expect("trade dataset write should succeed");

    let runtime = rustcta::backtest::runtime::BacktestRuntime::new(BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: bars.first().expect("bars").close_time,
        end: bars.last().expect("bars").close_time + Duration::milliseconds(1),
        output: PathBuf::from(temp_dir.path()),
    });

    let mut config = sample_config();
    config.account.allow_short = false;
    config.execution.base_improve = 0.0;

    let report = runtime
        .run_mean_reversion_backtest(config)
        .expect("runtime should execute trade-assisted mean reversion");

    assert!(report.signals.emitted_signals > 0);
    assert!(report.execution.trade_replay_enabled);
    assert!(report.execution.trade_events_processed > 0);
    assert!(report.execution.limit_orders_placed > 0);
    assert!(report.execution.limit_orders_filled > 0);
    assert!(report.execution.fills.iter().any(|fill| fill.is_maker));
}
