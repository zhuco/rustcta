use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use clap::Parser;
use rustcta::backtest::data::kline_dataset::KlineDatasetWriter;
use rustcta::backtest::runtime::{
    execute_cli, BacktestCli, BacktestCommandOutput, BacktestRuntime, BacktestRuntimeConfig,
    MeanReversionScanReport, MeanReversionScanRunSummary, MeanReversionScanSortBy,
    MeanReversionWalkForwardParameterSummary, MeanReversionWalkForwardReport,
    MeanReversionWalkForwardWindowReport,
};
use rustcta::core::types::{Interval, Kline};
use std::fs;
use tempfile::tempdir;

fn sample_kline(close_time_ms: i64, close: f64) -> Kline {
    let open_time = Utc.timestamp_millis_opt(close_time_ms - 60_000).unwrap();
    let close_time = Utc.timestamp_millis_opt(close_time_ms).unwrap();

    Kline {
        symbol: "BTC/USDT".to_string(),
        interval: "1m".to_string(),
        open_time,
        close_time,
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: 10.0,
        quote_volume: 1000.0,
        trade_count: 42,
    }
}

#[test]
fn runtime_loads_replay_and_plans_parallel_partitions() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let klines = vec![
        sample_kline(1_711_929_660_000, 99.0),
        sample_kline(1_711_929_720_000, 100.0),
        sample_kline(1_711_929_780_000, 101.0),
        sample_kline(1_711_929_840_000, 102.0),
    ];
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let config = BacktestRuntimeConfig {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        interval: Interval::OneMinute,
        start: Utc.timestamp_millis_opt(1_711_929_600_000).unwrap(),
        end: Utc.timestamp_millis_opt(1_711_929_900_000).unwrap(),
        output: PathBuf::from(temp_dir.path()),
    };

    let runtime = BacktestRuntime::new(config);
    let replay = runtime.load_kline_replay().expect("replay should load");
    let partitions = runtime
        .plan_kline_partitions(2)
        .expect("partition planning should succeed");

    assert_eq!(replay.collect_events().len(), 4);
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].start_index, 0);
    assert_eq!(partitions[1].end_index, 4);
}

#[tokio::test]
async fn scan_mean_reversion_writes_ranked_report() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let mut klines = Vec::new();
    for index in 0..1800 {
        klines.push(sample_kline(
            1_711_929_660_000 + index as i64 * 60_000,
            100.0 + index as f64 * 0.01,
        ));
    }
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let config_path = temp_dir.path().join("mean_reversion.yml");
    fs::write(
        &config_path,
        r#"
strategy:
  name: "offline_mean_reversion"
  version: "1.0.0"
account:
  account_id: "paper"
  market_type: "Futures"
  allow_short: false
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
  base_improve: 0.0
  alpha_atr: 1.0
  beta_sigma: 1.0
  take_profit_atr_k: 1.0
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
    .expect("config should write");

    let scan_config_path = temp_dir.path().join("scan.yml");
    fs::write(
        &scan_config_path,
        r#"
sort_by: final_equity
parameters:
  - path: execution.base_improve
    values: [0.0, 0.01]
  - path: risk.per_trade_notional
    values: [10.0, 20.0]
"#,
    )
    .expect("scan config should write");

    let report_path = temp_dir.path().join("scan-report.json");
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "scan-mean-reversion",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--start",
        "2024-04-01T00:00:00Z",
        "--end",
        "2024-04-02T12:00:00Z",
        "--dataset",
        temp_dir.path().to_str().expect("dataset path"),
        "--config",
        config_path.to_str().expect("config path"),
        "--scan-config",
        scan_config_path.to_str().expect("scan config path"),
        "--report-output",
        report_path.to_str().expect("report path"),
        "--max-workers",
        "2",
    ])
    .expect("scan cli should parse");

    let output = execute_cli(cli).await.expect("scan command should execute");
    let BacktestCommandOutput::ScanMeanReversion(manifest) = output else {
        panic!("expected scan manifest");
    };

    assert_eq!(manifest.total_runs, 4);
    assert!(manifest.report_path.exists());
    assert!(manifest.best_run.is_some());

    let report_json = fs::read_to_string(&report_path).expect("report should exist");
    assert!(report_json.contains("\"total_runs\": 4"));
    assert!(report_json.contains("execution.base_improve"));
    assert!(report_json.contains("risk.per_trade_notional"));
}

#[tokio::test]
async fn walk_forward_mean_reversion_writes_out_of_sample_report() {
    let temp_dir = tempdir().expect("temp dir");
    let writer = KlineDatasetWriter::new(temp_dir.path());

    let mut klines = Vec::new();
    for index in 0..3600 {
        klines.push(sample_kline(
            1_711_929_660_000 + index as i64 * 60_000,
            100.0 + (index % 200) as f64 * 0.03 - (index / 200) as f64 * 0.2,
        ));
    }
    writer
        .write_binance_futures_klines("BTC/USDT", "1m", &klines)
        .expect("dataset write should succeed");

    let config_path = temp_dir.path().join("mean_reversion.yml");
    fs::write(
        &config_path,
        r#"
strategy:
  name: "offline_mean_reversion"
  version: "1.0.0"
account:
  account_id: "paper"
  market_type: "Futures"
  allow_short: false
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
  base_improve: 0.0
  alpha_atr: 1.0
  beta_sigma: 1.0
  take_profit_atr_k: 1.0
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
    .expect("config should write");

    let walk_config_path = temp_dir.path().join("walk.yml");
    fs::write(
        &walk_config_path,
        r#"
sort_by: final_equity
train_bars: 1200
validation_bars: 600
step_bars: 600
top_n: 2
parameters:
  - path: execution.base_improve
    values: [0.0, 0.01]
  - path: risk.per_trade_notional
    values: [10.0, 20.0]
"#,
    )
    .expect("walk config should write");

    let report_path = temp_dir.path().join("walk-forward-report.json");
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "walk-forward-mean-reversion",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--start",
        "2024-04-01T00:00:00Z",
        "--end",
        "2024-04-03T12:00:00Z",
        "--dataset",
        temp_dir.path().to_str().expect("dataset path"),
        "--config",
        config_path.to_str().expect("config path"),
        "--walk-config",
        walk_config_path.to_str().expect("walk config path"),
        "--report-output",
        report_path.to_str().expect("report path"),
        "--max-workers",
        "2",
    ])
    .expect("walk-forward cli should parse");

    let output = execute_cli(cli)
        .await
        .expect("walk-forward command should execute");
    let BacktestCommandOutput::WalkForwardMeanReversion(manifest) = output else {
        panic!("expected walk-forward manifest");
    };

    assert!(manifest.window_count >= 2);
    assert!(manifest.report_path.exists());
    assert!(manifest.best_parameter_set.is_some());

    let report_json = fs::read_to_string(&report_path).expect("report should exist");
    assert!(report_json.contains("\"window_count\""));
    assert!(report_json.contains("\"validation_runs\""));
    assert!(report_json.contains("execution.base_improve"));
}

#[tokio::test]
async fn analyze_mean_reversion_combines_scan_and_walk_forward_reports() {
    let temp_dir = tempdir().expect("temp dir");
    let scan_report_path = temp_dir.path().join("scan-report.json");
    let walk_report_path = temp_dir.path().join("walk-forward-report.json");
    let analysis_report_path = temp_dir.path().join("analysis-report.json");

    let scan_report = MeanReversionScanReport {
        config_path: temp_dir.path().join("mean_reversion.yml"),
        scan_config_path: temp_dir.path().join("scan.yml"),
        sort_by: MeanReversionScanSortBy::FinalEquity,
        total_runs: 4,
        completed_runs: 4,
        workers_used: 2,
        best_run: Some(MeanReversionScanRunSummary {
            run_id: 1,
            parameters: [
                ("execution.base_improve".to_string(), 0.0),
                ("risk.per_trade_notional".to_string(), 10.0),
            ]
            .into_iter()
            .collect(),
            final_equity: 1050.0,
            realized_pnl: 55.0,
            net_realized_pnl: 50.0,
            total_fees_paid: 5.0,
            executed_signals: 12,
            closed_trades: 8,
            rejected_orders: 0,
        }),
        runs: vec![
            MeanReversionScanRunSummary {
                run_id: 1,
                parameters: [
                    ("execution.base_improve".to_string(), 0.0),
                    ("risk.per_trade_notional".to_string(), 10.0),
                ]
                .into_iter()
                .collect(),
                final_equity: 1050.0,
                realized_pnl: 55.0,
                net_realized_pnl: 50.0,
                total_fees_paid: 5.0,
                executed_signals: 12,
                closed_trades: 8,
                rejected_orders: 0,
            },
            MeanReversionScanRunSummary {
                run_id: 2,
                parameters: [
                    ("execution.base_improve".to_string(), 0.01),
                    ("risk.per_trade_notional".to_string(), 10.0),
                ]
                .into_iter()
                .collect(),
                final_equity: 1030.0,
                realized_pnl: 38.0,
                net_realized_pnl: 33.0,
                total_fees_paid: 5.0,
                executed_signals: 10,
                closed_trades: 7,
                rejected_orders: 0,
            },
            MeanReversionScanRunSummary {
                run_id: 3,
                parameters: [
                    ("execution.base_improve".to_string(), 0.0),
                    ("risk.per_trade_notional".to_string(), 20.0),
                ]
                .into_iter()
                .collect(),
                final_equity: 1010.0,
                realized_pnl: 18.0,
                net_realized_pnl: 10.0,
                total_fees_paid: 8.0,
                executed_signals: 9,
                closed_trades: 6,
                rejected_orders: 1,
            },
            MeanReversionScanRunSummary {
                run_id: 4,
                parameters: [
                    ("execution.base_improve".to_string(), 0.01),
                    ("risk.per_trade_notional".to_string(), 20.0),
                ]
                .into_iter()
                .collect(),
                final_equity: 990.0,
                realized_pnl: -5.0,
                net_realized_pnl: -12.0,
                total_fees_paid: 7.0,
                executed_signals: 8,
                closed_trades: 5,
                rejected_orders: 2,
            },
        ],
    };
    fs::write(
        &scan_report_path,
        serde_json::to_vec_pretty(&scan_report).expect("scan report serialize"),
    )
    .expect("scan report should write");

    let walk_report = MeanReversionWalkForwardReport {
        config_path: temp_dir.path().join("mean_reversion.yml"),
        walk_config_path: temp_dir.path().join("walk.yml"),
        sort_by: MeanReversionScanSortBy::FinalEquity,
        train_bars: 1200,
        validation_bars: 600,
        step_bars: 600,
        top_n: 2,
        total_parameter_sets: 4,
        window_count: 3,
        workers_used: 2,
        best_parameter_set: Some(MeanReversionWalkForwardParameterSummary {
            parameters: [
                ("execution.base_improve".to_string(), 0.0),
                ("risk.per_trade_notional".to_string(), 10.0),
            ]
            .into_iter()
            .collect(),
            windows_selected: 3,
            average_final_equity: 1025.0,
            average_realized_pnl: 28.0,
            average_net_realized_pnl: 24.0,
            average_fees_paid: 4.0,
            total_executed_signals: 22,
            total_closed_trades: 15,
            total_rejected_orders: 0,
        }),
        parameter_sets: vec![
            MeanReversionWalkForwardParameterSummary {
                parameters: [
                    ("execution.base_improve".to_string(), 0.0),
                    ("risk.per_trade_notional".to_string(), 10.0),
                ]
                .into_iter()
                .collect(),
                windows_selected: 3,
                average_final_equity: 1025.0,
                average_realized_pnl: 28.0,
                average_net_realized_pnl: 24.0,
                average_fees_paid: 4.0,
                total_executed_signals: 22,
                total_closed_trades: 15,
                total_rejected_orders: 0,
            },
            MeanReversionWalkForwardParameterSummary {
                parameters: [
                    ("execution.base_improve".to_string(), 0.01),
                    ("risk.per_trade_notional".to_string(), 10.0),
                ]
                .into_iter()
                .collect(),
                windows_selected: 2,
                average_final_equity: 1008.0,
                average_realized_pnl: 12.0,
                average_net_realized_pnl: 7.0,
                average_fees_paid: 5.0,
                total_executed_signals: 18,
                total_closed_trades: 11,
                total_rejected_orders: 1,
            },
        ],
        windows: vec![MeanReversionWalkForwardWindowReport {
            window_id: 1,
            train_start: Utc.timestamp_millis_opt(1_711_929_600_000).unwrap(),
            train_end: Utc
                .timestamp_millis_opt(1_711_929_600_000 + 1_200 * 60_000)
                .unwrap(),
            validation_start: Utc
                .timestamp_millis_opt(1_711_929_600_000 + 1_201 * 60_000)
                .unwrap(),
            validation_end: Utc
                .timestamp_millis_opt(1_711_929_600_000 + 1_800 * 60_000)
                .unwrap(),
            training_runs: 4,
            validation_runs: Vec::new(),
            best_train_run: scan_report.best_run.clone(),
            best_validation_run: scan_report.best_run.clone(),
        }],
    };
    fs::write(
        &walk_report_path,
        serde_json::to_vec_pretty(&walk_report).expect("walk report serialize"),
    )
    .expect("walk report should write");

    let cli = BacktestCli::try_parse_from([
        "backtest",
        "analyze-mean-reversion",
        "--scan-report",
        scan_report_path.to_str().expect("scan report path"),
        "--walk-forward-report",
        walk_report_path.to_str().expect("walk report path"),
        "--analysis-output",
        analysis_report_path.to_str().expect("analysis report path"),
        "--top-runs",
        "3",
    ])
    .expect("analysis cli should parse");

    let output = execute_cli(cli)
        .await
        .expect("analysis command should execute");
    let BacktestCommandOutput::AnalyzeMeanReversion(manifest) = output else {
        panic!("expected analysis manifest");
    };

    assert!(manifest.report_path.exists());
    assert_eq!(manifest.top_runs_count, 3);
    assert!(manifest.best_run.is_some());
    assert!(manifest.walk_forward_summary_present);
    assert!(manifest.scan_best_matches_walk_forward_best);

    let report_json = fs::read_to_string(&analysis_report_path).expect("report should exist");
    assert!(report_json.contains("\"parameter_insights\""));
    assert!(report_json.contains("\"walk_forward_summary\""));
    assert!(report_json.contains("execution.base_improve"));
}
