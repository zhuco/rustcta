use clap::Parser;
use rustcta::backtest::runtime::{BacktestCli, BacktestCommand};

#[test]
fn parses_fetch_klines_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "fetch-klines",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--start",
        "2026-04-01T00:00:00Z",
        "--end",
        "2026-04-01T01:00:00Z",
        "--output",
        "data",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::FetchKlines(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.interval, "1m");
            assert_eq!(args.start, "2026-04-01T00:00:00Z");
            assert_eq!(args.end, "2026-04-01T01:00:00Z");
            assert_eq!(args.output, "data");
        }
        BacktestCommand::RunMeanReversion(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::CaptureDepth(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::CaptureTrades(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::ScanMeanReversion(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::WalkForwardMeanReversion(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::AnalyzeMeanReversion(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::ScanTrend(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::ScanTrendFactor(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::ScanMtfTrendFactor(_) => {
            panic!("expected fetch-klines command")
        }
        BacktestCommand::RunShortLadder(_) => {
            panic!("expected fetch-klines command")
        }
    }
}

#[test]
fn parses_run_mean_reversion_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "run-mean-reversion",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1m",
        "--start",
        "2026-04-01T00:00:00Z",
        "--end",
        "2026-04-02T00:00:00Z",
        "--dataset",
        "data",
        "--config",
        "config/mean_reversion.yml",
        "--summary-output",
        "reports/mean_reversion.json",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::RunMeanReversion(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.interval, "1m");
            assert_eq!(args.start, "2026-04-01T00:00:00Z");
            assert_eq!(args.end, "2026-04-02T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.config, "config/mean_reversion.yml");
            assert_eq!(
                args.summary_output.as_deref(),
                Some("reports/mean_reversion.json")
            );
        }
        BacktestCommand::FetchKlines(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::CaptureDepth(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::CaptureTrades(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::ScanMeanReversion(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::WalkForwardMeanReversion(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::AnalyzeMeanReversion(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::ScanTrend(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::ScanTrendFactor(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::ScanMtfTrendFactor(_) => {
            panic!("expected run-mean-reversion command")
        }
        BacktestCommand::RunShortLadder(_) => {
            panic!("expected run-mean-reversion command")
        }
    }
}

#[test]
fn parses_capture_depth_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "capture-depth",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--duration-secs",
        "30",
        "--levels",
        "1000",
        "--cadence-ms",
        "100",
        "--output",
        "data",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::CaptureDepth(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.duration_secs, 30);
            assert_eq!(args.levels, 1000);
            assert_eq!(args.cadence_ms, 100);
            assert_eq!(args.output, "data");
        }
        _ => panic!("expected capture-depth command"),
    }
}

#[test]
fn parses_capture_trades_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "capture-trades",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--duration-secs",
        "30",
        "--output",
        "data",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::CaptureTrades(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.duration_secs, 30);
            assert_eq!(args.output, "data");
        }
        _ => panic!("expected capture-trades command"),
    }
}

#[test]
fn parses_scan_mean_reversion_command() {
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
        "2026-04-01T00:00:00Z",
        "--end",
        "2026-04-02T00:00:00Z",
        "--dataset",
        "data",
        "--config",
        "config/mean_reversion.yml",
        "--scan-config",
        "config/scan.yml",
        "--report-output",
        "reports/scan.json",
        "--max-workers",
        "8",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::ScanMeanReversion(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.interval, "1m");
            assert_eq!(args.start, "2026-04-01T00:00:00Z");
            assert_eq!(args.end, "2026-04-02T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.config, "config/mean_reversion.yml");
            assert_eq!(args.scan_config, "config/scan.yml");
            assert_eq!(args.report_output, "reports/scan.json");
            assert_eq!(args.max_workers, Some(8));
        }
        _ => panic!("expected scan-mean-reversion command"),
    }
}

#[test]
fn parses_walk_forward_mean_reversion_command() {
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
        "2026-04-01T00:00:00Z",
        "--end",
        "2026-04-03T00:00:00Z",
        "--dataset",
        "data",
        "--config",
        "config/mean_reversion.yml",
        "--walk-config",
        "config/walk.yml",
        "--report-output",
        "reports/walk-forward.json",
        "--max-workers",
        "6",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::WalkForwardMeanReversion(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDT");
            assert_eq!(args.interval, "1m");
            assert_eq!(args.start, "2026-04-01T00:00:00Z");
            assert_eq!(args.end, "2026-04-03T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.config, "config/mean_reversion.yml");
            assert_eq!(args.walk_config, "config/walk.yml");
            assert_eq!(args.report_output, "reports/walk-forward.json");
            assert_eq!(args.max_workers, Some(6));
        }
        _ => panic!("expected walk-forward-mean-reversion command"),
    }
}

#[test]
fn parses_scan_trend_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "scan-trend",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDC",
        "--interval",
        "5m",
        "--start",
        "2026-01-26T00:00:00Z",
        "--end",
        "2026-04-26T00:00:00Z",
        "--dataset",
        "data",
        "--scan-config",
        "config/trend_scan.yml",
        "--report-output",
        "reports/trend_scan.json",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::ScanTrend(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDC");
            assert_eq!(args.interval, "5m");
            assert_eq!(args.start, "2026-01-26T00:00:00Z");
            assert_eq!(args.end, "2026-04-26T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.scan_config, "config/trend_scan.yml");
            assert_eq!(args.report_output, "reports/trend_scan.json");
        }
        _ => panic!("expected scan-trend command"),
    }
}

#[test]
fn parses_scan_trend_factor_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "scan-trend-factor",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDC",
        "--interval",
        "5m",
        "--start",
        "2026-01-26T00:00:00Z",
        "--end",
        "2026-04-26T00:00:00Z",
        "--dataset",
        "data",
        "--scan-config",
        "config/trend_factor_scan.yml",
        "--report-output",
        "reports/trend_factor_scan.json",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::ScanTrendFactor(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDC");
            assert_eq!(args.interval, "5m");
            assert_eq!(args.start, "2026-01-26T00:00:00Z");
            assert_eq!(args.end, "2026-04-26T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.scan_config, "config/trend_factor_scan.yml");
            assert_eq!(args.report_output, "reports/trend_factor_scan.json");
        }
        _ => panic!("expected scan-trend-factor command"),
    }
}

#[test]
fn parses_scan_mtf_trend_factor_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "scan-mtf-trend-factor",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDC",
        "--start",
        "2026-01-26T00:00:00Z",
        "--end",
        "2026-04-26T00:00:00Z",
        "--dataset",
        "data",
        "--scan-config",
        "config/mtf_trend_factor_scan.yml",
        "--report-output",
        "reports/mtf_trend_factor_scan.json",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::ScanMtfTrendFactor(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "BTCUSDC");
            assert_eq!(args.start, "2026-01-26T00:00:00Z");
            assert_eq!(args.end, "2026-04-26T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.scan_config, "config/mtf_trend_factor_scan.yml");
            assert_eq!(args.report_output, "reports/mtf_trend_factor_scan.json");
        }
        _ => panic!("expected scan-mtf-trend-factor command"),
    }
}

#[test]
fn parses_run_short_ladder_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "run-short-ladder",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "SOLUSDT",
        "--interval",
        "5m",
        "--start",
        "2025-04-27T00:00:00Z",
        "--end",
        "2026-04-27T00:00:00Z",
        "--dataset",
        "data",
        "--config",
        "config/short_ladder.yml",
        "--mode",
        "adverse_averaging",
        "--report-output",
        "reports/short_ladder.json",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::RunShortLadder(args) => {
            assert_eq!(args.exchange, "binance");
            assert_eq!(args.market, "futures");
            assert_eq!(args.symbol, "SOLUSDT");
            assert_eq!(args.interval, "5m");
            assert_eq!(args.start, "2025-04-27T00:00:00Z");
            assert_eq!(args.end, "2026-04-27T00:00:00Z");
            assert_eq!(args.dataset, "data");
            assert_eq!(args.config, "config/short_ladder.yml");
            assert_eq!(args.mode, "adverse_averaging");
            assert_eq!(args.report_output, "reports/short_ladder.json");
        }
        _ => panic!("expected run-short-ladder command"),
    }
}

#[test]
fn parses_analyze_mean_reversion_command() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "analyze-mean-reversion",
        "--scan-report",
        "reports/scan.json",
        "--walk-forward-report",
        "reports/walk-forward.json",
        "--analysis-output",
        "reports/analysis.json",
        "--top-runs",
        "7",
    ])
    .expect("cli should parse");

    match cli.command {
        BacktestCommand::AnalyzeMeanReversion(args) => {
            assert_eq!(args.scan_report, "reports/scan.json");
            assert_eq!(
                args.walk_forward_report.as_deref(),
                Some("reports/walk-forward.json")
            );
            assert_eq!(args.analysis_output, "reports/analysis.json");
            assert_eq!(args.top_runs, 7);
        }
        _ => panic!("expected analyze-mean-reversion command"),
    }
}

#[test]
fn backtest_runtime_smoke() {
    let cli = BacktestCli::try_parse_from([
        "backtest",
        "fetch-klines",
        "--exchange",
        "binance",
        "--market",
        "futures",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "5m",
        "--start",
        "2026-04-01T00:00:00Z",
        "--end",
        "2026-04-01T02:00:00Z",
        "--output",
        "data",
    ])
    .expect("cli should parse");

    let config = rustcta::backtest::runtime::BacktestRuntimeConfig::from_cli(&cli)
        .expect("runtime config should build");

    assert_eq!(config.exchange, "binance");
    assert_eq!(config.market, "futures");
    assert_eq!(config.symbol, "BTC/USDT");
    assert_eq!(config.interval.to_string(), "5m");
}
