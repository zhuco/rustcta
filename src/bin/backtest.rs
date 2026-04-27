use clap::Parser;
use rustcta::backtest::runtime::BacktestCommandOutput;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = rustcta::backtest::runtime::BacktestCli::parse();
    let output = rustcta::backtest::runtime::execute_cli(cli).await?;

    match output {
        BacktestCommandOutput::FetchKlines(manifest) => {
            println!(
                "stored klines at {} with manifest {}",
                manifest.dataset.data_path.display(),
                manifest.dataset.manifest_path.display()
            );
        }
        BacktestCommandOutput::CaptureDepth(manifest) => {
            println!(
                "captured raw depth at {}, normalized dataset at {}, manifest {}, snapshot_update_id={}, raw_delta_count={}",
                manifest.raw_capture_path.display(),
                manifest.normalized_data_path.display(),
                manifest.normalized_manifest_path.display(),
                manifest.snapshot_update_id,
                manifest.raw_delta_count
            );
        }
        BacktestCommandOutput::CaptureTrades(manifest) => {
            println!(
                "captured raw trades at {}, normalized dataset at {}, manifest {}, raw_trade_count={}",
                manifest.raw_capture_path.display(),
                manifest.normalized_data_path.display(),
                manifest.normalized_manifest_path.display(),
                manifest.raw_trade_count
            );
        }
        BacktestCommandOutput::RunMeanReversion(manifest) => {
            if let Some(summary_path) = manifest.summary_path {
                println!(
                    "mean reversion processed {} events and emitted {} signals; summary written to {}",
                    manifest.summary.signals.processed_events,
                    manifest.summary.execution.executed_signals,
                    summary_path.display()
                );
            } else {
                println!(
                    "mean reversion processed {} events and emitted {} signals",
                    manifest.summary.signals.processed_events,
                    manifest.summary.execution.executed_signals
                );
            }
        }
        BacktestCommandOutput::ScanMeanReversion(manifest) => {
            if let Some(best_run) = manifest.best_run {
                println!(
                    "mean reversion scan completed {} runs with {} workers; best run #{} final_equity={}, report written to {}",
                    manifest.total_runs,
                    manifest.workers_used,
                    best_run.run_id,
                    best_run.final_equity,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "mean reversion scan completed 0 runs; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::WalkForwardMeanReversion(manifest) => {
            if let Some(best_parameter_set) = manifest.best_parameter_set {
                println!(
                    "walk forward completed {} windows across {} parameter sets with {} workers; best oos average_final_equity={}, report written to {}",
                    manifest.window_count,
                    manifest.total_parameter_sets,
                    manifest.workers_used,
                    best_parameter_set.average_final_equity,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "walk forward completed 0 parameter aggregations; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::AnalyzeMeanReversion(manifest) => {
            if let Some(best_run) = manifest.best_run {
                println!(
                    "analysis completed with {} top runs; best scan run #{}; walk_forward_summary_present={}; report written to {}",
                    manifest.top_runs_count,
                    best_run.run_id,
                    manifest.walk_forward_summary_present,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "analysis completed without ranked runs; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::ScanTrend(manifest) => {
            if let Some(best_run) = manifest.best_run {
                println!(
                    "trend scan completed {} runs; best run #{} {} roi={:.4}%, final_equity={:.4}; report written to {}",
                    manifest.total_runs,
                    best_run.run_id,
                    best_run.parameter_set,
                    best_run.roi_pct,
                    best_run.final_equity,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "trend scan completed 0 runs; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::ScanTrendFactor(manifest) => {
            if let Some(best_run) = manifest.best_run {
                println!(
                    "trend factor scan completed {} runs; best run #{} {} score={:.4}, roi={:.4}%, final_equity={:.4}; report written to {}",
                    manifest.total_runs,
                    best_run.run_id,
                    best_run.strategy_name,
                    best_run.score.composite_score,
                    best_run.roi_pct,
                    best_run.final_equity,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "trend factor scan completed 0 runs; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::ScanMtfTrendFactor(manifest) => {
            if let Some(best_run) = manifest.best_run {
                println!(
                    "mtf trend factor scan completed {} runs; best run #{} {} score={:.4}, roi={:.4}%, final_equity={:.4}; report written to {}",
                    manifest.total_runs,
                    best_run.run_id,
                    best_run.strategy_name,
                    best_run.score.composite_score,
                    best_run.roi_pct,
                    best_run.final_equity,
                    manifest.report_path.display()
                );
            } else {
                println!(
                    "mtf trend factor scan completed 0 runs; report written to {}",
                    manifest.report_path.display()
                );
            }
        }
        BacktestCommandOutput::RunShortLadder(manifest) => {
            println!(
                "short ladder {:?} completed {} trades, roi={:.4}%, pf={:.4}; report written to {}",
                manifest.report.mode,
                manifest.report.summary.trades,
                manifest.report.summary.roi_pct,
                manifest.report.summary.profit_factor,
                manifest.report_path.display()
            );
        }
    }

    Ok(())
}
