use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{fs, path::Path};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::backtest::data::depth_capture::{
    import_binance_futures_depth_capture, DepthCaptureSession, DepthCaptureWriter,
    RawDepthDeltaRecord, RawDepthSnapshot,
};
use crate::backtest::data::depth_dataset::DepthDatasetReader;
use crate::backtest::data::exchange_metadata::{ExchangeMetadataReader, ExchangeMetadataWriter};
use crate::backtest::data::kline_dataset::{KlineDatasetOutput, KlineDatasetWriter};
use crate::backtest::data::trade_capture::{
    import_binance_futures_trade_capture, RawTradeRecord, TradeCaptureSession, TradeCaptureWriter,
};
use crate::backtest::data::trade_dataset::TradeDatasetReader;
use crate::backtest::factors::{
    expand_mtf_trend_factor_runs, expand_trend_factor_runs, MtfTrendFactorScanSpec,
    TrendFactorScanSpec,
};
use crate::backtest::matching::engine::{BacktestEngineState, LimitOrderProcessingResult};
use crate::backtest::replay::{
    DepthReplay, KlineReplay, MergedReplay, ReplayPartition, TradeReplay,
};
use crate::backtest::schema::BacktestEvent;
use crate::backtest::strategy::mean_reversion::{
    MeanReversionBacktestStrategy, MeanReversionBacktestSummary,
};
use crate::backtest::strategy::mtf_trend_factor::run_mtf_trend_factor_definition;
use crate::backtest::strategy::short_ladder::{
    run_short_ladder_backtest, run_short_ladder_mtf_execution_backtest, ShortLadderConfig,
    ShortLadderMode, ShortLadderMtfExecutionConfig, ShortLadderMtfExecutionRunReport,
    ShortLadderRunReport,
};
use crate::backtest::strategy::trend_factor::{
    run_trend_factor_definition, TrendFactorRunReport, TrendFactorRunSummary,
};
use crate::backtest::strategy::{BacktestStrategy, StrategySignal};
use crate::core::config::{ApiKeys, Config};
use crate::core::types::{Interval, Kline, MarketType, OrderSide, TradingPair};
use crate::exchanges::BinanceExchange;
use crate::strategies::mean_reversion::MeanReversionConfig;
use crate::utils::SymbolConverter;
use crate::Exchange;

#[derive(Debug, Parser)]
#[command(name = "backtest")]
pub struct BacktestCli {
    #[command(subcommand)]
    pub command: BacktestCommand,
}

#[derive(Debug, Subcommand)]
pub enum BacktestCommand {
    FetchKlines(FetchKlinesArgs),
    CaptureDepth(CaptureDepthArgs),
    CaptureTrades(CaptureTradesArgs),
    RunMeanReversion(RunMeanReversionArgs),
    ScanMeanReversion(ScanMeanReversionArgs),
    WalkForwardMeanReversion(WalkForwardMeanReversionArgs),
    AnalyzeMeanReversion(AnalyzeMeanReversionArgs),
    ScanTrend(ScanTrendArgs),
    ScanTrendFactor(ScanTrendFactorArgs),
    ScanMtfTrendFactor(ScanMtfTrendFactorArgs),
    RunShortLadder(RunShortLadderArgs),
    RunShortLadderMtfExecution(RunShortLadderMtfExecutionArgs),
}

#[derive(Debug, Clone, Args)]
pub struct FetchKlinesArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub output: String,
}

#[derive(Debug, Clone, Args)]
pub struct CaptureDepthArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 1000)]
    pub levels: u32,
    #[arg(long, default_value_t = 100)]
    pub cadence_ms: u32,
    #[arg(long)]
    pub output: String,
}

#[derive(Debug, Clone, Args)]
pub struct CaptureTradesArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub duration_secs: u64,
    #[arg(long)]
    pub output: String,
}

#[derive(Debug, Clone, Args)]
pub struct RunMeanReversionArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub config: String,
    #[arg(long)]
    pub summary_output: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct ScanMeanReversionArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub config: String,
    #[arg(long)]
    pub scan_config: String,
    #[arg(long)]
    pub report_output: String,
    #[arg(long)]
    pub max_workers: Option<usize>,
}

#[derive(Debug, Clone, Args)]
pub struct WalkForwardMeanReversionArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub config: String,
    #[arg(long)]
    pub walk_config: String,
    #[arg(long)]
    pub report_output: String,
    #[arg(long)]
    pub max_workers: Option<usize>,
}

#[derive(Debug, Clone, Args)]
pub struct AnalyzeMeanReversionArgs {
    #[arg(long)]
    pub scan_report: String,
    #[arg(long)]
    pub walk_forward_report: Option<String>,
    #[arg(long)]
    pub analysis_output: String,
    #[arg(long, default_value_t = 5)]
    pub top_runs: usize,
}

#[derive(Debug, Clone, Args)]
pub struct ScanTrendArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub scan_config: String,
    #[arg(long)]
    pub report_output: String,
}

#[derive(Debug, Clone, Args)]
pub struct ScanTrendFactorArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub scan_config: String,
    #[arg(long)]
    pub report_output: String,
}

#[derive(Debug, Clone, Args)]
pub struct ScanMtfTrendFactorArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub scan_config: String,
    #[arg(long)]
    pub report_output: String,
}

#[derive(Debug, Clone, Args)]
pub struct RunShortLadderArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub config: String,
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub report_output: String,
}

#[derive(Debug, Clone, Args)]
pub struct RunShortLadderMtfExecutionArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub market: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value = "5m")]
    pub signal_interval: String,
    #[arg(long, default_value = "1m")]
    pub execution_interval: String,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub dataset: String,
    #[arg(long)]
    pub config: String,
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub report_output: String,
    #[arg(long, default_value_t = 300)]
    pub order_cooldown_secs: u64,
    #[arg(long, default_value_t = 15)]
    pub min_minutes_to_l4: u64,
    #[arg(long, default_value_t = 3)]
    pub max_layers_per_hour: usize,
}

#[derive(Debug, Clone)]
pub struct BacktestRuntimeConfig {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub interval: Interval,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub output: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BacktestRunManifest {
    pub dataset: KlineDatasetOutput,
    pub config: BacktestRuntimeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionRunManifest {
    pub config_path: PathBuf,
    pub summary_path: Option<PathBuf>,
    pub summary: MeanReversionBacktestReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderRunManifest {
    pub config_path: PathBuf,
    pub report_path: PathBuf,
    pub report: ShortLadderRunReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLadderMtfExecutionRunManifest {
    pub config_path: PathBuf,
    pub report_path: PathBuf,
    pub report: ShortLadderMtfExecutionRunReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanRunSummary {
    pub run_id: usize,
    pub parameters: BTreeMap<String, f64>,
    pub final_equity: f64,
    pub realized_pnl: f64,
    pub net_realized_pnl: f64,
    pub total_fees_paid: f64,
    pub executed_signals: usize,
    pub closed_trades: usize,
    pub rejected_orders: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanReport {
    pub config_path: PathBuf,
    pub scan_config_path: PathBuf,
    pub sort_by: MeanReversionScanSortBy,
    pub total_runs: usize,
    pub completed_runs: usize,
    pub workers_used: usize,
    pub best_run: Option<MeanReversionScanRunSummary>,
    pub runs: Vec<MeanReversionScanRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanManifest {
    pub config_path: PathBuf,
    pub scan_config_path: PathBuf,
    pub report_path: PathBuf,
    pub total_runs: usize,
    pub workers_used: usize,
    pub best_run: Option<MeanReversionScanRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardSpec {
    #[serde(default)]
    pub sort_by: MeanReversionScanSortBy,
    pub train_bars: usize,
    pub validation_bars: usize,
    pub step_bars: usize,
    #[serde(default = "default_walk_forward_top_n")]
    pub top_n: usize,
    #[serde(default)]
    pub max_workers: Option<usize>,
    pub parameters: Vec<MeanReversionScanParameterRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardCandidateSummary {
    pub run_id: usize,
    pub parameters: BTreeMap<String, f64>,
    pub train: MeanReversionScanRunSummary,
    pub validation: MeanReversionScanRunSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardWindowReport {
    pub window_id: usize,
    pub train_start: DateTime<Utc>,
    pub train_end: DateTime<Utc>,
    pub validation_start: DateTime<Utc>,
    pub validation_end: DateTime<Utc>,
    pub training_runs: usize,
    pub validation_runs: Vec<MeanReversionWalkForwardCandidateSummary>,
    pub best_train_run: Option<MeanReversionScanRunSummary>,
    pub best_validation_run: Option<MeanReversionScanRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardParameterSummary {
    pub parameters: BTreeMap<String, f64>,
    pub windows_selected: usize,
    pub average_final_equity: f64,
    pub average_realized_pnl: f64,
    pub average_net_realized_pnl: f64,
    pub average_fees_paid: f64,
    pub total_executed_signals: usize,
    pub total_closed_trades: usize,
    pub total_rejected_orders: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardReport {
    pub config_path: PathBuf,
    pub walk_config_path: PathBuf,
    pub sort_by: MeanReversionScanSortBy,
    pub train_bars: usize,
    pub validation_bars: usize,
    pub step_bars: usize,
    pub top_n: usize,
    pub total_parameter_sets: usize,
    pub window_count: usize,
    pub workers_used: usize,
    pub best_parameter_set: Option<MeanReversionWalkForwardParameterSummary>,
    pub parameter_sets: Vec<MeanReversionWalkForwardParameterSummary>,
    pub windows: Vec<MeanReversionWalkForwardWindowReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardManifest {
    pub config_path: PathBuf,
    pub walk_config_path: PathBuf,
    pub report_path: PathBuf,
    pub total_parameter_sets: usize,
    pub window_count: usize,
    pub workers_used: usize,
    pub best_parameter_set: Option<MeanReversionWalkForwardParameterSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionParameterValueInsight {
    pub value: f64,
    pub run_count: usize,
    pub average_final_equity: f64,
    pub average_net_realized_pnl: f64,
    pub best_final_equity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionParameterInsight {
    pub path: String,
    pub best_value: f64,
    pub value_insights: Vec<MeanReversionParameterValueInsight>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanAnalysisSummary {
    pub sort_by: MeanReversionScanSortBy,
    pub total_runs: usize,
    pub completed_runs: usize,
    pub workers_used: usize,
    pub best_run: Option<MeanReversionScanRunSummary>,
    pub median_final_equity: f64,
    pub worst_final_equity: f64,
    pub profitable_run_ratio: f64,
    pub top_final_equity_average: f64,
    pub top_final_equity_range: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionWalkForwardAnalysisSummary {
    pub window_count: usize,
    pub total_parameter_sets: usize,
    pub parameter_sets_analyzed: usize,
    pub best_parameter_set: Option<MeanReversionWalkForwardParameterSummary>,
    pub scan_best_matches_walk_forward_best: bool,
    pub scan_best_in_walk_forward_parameter_sets: bool,
    pub average_windows_selected: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionAnalysisReport {
    pub scan_report_path: PathBuf,
    pub walk_forward_report_path: Option<PathBuf>,
    pub top_runs_requested: usize,
    pub top_runs: Vec<MeanReversionScanRunSummary>,
    pub scan_summary: MeanReversionScanAnalysisSummary,
    pub parameter_insights: Vec<MeanReversionParameterInsight>,
    pub walk_forward_summary: Option<MeanReversionWalkForwardAnalysisSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionAnalysisManifest {
    pub scan_report_path: PathBuf,
    pub walk_forward_report_path: Option<PathBuf>,
    pub report_path: PathBuf,
    pub top_runs_count: usize,
    pub best_run: Option<MeanReversionScanRunSummary>,
    pub walk_forward_summary_present: bool,
    pub scan_best_matches_walk_forward_best: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthCaptureRunManifest {
    pub raw_capture_path: PathBuf,
    pub normalized_data_path: PathBuf,
    pub normalized_manifest_path: PathBuf,
    pub snapshot_update_id: u64,
    pub raw_delta_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeCaptureRunManifest {
    pub raw_capture_path: PathBuf,
    pub normalized_data_path: PathBuf,
    pub normalized_manifest_path: PathBuf,
    pub raw_trade_count: usize,
}

#[derive(Debug, Clone)]
pub enum BacktestCommandOutput {
    FetchKlines(BacktestRunManifest),
    CaptureDepth(DepthCaptureRunManifest),
    CaptureTrades(TradeCaptureRunManifest),
    RunMeanReversion(MeanReversionRunManifest),
    ScanMeanReversion(MeanReversionScanManifest),
    WalkForwardMeanReversion(MeanReversionWalkForwardManifest),
    AnalyzeMeanReversion(MeanReversionAnalysisManifest),
    ScanTrend(TrendScanManifest),
    ScanTrendFactor(TrendFactorScanManifest),
    ScanMtfTrendFactor(MtfTrendFactorScanManifest),
    RunShortLadder(ShortLadderRunManifest),
    RunShortLadderMtfExecution(ShortLadderMtfExecutionRunManifest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorScanReport {
    pub scan_config_path: PathBuf,
    pub runtime: TrendScanRuntimeSnapshot,
    pub total_runs: usize,
    pub best_run: Option<TrendFactorRunSummary>,
    pub runs: Vec<TrendFactorRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorScanManifest {
    pub scan_config_path: PathBuf,
    pub report_path: PathBuf,
    pub total_runs: usize,
    pub best_run: Option<TrendFactorRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendFactorScanReport {
    pub scan_config_path: PathBuf,
    pub runtime: MtfTrendScanRuntimeSnapshot,
    pub total_runs: usize,
    pub best_run: Option<TrendFactorRunSummary>,
    pub runs: Vec<TrendFactorRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendFactorScanManifest {
    pub scan_config_path: PathBuf,
    pub report_path: PathBuf,
    pub total_runs: usize,
    pub best_run: Option<TrendFactorRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendScanRuntimeSnapshot {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub trend_interval: String,
    pub structure_interval: String,
    pub trigger_interval: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub dataset: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanSpec {
    #[serde(default = "default_trend_initial_equity")]
    pub initial_equity: f64,
    #[serde(default = "default_trend_trade_notional")]
    pub trade_notional: f64,
    #[serde(default = "default_trend_fee_rate")]
    pub fee_rate: f64,
    #[serde(default = "default_trend_slippage_rate")]
    pub slippage_rate: f64,
    pub parameter_sets: Vec<TrendParameterSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendParameterSet {
    pub name: String,
    pub fast_ema: usize,
    pub slow_ema: usize,
    #[serde(default = "default_trend_adx_period")]
    pub adx_period: usize,
    pub adx_threshold: f64,
    #[serde(default = "default_trend_rsi_period")]
    pub rsi_period: usize,
    #[serde(default = "default_trend_rsi_overbought")]
    pub rsi_overbought: f64,
    #[serde(default = "default_trend_rsi_oversold")]
    pub rsi_oversold: f64,
    #[serde(default = "default_trend_atr_period")]
    pub atr_period: usize,
    pub atr_multiplier: f64,
    pub reward_r: f64,
    pub breakout_lookback: usize,
    pub max_hold_bars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanRunSummary {
    pub run_id: usize,
    pub symbol: String,
    pub parameter_set: String,
    pub fast_ema: usize,
    pub slow_ema: usize,
    pub adx_threshold: f64,
    pub atr_multiplier: f64,
    pub reward_r: f64,
    pub breakout_lookback: usize,
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
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanTrade {
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
pub struct TrendScanRunReport {
    pub summary: TrendScanRunSummary,
    pub trades: Vec<TrendScanTrade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanReport {
    pub scan_config_path: PathBuf,
    pub runtime: TrendScanRuntimeSnapshot,
    pub total_runs: usize,
    pub best_run: Option<TrendScanRunSummary>,
    pub runs: Vec<TrendScanRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanRuntimeSnapshot {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub interval: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub dataset: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendScanManifest {
    pub scan_config_path: PathBuf,
    pub report_path: PathBuf,
    pub total_runs: usize,
    pub best_run: Option<TrendScanRunSummary>,
}

#[derive(Debug, Clone)]
pub struct BacktestRuntime {
    config: BacktestRuntimeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutedSignal {
    pub symbol: String,
    pub side: OrderSide,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub quantity: f64,
    pub fee_paid: f64,
    pub is_maker: bool,
    pub order_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeExecutionLeg {
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub quantity: f64,
    pub fee_paid: f64,
    pub is_maker: bool,
    pub order_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPositionSummary {
    pub symbol: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub mark_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionExecutionSummary {
    pub settlement_currency: String,
    pub executed_signals: usize,
    pub depth_replay_enabled: bool,
    pub depth_events_processed: usize,
    pub trade_replay_enabled: bool,
    pub trade_events_processed: usize,
    pub limit_orders_placed: usize,
    pub limit_orders_filled: usize,
    pub limit_orders_cancelled: usize,
    pub venue_adjusted_orders: usize,
    pub rejected_orders: usize,
    pub rejection_reasons: BTreeMap<String, usize>,
    pub realized_pnl: f64,
    pub total_fees_paid: f64,
    pub total_funding_paid: f64,
    pub cash_balance: f64,
    pub final_equity: f64,
    pub positions: BTreeMap<String, ExecutionPositionSummary>,
    pub closed_trades: Vec<ClosedTrade>,
    pub fills: Vec<ExecutedSignal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionBacktestReport {
    pub signals: MeanReversionBacktestSummary,
    pub execution: MeanReversionExecutionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub symbol: String,
    pub side: OrderSide,
    pub entry_timestamp: DateTime<Utc>,
    pub exit_timestamp: DateTime<Utc>,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub realized_pnl: f64,
    pub net_realized_pnl: f64,
    pub entry_fill_count: usize,
    pub exit_fill_count: usize,
    pub entry_fees_paid: f64,
    pub exit_fees_paid: f64,
    pub average_entry_price: f64,
    pub average_exit_price: f64,
    pub entry_legs: Vec<TradeExecutionLeg>,
    pub exit_legs: Vec<TradeExecutionLeg>,
    pub exit_reason: String,
}

#[derive(Debug, Clone)]
struct ManagedPosition {
    symbol: String,
    side: OrderSide,
    quantity: f64,
    entry_price: f64,
    stop_price: f64,
    take_profit_price: f64,
    opened_at: DateTime<Utc>,
    entry_legs: Vec<TradeExecutionLeg>,
    entry_fees_paid: f64,
}

#[derive(Debug, Clone)]
struct PendingEntryPlan {
    symbol: String,
    side: OrderSide,
    logical_ts: DateTime<Utc>,
    price: f64,
    atr: f64,
    order_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct ExitDecision {
    reason: String,
    price: f64,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PendingEntryRequest {
    signal: StrategySignal,
    activate_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PendingMarketExit {
    reason: String,
    activate_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
struct VenueExecutionStats {
    adjusted_orders: usize,
    rejected_orders: usize,
    rejection_reasons: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
struct PreparedEntryOrder {
    price: f64,
    quantity: f64,
    adjusted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum MeanReversionScanSortBy {
    #[serde(rename = "final_equity")]
    #[default]
    FinalEquity,
    #[serde(rename = "net_realized_pnl")]
    NetRealizedPnl,
    #[serde(rename = "realized_pnl")]
    RealizedPnl,
    #[serde(rename = "executed_signals")]
    ExecutedSignals,
}

fn default_walk_forward_top_n() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanSpec {
    #[serde(default)]
    pub sort_by: MeanReversionScanSortBy,
    #[serde(default)]
    pub max_workers: Option<usize>,
    pub parameters: Vec<MeanReversionScanParameterRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionScanParameterRange {
    pub path: MeanReversionScanParameterPath,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MeanReversionScanParameterPath {
    #[serde(rename = "execution.base_improve")]
    ExecutionBaseImprove,
    #[serde(rename = "execution.take_profit_atr_k")]
    ExecutionTakeProfitAtrK,
    #[serde(rename = "indicators.atr.initial_stop_k")]
    IndicatorsAtrInitialStopK,
    #[serde(rename = "indicators.bollinger.entry_z_long")]
    IndicatorsBollingerEntryZLong,
    #[serde(rename = "indicators.bollinger.entry_z_short")]
    IndicatorsBollingerEntryZShort,
    #[serde(rename = "risk.per_trade_notional")]
    RiskPerTradeNotional,
    #[serde(rename = "risk.time_stop_bars")]
    RiskTimeStopBars,
}

impl MeanReversionScanParameterPath {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ExecutionBaseImprove => "execution.base_improve",
            Self::ExecutionTakeProfitAtrK => "execution.take_profit_atr_k",
            Self::IndicatorsAtrInitialStopK => "indicators.atr.initial_stop_k",
            Self::IndicatorsBollingerEntryZLong => "indicators.bollinger.entry_z_long",
            Self::IndicatorsBollingerEntryZShort => "indicators.bollinger.entry_z_short",
            Self::RiskPerTradeNotional => "risk.per_trade_notional",
            Self::RiskTimeStopBars => "risk.time_stop_bars",
        }
    }

    fn apply(self, config: &mut MeanReversionConfig, value: f64) -> Result<()> {
        match self {
            Self::ExecutionBaseImprove => {
                config.execution.base_improve = value;
            }
            Self::ExecutionTakeProfitAtrK => {
                config.execution.take_profit_atr_k = value;
            }
            Self::IndicatorsAtrInitialStopK => {
                config.indicators.atr.initial_stop_k = value;
            }
            Self::IndicatorsBollingerEntryZLong => {
                config.indicators.bollinger.entry_z_long = value;
            }
            Self::IndicatorsBollingerEntryZShort => {
                config.indicators.bollinger.entry_z_short = value;
            }
            Self::RiskPerTradeNotional => {
                config.risk.per_trade_notional = value;
            }
            Self::RiskTimeStopBars => {
                if value < 0.0 || !value.is_finite() {
                    return Err(anyhow!(
                        "risk.time_stop_bars must be a finite non-negative integer"
                    ));
                }
                let rounded = value.round();
                if (rounded - value).abs() > 1e-9 {
                    return Err(anyhow!("risk.time_stop_bars must be an integer"));
                }
                config.risk.time_stop_bars = rounded as u32;
            }
        }

        Ok(())
    }
}

impl BacktestRuntimeConfig {
    pub fn from_cli(cli: &BacktestCli) -> Result<Self> {
        match &cli.command {
            BacktestCommand::FetchKlines(args) => Self::from_fetch_args(args),
            BacktestCommand::CaptureDepth(_) => Err(anyhow!(
                "capture-depth does not map to BacktestRuntimeConfig"
            )),
            BacktestCommand::CaptureTrades(_) => Err(anyhow!(
                "capture-trades does not map to BacktestRuntimeConfig"
            )),
            BacktestCommand::RunMeanReversion(args) => Self::from_run_mean_reversion_args(args),
            BacktestCommand::ScanMeanReversion(args) => Self::from_scan_mean_reversion_args(args),
            BacktestCommand::WalkForwardMeanReversion(args) => {
                Self::from_walk_forward_mean_reversion_args(args)
            }
            BacktestCommand::AnalyzeMeanReversion(_) => Err(anyhow!(
                "analyze-mean-reversion does not map to BacktestRuntimeConfig"
            )),
            BacktestCommand::ScanTrend(args) => Self::from_scan_trend_args(args),
            BacktestCommand::ScanTrendFactor(args) => Self::from_scan_trend_factor_args(args),
            BacktestCommand::ScanMtfTrendFactor(_) => Err(anyhow!(
                "scan-mtf-trend-factor requires timeframes from scan config"
            )),
            BacktestCommand::RunShortLadder(args) => Self::from_run_short_ladder_args(args),
            BacktestCommand::RunShortLadderMtfExecution(args) => {
                Self::from_run_short_ladder_mtf_execution_args(args)
            }
        }
    }

    pub fn from_fetch_args(args: &FetchKlinesArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.output),
        })
    }

    pub fn from_run_mean_reversion_args(args: &RunMeanReversionArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_scan_mean_reversion_args(args: &ScanMeanReversionArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_scan_trend_args(args: &ScanTrendArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_scan_trend_factor_args(args: &ScanTrendFactorArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_scan_mtf_trend_factor_args(
        args: &ScanMtfTrendFactorArgs,
        trigger_interval: &str,
    ) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(trigger_interval)
                .map_err(|err| anyhow!("invalid trigger interval {}: {}", trigger_interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_run_short_ladder_args(args: &RunShortLadderArgs) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_run_short_ladder_mtf_execution_args(
        args: &RunShortLadderMtfExecutionArgs,
    ) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.execution_interval).map_err(|err| {
                anyhow!(
                    "invalid execution interval {}: {}",
                    args.execution_interval,
                    err
                )
            })?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }

    pub fn from_walk_forward_mean_reversion_args(
        args: &WalkForwardMeanReversionArgs,
    ) -> Result<Self> {
        let symbol = normalize_symbol_input(&args.symbol)?;

        Ok(Self {
            exchange: args.exchange.clone(),
            market: args.market.clone(),
            symbol,
            interval: Interval::from_string(&args.interval)
                .map_err(|err| anyhow!("invalid interval {}: {}", args.interval, err))?,
            start: args
                .start
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid start timestamp {}: {}", args.start, err))?,
            end: args
                .end
                .parse::<DateTime<Utc>>()
                .map_err(|err| anyhow!("invalid end timestamp {}: {}", args.end, err))?,
            output: PathBuf::from(&args.dataset),
        })
    }
}

impl BacktestRuntime {
    pub fn new(config: BacktestRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn load_kline_replay(&self) -> Result<KlineReplay> {
        KlineReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            &self.config.interval.to_string(),
            Some(self.config.start),
            Some(self.config.end),
        )
    }

    pub fn load_kline_replay_for_interval(&self, interval: &str) -> Result<KlineReplay> {
        Interval::from_string(interval)
            .map_err(|err| anyhow!("invalid interval {}: {}", interval, err))?;
        KlineReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            interval,
            Some(self.config.start),
            Some(self.config.end),
        )
    }

    fn collect_kline_candles_for_interval(&self, interval: &str) -> Result<Vec<Kline>> {
        Ok(self
            .load_kline_replay_for_interval(interval)?
            .collect_events()
            .into_iter()
            .filter_map(|event| match event {
                BacktestEvent::Kline(kline) => Some(kline.kline),
                _ => None,
            })
            .collect::<Vec<_>>())
    }

    pub fn run_short_ladder_backtest(
        &self,
        mode: ShortLadderMode,
        config: ShortLadderConfig,
    ) -> Result<ShortLadderRunReport> {
        let candles = self.collect_kline_candles_for_interval(&self.config.interval.to_string())?;
        Ok(run_short_ladder_backtest(
            &self.config.symbol,
            &candles,
            mode,
            config,
        ))
    }

    pub fn run_short_ladder_mtf_execution_backtest(
        &self,
        mode: ShortLadderMode,
        config: ShortLadderConfig,
        execution_config: ShortLadderMtfExecutionConfig,
    ) -> Result<ShortLadderMtfExecutionRunReport> {
        let candles = self.collect_kline_candles_for_interval(&self.config.interval.to_string())?;
        Ok(run_short_ladder_mtf_execution_backtest(
            &self.config.symbol,
            &candles,
            mode,
            config,
            execution_config,
        ))
    }

    pub fn load_depth_replay(&self) -> Result<Option<DepthReplay>> {
        let reader = DepthDatasetReader::new(&self.config.output);
        if !reader.has_binance_futures_depth_deltas(&self.config.symbol) {
            return Ok(None);
        }

        Ok(Some(DepthReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            Some(self.config.start),
            Some(self.config.end),
        )?))
    }

    pub fn load_trade_replay(&self) -> Result<Option<TradeReplay>> {
        let reader = TradeDatasetReader::new(&self.config.output);
        if !reader.has_binance_futures_trades(&self.config.symbol) {
            return Ok(None);
        }

        Ok(Some(TradeReplay::load_binance_futures(
            &self.config.output,
            &self.config.symbol,
            Some(self.config.start),
            Some(self.config.end),
        )?))
    }

    pub fn plan_kline_partitions(&self, partitions: usize) -> Result<Vec<ReplayPartition>> {
        let replay = self.load_kline_replay()?;
        Ok(replay.plan_partitions(partitions))
    }

    pub fn run_mean_reversion(
        &self,
        config: MeanReversionConfig,
    ) -> Result<MeanReversionBacktestSummary> {
        let replay = self.load_kline_replay()?;
        let mut strategy = MeanReversionBacktestStrategy::new(config)?;

        for event in replay.collect_events() {
            strategy.on_event(&event)?;
        }

        Ok(strategy.summary())
    }

    pub fn run_mean_reversion_backtest(
        &self,
        config: MeanReversionConfig,
    ) -> Result<MeanReversionBacktestReport> {
        let kline_replay = self.load_kline_replay()?;
        let depth_replay = self.load_depth_replay()?;
        let trade_replay = self.load_trade_replay()?;
        let has_depth_replay = depth_replay.is_some();
        let has_trade_replay = trade_replay.is_some();
        let replay = MergedReplay::from_streams(
            std::iter::once(kline_replay.collect_events())
                .chain(
                    depth_replay
                        .into_iter()
                        .map(|replay| replay.collect_events()),
                )
                .chain(
                    trade_replay
                        .into_iter()
                        .map(|replay| replay.collect_events()),
                )
                .collect(),
        );
        let market_type = market_type_from_string(&self.config.market)?;
        let mut strategy = MeanReversionBacktestStrategy::new(config.clone())?;
        let settlement_currency = settlement_currency_from_config(&config);
        let mut engine =
            BacktestEngineState::new(&settlement_currency, initial_backtest_cash(&config), 32);
        let mut managed_positions = BTreeMap::new();
        let mut pending_entries: BTreeMap<u64, PendingEntryPlan> = BTreeMap::new();
        let mut pending_entry_requests = Vec::new();
        let mut pending_market_exits: BTreeMap<String, PendingMarketExit> = BTreeMap::new();
        let mut fills = Vec::new();
        let mut closed_trades = Vec::new();
        let mut depth_events_processed = 0usize;
        let mut trade_events_processed = 0usize;
        let mut limit_orders_placed = 0usize;
        let mut limit_orders_filled = 0usize;
        let mut limit_orders_cancelled = 0usize;
        let mut venue_stats = VenueExecutionStats::default();

        for event in replay.collect_events() {
            let event_ts = event.logical_ts();
            if matches!(&event, BacktestEvent::DepthDelta(_)) {
                depth_events_processed += 1;
            }
            if matches!(&event, BacktestEvent::Trade(_)) {
                trade_events_processed += 1;
            }
            let event_limit_result = engine.apply_event(&event)?;
            apply_limit_order_result(
                event_limit_result,
                &mut pending_entries,
                &mut managed_positions,
                &mut fills,
                &mut limit_orders_filled,
                &mut limit_orders_cancelled,
                &config,
            );

            activate_pending_entry_requests(
                &event,
                event_ts,
                &config,
                market_type,
                &mut engine,
                &mut pending_entry_requests,
                &mut pending_entries,
                &mut managed_positions,
                &mut fills,
                &mut limit_orders_placed,
                &mut limit_orders_filled,
                &mut limit_orders_cancelled,
                &mut venue_stats,
            )?;

            if let BacktestEvent::Kline(kline_event) = &event {
                if !has_depth_replay && !has_trade_replay {
                    let limit_result = engine.process_limit_orders_on_kline(
                        &kline_event.kline,
                        market_type,
                        config.execution.maker_fee_bps,
                    )?;
                    apply_limit_order_result(
                        limit_result,
                        &mut pending_entries,
                        &mut managed_positions,
                        &mut fills,
                        &mut limit_orders_filled,
                        &mut limit_orders_cancelled,
                        &config,
                    );
                }

                execute_pending_market_exits(
                    &event,
                    event_ts,
                    market_type,
                    config.execution.taker_fee_bps,
                    config.execution.market_slippage_bps,
                    &mut engine,
                    &mut managed_positions,
                    &mut pending_market_exits,
                    &mut fills,
                    &mut closed_trades,
                )?;

                if let Some(position) = managed_positions.get(&kline_event.symbol).cloned() {
                    if pending_market_exits.contains_key(&kline_event.symbol) {
                        engine.ledger_mut().apply_mark_price(
                            &kline_event.symbol,
                            kline_event.kline.close,
                            kline_event.logical_ts,
                        );
                    } else {
                        if let Some(decision) = evaluate_exit_on_kline(
                            &position,
                            &kline_event.kline,
                            config.risk.time_stop_bars,
                        ) {
                            if config.execution.market_latency_ms == 0 {
                                execute_market_exit_now(
                                    &position,
                                    decision.reason,
                                    decision.timestamp,
                                    decision.price,
                                    market_type,
                                    config.execution.taker_fee_bps,
                                    config.execution.market_slippage_bps,
                                    &mut engine,
                                    &mut managed_positions,
                                    &mut fills,
                                    &mut closed_trades,
                                )?;
                            } else {
                                pending_market_exits.insert(
                                    position.symbol.clone(),
                                    PendingMarketExit {
                                        reason: decision.reason,
                                        activate_at: decision.timestamp
                                            + latency_duration_ms(
                                                config.execution.market_latency_ms,
                                            ),
                                    },
                                );
                            }
                        } else {
                            engine.ledger_mut().apply_mark_price(
                                &kline_event.symbol,
                                kline_event.kline.close,
                                kline_event.logical_ts,
                            );
                        }
                    }
                } else {
                    engine.ledger_mut().apply_mark_price(
                        &kline_event.symbol,
                        kline_event.kline.close,
                        kline_event.logical_ts,
                    );
                }
            }

            if !matches!(&event, BacktestEvent::Kline(_)) {
                execute_pending_market_exits(
                    &event,
                    event_ts,
                    market_type,
                    config.execution.taker_fee_bps,
                    config.execution.market_slippage_bps,
                    &mut engine,
                    &mut managed_positions,
                    &mut pending_market_exits,
                    &mut fills,
                    &mut closed_trades,
                )?;
            }

            let signals = strategy.on_event(&event)?;
            for signal in signals {
                if pending_entries
                    .values()
                    .any(|entry| entry.symbol == signal.symbol)
                    || pending_entry_requests
                        .iter()
                        .any(|entry| entry.signal.symbol == signal.symbol)
                    || pending_market_exits.contains_key(&signal.symbol)
                {
                    continue;
                }

                if let Some(position) = managed_positions.get(&signal.symbol).cloned() {
                    if position.side == signal.side {
                        continue;
                    }

                    if config.execution.market_latency_ms == 0 {
                        execute_market_exit_now(
                            &position,
                            "reverse_signal".to_string(),
                            signal.logical_ts,
                            signal.price,
                            market_type,
                            config.execution.taker_fee_bps,
                            config.execution.market_slippage_bps,
                            &mut engine,
                            &mut managed_positions,
                            &mut fills,
                            &mut closed_trades,
                        )?;
                    } else {
                        pending_market_exits.insert(
                            signal.symbol.clone(),
                            PendingMarketExit {
                                reason: "reverse_signal".to_string(),
                                activate_at: signal.logical_ts
                                    + latency_duration_ms(config.execution.market_latency_ms),
                            },
                        );
                    }
                    continue;
                }

                if managed_positions.contains_key(&signal.symbol) {
                    continue;
                }

                if config.execution.submit_latency_ms == 0 {
                    place_entry_order_from_signal(
                        &signal,
                        signal.logical_ts,
                        &event,
                        &config,
                        market_type,
                        &mut engine,
                        &mut pending_entries,
                        &mut managed_positions,
                        &mut fills,
                        &mut limit_orders_placed,
                        &mut limit_orders_filled,
                        &mut limit_orders_cancelled,
                        &mut venue_stats,
                    )?;
                } else {
                    let activate_at =
                        signal.logical_ts + latency_duration_ms(config.execution.submit_latency_ms);
                    pending_entry_requests.push(PendingEntryRequest {
                        signal,
                        activate_at,
                    });
                }
            }
        }

        for order_id in pending_entries.keys().copied().collect::<Vec<_>>() {
            if engine.cancel_limit_order(order_id).is_some() {
                limit_orders_cancelled += 1;
            }
        }

        let signal_summary = strategy.summary();
        let execution_summary = MeanReversionExecutionSummary::from_engine(
            &engine,
            has_depth_replay,
            depth_events_processed,
            has_trade_replay,
            trade_events_processed,
            fills,
            closed_trades,
            limit_orders_placed,
            limit_orders_filled,
            limit_orders_cancelled,
            venue_stats,
        );

        Ok(MeanReversionBacktestReport {
            signals: signal_summary,
            execution: execution_summary,
        })
    }

    pub fn scan_trend(
        &self,
        spec: TrendScanSpec,
        scan_config_path: PathBuf,
    ) -> Result<TrendScanReport> {
        validate_trend_scan_spec(&spec)?;
        let candles = self
            .load_kline_replay()?
            .collect_events()
            .into_iter()
            .filter_map(|event| match event {
                BacktestEvent::Kline(kline) => Some(kline.kline),
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut runs = Vec::new();
        for (index, params) in spec.parameter_sets.iter().enumerate() {
            runs.push(run_trend_parameter_set(
                index + 1,
                &self.config.symbol,
                &candles,
                params,
                &spec,
            )?);
        }

        let best_run = runs
            .iter()
            .map(|run| run.summary.clone())
            .max_by(|lhs, rhs| lhs.final_equity.total_cmp(&rhs.final_equity));

        Ok(TrendScanReport {
            scan_config_path,
            runtime: TrendScanRuntimeSnapshot {
                exchange: self.config.exchange.clone(),
                market: self.config.market.clone(),
                symbol: self.config.symbol.clone(),
                interval: self.config.interval.to_string(),
                start: self.config.start,
                end: self.config.end,
                dataset: self.config.output.clone(),
            },
            total_runs: runs.len(),
            best_run,
            runs,
        })
    }

    pub fn scan_trend_factor(
        &self,
        spec: TrendFactorScanSpec,
        scan_config_path: PathBuf,
    ) -> Result<TrendFactorScanReport> {
        let candles = self
            .load_kline_replay()?
            .collect_events()
            .into_iter()
            .filter_map(|event| match event {
                BacktestEvent::Kline(kline) => Some(kline.kline),
                _ => None,
            })
            .collect::<Vec<_>>();
        let definitions = expand_trend_factor_runs(&spec)?;
        let mut runs = Vec::new();
        for definition in &definitions {
            runs.push(run_trend_factor_definition(
                &self.config.symbol,
                &candles,
                definition,
                &spec,
            )?);
        }
        runs.sort_by(|lhs, rhs| {
            rhs.summary
                .score
                .composite_score
                .total_cmp(&lhs.summary.score.composite_score)
                .then_with(|| {
                    rhs.summary
                        .final_equity
                        .total_cmp(&lhs.summary.final_equity)
                })
        });
        let best_run = runs.first().map(|run| run.summary.clone());
        Ok(TrendFactorScanReport {
            scan_config_path,
            runtime: TrendScanRuntimeSnapshot {
                exchange: self.config.exchange.clone(),
                market: self.config.market.clone(),
                symbol: self.config.symbol.clone(),
                interval: self.config.interval.to_string(),
                start: self.config.start,
                end: self.config.end,
                dataset: self.config.output.clone(),
            },
            total_runs: runs.len(),
            best_run,
            runs,
        })
    }

    pub fn scan_mtf_trend_factor(
        &self,
        spec: MtfTrendFactorScanSpec,
        scan_config_path: PathBuf,
    ) -> Result<MtfTrendFactorScanReport> {
        let trend_candles = self.collect_kline_candles_for_interval(&spec.timeframes.trend)?;
        let structure_candles =
            self.collect_kline_candles_for_interval(&spec.timeframes.structure)?;
        let trigger_candles = self.collect_kline_candles_for_interval(&spec.timeframes.trigger)?;
        let definitions = expand_mtf_trend_factor_runs(&spec)?;
        let mut runs = Vec::new();
        for definition in &definitions {
            runs.push(run_mtf_trend_factor_definition(
                &self.config.symbol,
                &trend_candles,
                &structure_candles,
                &trigger_candles,
                definition,
                &spec,
            )?);
        }
        runs.sort_by(|lhs, rhs| {
            rhs.summary
                .score
                .composite_score
                .total_cmp(&lhs.summary.score.composite_score)
                .then_with(|| {
                    rhs.summary
                        .final_equity
                        .total_cmp(&lhs.summary.final_equity)
                })
        });
        let best_run = runs.first().map(|run| run.summary.clone());
        Ok(MtfTrendFactorScanReport {
            scan_config_path,
            runtime: MtfTrendScanRuntimeSnapshot {
                exchange: self.config.exchange.clone(),
                market: self.config.market.clone(),
                symbol: self.config.symbol.clone(),
                trend_interval: spec.timeframes.trend.clone(),
                structure_interval: spec.timeframes.structure.clone(),
                trigger_interval: spec.timeframes.trigger.clone(),
                start: self.config.start,
                end: self.config.end,
                dataset: self.config.output.clone(),
            },
            total_runs: runs.len(),
            best_run,
            runs,
        })
    }
}

pub async fn execute_cli(cli: BacktestCli) -> Result<BacktestCommandOutput> {
    match cli.command {
        BacktestCommand::FetchKlines(args) => {
            let config = BacktestRuntimeConfig::from_fetch_args(&args)?;
            let dataset = fetch_and_store_klines(&config).await?;

            Ok(BacktestCommandOutput::FetchKlines(BacktestRunManifest {
                dataset,
                config,
            }))
        }
        BacktestCommand::CaptureDepth(args) => {
            let manifest = capture_and_store_depth(args).await?;
            Ok(BacktestCommandOutput::CaptureDepth(manifest))
        }
        BacktestCommand::CaptureTrades(args) => {
            let manifest = capture_and_store_trades(args).await?;
            Ok(BacktestCommandOutput::CaptureTrades(manifest))
        }
        BacktestCommand::RunMeanReversion(args) => {
            let runtime_config = BacktestRuntimeConfig::from_run_mean_reversion_args(&args)?;
            let runtime = BacktestRuntime::new(runtime_config);
            let config_path = PathBuf::from(&args.config);
            let mut strategy_config = load_mean_reversion_config(&config_path)?;
            resolve_cli_execution_constraints(&runtime.config, &mut strategy_config).await?;
            let summary = runtime.run_mean_reversion_backtest(strategy_config)?;
            let summary_path = args.summary_output.as_ref().map(PathBuf::from);

            if let Some(path) = summary_path.as_ref() {
                persist_summary(path, &summary)?;
            }

            Ok(BacktestCommandOutput::RunMeanReversion(
                MeanReversionRunManifest {
                    config_path,
                    summary_path,
                    summary,
                },
            ))
        }
        BacktestCommand::ScanMeanReversion(args) => {
            let manifest = scan_mean_reversion(args).await?;
            Ok(BacktestCommandOutput::ScanMeanReversion(manifest))
        }
        BacktestCommand::WalkForwardMeanReversion(args) => {
            let manifest = walk_forward_mean_reversion(args).await?;
            Ok(BacktestCommandOutput::WalkForwardMeanReversion(manifest))
        }
        BacktestCommand::AnalyzeMeanReversion(args) => {
            let manifest = analyze_mean_reversion(args)?;
            Ok(BacktestCommandOutput::AnalyzeMeanReversion(manifest))
        }
        BacktestCommand::ScanTrend(args) => {
            let runtime_config = BacktestRuntimeConfig::from_scan_trend_args(&args)?;
            let runtime = BacktestRuntime::new(runtime_config);
            let scan_config_path = PathBuf::from(&args.scan_config);
            let report_path = PathBuf::from(&args.report_output);
            let spec = load_trend_scan_spec(&scan_config_path)?;
            let report = runtime.scan_trend(spec, scan_config_path.clone())?;
            persist_json(&report_path, &report)?;

            Ok(BacktestCommandOutput::ScanTrend(TrendScanManifest {
                scan_config_path,
                report_path,
                total_runs: report.total_runs,
                best_run: report.best_run,
            }))
        }
        BacktestCommand::ScanTrendFactor(args) => {
            let runtime_config = BacktestRuntimeConfig::from_scan_trend_factor_args(&args)?;
            let runtime = BacktestRuntime::new(runtime_config);
            let scan_config_path = PathBuf::from(&args.scan_config);
            let report_path = PathBuf::from(&args.report_output);
            let spec = load_trend_factor_scan_spec(&scan_config_path)?;
            let report = runtime.scan_trend_factor(spec, scan_config_path.clone())?;
            persist_json(&report_path, &report)?;

            Ok(BacktestCommandOutput::ScanTrendFactor(
                TrendFactorScanManifest {
                    scan_config_path,
                    report_path,
                    total_runs: report.total_runs,
                    best_run: report.best_run,
                },
            ))
        }
        BacktestCommand::ScanMtfTrendFactor(args) => {
            let scan_config_path = PathBuf::from(&args.scan_config);
            let report_path = PathBuf::from(&args.report_output);
            let spec = load_mtf_trend_factor_scan_spec(&scan_config_path)?;
            let runtime_config = BacktestRuntimeConfig::from_scan_mtf_trend_factor_args(
                &args,
                &spec.timeframes.trigger,
            )?;
            let runtime = BacktestRuntime::new(runtime_config);
            let report = runtime.scan_mtf_trend_factor(spec, scan_config_path.clone())?;
            persist_json(&report_path, &report)?;

            Ok(BacktestCommandOutput::ScanMtfTrendFactor(
                MtfTrendFactorScanManifest {
                    scan_config_path,
                    report_path,
                    total_runs: report.total_runs,
                    best_run: report.best_run,
                },
            ))
        }
        BacktestCommand::RunShortLadder(args) => {
            let runtime_config = BacktestRuntimeConfig::from_run_short_ladder_args(&args)?;
            let runtime = BacktestRuntime::new(runtime_config);
            let config_path = PathBuf::from(&args.config);
            let report_path = PathBuf::from(&args.report_output);
            let config = load_short_ladder_config(&config_path)?;
            let mode = parse_short_ladder_mode(&args.mode)?;
            let report = runtime.run_short_ladder_backtest(mode, config)?;
            persist_json(&report_path, &report)?;

            Ok(BacktestCommandOutput::RunShortLadder(
                ShortLadderRunManifest {
                    config_path,
                    report_path,
                    report,
                },
            ))
        }
        BacktestCommand::RunShortLadderMtfExecution(args) => {
            let runtime_config =
                BacktestRuntimeConfig::from_run_short_ladder_mtf_execution_args(&args)?;
            let runtime = BacktestRuntime::new(runtime_config);
            let config_path = PathBuf::from(&args.config);
            let report_path = PathBuf::from(&args.report_output);
            let config = load_short_ladder_config(&config_path)?;
            let mode = parse_short_ladder_mode(&args.mode)?;
            let execution_config = ShortLadderMtfExecutionConfig {
                signal_interval: args.signal_interval.clone(),
                execution_interval: args.execution_interval.clone(),
                order_cooldown_secs: args.order_cooldown_secs,
                min_minutes_to_l4: args.min_minutes_to_l4,
                max_layers_per_hour: args.max_layers_per_hour,
            };
            let report =
                runtime.run_short_ladder_mtf_execution_backtest(mode, config, execution_config)?;
            persist_json(&report_path, &report)?;

            Ok(BacktestCommandOutput::RunShortLadderMtfExecution(
                ShortLadderMtfExecutionRunManifest {
                    config_path,
                    report_path,
                    report,
                },
            ))
        }
    }
}

async fn resolve_cli_execution_constraints(
    runtime_config: &BacktestRuntimeConfig,
    config: &mut MeanReversionConfig,
) -> Result<()> {
    if !config.execution.auto_resolve_constraints {
        return Ok(());
    }
    if positive_constraint(config.execution.venue.tick_size).is_some()
        && positive_constraint(config.execution.venue.step_size).is_some()
        && positive_constraint(config.execution.venue.min_notional).is_some()
    {
        return Ok(());
    }
    if runtime_config.exchange != "binance" {
        return Ok(());
    }
    if apply_symbol_constraints_from_snapshot(
        &runtime_config.output,
        &runtime_config.exchange,
        &runtime_config.market,
        &runtime_config.symbol,
        &mut config.execution.venue,
    )? {
        return Ok(());
    }

    let market_type = market_type_from_string(&runtime_config.market)?;
    let exchange = build_public_binance_exchange();
    let symbol_info = exchange
        .get_symbol_info(&runtime_config.symbol, market_type)
        .await?;
    ExchangeMetadataWriter::new(&runtime_config.output).write_trading_pair(
        &runtime_config.exchange,
        &runtime_config.market,
        &symbol_info,
    )?;
    apply_symbol_constraints(&mut config.execution.venue, &symbol_info);
    Ok(())
}

fn apply_symbol_constraints_from_snapshot(
    root: impl AsRef<Path>,
    exchange: &str,
    market: &str,
    symbol: &str,
    venue: &mut crate::strategies::mean_reversion::config::ExecutionVenueConstraints,
) -> Result<bool> {
    let reader = ExchangeMetadataReader::new(root);
    if !reader.has_trading_pair(exchange, market, symbol) {
        return Ok(false);
    }

    let symbol_info = reader.read_trading_pair(exchange, market, symbol)?;
    apply_symbol_constraints(venue, &symbol_info);
    Ok(true)
}

fn apply_symbol_constraints(
    venue: &mut crate::strategies::mean_reversion::config::ExecutionVenueConstraints,
    symbol_info: &TradingPair,
) {
    if positive_constraint(venue.tick_size).is_none() && symbol_info.tick_size > f64::EPSILON {
        venue.tick_size = Some(symbol_info.tick_size);
    }
    if positive_constraint(venue.step_size).is_none() && symbol_info.step_size > f64::EPSILON {
        venue.step_size = Some(symbol_info.step_size);
    }
    if positive_constraint(venue.min_notional).is_none() {
        venue.min_notional = positive_constraint(symbol_info.min_notional);
    }
}

fn apply_limit_order_result(
    limit_result: LimitOrderProcessingResult,
    pending_entries: &mut BTreeMap<u64, PendingEntryPlan>,
    managed_positions: &mut BTreeMap<String, ManagedPosition>,
    fills: &mut Vec<ExecutedSignal>,
    limit_orders_filled: &mut usize,
    limit_orders_cancelled: &mut usize,
    config: &MeanReversionConfig,
) {
    *limit_orders_filled += limit_result.filled_order_ids.len();
    *limit_orders_cancelled += limit_result.cancelled_order_ids.len();

    for order_id in &limit_result.cancelled_order_ids {
        pending_entries.remove(order_id);
    }

    for fill_update in limit_result.fills {
        if let Some(plan) = pending_entries.get(&fill_update.order_id).cloned() {
            let position = managed_positions
                .entry(plan.symbol.clone())
                .or_insert_with(|| {
                    ManagedPosition::from_entry_plan(
                        &plan,
                        fill_update.fill.price,
                        fill_update.fill.timestamp,
                        0.0,
                        config,
                    )
                });
            position.apply_entry_fill_result(&plan, &fill_update.fill, config);

            if fill_update.is_terminal {
                pending_entries.remove(&fill_update.order_id);
            }
        }
        fills.push(ExecutedSignal::from_fill_result(&fill_update.fill));
    }
}

fn activate_pending_entry_requests(
    event: &BacktestEvent,
    event_ts: DateTime<Utc>,
    config: &MeanReversionConfig,
    market_type: MarketType,
    engine: &mut BacktestEngineState,
    pending_entry_requests: &mut Vec<PendingEntryRequest>,
    pending_entries: &mut BTreeMap<u64, PendingEntryPlan>,
    managed_positions: &mut BTreeMap<String, ManagedPosition>,
    fills: &mut Vec<ExecutedSignal>,
    limit_orders_placed: &mut usize,
    limit_orders_filled: &mut usize,
    limit_orders_cancelled: &mut usize,
    venue_stats: &mut VenueExecutionStats,
) -> Result<()> {
    let ready_requests = pending_entry_requests
        .iter()
        .filter(|entry| entry.activate_at <= event_ts)
        .cloned()
        .collect::<Vec<_>>();
    pending_entry_requests.retain(|entry| entry.activate_at > event_ts);

    for request in ready_requests {
        if pending_entries
            .values()
            .any(|entry| entry.symbol == request.signal.symbol)
            || managed_positions.contains_key(&request.signal.symbol)
        {
            continue;
        }

        place_entry_order_from_signal(
            &request.signal,
            request.activate_at,
            event,
            config,
            market_type,
            engine,
            pending_entries,
            managed_positions,
            fills,
            limit_orders_placed,
            limit_orders_filled,
            limit_orders_cancelled,
            venue_stats,
        )?;
    }

    Ok(())
}

fn place_entry_order_from_signal(
    signal: &StrategySignal,
    activate_at: DateTime<Utc>,
    event: &BacktestEvent,
    config: &MeanReversionConfig,
    market_type: MarketType,
    engine: &mut BacktestEngineState,
    pending_entries: &mut BTreeMap<u64, PendingEntryPlan>,
    managed_positions: &mut BTreeMap<String, ManagedPosition>,
    fills: &mut Vec<ExecutedSignal>,
    limit_orders_placed: &mut usize,
    limit_orders_filled: &mut usize,
    limit_orders_cancelled: &mut usize,
    venue_stats: &mut VenueExecutionStats,
) -> Result<()> {
    let prepared = match prepare_entry_order(signal, config) {
        Ok(order) => order,
        Err(reason) => {
            record_rejection(venue_stats, reason);
            return Ok(());
        }
    };

    let expires_at = activate_at
        + Duration::seconds(config.execution.ttl_secs as i64)
        + latency_duration_ms(config.execution.cancel_latency_ms);
    let order_id = match engine.place_limit_order(
        &signal.symbol,
        signal.side,
        prepared.price,
        prepared.quantity,
        market_type,
        activate_at,
        expires_at,
    ) {
        Ok(order_id) => order_id,
        Err(err) => {
            let reason = normalize_order_error_reason(&err);
            record_rejection(venue_stats, reason);
            return Ok(());
        }
    };

    if prepared.adjusted {
        venue_stats.adjusted_orders += 1;
    }

    pending_entries.insert(
        order_id,
        PendingEntryPlan::from_signal(signal, order_id, prepared.price),
    );
    *limit_orders_placed += 1;

    if matches!(event, BacktestEvent::DepthDelta(_)) {
        let limit_result = engine.match_limit_orders_at_book(&signal.symbol, event.logical_ts())?;
        apply_limit_order_result(
            limit_result,
            pending_entries,
            managed_positions,
            fills,
            limit_orders_filled,
            limit_orders_cancelled,
            config,
        );
    }

    Ok(())
}

fn execute_pending_market_exits(
    event: &BacktestEvent,
    event_ts: DateTime<Utc>,
    market_type: MarketType,
    taker_fee_bps: f64,
    market_slippage_bps: f64,
    engine: &mut BacktestEngineState,
    managed_positions: &mut BTreeMap<String, ManagedPosition>,
    pending_market_exits: &mut BTreeMap<String, PendingMarketExit>,
    fills: &mut Vec<ExecutedSignal>,
    closed_trades: &mut Vec<ClosedTrade>,
) -> Result<()> {
    let due_symbols = pending_market_exits
        .iter()
        .filter(|(_, exit)| exit.activate_at <= event_ts)
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();

    for symbol in due_symbols {
        let Some(exit) = pending_market_exits.get(&symbol).cloned() else {
            continue;
        };
        let Some(position) = managed_positions.get(&symbol).cloned() else {
            pending_market_exits.remove(&symbol);
            continue;
        };
        let Some(price) = market_exit_price(event, engine, &symbol, opposite_side(position.side))
        else {
            continue;
        };

        execute_market_exit_now(
            &position,
            exit.reason,
            event_ts.max(exit.activate_at),
            price,
            market_type,
            taker_fee_bps,
            market_slippage_bps,
            engine,
            managed_positions,
            fills,
            closed_trades,
        )?;
        pending_market_exits.remove(&symbol);
    }

    Ok(())
}

fn execute_market_exit_now(
    position: &ManagedPosition,
    reason: String,
    timestamp: DateTime<Utc>,
    price: f64,
    market_type: MarketType,
    taker_fee_bps: f64,
    market_slippage_bps: f64,
    engine: &mut BacktestEngineState,
    managed_positions: &mut BTreeMap<String, ManagedPosition>,
    fills: &mut Vec<ExecutedSignal>,
    closed_trades: &mut Vec<ClosedTrade>,
) -> Result<()> {
    let exit_fill = engine.execute_market_order(
        &position.symbol,
        opposite_side(position.side),
        timestamp,
        price,
        position.quantity,
        market_type,
        taker_fee_bps,
        market_slippage_bps,
    )?;
    closed_trades.push(ClosedTrade::from_position(
        position,
        vec![TradeExecutionLeg::from_fill_result(&exit_fill)],
        reason,
    ));
    fills.push(ExecutedSignal::from_fill_result(&exit_fill));
    managed_positions.remove(&position.symbol);
    Ok(())
}

fn prepare_entry_order(
    signal: &StrategySignal,
    config: &MeanReversionConfig,
) -> std::result::Result<PreparedEntryOrder, &'static str> {
    let mut price = limit_entry_price(signal, config);
    if price <= f64::EPSILON {
        return Err("invalid_price");
    }

    let mut adjusted = false;
    if let Some(tick_size) = positive_constraint(config.execution.venue.tick_size) {
        let rounded_price = round_limit_price(price, tick_size, signal.side);
        adjusted |= !approx_equal(price, rounded_price);
        price = rounded_price;
    }
    if price <= f64::EPSILON {
        return Err("invalid_price");
    }

    let mut quantity = config.risk.per_trade_notional / price;
    if !quantity.is_finite() || quantity <= f64::EPSILON {
        return Err("invalid_quantity");
    }

    if let Some(step_size) = positive_constraint(config.execution.venue.step_size) {
        let rounded_quantity = round_down_to_step(quantity, step_size);
        adjusted |= !approx_equal(quantity, rounded_quantity);
        quantity = rounded_quantity;
    }
    if quantity <= f64::EPSILON {
        return Err("below_step_size");
    }

    let notional = price * quantity;
    if let Some(min_notional) = positive_constraint(config.execution.venue.min_notional) {
        if notional + 1e-9 < min_notional {
            return Err("below_min_notional");
        }
    }

    Ok(PreparedEntryOrder {
        price,
        quantity,
        adjusted,
    })
}

fn market_exit_price(
    event: &BacktestEvent,
    engine: &BacktestEngineState,
    symbol: &str,
    side: OrderSide,
) -> Option<f64> {
    match event {
        BacktestEvent::DepthDelta(depth) if depth.symbol == symbol => engine
            .order_book(symbol)
            .and_then(|book| match side {
                OrderSide::Buy => book.best_ask().map(|level| level[0]),
                OrderSide::Sell => book.best_bid().map(|level| level[0]),
            })
            .or_else(|| {
                engine
                    .ledger()
                    .position(symbol)
                    .map(|position| position.mark_price)
            }),
        BacktestEvent::BookTicker(book) if book.symbol == symbol => Some(match side {
            OrderSide::Buy => book.best_ask,
            OrderSide::Sell => book.best_bid,
        }),
        BacktestEvent::MarkPrice(mark) if mark.symbol == symbol => Some(mark.mark_price),
        BacktestEvent::FundingRate(funding) if funding.symbol == symbol => {
            funding.mark_price.or_else(|| {
                engine
                    .ledger()
                    .position(symbol)
                    .map(|position| position.mark_price)
            })
        }
        BacktestEvent::Trade(trade) if trade.symbol == symbol => Some(trade.trade.price),
        BacktestEvent::Kline(kline) if kline.symbol == symbol => Some(kline.kline.close),
        _ => engine
            .order_book(symbol)
            .and_then(|book| match side {
                OrderSide::Buy => book.best_ask().map(|level| level[0]),
                OrderSide::Sell => book.best_bid().map(|level| level[0]),
            })
            .or_else(|| {
                engine
                    .ledger()
                    .position(symbol)
                    .map(|position| position.mark_price)
            }),
    }
    .filter(|price| price.is_finite() && *price > f64::EPSILON)
}

fn positive_constraint(value: Option<f64>) -> Option<f64> {
    value.filter(|constraint| constraint.is_finite() && *constraint > f64::EPSILON)
}

fn round_limit_price(price: f64, tick_size: f64, side: OrderSide) -> f64 {
    let ticks = price / tick_size;
    match side {
        OrderSide::Buy => ticks.floor() * tick_size,
        OrderSide::Sell => ticks.ceil() * tick_size,
    }
}

fn round_down_to_step(quantity: f64, step_size: f64) -> f64 {
    (quantity / step_size).floor() * step_size
}

fn approx_equal(lhs: f64, rhs: f64) -> bool {
    (lhs - rhs).abs() <= 1e-9
}

fn record_rejection(stats: &mut VenueExecutionStats, reason: &'static str) {
    stats.rejected_orders += 1;
    *stats
        .rejection_reasons
        .entry(reason.to_string())
        .or_insert(0) += 1;
}

fn normalize_order_error_reason(err: &anyhow::Error) -> &'static str {
    let message = err.to_string();
    if message.contains("post-only limit order would cross") {
        "post_only_cross"
    } else if message.contains("limit price must be positive") {
        "invalid_price"
    } else if message.contains("limit quantity must be positive") {
        "invalid_quantity"
    } else if message.contains("expiry") {
        "invalid_expiry"
    } else {
        "order_rejected"
    }
}

fn latency_duration_ms(latency_ms: u64) -> Duration {
    Duration::milliseconds(latency_ms.min(i64::MAX as u64) as i64)
}

async fn fetch_and_store_klines(config: &BacktestRuntimeConfig) -> Result<KlineDatasetOutput> {
    if config.exchange != "binance" {
        return Err(anyhow!("unsupported exchange {}", config.exchange));
    }
    if config.market != "futures" {
        return Err(anyhow!("unsupported market {}", config.market));
    }

    let exchange = build_public_binance_exchange();
    let request_limit = 1500u32;
    let interval_duration = interval_duration(config.interval);
    let mut cursor = config.start;
    let mut klines = Vec::new();

    while cursor < config.end {
        let mut batch = exchange
            .get_klines_window(
                &config.symbol,
                config.interval,
                crate::core::types::MarketType::Futures,
                Some(request_limit),
                Some(cursor),
                Some(config.end),
            )
            .await?;
        batch.retain(|kline| kline.open_time >= cursor && kline.open_time < config.end);
        if batch.is_empty() {
            break;
        }

        let next_cursor = batch
            .last()
            .map(|kline| kline.open_time + interval_duration)
            .unwrap_or(cursor + interval_duration);
        klines.extend(batch);
        if next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
    }

    klines.sort_by_key(|kline| kline.open_time);
    klines.dedup_by_key(|kline| kline.open_time);

    let writer = KlineDatasetWriter::new(&config.output);
    writer.write_binance_futures_klines(&config.symbol, &config.interval.to_string(), &klines)
}

fn interval_duration(interval: Interval) -> Duration {
    match interval {
        Interval::OneMinute => Duration::minutes(1),
        Interval::ThreeMinutes => Duration::minutes(3),
        Interval::FiveMinutes => Duration::minutes(5),
        Interval::FifteenMinutes => Duration::minutes(15),
        Interval::ThirtyMinutes => Duration::minutes(30),
        Interval::OneHour => Duration::hours(1),
        Interval::TwoHours => Duration::hours(2),
        Interval::FourHours => Duration::hours(4),
        Interval::SixHours => Duration::hours(6),
        Interval::EightHours => Duration::hours(8),
        Interval::TwelveHours => Duration::hours(12),
        Interval::OneDay => Duration::days(1),
        Interval::ThreeDays => Duration::days(3),
        Interval::OneWeek => Duration::weeks(1),
        Interval::OneMonth => Duration::days(30),
    }
}

async fn capture_and_store_depth(args: CaptureDepthArgs) -> Result<DepthCaptureRunManifest> {
    if args.exchange != "binance" {
        return Err(anyhow!("unsupported exchange {}", args.exchange));
    }
    if args.market != "futures" {
        return Err(anyhow!("unsupported market {}", args.market));
    }
    if args.duration_secs == 0 {
        return Err(anyhow!("duration_secs must be positive"));
    }

    let symbol = normalize_symbol_input(&args.symbol)?;
    let exchange_symbol = to_binance_futures_symbol(&symbol)?;
    let stream = format!(
        "{}@depth@{}ms",
        exchange_symbol.to_lowercase(),
        args.cadence_ms
    );
    let ws_url = format!("wss://fstream.binance.com/ws/{}", stream);

    let (ws_stream, _) = connect_async(&ws_url).await?;
    let (_write, mut read) = ws_stream.split();

    let exchange = build_public_binance_exchange();
    let snapshot_book = exchange
        .get_orderbook(&symbol, MarketType::Futures, Some(args.levels))
        .await?;
    let snapshot_update_id = snapshot_book
        .info
        .get("lastUpdateId")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| anyhow!("missing lastUpdateId in Binance snapshot"))?
        as u64;

    let snapshot = RawDepthSnapshot {
        last_update_id: snapshot_update_id,
        timestamp: snapshot_book.timestamp,
        bids: snapshot_book.bids,
        asks: snapshot_book.asks,
    };

    let deadline =
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(args.duration_secs);
    let mut deltas = Vec::new();
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.duration_since(now);
        let message = match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(error))) => {
                return Err(anyhow!("depth websocket receive failed: {}", error))
            }
            Ok(None) => break,
            Err(_) => break,
        };

        match message {
            Message::Text(text) => {
                if let Some(delta) = parse_binance_depth_delta_message(&text, &symbol)? {
                    deltas.push(delta);
                }
            }
            Message::Binary(binary) => {
                let text = String::from_utf8_lossy(&binary).to_string();
                if let Some(delta) = parse_binance_depth_delta_message(&text, &symbol)? {
                    deltas.push(delta);
                }
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => break,
            _ => {}
        }
    }

    let session = DepthCaptureSession {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: symbol.clone(),
        stream,
        snapshot,
        deltas,
    };
    let output_root = PathBuf::from(&args.output);
    let raw_capture_path =
        DepthCaptureWriter::new(&output_root).write_binance_futures_capture(&symbol, &session)?;
    let normalized = import_binance_futures_depth_capture(&output_root, &symbol)?;

    Ok(DepthCaptureRunManifest {
        raw_capture_path,
        normalized_data_path: normalized.data_path,
        normalized_manifest_path: normalized.manifest_path,
        snapshot_update_id,
        raw_delta_count: session.deltas.len(),
    })
}

async fn capture_and_store_trades(args: CaptureTradesArgs) -> Result<TradeCaptureRunManifest> {
    if args.exchange != "binance" {
        return Err(anyhow!("unsupported exchange {}", args.exchange));
    }
    if args.market != "futures" {
        return Err(anyhow!("unsupported market {}", args.market));
    }
    if args.duration_secs == 0 {
        return Err(anyhow!("duration_secs must be positive"));
    }

    let symbol = normalize_symbol_input(&args.symbol)?;
    let exchange_symbol = to_binance_futures_symbol(&symbol)?;
    let stream = format!("{}@trade", exchange_symbol.to_lowercase());
    let ws_url = format!("wss://fstream.binance.com/ws/{}", stream);

    let (ws_stream, _) = connect_async(&ws_url).await?;
    let (_write, mut read) = ws_stream.split();

    let deadline =
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(args.duration_secs);
    let mut trades = Vec::new();
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.duration_since(now);
        let message = match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(error))) => {
                return Err(anyhow!("trade websocket receive failed: {}", error))
            }
            Ok(None) => break,
            Err(_) => break,
        };

        match message {
            Message::Text(text) => {
                if let Some(trade) = parse_binance_trade_message(&text, &symbol)? {
                    trades.push(trade);
                }
            }
            Message::Binary(binary) => {
                let text = String::from_utf8_lossy(&binary).to_string();
                if let Some(trade) = parse_binance_trade_message(&text, &symbol)? {
                    trades.push(trade);
                }
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => break,
            _ => {}
        }
    }

    let session = TradeCaptureSession {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: symbol.clone(),
        stream,
        trades,
    };
    let output_root = PathBuf::from(&args.output);
    let raw_capture_path =
        TradeCaptureWriter::new(&output_root).write_binance_futures_capture(&symbol, &session)?;
    let normalized = import_binance_futures_trade_capture(&output_root, &symbol)?;

    Ok(TradeCaptureRunManifest {
        raw_capture_path,
        normalized_data_path: normalized.data_path,
        normalized_manifest_path: normalized.manifest_path,
        raw_trade_count: session.trades.len(),
    })
}

fn build_public_binance_exchange() -> BinanceExchange {
    let config = Config {
        name: "binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443".to_string(),
        ws_futures_url: "wss://fstream.binance.com".to_string(),
    };
    let api_keys = ApiKeys {
        api_key: String::new(),
        api_secret: String::new(),
        passphrase: None,
        memo: None,
    };

    BinanceExchange::new(config, api_keys)
}

fn to_binance_futures_symbol(symbol: &str) -> Result<String> {
    let config = Config {
        name: "binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443".to_string(),
        ws_futures_url: "wss://fstream.binance.com".to_string(),
    };
    let converter = SymbolConverter::new(config);
    Ok(converter.to_exchange_symbol(symbol, "binance", MarketType::Futures)?)
}

fn parse_binance_depth_delta_message(
    text: &str,
    normalized_symbol: &str,
) -> Result<Option<RawDepthDeltaRecord>> {
    #[derive(Debug, Deserialize)]
    struct BinanceDepthDeltaMessage {
        #[serde(rename = "e")]
        event_type: String,
        #[serde(rename = "E")]
        event_time: i64,
        #[serde(rename = "T")]
        transaction_time: Option<i64>,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "U")]
        first_update_id: u64,
        #[serde(rename = "u")]
        final_update_id: u64,
        #[serde(rename = "pu")]
        previous_final_update_id: Option<u64>,
        #[serde(rename = "b")]
        bids: Vec<[String; 2]>,
        #[serde(rename = "a")]
        asks: Vec<[String; 2]>,
    }

    let message: BinanceDepthDeltaMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };
    if message.event_type != "depthUpdate" {
        return Ok(None);
    }
    if normalize_symbol_input(&message.symbol)? != normalized_symbol {
        return Ok(None);
    }

    Ok(Some(RawDepthDeltaRecord {
        exchange_ts: DateTime::from_timestamp_millis(message.event_time).unwrap_or_else(Utc::now),
        logical_ts: DateTime::from_timestamp_millis(
            message.transaction_time.unwrap_or(message.event_time),
        )
        .unwrap_or_else(Utc::now),
        first_update_id: message.first_update_id,
        final_update_id: message.final_update_id,
        previous_final_update_id: message.previous_final_update_id,
        bids: parse_depth_levels(&message.bids),
        asks: parse_depth_levels(&message.asks),
    }))
}

fn parse_binance_trade_message(
    text: &str,
    normalized_symbol: &str,
) -> Result<Option<RawTradeRecord>> {
    #[derive(Debug, Deserialize)]
    struct BinanceTradeMessage {
        #[serde(rename = "e")]
        event_type: String,
        #[serde(rename = "E")]
        event_time: Option<i64>,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "t")]
        trade_id: u64,
        #[serde(rename = "p")]
        price: String,
        #[serde(rename = "q")]
        quantity: String,
        #[serde(rename = "T")]
        trade_time: i64,
        #[serde(rename = "m")]
        is_buyer_maker: bool,
    }

    let message: BinanceTradeMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };
    if message.event_type != "trade" {
        return Ok(None);
    }
    if normalize_symbol_input(&message.symbol)? != normalized_symbol {
        return Ok(None);
    }

    Ok(Some(RawTradeRecord {
        exchange_ts: DateTime::from_timestamp_millis(
            message.event_time.unwrap_or(message.trade_time),
        )
        .unwrap_or_else(Utc::now),
        logical_ts: DateTime::from_timestamp_millis(message.trade_time).unwrap_or_else(Utc::now),
        trade_id: message.trade_id.to_string(),
        side: if message.is_buyer_maker {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        price: message.price.parse().unwrap_or(0.0),
        quantity: message.quantity.parse().unwrap_or(0.0),
    }))
}

fn parse_depth_levels(levels: &[[String; 2]]) -> Vec<[f64; 2]> {
    levels
        .iter()
        .filter_map(|level| {
            let price = level[0].parse::<f64>().ok()?;
            let quantity = level[1].parse::<f64>().ok()?;
            Some([price, quantity])
        })
        .collect()
}

fn load_mean_reversion_config(path: impl AsRef<Path>) -> Result<MeanReversionConfig> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_yaml::from_str(&content)?)
}

fn load_trend_scan_spec(path: impl AsRef<Path>) -> Result<TrendScanSpec> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_yaml::from_str(&content)?)
}

fn load_trend_factor_scan_spec(path: impl AsRef<Path>) -> Result<TrendFactorScanSpec> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_yaml::from_str(&content)?)
}

fn load_mtf_trend_factor_scan_spec(path: impl AsRef<Path>) -> Result<MtfTrendFactorScanSpec> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_yaml::from_str(&content)?)
}

fn load_short_ladder_config(path: impl AsRef<Path>) -> Result<ShortLadderConfig> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_yaml::from_str(&content)?)
}

fn parse_short_ladder_mode(value: &str) -> Result<ShortLadderMode> {
    match value {
        "adverse_averaging" | "a" | "A" => Ok(ShortLadderMode::AdverseAveraging),
        "favorable_pyramiding" | "b" | "B" => Ok(ShortLadderMode::FavorablePyramiding),
        other => Err(anyhow!("unsupported short ladder mode {}", other)),
    }
}

fn persist_summary(path: impl AsRef<Path>, summary: &MeanReversionBacktestReport) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(summary)?)?;
    Ok(())
}

async fn scan_mean_reversion(args: ScanMeanReversionArgs) -> Result<MeanReversionScanManifest> {
    let runtime_config = BacktestRuntimeConfig::from_scan_mean_reversion_args(&args)?;
    let runtime = BacktestRuntime::new(runtime_config);
    let config_path = PathBuf::from(&args.config);
    let scan_config_path = PathBuf::from(&args.scan_config);
    let report_path = PathBuf::from(&args.report_output);

    let mut base_config = load_mean_reversion_config(&config_path)?;
    resolve_cli_execution_constraints(&runtime.config, &mut base_config).await?;
    let scan_spec = load_mean_reversion_scan_spec(&scan_config_path)?;
    let runs = expand_scan_runs(&scan_spec)?;
    let requested_workers = args.max_workers.or(scan_spec.max_workers);
    let workers_used = resolve_scan_worker_count(requested_workers, runs.len());
    let mut summaries = execute_scan_runs(&runtime, &base_config, &runs, workers_used)?;
    sort_scan_runs(&mut summaries, scan_spec.sort_by.clone());

    let report = MeanReversionScanReport {
        config_path: config_path.clone(),
        scan_config_path: scan_config_path.clone(),
        sort_by: scan_spec.sort_by,
        total_runs: runs.len(),
        completed_runs: summaries.len(),
        workers_used,
        best_run: summaries.first().cloned(),
        runs: summaries,
    };
    persist_json(&report_path, &report)?;

    Ok(MeanReversionScanManifest {
        config_path,
        scan_config_path,
        report_path,
        total_runs: report.total_runs,
        workers_used: report.workers_used,
        best_run: report.best_run,
    })
}

async fn walk_forward_mean_reversion(
    args: WalkForwardMeanReversionArgs,
) -> Result<MeanReversionWalkForwardManifest> {
    let runtime_config = BacktestRuntimeConfig::from_walk_forward_mean_reversion_args(&args)?;
    let runtime = BacktestRuntime::new(runtime_config);
    let config_path = PathBuf::from(&args.config);
    let walk_config_path = PathBuf::from(&args.walk_config);
    let report_path = PathBuf::from(&args.report_output);

    let mut base_config = load_mean_reversion_config(&config_path)?;
    resolve_cli_execution_constraints(&runtime.config, &mut base_config).await?;
    let walk_spec = load_mean_reversion_walk_forward_spec(&walk_config_path)?;
    let runs = expand_scan_runs_from_ranges(&walk_spec.parameters)?;
    let requested_workers = args.max_workers.or(walk_spec.max_workers);
    let workers_used = resolve_scan_worker_count(requested_workers, runs.len());
    let windows = plan_walk_forward_windows(&runtime, &walk_spec)?;

    let mut window_reports = Vec::with_capacity(windows.len());
    let mut selected_validation_runs = Vec::new();

    for window in &windows {
        let train_runtime = BacktestRuntime::new(build_window_runtime_config(
            &runtime.config,
            window.train_start,
            window.train_end,
        ));
        let validation_runtime = BacktestRuntime::new(build_window_runtime_config(
            &runtime.config,
            window.validation_start,
            window.validation_end,
        ));

        let mut train_summaries =
            execute_scan_runs(&train_runtime, &base_config, &runs, workers_used)?;
        sort_scan_runs(&mut train_summaries, walk_spec.sort_by.clone());

        let selected_train_runs = train_summaries
            .iter()
            .take(walk_spec.top_n.min(train_summaries.len()))
            .cloned()
            .collect::<Vec<_>>();

        let selected_run_ids = selected_train_runs
            .iter()
            .map(|summary| summary.run_id)
            .collect::<Vec<_>>();

        let selected_run_definitions = runs
            .iter()
            .filter(|run| selected_run_ids.contains(&run.run_id))
            .cloned()
            .collect::<Vec<_>>();

        let mut validation_summaries = execute_scan_runs(
            &validation_runtime,
            &base_config,
            &selected_run_definitions,
            resolve_scan_worker_count(Some(workers_used), selected_run_definitions.len()),
        )?;
        sort_scan_runs(&mut validation_summaries, walk_spec.sort_by.clone());

        let mut validation_by_run_id = BTreeMap::new();
        for summary in &validation_summaries {
            validation_by_run_id.insert(summary.run_id, summary.clone());
        }

        let mut candidate_summaries = Vec::new();
        for train_summary in selected_train_runs {
            if let Some(validation_summary) = validation_by_run_id.get(&train_summary.run_id) {
                let candidate = MeanReversionWalkForwardCandidateSummary {
                    run_id: train_summary.run_id,
                    parameters: train_summary.parameters.clone(),
                    train: train_summary.clone(),
                    validation: validation_summary.clone(),
                };
                selected_validation_runs.push(candidate.clone());
                candidate_summaries.push(candidate);
            }
        }

        window_reports.push(MeanReversionWalkForwardWindowReport {
            window_id: window.window_id,
            train_start: window.train_start,
            train_end: window.train_end,
            validation_start: window.validation_start,
            validation_end: window.validation_end,
            training_runs: train_summaries.len(),
            validation_runs: candidate_summaries,
            best_train_run: train_summaries.first().cloned(),
            best_validation_run: validation_summaries.first().cloned(),
        });
    }

    let mut parameter_sets =
        aggregate_walk_forward_parameter_sets(&selected_validation_runs, walk_spec.sort_by.clone());
    let best_parameter_set = parameter_sets.first().cloned();
    let report = MeanReversionWalkForwardReport {
        config_path: config_path.clone(),
        walk_config_path: walk_config_path.clone(),
        sort_by: walk_spec.sort_by,
        train_bars: walk_spec.train_bars,
        validation_bars: walk_spec.validation_bars,
        step_bars: walk_spec.step_bars,
        top_n: walk_spec.top_n,
        total_parameter_sets: runs.len(),
        window_count: window_reports.len(),
        workers_used,
        best_parameter_set: best_parameter_set.clone(),
        parameter_sets: std::mem::take(&mut parameter_sets),
        windows: window_reports,
    };
    persist_json(&report_path, &report)?;

    Ok(MeanReversionWalkForwardManifest {
        config_path,
        walk_config_path,
        report_path,
        total_parameter_sets: report.total_parameter_sets,
        window_count: report.window_count,
        workers_used: report.workers_used,
        best_parameter_set,
    })
}

fn analyze_mean_reversion(args: AnalyzeMeanReversionArgs) -> Result<MeanReversionAnalysisManifest> {
    let scan_report_path = PathBuf::from(&args.scan_report);
    let walk_forward_report_path = args.walk_forward_report.as_ref().map(PathBuf::from);
    let report_path = PathBuf::from(&args.analysis_output);

    let scan_report = load_scan_report(&scan_report_path)?;
    let walk_forward_report = match walk_forward_report_path.as_ref() {
        Some(path) => Some(load_walk_forward_report(path)?),
        None => None,
    };

    let top_runs = select_top_scan_runs(&scan_report, args.top_runs);
    let parameter_insights = build_parameter_insights(&scan_report);
    let scan_summary = build_scan_analysis_summary(&scan_report, &top_runs);
    let walk_forward_summary = walk_forward_report
        .as_ref()
        .map(|report| build_walk_forward_analysis_summary(&scan_report, report));
    let report = MeanReversionAnalysisReport {
        scan_report_path: scan_report_path.clone(),
        walk_forward_report_path: walk_forward_report_path.clone(),
        top_runs_requested: args.top_runs,
        top_runs: top_runs.clone(),
        scan_summary: scan_summary.clone(),
        parameter_insights,
        walk_forward_summary: walk_forward_summary.clone(),
    };
    persist_json(&report_path, &report)?;

    Ok(MeanReversionAnalysisManifest {
        scan_report_path,
        walk_forward_report_path,
        report_path,
        top_runs_count: top_runs.len(),
        best_run: scan_summary.best_run,
        walk_forward_summary_present: walk_forward_summary.is_some(),
        scan_best_matches_walk_forward_best: walk_forward_summary
            .as_ref()
            .is_some_and(|summary| summary.scan_best_matches_walk_forward_best),
    })
}

fn load_mean_reversion_scan_spec(path: impl AsRef<Path>) -> Result<MeanReversionScanSpec> {
    let content = fs::read_to_string(path.as_ref())?;
    let spec: MeanReversionScanSpec = serde_yaml::from_str(&content)?;
    validate_scan_parameter_ranges(&spec.parameters)?;
    Ok(spec)
}

fn load_scan_report(path: impl AsRef<Path>) -> Result<MeanReversionScanReport> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_json::from_str(&content)?)
}

fn load_walk_forward_report(path: impl AsRef<Path>) -> Result<MeanReversionWalkForwardReport> {
    let content = fs::read_to_string(path.as_ref())?;
    Ok(serde_json::from_str(&content)?)
}

fn load_mean_reversion_walk_forward_spec(
    path: impl AsRef<Path>,
) -> Result<MeanReversionWalkForwardSpec> {
    let content = fs::read_to_string(path.as_ref())?;
    let spec: MeanReversionWalkForwardSpec = serde_yaml::from_str(&content)?;
    validate_scan_parameter_ranges(&spec.parameters)?;
    if spec.train_bars == 0 {
        return Err(anyhow!(
            "walk-forward spec train_bars must be greater than 0"
        ));
    }
    if spec.validation_bars == 0 {
        return Err(anyhow!(
            "walk-forward spec validation_bars must be greater than 0"
        ));
    }
    if spec.step_bars == 0 {
        return Err(anyhow!(
            "walk-forward spec step_bars must be greater than 0"
        ));
    }
    if spec.top_n == 0 {
        return Err(anyhow!("walk-forward spec top_n must be greater than 0"));
    }
    Ok(spec)
}

fn validate_scan_parameter_ranges(parameters: &[MeanReversionScanParameterRange]) -> Result<()> {
    if parameters.is_empty() {
        return Err(anyhow!("scan spec must contain at least one parameter"));
    }
    for parameter in parameters {
        if parameter.values.is_empty() {
            return Err(anyhow!(
                "scan parameter {} must contain at least one value",
                parameter.path.as_str()
            ));
        }
    }
    Ok(())
}

fn persist_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn default_trend_initial_equity() -> f64 {
    10_000.0
}

fn default_trend_trade_notional() -> f64 {
    1_000.0
}

fn default_trend_fee_rate() -> f64 {
    0.001
}

fn default_trend_slippage_rate() -> f64 {
    0.0002
}

fn default_trend_adx_period() -> usize {
    14
}

fn default_trend_rsi_period() -> usize {
    14
}

fn default_trend_rsi_overbought() -> f64 {
    70.0
}

fn default_trend_rsi_oversold() -> f64 {
    30.0
}

fn default_trend_atr_period() -> usize {
    14
}

fn validate_trend_scan_spec(spec: &TrendScanSpec) -> Result<()> {
    if spec.initial_equity <= 0.0 || !spec.initial_equity.is_finite() {
        return Err(anyhow!("initial_equity must be positive"));
    }
    if spec.trade_notional <= 0.0 || !spec.trade_notional.is_finite() {
        return Err(anyhow!("trade_notional must be positive"));
    }
    if spec.parameter_sets.is_empty() {
        return Err(anyhow!("trend scan requires at least one parameter set"));
    }
    for params in &spec.parameter_sets {
        if params.name.trim().is_empty() {
            return Err(anyhow!("trend parameter set name must not be empty"));
        }
        if params.fast_ema == 0 || params.slow_ema == 0 || params.fast_ema >= params.slow_ema {
            return Err(anyhow!(
                "trend parameter set {} requires 0 < fast_ema < slow_ema",
                params.name
            ));
        }
        if params.breakout_lookback == 0 || params.max_hold_bars == 0 {
            return Err(anyhow!(
                "trend parameter set {} requires positive breakout_lookback and max_hold_bars",
                params.name
            ));
        }
    }
    Ok(())
}

fn run_trend_parameter_set(
    run_id: usize,
    symbol: &str,
    candles: &[Kline],
    params: &TrendParameterSet,
    spec: &TrendScanSpec,
) -> Result<TrendScanRunReport> {
    if candles.is_empty() {
        return Err(anyhow!("trend scan dataset is empty"));
    }

    let closes = candles.iter().map(|kline| kline.close).collect::<Vec<_>>();
    let fast = ema_series(&closes, params.fast_ema);
    let slow = ema_series(&closes, params.slow_ema);
    let rsi = rsi_series(&closes, params.rsi_period);
    let (atr, adx) = atr_adx_series(candles, params.adx_period);
    let macd = macd_histogram_series(&closes, 12, 26, 9);

    let mut cash = spec.initial_equity;
    let mut position: Option<TrendPosition> = None;
    let mut trades = Vec::new();
    let mut equity_curve = vec![spec.initial_equity];
    let mut wins = 0usize;
    let mut gross_profit = 0.0;
    let mut gross_loss = 0.0;
    let mut exposure_bars = 0usize;
    let warmup = params
        .slow_ema
        .max(params.atr_period * 2)
        .max(params.breakout_lookback)
        .max(40)
        + 2;

    for index in warmup..candles.len() {
        let candle = &candles[index];
        let Some(fast_now) = fast[index] else {
            continue;
        };
        let Some(slow_now) = slow[index] else {
            continue;
        };
        let Some(rsi_now) = rsi[index] else { continue };
        let Some(atr_now) = atr[index] else { continue };
        let Some(adx_now) = adx[index] else { continue };
        let Some(macd_now) = macd[index] else {
            continue;
        };

        let recent_high = candles[index - params.breakout_lookback..index]
            .iter()
            .map(|kline| kline.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let recent_low = candles[index - params.breakout_lookback..index]
            .iter()
            .map(|kline| kline.low)
            .fold(f64::INFINITY, f64::min);
        let long_signal = fast_now > slow_now
            && adx_now >= params.adx_threshold
            && rsi_now > 50.0
            && rsi_now < params.rsi_overbought
            && macd_now > 0.0
            && candle.close >= recent_high;
        let short_signal = fast_now < slow_now
            && adx_now >= params.adx_threshold
            && rsi_now > params.rsi_oversold
            && rsi_now < 50.0
            && macd_now < 0.0
            && candle.close <= recent_low;

        if let Some(open_position) = position.as_ref() {
            exposure_bars += 1;
            let (exit_price, exit_reason) = trend_exit_decision(
                open_position,
                candle,
                long_signal,
                short_signal,
                index,
                params.max_hold_bars,
                spec.slippage_rate,
            );

            if let Some((exit_price, exit_reason)) = exit_price.zip(exit_reason) {
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
                trades.push(TrendScanTrade {
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
            let quantity = spec.trade_notional / entry_price;
            let risk = (atr_now * params.atr_multiplier).max(entry_price * 0.003);
            let stop_price = match side {
                OrderSide::Buy => entry_price - risk,
                OrderSide::Sell => entry_price + risk,
            };
            let target_price = match side {
                OrderSide::Buy => entry_price + risk * params.reward_r,
                OrderSide::Sell => entry_price - risk * params.reward_r,
            };
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
            let unrealized = match open_position.side {
                OrderSide::Buy => {
                    (candle.close - open_position.entry_price) * open_position.quantity
                }
                OrderSide::Sell => {
                    (open_position.entry_price - candle.close) * open_position.quantity
                }
            };
            mark_equity += unrealized;
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
        trades.push(TrendScanTrade {
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
    let summary = TrendScanRunSummary {
        run_id,
        symbol: symbol.to_string(),
        parameter_set: params.name.clone(),
        fast_ema: params.fast_ema,
        slow_ema: params.slow_ema,
        adx_threshold: params.adx_threshold,
        atr_multiplier: params.atr_multiplier,
        reward_r: params.reward_r,
        breakout_lookback: params.breakout_lookback,
        candles: candles.len(),
        trades: trade_count,
        wins,
        win_rate_pct: if trade_count == 0 {
            0.0
        } else {
            wins as f64 / trade_count as f64 * 100.0
        },
        final_equity,
        net_pnl: final_equity - spec.initial_equity,
        roi_pct: (final_equity / spec.initial_equity - 1.0) * 100.0,
        max_drawdown_pct: max_drawdown_pct(&equity_curve),
        profit_factor: if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            999.0
        } else {
            0.0
        },
        avg_trade_pnl: if trade_count == 0 {
            0.0
        } else {
            (final_equity - spec.initial_equity) / trade_count as f64
        },
        exposure_pct: exposure_bars as f64 / candles.len().max(1) as f64 * 100.0,
        buy_hold_pct: (candles.last().unwrap().close / candles.first().unwrap().close - 1.0)
            * 100.0,
        start: candles.first().unwrap().close_time,
        end: candles.last().unwrap().close_time,
    };

    Ok(TrendScanRunReport { summary, trades })
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

fn trend_exit_decision(
    position: &TrendPosition,
    candle: &Kline,
    long_signal: bool,
    short_signal: bool,
    index: usize,
    max_hold_bars: usize,
    slippage_rate: f64,
) -> (Option<f64>, Option<String>) {
    match position.side {
        OrderSide::Buy => {
            if candle.low <= position.stop_price {
                return (
                    Some(position.stop_price * (1.0 - slippage_rate)),
                    Some("stop".to_string()),
                );
            }
            if candle.high >= position.target_price {
                return (
                    Some(position.target_price * (1.0 - slippage_rate)),
                    Some("target".to_string()),
                );
            }
            if short_signal {
                return (
                    Some(candle.close * (1.0 - slippage_rate)),
                    Some("reverse".to_string()),
                );
            }
        }
        OrderSide::Sell => {
            if candle.high >= position.stop_price {
                return (
                    Some(position.stop_price * (1.0 + slippage_rate)),
                    Some("stop".to_string()),
                );
            }
            if candle.low <= position.target_price {
                return (
                    Some(position.target_price * (1.0 + slippage_rate)),
                    Some("target".to_string()),
                );
            }
            if long_signal {
                return (
                    Some(candle.close * (1.0 + slippage_rate)),
                    Some("reverse".to_string()),
                );
            }
        }
    }
    if index.saturating_sub(position.entry_index) >= max_hold_bars {
        let exit_price = match position.side {
            OrderSide::Buy => candle.close * (1.0 - slippage_rate),
            OrderSide::Sell => candle.close * (1.0 + slippage_rate),
        };
        return (Some(exit_price), Some("time".to_string()));
    }
    (None, None)
}

fn ema_series(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() < period {
        return output;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut current = values[..period].iter().sum::<f64>() / period as f64;
    output[period - 1] = Some(current);
    for index in period..values.len() {
        current = values[index] * alpha + current * (1.0 - alpha);
        output[index] = Some(current);
    }
    output
}

fn rsi_series(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; values.len()];
    if period == 0 || values.len() <= period {
        return output;
    }
    let mut gains = vec![0.0; values.len()];
    let mut losses = vec![0.0; values.len()];
    for index in 1..values.len() {
        let change = values[index] - values[index - 1];
        gains[index] = change.max(0.0);
        losses[index] = (-change).max(0.0);
    }
    let mut avg_gain = gains[1..=period].iter().sum::<f64>() / period as f64;
    let mut avg_loss = losses[1..=period].iter().sum::<f64>() / period as f64;
    output[period] = Some(if avg_loss == 0.0 {
        100.0
    } else {
        100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    });
    for index in period + 1..values.len() {
        avg_gain = (avg_gain * (period as f64 - 1.0) + gains[index]) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + losses[index]) / period as f64;
        output[index] = Some(if avg_loss == 0.0 {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
        });
    }
    output
}

fn atr_adx_series(candles: &[Kline], period: usize) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    let mut atr_values = vec![None; candles.len()];
    let mut adx_values = vec![None; candles.len()];
    if period == 0 || candles.len() <= period * 2 {
        return (atr_values, adx_values);
    }
    let mut tr = vec![0.0; candles.len()];
    let mut plus_dm = vec![0.0; candles.len()];
    let mut minus_dm = vec![0.0; candles.len()];
    for index in 1..candles.len() {
        let candle = &candles[index];
        let previous = &candles[index - 1];
        tr[index] = (candle.high - candle.low)
            .max((candle.high - previous.close).abs())
            .max((candle.low - previous.close).abs());
        let up_move = candle.high - previous.high;
        let down_move = previous.low - candle.low;
        plus_dm[index] = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        minus_dm[index] = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
    }
    let mut atr_smoothed = tr[1..=period].iter().sum::<f64>();
    let mut plus_smoothed = plus_dm[1..=period].iter().sum::<f64>();
    let mut minus_smoothed = minus_dm[1..=period].iter().sum::<f64>();
    let mut dx_values = vec![None; candles.len()];
    for index in period..candles.len() {
        if index > period {
            atr_smoothed = atr_smoothed - atr_smoothed / period as f64 + tr[index];
            plus_smoothed = plus_smoothed - plus_smoothed / period as f64 + plus_dm[index];
            minus_smoothed = minus_smoothed - minus_smoothed / period as f64 + minus_dm[index];
        }
        atr_values[index] = Some(atr_smoothed / period as f64);
        if atr_smoothed <= 0.0 {
            continue;
        }
        let plus_di = 100.0 * plus_smoothed / atr_smoothed;
        let minus_di = 100.0 * minus_smoothed / atr_smoothed;
        let denominator = plus_di + minus_di;
        dx_values[index] = Some(if denominator == 0.0 {
            0.0
        } else {
            100.0 * (plus_di - minus_di).abs() / denominator
        });
    }
    let first_adx = period * 2 - 1;
    let initial_dx = dx_values[period..=first_adx]
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    if initial_dx.len() == period {
        let mut adx = initial_dx.iter().sum::<f64>() / period as f64;
        adx_values[first_adx] = Some(adx);
        for index in first_adx + 1..candles.len() {
            if let Some(dx) = dx_values[index] {
                adx = (adx * (period as f64 - 1.0) + dx) / period as f64;
                adx_values[index] = Some(adx);
            }
        }
    }
    (atr_values, adx_values)
}

fn macd_histogram_series(
    values: &[f64],
    fast: usize,
    slow: usize,
    signal: usize,
) -> Vec<Option<f64>> {
    let fast_values = ema_series(values, fast);
    let slow_values = ema_series(values, slow);
    let mut compact = Vec::new();
    let mut indexes = Vec::new();
    for (index, (fast_value, slow_value)) in fast_values.iter().zip(slow_values.iter()).enumerate()
    {
        if let (Some(fast_value), Some(slow_value)) = (fast_value, slow_value) {
            compact.push(fast_value - slow_value);
            indexes.push(index);
        }
    }
    let signal_values = ema_series(&compact, signal);
    let mut output = vec![None; values.len()];
    for (compact_index, signal_value) in signal_values.iter().enumerate() {
        if let Some(signal_value) = signal_value {
            let original_index = indexes[compact_index];
            output[original_index] = Some(compact[compact_index] - signal_value);
        }
    }
    output
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

#[derive(Debug, Clone)]
struct MeanReversionScanRunDefinition {
    run_id: usize,
    parameters: BTreeMap<String, f64>,
}

fn expand_scan_runs(spec: &MeanReversionScanSpec) -> Result<Vec<MeanReversionScanRunDefinition>> {
    expand_scan_runs_from_ranges(&spec.parameters)
}

fn expand_scan_runs_from_ranges(
    parameters: &[MeanReversionScanParameterRange],
) -> Result<Vec<MeanReversionScanRunDefinition>> {
    let mut runs = vec![MeanReversionScanRunDefinition {
        run_id: 0,
        parameters: BTreeMap::new(),
    }];

    for parameter in parameters {
        let mut next_runs = Vec::new();
        for run in &runs {
            for value in &parameter.values {
                let mut parameters = run.parameters.clone();
                parameters.insert(parameter.path.as_str().to_string(), *value);
                next_runs.push(MeanReversionScanRunDefinition {
                    run_id: 0,
                    parameters,
                });
            }
        }
        runs = next_runs;
    }

    for (index, run) in runs.iter_mut().enumerate() {
        run.run_id = index + 1;
    }

    if runs.is_empty() {
        return Err(anyhow!("scan expansion produced no runs"));
    }

    Ok(runs)
}

#[derive(Debug, Clone)]
struct MeanReversionWalkForwardWindow {
    window_id: usize,
    train_start: DateTime<Utc>,
    train_end: DateTime<Utc>,
    validation_start: DateTime<Utc>,
    validation_end: DateTime<Utc>,
}

fn plan_walk_forward_windows(
    runtime: &BacktestRuntime,
    spec: &MeanReversionWalkForwardSpec,
) -> Result<Vec<MeanReversionWalkForwardWindow>> {
    let events = runtime.load_kline_replay()?.collect_events();
    let timestamps = events
        .into_iter()
        .map(|event| event.logical_ts())
        .collect::<Vec<_>>();
    let required_bars = spec.train_bars + spec.validation_bars;
    if timestamps.len() < required_bars {
        return Err(anyhow!(
            "walk-forward requires at least {} bars, but dataset window only has {}",
            required_bars,
            timestamps.len()
        ));
    }

    let mut windows = Vec::new();
    let mut start_index = 0usize;
    while start_index + required_bars <= timestamps.len() {
        let train_end_index = start_index + spec.train_bars - 1;
        let validation_start_index = train_end_index + 1;
        let validation_end_index = validation_start_index + spec.validation_bars - 1;
        windows.push(MeanReversionWalkForwardWindow {
            window_id: windows.len() + 1,
            train_start: timestamps[start_index],
            train_end: timestamps[train_end_index],
            validation_start: timestamps[validation_start_index],
            validation_end: timestamps[validation_end_index],
        });
        start_index += spec.step_bars;
    }

    if windows.is_empty() {
        return Err(anyhow!("walk-forward planning produced no windows"));
    }

    Ok(windows)
}

fn build_window_runtime_config(
    base: &BacktestRuntimeConfig,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> BacktestRuntimeConfig {
    BacktestRuntimeConfig {
        exchange: base.exchange.clone(),
        market: base.market.clone(),
        symbol: base.symbol.clone(),
        interval: base.interval,
        start,
        end,
        output: base.output.clone(),
    }
}

fn resolve_scan_worker_count(requested_workers: Option<usize>, total_runs: usize) -> usize {
    if total_runs == 0 {
        return 1;
    }

    let fallback = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    requested_workers.unwrap_or(fallback).max(1).min(total_runs)
}

fn execute_scan_runs(
    runtime: &BacktestRuntime,
    base_config: &MeanReversionConfig,
    runs: &[MeanReversionScanRunDefinition],
    workers: usize,
) -> Result<Vec<MeanReversionScanRunSummary>> {
    if runs.is_empty() {
        return Ok(Vec::new());
    }

    let runtime = Arc::new(runtime.clone());
    let base_config = Arc::new(base_config.clone());
    let runs = Arc::new(runs.to_vec());
    let next_index = Arc::new(AtomicUsize::new(0));
    let results = Arc::new(Mutex::new(Vec::with_capacity(runs.len())));
    let mut handles = Vec::new();

    for _ in 0..workers.max(1) {
        let runtime = Arc::clone(&runtime);
        let base_config = Arc::clone(&base_config);
        let runs = Arc::clone(&runs);
        let next_index = Arc::clone(&next_index);
        let results = Arc::clone(&results);

        handles.push(std::thread::spawn(move || -> Result<()> {
            loop {
                let index = next_index.fetch_add(1, Ordering::Relaxed);
                if index >= runs.len() {
                    break;
                }

                let run = &runs[index];
                let mut config = (*base_config).clone();
                apply_scan_parameters(&mut config, &run.parameters)?;
                let report = runtime.run_mean_reversion_backtest(config)?;
                let summary = summarize_scan_run(run.run_id, run.parameters.clone(), &report);
                let mut guard = results
                    .lock()
                    .map_err(|_| anyhow!("scan results mutex poisoned"))?;
                guard.push(summary);
            }
            Ok(())
        }));
    }

    for handle in handles {
        let worker_result = handle
            .join()
            .map_err(|_| anyhow!("scan worker thread panicked"))?;
        worker_result?;
    }

    let mut collected = results
        .lock()
        .map_err(|_| anyhow!("scan results mutex poisoned"))?
        .clone();
    collected.sort_by_key(|item| item.run_id);
    Ok(collected)
}

fn apply_scan_parameters(
    config: &mut MeanReversionConfig,
    parameters: &BTreeMap<String, f64>,
) -> Result<()> {
    for (path, value) in parameters {
        let parameter = parse_scan_parameter_path(path)?;
        parameter.apply(config, *value)?;
    }

    Ok(())
}

fn parse_scan_parameter_path(path: &str) -> Result<MeanReversionScanParameterPath> {
    match path {
        "execution.base_improve" => Ok(MeanReversionScanParameterPath::ExecutionBaseImprove),
        "execution.take_profit_atr_k" => {
            Ok(MeanReversionScanParameterPath::ExecutionTakeProfitAtrK)
        }
        "indicators.atr.initial_stop_k" => {
            Ok(MeanReversionScanParameterPath::IndicatorsAtrInitialStopK)
        }
        "indicators.bollinger.entry_z_long" => {
            Ok(MeanReversionScanParameterPath::IndicatorsBollingerEntryZLong)
        }
        "indicators.bollinger.entry_z_short" => {
            Ok(MeanReversionScanParameterPath::IndicatorsBollingerEntryZShort)
        }
        "risk.per_trade_notional" => Ok(MeanReversionScanParameterPath::RiskPerTradeNotional),
        "risk.time_stop_bars" => Ok(MeanReversionScanParameterPath::RiskTimeStopBars),
        other => Err(anyhow!("unsupported scan parameter path {}", other)),
    }
}

fn summarize_scan_run(
    run_id: usize,
    parameters: BTreeMap<String, f64>,
    report: &MeanReversionBacktestReport,
) -> MeanReversionScanRunSummary {
    MeanReversionScanRunSummary {
        run_id,
        parameters,
        final_equity: report.execution.final_equity,
        realized_pnl: report.execution.realized_pnl,
        net_realized_pnl: report.execution.realized_pnl - report.execution.total_fees_paid,
        total_fees_paid: report.execution.total_fees_paid,
        executed_signals: report.execution.executed_signals,
        closed_trades: report.execution.closed_trades.len(),
        rejected_orders: report.execution.rejected_orders,
    }
}

fn sort_scan_runs(runs: &mut [MeanReversionScanRunSummary], sort_by: MeanReversionScanSortBy) {
    runs.sort_by(|lhs, rhs| {
        let ordering = match sort_by {
            MeanReversionScanSortBy::FinalEquity => rhs.final_equity.total_cmp(&lhs.final_equity),
            MeanReversionScanSortBy::NetRealizedPnl => {
                rhs.net_realized_pnl.total_cmp(&lhs.net_realized_pnl)
            }
            MeanReversionScanSortBy::RealizedPnl => rhs.realized_pnl.total_cmp(&lhs.realized_pnl),
            MeanReversionScanSortBy::ExecutedSignals => {
                rhs.executed_signals.cmp(&lhs.executed_signals)
            }
        };

        ordering.then_with(|| lhs.run_id.cmp(&rhs.run_id))
    });
}

#[derive(Debug, Clone)]
struct WalkForwardAggregateBuilder {
    parameters: BTreeMap<String, f64>,
    windows_selected: usize,
    total_final_equity: f64,
    total_realized_pnl: f64,
    total_net_realized_pnl: f64,
    total_fees_paid: f64,
    total_executed_signals: usize,
    total_closed_trades: usize,
    total_rejected_orders: usize,
}

fn aggregate_walk_forward_parameter_sets(
    candidates: &[MeanReversionWalkForwardCandidateSummary],
    sort_by: MeanReversionScanSortBy,
) -> Vec<MeanReversionWalkForwardParameterSummary> {
    let mut aggregates = BTreeMap::<String, WalkForwardAggregateBuilder>::new();

    for candidate in candidates {
        let key = serialize_parameter_key(&candidate.parameters);
        let entry = aggregates
            .entry(key)
            .or_insert_with(|| WalkForwardAggregateBuilder {
                parameters: candidate.parameters.clone(),
                windows_selected: 0,
                total_final_equity: 0.0,
                total_realized_pnl: 0.0,
                total_net_realized_pnl: 0.0,
                total_fees_paid: 0.0,
                total_executed_signals: 0,
                total_closed_trades: 0,
                total_rejected_orders: 0,
            });
        entry.windows_selected += 1;
        entry.total_final_equity += candidate.validation.final_equity;
        entry.total_realized_pnl += candidate.validation.realized_pnl;
        entry.total_net_realized_pnl += candidate.validation.net_realized_pnl;
        entry.total_fees_paid += candidate.validation.total_fees_paid;
        entry.total_executed_signals += candidate.validation.executed_signals;
        entry.total_closed_trades += candidate.validation.closed_trades;
        entry.total_rejected_orders += candidate.validation.rejected_orders;
    }

    let mut summaries = aggregates
        .into_values()
        .map(|aggregate| {
            let count = aggregate.windows_selected.max(1) as f64;
            MeanReversionWalkForwardParameterSummary {
                parameters: aggregate.parameters,
                windows_selected: aggregate.windows_selected,
                average_final_equity: aggregate.total_final_equity / count,
                average_realized_pnl: aggregate.total_realized_pnl / count,
                average_net_realized_pnl: aggregate.total_net_realized_pnl / count,
                average_fees_paid: aggregate.total_fees_paid / count,
                total_executed_signals: aggregate.total_executed_signals,
                total_closed_trades: aggregate.total_closed_trades,
                total_rejected_orders: aggregate.total_rejected_orders,
            }
        })
        .collect::<Vec<_>>();
    sort_walk_forward_parameter_sets(&mut summaries, sort_by);
    summaries
}

fn sort_walk_forward_parameter_sets(
    summaries: &mut [MeanReversionWalkForwardParameterSummary],
    sort_by: MeanReversionScanSortBy,
) {
    summaries.sort_by(|lhs, rhs| {
        let ordering = match sort_by {
            MeanReversionScanSortBy::FinalEquity => rhs
                .average_final_equity
                .total_cmp(&lhs.average_final_equity),
            MeanReversionScanSortBy::NetRealizedPnl => rhs
                .average_net_realized_pnl
                .total_cmp(&lhs.average_net_realized_pnl),
            MeanReversionScanSortBy::RealizedPnl => rhs
                .average_realized_pnl
                .total_cmp(&lhs.average_realized_pnl),
            MeanReversionScanSortBy::ExecutedSignals => {
                rhs.total_executed_signals.cmp(&lhs.total_executed_signals)
            }
        };

        ordering.then_with(|| {
            serialize_parameter_key(&lhs.parameters).cmp(&serialize_parameter_key(&rhs.parameters))
        })
    });
}

fn select_top_scan_runs(
    report: &MeanReversionScanReport,
    top_runs: usize,
) -> Vec<MeanReversionScanRunSummary> {
    let mut runs = report.runs.clone();
    sort_scan_runs(&mut runs, report.sort_by.clone());
    runs.into_iter().take(top_runs.max(1)).collect()
}

fn build_scan_analysis_summary(
    report: &MeanReversionScanReport,
    top_runs: &[MeanReversionScanRunSummary],
) -> MeanReversionScanAnalysisSummary {
    let mut final_equities = report
        .runs
        .iter()
        .map(|run| run.final_equity)
        .collect::<Vec<_>>();
    final_equities.sort_by(|lhs, rhs| lhs.total_cmp(rhs));
    let median_final_equity = if final_equities.is_empty() {
        0.0
    } else {
        final_equities[final_equities.len() / 2]
    };
    let worst_final_equity = final_equities.first().copied().unwrap_or_default();
    let profitable_count = report
        .runs
        .iter()
        .filter(|run| run.net_realized_pnl > 0.0)
        .count();
    let profitable_run_ratio = if report.runs.is_empty() {
        0.0
    } else {
        profitable_count as f64 / report.runs.len() as f64
    };
    let top_final_equity_average = if top_runs.is_empty() {
        0.0
    } else {
        top_runs.iter().map(|run| run.final_equity).sum::<f64>() / top_runs.len() as f64
    };
    let top_final_equity_range = if top_runs.len() <= 1 {
        0.0
    } else {
        let best = top_runs
            .first()
            .map(|run| run.final_equity)
            .unwrap_or_default();
        let worst = top_runs
            .last()
            .map(|run| run.final_equity)
            .unwrap_or_default();
        best - worst
    };

    MeanReversionScanAnalysisSummary {
        sort_by: report.sort_by.clone(),
        total_runs: report.total_runs,
        completed_runs: report.completed_runs,
        workers_used: report.workers_used,
        best_run: top_runs
            .first()
            .cloned()
            .or_else(|| report.best_run.clone()),
        median_final_equity,
        worst_final_equity,
        profitable_run_ratio,
        top_final_equity_average,
        top_final_equity_range,
    }
}

fn build_parameter_insights(
    report: &MeanReversionScanReport,
) -> Vec<MeanReversionParameterInsight> {
    let mut grouped =
        BTreeMap::<String, BTreeMap<String, Vec<&MeanReversionScanRunSummary>>>::new();
    let mut value_lookup = BTreeMap::<(String, String), f64>::new();

    for run in &report.runs {
        for (path, value) in &run.parameters {
            let value_key = format!("{value:.10}");
            grouped
                .entry(path.clone())
                .or_default()
                .entry(value_key.clone())
                .or_default()
                .push(run);
            value_lookup.insert((path.clone(), value_key), *value);
        }
    }

    grouped
        .into_iter()
        .map(|(path, value_groups)| {
            let mut value_insights = value_groups
                .into_iter()
                .map(|(value_key, runs)| {
                    let run_count = runs.len();
                    let average_final_equity =
                        runs.iter().map(|run| run.final_equity).sum::<f64>() / run_count as f64;
                    let average_net_realized_pnl =
                        runs.iter().map(|run| run.net_realized_pnl).sum::<f64>() / run_count as f64;
                    let best_final_equity = runs
                        .iter()
                        .map(|run| run.final_equity)
                        .max_by(|lhs, rhs| lhs.total_cmp(rhs))
                        .unwrap_or_default();

                    MeanReversionParameterValueInsight {
                        value: value_lookup
                            .get(&(path.clone(), value_key))
                            .copied()
                            .unwrap_or_default(),
                        run_count,
                        average_final_equity,
                        average_net_realized_pnl,
                        best_final_equity,
                    }
                })
                .collect::<Vec<_>>();

            value_insights.sort_by(|lhs, rhs| {
                rhs.average_final_equity
                    .total_cmp(&lhs.average_final_equity)
                    .then_with(|| lhs.value.total_cmp(&rhs.value))
            });

            MeanReversionParameterInsight {
                path,
                best_value: value_insights
                    .first()
                    .map(|item| item.value)
                    .unwrap_or_default(),
                value_insights,
            }
        })
        .collect()
}

fn build_walk_forward_analysis_summary(
    scan_report: &MeanReversionScanReport,
    walk_forward_report: &MeanReversionWalkForwardReport,
) -> MeanReversionWalkForwardAnalysisSummary {
    let scan_best = scan_report
        .best_run
        .as_ref()
        .or_else(|| scan_report.runs.first());
    let scan_best_key = scan_best.map(|run| serialize_parameter_key(&run.parameters));
    let walk_best_key = walk_forward_report
        .best_parameter_set
        .as_ref()
        .map(|item| serialize_parameter_key(&item.parameters));
    let scan_best_matches_walk_forward_best =
        scan_best_key.is_some() && scan_best_key == walk_best_key;
    let scan_best_in_walk_forward_parameter_sets = scan_best_key.as_ref().is_some_and(|best_key| {
        walk_forward_report
            .parameter_sets
            .iter()
            .any(|item| serialize_parameter_key(&item.parameters) == *best_key)
    });
    let average_windows_selected = if walk_forward_report.parameter_sets.is_empty() {
        0.0
    } else {
        walk_forward_report
            .parameter_sets
            .iter()
            .map(|item| item.windows_selected as f64)
            .sum::<f64>()
            / walk_forward_report.parameter_sets.len() as f64
    };

    MeanReversionWalkForwardAnalysisSummary {
        window_count: walk_forward_report.window_count,
        total_parameter_sets: walk_forward_report.total_parameter_sets,
        parameter_sets_analyzed: walk_forward_report.parameter_sets.len(),
        best_parameter_set: walk_forward_report.best_parameter_set.clone(),
        scan_best_matches_walk_forward_best,
        scan_best_in_walk_forward_parameter_sets,
        average_windows_selected,
    }
}

fn serialize_parameter_key(parameters: &BTreeMap<String, f64>) -> String {
    parameters
        .iter()
        .map(|(path, value)| format!("{path}={value:.10}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn settlement_currency_from_config(config: &MeanReversionConfig) -> String {
    config
        .symbols
        .iter()
        .filter(|symbol| symbol.enabled)
        .find_map(|symbol| quote_asset_from_symbol(&symbol.symbol))
        .unwrap_or_else(|| "USDT".to_string())
}

fn quote_asset_from_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if let Some((_, quote)) = trimmed.rsplit_once('/') {
        let quote = quote.trim();
        if !quote.is_empty() {
            return Some(quote.to_ascii_uppercase());
        }
    }

    let upper = trimmed.to_ascii_uppercase();
    ["USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH"]
        .into_iter()
        .find(|quote| upper.ends_with(quote) && upper.len() > quote.len())
        .map(str::to_string)
}
fn initial_backtest_cash(config: &MeanReversionConfig) -> f64 {
    let notional_budget = config.risk.per_trade_notional * config.risk.max_positions.max(1) as f64;
    config
        .risk
        .max_net_exposure
        .max(notional_budget)
        .max(1_000.0)
}

fn default_taker_fee_bps() -> f64 {
    5.0
}

fn default_maker_fee_bps() -> f64 {
    2.0
}

fn limit_entry_price(signal: &StrategySignal, config: &MeanReversionConfig) -> f64 {
    let improve = config.execution.base_improve.max(0.0);
    match signal.side {
        OrderSide::Buy => signal.price * (1.0 - improve),
        OrderSide::Sell => signal.price * (1.0 + improve),
    }
}

fn market_type_from_string(market: &str) -> Result<MarketType> {
    match market {
        "futures" => Ok(MarketType::Futures),
        "spot" => Ok(MarketType::Spot),
        other => Err(anyhow!("unsupported market {}", other)),
    }
}

impl ExecutedSignal {
    fn from_fill(signal: &StrategySignal, quantity: f64, fee_paid: f64) -> Self {
        Self {
            symbol: signal.symbol.clone(),
            side: signal.side,
            timestamp: signal.logical_ts,
            price: signal.price,
            quantity,
            fee_paid,
            is_maker: false,
            order_type: "market".to_string(),
        }
    }

    fn from_fill_result(fill: &crate::backtest::matching::ledger::FillResult) -> Self {
        Self {
            symbol: fill.symbol.clone(),
            side: fill.side,
            timestamp: fill.timestamp,
            price: fill.price,
            quantity: fill.quantity,
            fee_paid: fill.fee_paid,
            is_maker: fill.is_maker,
            order_type: match fill.order_type {
                crate::core::types::OrderType::Market => "market".to_string(),
                crate::core::types::OrderType::Limit => "limit".to_string(),
                other => format!("{:?}", other).to_lowercase(),
            },
        }
    }

    fn from_order(
        symbol: &str,
        side: OrderSide,
        timestamp: DateTime<Utc>,
        price: f64,
        quantity: f64,
        is_maker: bool,
        order_type: String,
        fee_paid: f64,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            side,
            timestamp,
            price,
            quantity,
            fee_paid,
            is_maker,
            order_type,
        }
    }
}

impl TradeExecutionLeg {
    fn from_fill_result(fill: &crate::backtest::matching::ledger::FillResult) -> Self {
        Self {
            timestamp: fill.timestamp,
            price: fill.price,
            quantity: fill.quantity,
            fee_paid: fill.fee_paid,
            is_maker: fill.is_maker,
            order_type: match fill.order_type {
                crate::core::types::OrderType::Market => "market".to_string(),
                crate::core::types::OrderType::Limit => "limit".to_string(),
                other => format!("{:?}", other).to_lowercase(),
            },
        }
    }
}

impl MeanReversionExecutionSummary {
    fn from_engine(
        engine: &BacktestEngineState,
        depth_replay_enabled: bool,
        depth_events_processed: usize,
        trade_replay_enabled: bool,
        trade_events_processed: usize,
        fills: Vec<ExecutedSignal>,
        closed_trades: Vec<ClosedTrade>,
        limit_orders_placed: usize,
        limit_orders_filled: usize,
        limit_orders_cancelled: usize,
        venue_stats: VenueExecutionStats,
    ) -> Self {
        let positions = engine
            .ledger()
            .positions()
            .map(|position| {
                (
                    position.symbol.clone(),
                    ExecutionPositionSummary {
                        symbol: position.symbol.clone(),
                        quantity: position.quantity,
                        entry_price: position.entry_price,
                        mark_price: position.mark_price,
                    },
                )
            })
            .collect();

        Self {
            settlement_currency: engine.ledger().settlement_currency().to_string(),
            executed_signals: fills.len(),
            depth_replay_enabled,
            depth_events_processed,
            trade_replay_enabled,
            trade_events_processed,
            limit_orders_placed,
            limit_orders_filled,
            limit_orders_cancelled,
            venue_adjusted_orders: venue_stats.adjusted_orders,
            rejected_orders: venue_stats.rejected_orders,
            rejection_reasons: venue_stats.rejection_reasons,
            realized_pnl: engine.ledger().realized_pnl(),
            total_fees_paid: engine.ledger().total_fees_paid(),
            total_funding_paid: engine.ledger().total_funding_paid(),
            cash_balance: engine.ledger().cash_balance(),
            final_equity: engine.ledger().total_equity(),
            positions,
            closed_trades,
            fills,
        }
    }
}

impl ManagedPosition {
    fn from_entry_plan(
        plan: &PendingEntryPlan,
        fill_price: f64,
        fill_timestamp: DateTime<Utc>,
        quantity: f64,
        config: &MeanReversionConfig,
    ) -> Self {
        let stop_distance = config.indicators.atr.initial_stop_k * plan.atr;
        let take_profit_distance = config.execution.take_profit_atr_k * plan.atr;
        let (stop_price, take_profit_price) = match plan.side {
            OrderSide::Buy => (
                fill_price - stop_distance,
                fill_price + take_profit_distance,
            ),
            OrderSide::Sell => (
                fill_price + stop_distance,
                fill_price - take_profit_distance,
            ),
        };

        Self {
            symbol: plan.symbol.clone(),
            side: plan.side,
            quantity,
            entry_price: fill_price,
            stop_price,
            take_profit_price,
            opened_at: fill_timestamp,
            entry_legs: if quantity > f64::EPSILON {
                vec![TradeExecutionLeg {
                    timestamp: fill_timestamp,
                    price: fill_price,
                    quantity,
                    fee_paid: 0.0,
                    is_maker: true,
                    order_type: "limit".to_string(),
                }]
            } else {
                Vec::new()
            },
            entry_fees_paid: 0.0,
        }
    }

    fn from_signal(signal: &StrategySignal, quantity: f64, config: &MeanReversionConfig) -> Self {
        let stop_distance = config.indicators.atr.initial_stop_k * signal.atr;
        let take_profit_distance = config.execution.take_profit_atr_k * signal.atr;
        let (stop_price, take_profit_price) = match signal.side {
            OrderSide::Buy => (
                signal.price - stop_distance,
                signal.price + take_profit_distance,
            ),
            OrderSide::Sell => (
                signal.price + stop_distance,
                signal.price - take_profit_distance,
            ),
        };

        Self {
            symbol: signal.symbol.clone(),
            side: signal.side,
            quantity,
            entry_price: signal.price,
            stop_price,
            take_profit_price,
            opened_at: signal.logical_ts,
            entry_legs: if quantity > f64::EPSILON {
                vec![TradeExecutionLeg {
                    timestamp: signal.logical_ts,
                    price: signal.price,
                    quantity,
                    fee_paid: 0.0,
                    is_maker: false,
                    order_type: "market".to_string(),
                }]
            } else {
                Vec::new()
            },
            entry_fees_paid: 0.0,
        }
    }

    fn apply_entry_fill(
        &mut self,
        plan: &PendingEntryPlan,
        fill_price: f64,
        fill_timestamp: DateTime<Utc>,
        quantity: f64,
        config: &MeanReversionConfig,
    ) {
        if quantity <= f64::EPSILON {
            return;
        }

        if self.quantity <= f64::EPSILON {
            *self = Self::from_entry_plan(plan, fill_price, fill_timestamp, quantity, config);
            return;
        }

        let total_quantity = self.quantity + quantity;
        let weighted_entry_price = (self.entry_price * self.quantity + fill_price * quantity)
            / total_quantity.max(f64::EPSILON);
        let stop_distance = config.indicators.atr.initial_stop_k * plan.atr;
        let take_profit_distance = config.execution.take_profit_atr_k * plan.atr;
        let (stop_price, take_profit_price) = match plan.side {
            OrderSide::Buy => (
                weighted_entry_price - stop_distance,
                weighted_entry_price + take_profit_distance,
            ),
            OrderSide::Sell => (
                weighted_entry_price + stop_distance,
                weighted_entry_price - take_profit_distance,
            ),
        };

        self.quantity = total_quantity;
        self.entry_price = weighted_entry_price;
        self.stop_price = stop_price;
        self.take_profit_price = take_profit_price;
        if fill_timestamp < self.opened_at {
            self.opened_at = fill_timestamp;
        }
        self.entry_legs.push(TradeExecutionLeg {
            timestamp: fill_timestamp,
            price: fill_price,
            quantity,
            fee_paid: 0.0,
            is_maker: true,
            order_type: "limit".to_string(),
        });
    }

    fn apply_entry_fill_result(
        &mut self,
        plan: &PendingEntryPlan,
        fill: &crate::backtest::matching::ledger::FillResult,
        config: &MeanReversionConfig,
    ) {
        self.apply_entry_fill(plan, fill.price, fill.timestamp, fill.quantity, config);
        if let Some(last_leg) = self.entry_legs.last_mut() {
            last_leg.fee_paid = fill.fee_paid;
            last_leg.is_maker = fill.is_maker;
            last_leg.order_type = match fill.order_type {
                crate::core::types::OrderType::Market => "market".to_string(),
                crate::core::types::OrderType::Limit => "limit".to_string(),
                other => format!("{:?}", other).to_lowercase(),
            };
        }
        self.entry_fees_paid += fill.fee_paid;
    }
}

impl PendingEntryPlan {
    fn from_signal(signal: &StrategySignal, order_id: u64, price: f64) -> Self {
        Self {
            symbol: signal.symbol.clone(),
            side: signal.side,
            logical_ts: signal.logical_ts,
            price,
            atr: signal.atr,
            order_id,
        }
    }
}

impl ClosedTrade {
    fn from_position(
        position: &ManagedPosition,
        exit_legs: Vec<TradeExecutionLeg>,
        exit_reason: String,
    ) -> Self {
        let exit_timestamp = exit_legs
            .last()
            .map(|leg| leg.timestamp)
            .unwrap_or(position.opened_at);
        let exit_price = weighted_average_price(&exit_legs);
        let exit_fees_paid = exit_legs.iter().map(|leg| leg.fee_paid).sum::<f64>();
        let realized_pnl = match position.side {
            OrderSide::Buy => (exit_price - position.entry_price) * position.quantity,
            OrderSide::Sell => (position.entry_price - exit_price) * position.quantity,
        };

        Self {
            symbol: position.symbol.clone(),
            side: position.side,
            entry_timestamp: position.opened_at,
            exit_timestamp,
            entry_price: position.entry_price,
            exit_price,
            quantity: position.quantity,
            realized_pnl,
            net_realized_pnl: realized_pnl - position.entry_fees_paid - exit_fees_paid,
            entry_fill_count: position.entry_legs.len(),
            exit_fill_count: exit_legs.len(),
            entry_fees_paid: position.entry_fees_paid,
            exit_fees_paid,
            average_entry_price: weighted_average_price(&position.entry_legs),
            average_exit_price: exit_price,
            entry_legs: position.entry_legs.clone(),
            exit_legs,
            exit_reason,
        }
    }
}

fn weighted_average_price(legs: &[TradeExecutionLeg]) -> f64 {
    let total_qty = legs.iter().map(|leg| leg.quantity).sum::<f64>();
    if total_qty <= f64::EPSILON {
        return 0.0;
    }

    legs.iter().map(|leg| leg.price * leg.quantity).sum::<f64>() / total_qty
}

fn evaluate_exit_on_kline(
    position: &ManagedPosition,
    kline: &Kline,
    time_stop_bars: u32,
) -> Option<ExitDecision> {
    let barrier_hit = match position.side {
        OrderSide::Buy => {
            let hits_stop = kline.low <= position.stop_price;
            let hits_take_profit = kline.high >= position.take_profit_price;

            if hits_stop {
                Some(("stop_loss".to_string(), position.stop_price))
            } else if hits_take_profit {
                Some(("take_profit".to_string(), position.take_profit_price))
            } else {
                None
            }
        }
        OrderSide::Sell => {
            let hits_stop = kline.high >= position.stop_price;
            let hits_take_profit = kline.low <= position.take_profit_price;

            if hits_stop {
                Some(("stop_loss".to_string(), position.stop_price))
            } else if hits_take_profit {
                Some(("take_profit".to_string(), position.take_profit_price))
            } else {
                None
            }
        }
    };

    if let Some((reason, price)) = barrier_hit {
        return Some(ExitDecision {
            reason,
            price,
            timestamp: kline.close_time,
        });
    }

    let max_hold_minutes = (time_stop_bars as i64) * 15;
    if max_hold_minutes > 0
        && kline
            .close_time
            .signed_duration_since(position.opened_at)
            .num_minutes()
            >= max_hold_minutes
    {
        return Some(ExitDecision {
            reason: "time_stop".to_string(),
            price: kline.close,
            timestamp: kline.close_time,
        });
    }

    None
}

fn opposite_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn normalize_symbol_input(symbol: &str) -> Result<String> {
    let config = Config {
        name: "binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443".to_string(),
        ws_futures_url: "wss://fstream.binance.com".to_string(),
    };
    let converter = SymbolConverter::new(config);
    converter
        .normalize_symbol(symbol)
        .map_err(|err| anyhow!("invalid symbol {}: {}", symbol, err))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        apply_symbol_constraints, apply_symbol_constraints_from_snapshot, evaluate_exit_on_kline,
        execute_market_exit_now, ExitDecision, ManagedPosition,
    };
    use crate::backtest::data::exchange_metadata::ExchangeMetadataWriter;
    use crate::backtest::matching::engine::BacktestEngineState;
    use crate::backtest::matching::ledger::FillResult;
    use crate::core::types::{Kline, MarketType, OrderSide, OrderType, TradingPair};
    use crate::strategies::mean_reversion::config::ExecutionVenueConstraints;
    use chrono::{Duration, TimeZone, Utc};
    use tempfile::tempdir;

    fn sample_bar(open: f64, high: f64, low: f64, close: f64) -> Kline {
        let open_time = Utc.with_ymd_and_hms(2024, 4, 2, 0, 0, 0).unwrap();
        let close_time = open_time + Duration::seconds(59);
        Kline {
            symbol: "BTC/USDT".to_string(),
            interval: "1m".to_string(),
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume: 1.0,
            quote_volume: 100.0,
            trade_count: 1,
        }
    }

    fn managed_long() -> ManagedPosition {
        ManagedPosition {
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            quantity: 1.0,
            entry_price: 100.0,
            stop_price: 98.0,
            take_profit_price: 102.0,
            opened_at: Utc.with_ymd_and_hms(2024, 4, 2, 0, 0, 0).unwrap(),
            entry_legs: Vec::new(),
            entry_fees_paid: 0.0,
        }
    }

    #[test]
    fn intrabar_exit_hits_take_profit_even_when_close_does_not() {
        let bar = sample_bar(100.0, 102.5, 99.5, 100.5);
        let decision =
            evaluate_exit_on_kline(&managed_long(), &bar, 10).expect("take profit should trigger");

        assert_eq!(decision.reason, "take_profit");
        assert_eq!(decision.price, 102.0);
    }

    #[test]
    fn intrabar_exit_prefers_conservative_stop_when_both_barriers_hit() {
        let bar = sample_bar(100.0, 103.0, 97.5, 101.0);
        let decision =
            evaluate_exit_on_kline(&managed_long(), &bar, 10).expect("one barrier should trigger");

        assert_eq!(
            decision,
            ExitDecision {
                reason: "stop_loss".to_string(),
                price: 98.0,
                timestamp: bar.close_time,
            }
        );
    }

    #[test]
    fn apply_symbol_constraints_only_backfills_missing_values() {
        let mut venue = ExecutionVenueConstraints {
            tick_size: Some(0.1),
            step_size: None,
            min_notional: None,
        };
        let symbol_info = TradingPair {
            symbol: "BTC/USDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            status: "TRADING".to_string(),
            min_order_size: 0.001,
            max_order_size: 100.0,
            tick_size: 0.01,
            step_size: 0.001,
            min_notional: Some(5.0),
            is_trading: true,
            market_type: MarketType::Futures,
        };

        apply_symbol_constraints(&mut venue, &symbol_info);

        assert_eq!(venue.tick_size, Some(0.1));
        assert_eq!(venue.step_size, Some(0.001));
        assert_eq!(venue.min_notional, Some(5.0));
    }

    #[test]
    fn apply_symbol_constraints_from_snapshot_prefers_local_metadata() {
        let temp_dir = tempdir().expect("temp dir");
        let writer = ExchangeMetadataWriter::new(temp_dir.path());
        let trading_pair = TradingPair {
            symbol: "BTC/USDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            status: "TRADING".to_string(),
            min_order_size: 0.001,
            max_order_size: 100.0,
            tick_size: 0.5,
            step_size: 0.01,
            min_notional: Some(15.0),
            is_trading: true,
            market_type: MarketType::Futures,
        };
        writer
            .write_trading_pair("binance", "futures", &trading_pair)
            .expect("snapshot should write");

        let mut venue = ExecutionVenueConstraints::default();
        let restored = apply_symbol_constraints_from_snapshot(
            temp_dir.path(),
            "binance",
            "futures",
            "BTC/USDT",
            &mut venue,
        )
        .expect("snapshot load should succeed");

        assert!(restored);
        assert_eq!(venue.tick_size, Some(0.5));
        assert_eq!(venue.step_size, Some(0.01));
        assert_eq!(venue.min_notional, Some(15.0));
    }

    #[test]
    fn execute_market_exit_now_uses_depth_weighted_price_for_closed_trade() {
        let opened_at = Utc.with_ymd_and_hms(2024, 4, 2, 0, 0, 0).unwrap();
        let exit_ts = opened_at + Duration::seconds(30);
        let position = ManagedPosition {
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            quantity: 1.5,
            entry_price: 100.0,
            stop_price: 98.0,
            take_profit_price: 102.0,
            opened_at,
            entry_legs: Vec::new(),
            entry_fees_paid: 0.0,
        };
        let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);
        engine
            .ledger_mut()
            .apply_fill(FillResult {
                symbol: "BTC/USDT".to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                market_type: MarketType::Futures,
                quantity: 1.5,
                price: 100.0,
                is_maker: false,
                fee_paid: 0.0,
                timestamp: opened_at,
            })
            .expect("entry fill should seed ledger position");
        engine
            .seed_order_book(
                "BTC/USDT",
                vec![[101.0, 0.5], [100.5, 2.0], [100.0, 3.0]],
                vec![[101.5, 4.0], [102.0, 5.0]],
                10,
                opened_at,
            )
            .expect("book should seed");

        let mut managed_positions = BTreeMap::new();
        managed_positions.insert(position.symbol.clone(), position.clone());
        let mut fills = Vec::new();
        let mut closed_trades = Vec::new();

        execute_market_exit_now(
            &position,
            "take_profit".to_string(),
            exit_ts,
            101.0,
            MarketType::Futures,
            0.0,
            0.0,
            &mut engine,
            &mut managed_positions,
            &mut fills,
            &mut closed_trades,
        )
        .expect("market exit should execute");

        assert!(managed_positions.is_empty());
        assert_eq!(fills.len(), 1);
        assert_eq!(closed_trades.len(), 1);
        assert!((fills[0].price - 100.66666666666667).abs() < 1e-9);
        assert!((closed_trades[0].exit_price - 100.66666666666667).abs() < 1e-9);
        assert_eq!(closed_trades[0].exit_fill_count, 1);
    }
}
