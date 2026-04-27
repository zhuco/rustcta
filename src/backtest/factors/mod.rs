use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorScanSpec {
    #[serde(default = "default_initial_equity")]
    pub initial_equity: f64,
    #[serde(default = "default_trade_notional")]
    pub trade_notional: f64,
    #[serde(default = "default_fee_rate")]
    pub fee_rate: f64,
    #[serde(default = "default_slippage_rate")]
    pub slippage_rate: f64,
    #[serde(default)]
    pub search: TrendFactorSearchConfig,
    #[serde(default = "default_cost_scenarios")]
    pub cost_scenarios: Vec<TrendCostScenario>,
    #[serde(default)]
    pub candidate_filter: TrendCandidateFilter,
    pub strategies: Vec<TrendFactorStrategyTemplate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendCostScenario {
    pub name: String,
    pub fee_rate: f64,
    pub slippage_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendCandidateFilter {
    #[serde(default = "default_min_trades")]
    pub min_trades: usize,
    #[serde(default = "default_min_profit_factor")]
    pub min_profit_factor: f64,
    #[serde(default)]
    pub min_roi_pct: f64,
    #[serde(default = "default_max_drawdown_pct")]
    pub max_drawdown_pct: f64,
    #[serde(default = "default_required_profitable_cost_scenarios")]
    pub require_profitable_cost_scenarios: usize,
}

impl Default for TrendCandidateFilter {
    fn default() -> Self {
        Self {
            min_trades: default_min_trades(),
            min_profit_factor: default_min_profit_factor(),
            min_roi_pct: 0.0,
            max_drawdown_pct: default_max_drawdown_pct(),
            require_profitable_cost_scenarios: default_required_profitable_cost_scenarios(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorSearchConfig {
    #[serde(default = "default_min_trades")]
    pub min_trades: usize,
    #[serde(default = "default_sort_by")]
    pub sort_by: String,
}

impl Default for TrendFactorSearchConfig {
    fn default() -> Self {
        Self {
            min_trades: default_min_trades(),
            sort_by: default_sort_by(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorStrategyTemplate {
    pub name: String,
    pub entry: TrendFactorEntryConfig,
    #[serde(default)]
    pub filters: TrendFactorFilterConfig,
    pub exit: TrendFactorExitConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrendFactorEntryConfig {
    pub ema_trend: Option<EmaTrendGrid>,
    pub ema_slope: Option<EmaSlopeGrid>,
    pub donchian_breakout: Option<PeriodGrid>,
    pub keltner_breakout: Option<KeltnerGrid>,
    pub supertrend_direction: Option<SuperTrendGrid>,
    pub macd_histogram_direction: Option<EnabledGrid>,
    pub roc_direction: Option<PeriodThresholdGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrendFactorFilterConfig {
    pub adx_min: Option<Vec<f64>>,
    pub rsi_range: Option<RangeGrid>,
    pub bb_width_min: Option<PeriodThresholdGrid>,
    pub volume_ratio_min: Option<PeriodThresholdGrid>,
    pub volume_zscore_min: Option<PeriodThresholdGrid>,
    pub vwap_deviation_max_abs: Option<PeriodThresholdGrid>,
    pub mfi_range: Option<PeriodRangeGrid>,
    pub stoch_rsi_range: Option<StochRsiRangeGrid>,
    pub choppiness_max: Option<PeriodThresholdGrid>,
    pub atr_percentile_range: Option<AtrPercentileRangeGrid>,
    pub atr_pct_range: Option<PeriodRangeGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorExitConfig {
    pub atr_stop: Vec<f64>,
    pub reward_r: Vec<f64>,
    pub time_stop_bars: Vec<usize>,
    #[serde(default)]
    pub reverse_signal_exit: bool,
    pub trailing_atr: Option<TrailingAtrGrid>,
    pub donchian_exit: Option<PeriodGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmaTrendGrid {
    pub fast: Vec<usize>,
    pub slow: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodGrid {
    pub lookback: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperTrendGrid {
    pub period: Vec<usize>,
    pub multiplier: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmaSlopeGrid {
    pub period: Vec<usize>,
    pub lookback: Vec<usize>,
    pub threshold: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeltnerGrid {
    pub ema_period: Vec<usize>,
    pub atr_period: Vec<usize>,
    pub multiplier: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnabledGrid {
    #[serde(default = "default_true_values")]
    pub enabled: Vec<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodThresholdGrid {
    pub period: Vec<usize>,
    pub threshold: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeGrid {
    pub min: Vec<f64>,
    pub max: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodRangeGrid {
    pub period: Vec<usize>,
    pub min: Vec<f64>,
    pub max: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StochRsiRangeGrid {
    pub rsi_period: Vec<usize>,
    pub stoch_period: Vec<usize>,
    pub min: Vec<f64>,
    pub max: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtrPercentileRangeGrid {
    pub atr_period: Vec<usize>,
    pub percentile_window: Vec<usize>,
    pub min: Vec<f64>,
    pub max: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullbackEntryGrid {
    pub max_pullback_pct: Vec<f64>,
    pub reclaim_pct: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrailingAtrGrid {
    pub period: Vec<usize>,
    pub multiplier: Vec<f64>,
    pub activation_r: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFactorRunDefinition {
    pub run_id: usize,
    pub strategy_name: String,
    pub parameters: BTreeMap<String, f64>,
    pub flags: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendFactorScanSpec {
    #[serde(default = "default_initial_equity")]
    pub initial_equity: f64,
    #[serde(default = "default_trade_notional")]
    pub trade_notional: f64,
    #[serde(default = "default_fee_rate")]
    pub fee_rate: f64,
    #[serde(default = "default_slippage_rate")]
    pub slippage_rate: f64,
    #[serde(default)]
    pub search: TrendFactorSearchConfig,
    #[serde(default = "default_cost_scenarios")]
    pub cost_scenarios: Vec<TrendCostScenario>,
    #[serde(default)]
    pub candidate_filter: TrendCandidateFilter,
    pub timeframes: MtfTimeframes,
    pub strategies: Vec<MtfTrendFactorStrategyTemplate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTimeframes {
    pub trend: String,
    pub structure: String,
    pub trigger: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendFactorStrategyTemplate {
    pub name: String,
    pub trend: MtfTrendLayerConfig,
    pub structure: MtfStructureLayerConfig,
    pub trigger: MtfTriggerLayerConfig,
    pub exit: TrendFactorExitConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MtfTrendLayerConfig {
    pub ema_trend: Option<EmaTrendGrid>,
    pub adx_min: Option<Vec<f64>>,
    pub choppiness_max: Option<PeriodThresholdGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MtfStructureLayerConfig {
    pub donchian_breakout: Option<PeriodGrid>,
    pub bb_width_min: Option<PeriodThresholdGrid>,
    pub keltner_breakout: Option<KeltnerGrid>,
    pub breakout_valid_bars: Option<Vec<usize>>,
    pub pullback_entry: Option<PullbackEntryGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MtfTriggerLayerConfig {
    pub roc_direction: Option<PeriodThresholdGrid>,
    pub volume_zscore_min: Option<PeriodThresholdGrid>,
    pub supertrend_direction: Option<SuperTrendGrid>,
    pub mfi_range: Option<PeriodRangeGrid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfTrendFactorRunDefinition {
    pub run_id: usize,
    pub strategy_name: String,
    pub parameters: BTreeMap<String, f64>,
    pub flags: BTreeMap<String, bool>,
}

pub fn expand_trend_factor_runs(
    spec: &TrendFactorScanSpec,
) -> Result<Vec<TrendFactorRunDefinition>> {
    if spec.strategies.is_empty() {
        return Err(anyhow!("trend factor scan requires at least one strategy"));
    }
    let mut runs = Vec::new();
    for strategy in &spec.strategies {
        let mut partials = vec![(BTreeMap::new(), BTreeMap::new())];
        expand_entry(&mut partials, &strategy.entry);
        expand_filters(&mut partials, &strategy.filters);
        expand_exit(&mut partials, &strategy.exit)?;
        for (parameters, flags) in partials {
            runs.push(TrendFactorRunDefinition {
                run_id: runs.len() + 1,
                strategy_name: strategy.name.clone(),
                parameters,
                flags,
            });
        }
    }
    Ok(runs)
}

pub fn expand_mtf_trend_factor_runs(
    spec: &MtfTrendFactorScanSpec,
) -> Result<Vec<MtfTrendFactorRunDefinition>> {
    if spec.strategies.is_empty() {
        return Err(anyhow!(
            "mtf trend factor scan requires at least one strategy"
        ));
    }
    let mut runs = Vec::new();
    for strategy in &spec.strategies {
        let mut partials = vec![(BTreeMap::new(), BTreeMap::new())];
        expand_mtf_trend(&mut partials, &strategy.trend);
        expand_mtf_structure(&mut partials, &strategy.structure);
        expand_mtf_trigger(&mut partials, &strategy.trigger);
        expand_exit(&mut partials, &strategy.exit)?;
        for (parameters, flags) in partials {
            runs.push(MtfTrendFactorRunDefinition {
                run_id: runs.len() + 1,
                strategy_name: strategy.name.clone(),
                parameters,
                flags,
            });
        }
    }
    Ok(runs)
}

fn expand_mtf_trend(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    trend: &MtfTrendLayerConfig,
) {
    if let Some(grid) = &trend.ema_trend {
        expand_usize(partials, "trend.ema.fast", &grid.fast);
        expand_usize(partials, "trend.ema.slow", &grid.slow);
    }
    if let Some(values) = &trend.adx_min {
        expand_f64(partials, "trend.adx_min", values);
    }
    if let Some(grid) = &trend.choppiness_max {
        expand_usize(partials, "trend.choppiness.period", &grid.period);
        expand_f64(partials, "trend.choppiness.max", &grid.threshold);
    }
}

fn expand_mtf_structure(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    structure: &MtfStructureLayerConfig,
) {
    if let Some(grid) = &structure.donchian_breakout {
        expand_usize(partials, "structure.donchian.lookback", &grid.lookback);
    }
    if let Some(grid) = &structure.bb_width_min {
        expand_usize(partials, "structure.bb_width.period", &grid.period);
        expand_f64(partials, "structure.bb_width.threshold", &grid.threshold);
    }
    if let Some(grid) = &structure.keltner_breakout {
        expand_usize(partials, "structure.keltner.ema_period", &grid.ema_period);
        expand_usize(partials, "structure.keltner.atr_period", &grid.atr_period);
        expand_f64(partials, "structure.keltner.multiplier", &grid.multiplier);
    }
    if let Some(values) = &structure.breakout_valid_bars {
        expand_usize(partials, "structure.breakout_valid_bars", values);
    }
    if let Some(grid) = &structure.pullback_entry {
        expand_f64(
            partials,
            "structure.pullback.max_pct",
            &grid.max_pullback_pct,
        );
        expand_f64(
            partials,
            "structure.pullback.reclaim_pct",
            &grid.reclaim_pct,
        );
    }
}

fn expand_mtf_trigger(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    trigger: &MtfTriggerLayerConfig,
) {
    if let Some(grid) = &trigger.roc_direction {
        expand_usize(partials, "trigger.roc.period", &grid.period);
        expand_f64(partials, "trigger.roc.threshold", &grid.threshold);
    }
    if let Some(grid) = &trigger.volume_zscore_min {
        expand_usize(partials, "trigger.volume_zscore.period", &grid.period);
        expand_f64(partials, "trigger.volume_zscore.threshold", &grid.threshold);
    }
    if let Some(grid) = &trigger.supertrend_direction {
        expand_usize(partials, "trigger.supertrend.period", &grid.period);
        expand_f64(partials, "trigger.supertrend.multiplier", &grid.multiplier);
    }
    if let Some(grid) = &trigger.mfi_range {
        expand_usize(partials, "trigger.mfi.period", &grid.period);
        expand_f64(partials, "trigger.mfi.min", &grid.min);
        expand_f64(partials, "trigger.mfi.max", &grid.max);
    }
}

fn expand_entry(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    entry: &TrendFactorEntryConfig,
) {
    if let Some(grid) = &entry.ema_trend {
        expand_usize(partials, "entry.ema_trend.fast", &grid.fast);
        expand_usize(partials, "entry.ema_trend.slow", &grid.slow);
    }
    if let Some(grid) = &entry.ema_slope {
        expand_usize(partials, "entry.ema_slope.period", &grid.period);
        expand_usize(partials, "entry.ema_slope.lookback", &grid.lookback);
        expand_f64(partials, "entry.ema_slope.threshold", &grid.threshold);
    }
    if let Some(grid) = &entry.donchian_breakout {
        expand_usize(partials, "entry.donchian_breakout.lookback", &grid.lookback);
    }
    if let Some(grid) = &entry.keltner_breakout {
        expand_usize(partials, "entry.keltner.ema_period", &grid.ema_period);
        expand_usize(partials, "entry.keltner.atr_period", &grid.atr_period);
        expand_f64(partials, "entry.keltner.multiplier", &grid.multiplier);
    }
    if let Some(grid) = &entry.supertrend_direction {
        expand_usize(partials, "entry.supertrend.period", &grid.period);
        expand_f64(partials, "entry.supertrend.multiplier", &grid.multiplier);
    }
    if let Some(grid) = &entry.macd_histogram_direction {
        expand_bool(partials, "entry.macd_histogram.enabled", &grid.enabled);
    }
    if let Some(grid) = &entry.roc_direction {
        expand_usize(partials, "entry.roc.period", &grid.period);
        expand_f64(partials, "entry.roc.threshold", &grid.threshold);
    }
}

fn expand_filters(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    filters: &TrendFactorFilterConfig,
) {
    if let Some(values) = &filters.adx_min {
        expand_f64(partials, "filter.adx_min", values);
    }
    if let Some(grid) = &filters.rsi_range {
        expand_f64(partials, "filter.rsi_min", &grid.min);
        expand_f64(partials, "filter.rsi_max", &grid.max);
    }
    if let Some(grid) = &filters.bb_width_min {
        expand_usize(partials, "filter.bb_width.period", &grid.period);
        expand_f64(partials, "filter.bb_width.threshold", &grid.threshold);
    }
    if let Some(grid) = &filters.volume_ratio_min {
        expand_usize(partials, "filter.volume_ratio.period", &grid.period);
        expand_f64(partials, "filter.volume_ratio.threshold", &grid.threshold);
    }
    if let Some(grid) = &filters.volume_zscore_min {
        expand_usize(partials, "filter.volume_zscore.period", &grid.period);
        expand_f64(partials, "filter.volume_zscore.threshold", &grid.threshold);
    }
    if let Some(grid) = &filters.vwap_deviation_max_abs {
        expand_usize(partials, "filter.vwap_deviation.period", &grid.period);
        expand_f64(partials, "filter.vwap_deviation.max_abs", &grid.threshold);
    }
    if let Some(grid) = &filters.mfi_range {
        expand_usize(partials, "filter.mfi.period", &grid.period);
        expand_f64(partials, "filter.mfi.min", &grid.min);
        expand_f64(partials, "filter.mfi.max", &grid.max);
    }
    if let Some(grid) = &filters.stoch_rsi_range {
        expand_usize(partials, "filter.stoch_rsi.rsi_period", &grid.rsi_period);
        expand_usize(
            partials,
            "filter.stoch_rsi.stoch_period",
            &grid.stoch_period,
        );
        expand_f64(partials, "filter.stoch_rsi.min", &grid.min);
        expand_f64(partials, "filter.stoch_rsi.max", &grid.max);
    }
    if let Some(grid) = &filters.choppiness_max {
        expand_usize(partials, "filter.choppiness.period", &grid.period);
        expand_f64(partials, "filter.choppiness.max", &grid.threshold);
    }
    if let Some(grid) = &filters.atr_percentile_range {
        expand_usize(
            partials,
            "filter.atr_percentile.atr_period",
            &grid.atr_period,
        );
        expand_usize(
            partials,
            "filter.atr_percentile.window",
            &grid.percentile_window,
        );
        expand_f64(partials, "filter.atr_percentile.min", &grid.min);
        expand_f64(partials, "filter.atr_percentile.max", &grid.max);
    }
    if let Some(grid) = &filters.atr_pct_range {
        expand_usize(partials, "filter.atr_pct.period", &grid.period);
        expand_f64(partials, "filter.atr_pct.min", &grid.min);
        expand_f64(partials, "filter.atr_pct.max", &grid.max);
    }
}

fn expand_exit(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    exit: &TrendFactorExitConfig,
) -> Result<()> {
    if exit.atr_stop.is_empty() || exit.reward_r.is_empty() || exit.time_stop_bars.is_empty() {
        return Err(anyhow!(
            "exit atr_stop, reward_r and time_stop_bars must be non-empty"
        ));
    }
    expand_f64(partials, "exit.atr_stop", &exit.atr_stop);
    expand_f64(partials, "exit.reward_r", &exit.reward_r);
    expand_usize(partials, "exit.time_stop_bars", &exit.time_stop_bars);
    if let Some(grid) = &exit.trailing_atr {
        expand_usize(partials, "exit.trailing_atr.period", &grid.period);
        expand_f64(partials, "exit.trailing_atr.multiplier", &grid.multiplier);
        expand_f64(
            partials,
            "exit.trailing_atr.activation_r",
            &grid.activation_r,
        );
    }
    if let Some(grid) = &exit.donchian_exit {
        expand_usize(partials, "exit.donchian.lookback", &grid.lookback);
    }
    for (_, flags) in partials.iter_mut() {
        flags.insert(
            "exit.reverse_signal_exit".to_string(),
            exit.reverse_signal_exit,
        );
    }
    Ok(())
}

fn expand_f64(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    key: &str,
    values: &[f64],
) {
    let current = std::mem::take(partials);
    for (parameters, flags) in current {
        for value in values {
            let mut next_params = parameters.clone();
            next_params.insert(key.to_string(), *value);
            partials.push((next_params, flags.clone()));
        }
    }
}

fn expand_usize(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    key: &str,
    values: &[usize],
) {
    expand_f64(
        partials,
        key,
        &values.iter().map(|value| *value as f64).collect::<Vec<_>>(),
    );
}

fn expand_bool(
    partials: &mut Vec<(BTreeMap<String, f64>, BTreeMap<String, bool>)>,
    key: &str,
    values: &[bool],
) {
    let current = std::mem::take(partials);
    for (parameters, flags) in current {
        for value in values {
            let mut next_flags = flags.clone();
            next_flags.insert(key.to_string(), *value);
            partials.push((parameters.clone(), next_flags));
        }
    }
}

fn default_initial_equity() -> f64 {
    10_000.0
}

fn default_cost_scenarios() -> Vec<TrendCostScenario> {
    vec![TrendCostScenario {
        name: "base".to_string(),
        fee_rate: default_fee_rate(),
        slippage_rate: default_slippage_rate(),
    }]
}

fn default_min_profit_factor() -> f64 {
    1.0
}

fn default_max_drawdown_pct() -> f64 {
    -10.0
}

fn default_required_profitable_cost_scenarios() -> usize {
    1
}
fn default_trade_notional() -> f64 {
    1_000.0
}
fn default_fee_rate() -> f64 {
    0.001
}
fn default_slippage_rate() -> f64 {
    0.0002
}
fn default_min_trades() -> usize {
    20
}
fn default_sort_by() -> String {
    "composite_score".to_string()
}
fn default_true_values() -> Vec<bool> {
    vec![true]
}
