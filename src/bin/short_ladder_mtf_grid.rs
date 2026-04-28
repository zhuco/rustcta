use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::backtest::data::kline_dataset::KlineDatasetReader;
use rustcta::backtest::strategy::short_ladder::{
    run_short_ladder_mtf_execution_backtest, EntrySignalMode, LayerPriceMode, ShortLadderConfig,
    ShortLadderMode, ShortLadderMtfExecutionConfig, ShortLadderMtfExecutionRunReport,
    ShortLadderTrade, TakeProfitMode,
};
use rustcta::core::types::Kline;

#[derive(Debug, Parser)]
#[command(name = "short-ladder-mtf-grid")]
struct Args {
    #[arg(long)]
    dataset: PathBuf,
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    symbols: String,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long, default_value = "300,600,900,1200")]
    order_cooldown_secs: String,
    #[arg(long, default_value = "15,30,45,60")]
    min_minutes_to_l4: String,
    #[arg(long, default_value = "1,2,3")]
    max_layers_per_hour: String,
    #[arg(long, default_value = "adverse_averaging")]
    mode: String,
    #[arg(long, default_value = "5m")]
    signal_interval: String,
    #[arg(long, default_value = "1m")]
    execution_interval: String,
    #[arg(long, default_value = "fixed:1.2")]
    take_profit_specs: String,
    #[arg(long, default_value = "atr")]
    layer_price_specs: String,
    #[arg(long, default_value = "legacy")]
    entry_signal_specs: String,
    #[arg(long)]
    initial_notional_specs: Option<String>,
    #[arg(long)]
    layer_weight_specs: Option<String>,
    #[arg(long)]
    stop_loss_atr_specs: Option<String>,
    #[arg(long)]
    stop_loss_min_layers_specs: Option<String>,
    #[arg(long)]
    initial_order_taker_fallback_secs: Option<u64>,
    #[arg(long)]
    entry_range_position_specs: Option<String>,
    #[arg(long)]
    final_layer_specs: Option<String>,
    #[arg(long)]
    skip_json_reports: bool,
}

#[derive(Debug, Clone)]
struct TakeProfitSpec {
    label: String,
    mode: TakeProfitMode,
    fixed_atr: f64,
    activation_atr: f64,
    distance_atr: f64,
}

#[derive(Debug, Clone)]
struct LayerPriceSpec {
    label: String,
    mode: LayerPriceMode,
    resistance_lookback_bars: usize,
    resistance_buffer_atr: f64,
    min_loss_bps: Vec<f64>,
}

#[derive(Debug, Clone)]
struct EntrySignalSpec {
    label: String,
    mode: EntrySignalMode,
    rsi_min: f64,
    resistance_lookback_bars: usize,
    upper_wick_min_ratio: f64,
    breakdown_lookback_bars: usize,
    pullback_max_bars: usize,
    pullback_tolerance_atr: f64,
    body_atr_min: f64,
    range_atr_min: f64,
    close_near_low_ratio: f64,
    volume_sma_bars: usize,
    volume_multiplier: f64,
}

#[derive(Debug, Clone)]
struct CoreRiskSpec {
    label: String,
    initial_notional: Option<f64>,
    layer_weights: Option<Vec<f64>>,
    stop_loss_atr: Option<f64>,
    stop_loss_min_layers: Option<usize>,
}

#[derive(Debug, Clone)]
struct EntryRangePositionSpec {
    label: String,
    lookback_bars: usize,
    min: f64,
    max: f64,
}

#[derive(Debug, Clone)]
struct FinalLayerSpec {
    label: String,
    spacing_atr: Option<f64>,
    lookback_bars: Option<usize>,
    buffer_atr: Option<f64>,
}

#[derive(Debug, Clone)]
struct GridSummaryRow {
    symbol: String,
    order_cooldown_secs: u64,
    min_minutes_to_l4: u64,
    max_layers_per_hour: usize,
    entry_signal_label: String,
    entry_signal_mode: EntrySignalMode,
    layer_price_label: String,
    layer_price_mode: LayerPriceMode,
    layer_resistance_lookback_bars: usize,
    layer_resistance_buffer_atr: f64,
    layer_min_loss_bps: String,
    take_profit_label: String,
    take_profit_mode: TakeProfitMode,
    take_profit_atr: f64,
    trailing_activation_atr: f64,
    trailing_distance_atr: f64,
    annualized_return_pct: f64,
    rows_1m: usize,
    bars_1m: usize,
    trades: usize,
    wins: usize,
    losses: usize,
    win_rate_pct: f64,
    net_pnl: f64,
    roi_pct: f64,
    profit_factor: f64,
    max_drawdown_pct: f64,
    max_layers_filled: usize,
    max_position_notional: f64,
    total_fees_paid: f64,
    take_profit_count: usize,
    trailing_take_profit_count: usize,
    stop_loss_count: usize,
    time_stop_count: usize,
    session_end_count: usize,
    l4_trades: usize,
    l4_stop_loss_count: usize,
    l4_net_pnl: f64,
    fast_l4_25_count: usize,
    fast_l4_25_stop_loss_count: usize,
    fast_l4_25_net_pnl: f64,
    avg_minutes_to_l4: f64,
    worst_trade_net_pnl: f64,
    max_unrealized_loss: f64,
    report_path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = load_short_ladder_config(&args.config)?;
    let mode = parse_mode(&args.mode)?;
    let symbols = parse_symbols(&args.symbols)?;
    let cooldowns = parse_list::<u64>(&args.order_cooldown_secs, "order_cooldown_secs")?;
    let min_l4_values = parse_list::<u64>(&args.min_minutes_to_l4, "min_minutes_to_l4")?;
    let max_layers_values = parse_list::<usize>(&args.max_layers_per_hour, "max_layers_per_hour")?;
    let take_profit_specs = parse_take_profit_specs(&args.take_profit_specs)?;
    let layer_price_specs = parse_layer_price_specs(&args.layer_price_specs)?;
    let entry_signal_specs = parse_entry_signal_specs(&args.entry_signal_specs)?;
    let core_risk_specs = parse_core_risk_specs(
        args.initial_notional_specs.as_deref(),
        args.layer_weight_specs.as_deref(),
        args.stop_loss_atr_specs.as_deref(),
        args.stop_loss_min_layers_specs.as_deref(),
    )?;
    let entry_range_position_specs =
        parse_entry_range_position_specs(args.entry_range_position_specs.as_deref())?;
    let final_layer_specs = parse_final_layer_specs(args.final_layer_specs.as_deref())?;
    let start = args
        .start
        .parse::<DateTime<Utc>>()
        .map_err(|err| anyhow!("invalid start {}: {}", args.start, err))?;
    let end = args
        .end
        .parse::<DateTime<Utc>>()
        .map_err(|err| anyhow!("invalid end {}: {}", args.end, err))?;

    if args.skip_json_reports {
        fs::create_dir_all(&args.output_dir)?;
    } else {
        fs::create_dir_all(args.output_dir.join("reports"))?;
    }
    let reader = KlineDatasetReader::new(&args.dataset);
    let mut rows = Vec::new();

    for symbol in symbols {
        let (_, klines) = reader.read_binance_futures_klines(&symbol, &args.execution_interval)?;
        let filtered = filter_klines(&klines, start, end);
        println!(
            "{} loaded {} {} candles from {} to {}",
            symbol,
            filtered.len(),
            args.execution_interval,
            start,
            end
        );

        for cooldown in &cooldowns {
            for min_minutes_to_l4 in &min_l4_values {
                for max_layers_per_hour in &max_layers_values {
                    for core_risk_spec in &core_risk_specs {
                        for entry_range_position_spec in &entry_range_position_specs {
                            for final_layer_spec in &final_layer_specs {
                                for entry_signal_spec in &entry_signal_specs {
                                    for layer_price_spec in &layer_price_specs {
                                        for take_profit_spec in &take_profit_specs {
                                            let execution_config = ShortLadderMtfExecutionConfig {
                                                signal_interval: args.signal_interval.clone(),
                                                execution_interval: args.execution_interval.clone(),
                                                order_cooldown_secs: *cooldown,
                                                min_minutes_to_l4: *min_minutes_to_l4,
                                                max_layers_per_hour: *max_layers_per_hour,
                                            };
                                            let mut run_config = config.clone();
                                            apply_core_risk_spec(&mut run_config, core_risk_spec);
                                            apply_entry_range_position_spec(
                                                &mut run_config,
                                                entry_range_position_spec,
                                            );
                                            apply_final_layer_spec(
                                                &mut run_config,
                                                final_layer_spec,
                                            );
                                            if let Some(fallback_secs) =
                                                args.initial_order_taker_fallback_secs
                                            {
                                                run_config.initial_order_taker_fallback_secs =
                                                    fallback_secs;
                                            }
                                            apply_entry_signal_spec(
                                                &mut run_config,
                                                entry_signal_spec,
                                            );
                                            apply_layer_price_spec(
                                                &mut run_config,
                                                layer_price_spec,
                                            );
                                            apply_take_profit_spec(
                                                &mut run_config,
                                                take_profit_spec,
                                            );
                                            let report = run_short_ladder_mtf_execution_backtest(
                                                &symbol,
                                                &filtered,
                                                mode,
                                                run_config,
                                                execution_config,
                                            );
                                            let report_label = format!(
                                                "{}_{}_{}_{}_{}_{}",
                                                core_risk_spec.label,
                                                entry_range_position_spec.label,
                                                final_layer_spec.label,
                                                entry_signal_spec.label,
                                                layer_price_spec.label,
                                                take_profit_spec.label
                                            );
                                            let report_path =
                                                args.output_dir.join("reports").join(format!(
                                                    "{}_{}_cd{}_l4{}m_mph{}.json",
                                                    sanitize_filename(&symbol),
                                                    sanitize_filename(&report_label),
                                                    cooldown,
                                                    min_minutes_to_l4,
                                                    max_layers_per_hour
                                                ));
                                            if !args.skip_json_reports {
                                                fs::write(
                                                    &report_path,
                                                    serde_json::to_vec_pretty(&report)?,
                                                )?;
                                            }
                                            rows.push(summarize_report(
                                                symbol.clone(),
                                                filtered.len(),
                                                report_path,
                                                &report,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    write_summary_csv(&args.output_dir.join("summary.csv"), &rows)?;
    write_report_markdown(&args.output_dir.join("report.md"), &args, &rows)?;
    println!(
        "completed {} runs; summary written to {}",
        rows.len(),
        args.output_dir.join("summary.csv").display()
    );
    Ok(())
}

fn load_short_ladder_config(path: &Path) -> Result<ShortLadderConfig> {
    let content = fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

fn parse_mode(value: &str) -> Result<ShortLadderMode> {
    match value {
        "adverse_averaging" | "a" | "A" => Ok(ShortLadderMode::AdverseAveraging),
        "favorable_pyramiding" | "b" | "B" => Ok(ShortLadderMode::FavorablePyramiding),
        other => Err(anyhow!("unsupported short ladder mode {other}")),
    }
}

fn parse_take_profit_specs(value: &str) -> Result<Vec<TakeProfitSpec>> {
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let parts = item.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["fixed", take_profit_atr] => {
                let fixed_atr = take_profit_atr.parse::<f64>().map_err(|err| {
                    anyhow!("invalid fixed take profit atr {}: {}", take_profit_atr, err)
                })?;
                specs.push(TakeProfitSpec {
                    label: format!("fixed_{fixed_atr:.2}atr"),
                    mode: TakeProfitMode::FixedAtr,
                    fixed_atr,
                    activation_atr: 0.0,
                    distance_atr: 0.0,
                });
            }
            ["trail", activation_atr, distance_atr] => {
                let activation_atr = activation_atr.parse::<f64>().map_err(|err| {
                    anyhow!(
                        "invalid trailing activation atr {}: {}",
                        activation_atr,
                        err
                    )
                })?;
                let distance_atr = distance_atr.parse::<f64>().map_err(|err| {
                    anyhow!("invalid trailing distance atr {}: {}", distance_atr, err)
                })?;
                specs.push(TakeProfitSpec {
                    label: format!("trail_a{activation_atr:.2}_d{distance_atr:.2}"),
                    mode: TakeProfitMode::AtrTrailing,
                    fixed_atr: 0.0,
                    activation_atr,
                    distance_atr,
                });
            }
            _ => {
                return Err(anyhow!(
                    "invalid take_profit_specs item {}; use fixed:<atr> or trail:<activation_atr>:<distance_atr>",
                    item
                ));
            }
        }
    }
    if specs.is_empty() {
        return Err(anyhow!("take_profit_specs must not be empty"));
    }
    Ok(specs)
}

fn parse_layer_price_specs(value: &str) -> Result<Vec<LayerPriceSpec>> {
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let parts = item.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["atr"] | ["atr_spacing"] => specs.push(LayerPriceSpec {
                label: "layer_atr".to_string(),
                mode: LayerPriceMode::AtrSpacing,
                resistance_lookback_bars: 0,
                resistance_buffer_atr: 0.0,
                min_loss_bps: Vec::new(),
            }),
            ["struct", lookback, buffer_atr, min_loss_bps]
            | ["res", lookback, buffer_atr, min_loss_bps]
            | ["atr_resistance_loss", lookback, buffer_atr, min_loss_bps] => {
                let resistance_lookback_bars = lookback.parse::<usize>().map_err(|err| {
                    anyhow!("invalid layer resistance lookback {}: {}", lookback, err)
                })?;
                let resistance_buffer_atr = buffer_atr.parse::<f64>().map_err(|err| {
                    anyhow!(
                        "invalid layer resistance buffer atr {}: {}",
                        buffer_atr,
                        err
                    )
                })?;
                let min_loss_bps = parse_bps_vector(min_loss_bps)?;
                let loss_label = min_loss_bps
                    .iter()
                    .map(|value| format!("{value:.0}"))
                    .collect::<Vec<_>>()
                    .join("-");
                specs.push(LayerPriceSpec {
                    label: format!(
                        "layer_struct_lb{}_buf{:.2}_loss{}",
                        resistance_lookback_bars, resistance_buffer_atr, loss_label
                    ),
                    mode: LayerPriceMode::AtrResistanceLoss,
                    resistance_lookback_bars,
                    resistance_buffer_atr,
                    min_loss_bps,
                });
            }
            _ => {
                return Err(anyhow!(
                    "invalid layer_price_specs item {}; use atr or struct:<lookback_bars>:<buffer_atr>:<loss-loss-loss-loss>",
                    item
                ));
            }
        }
    }
    if specs.is_empty() {
        return Err(anyhow!("layer_price_specs must not be empty"));
    }
    Ok(specs)
}

fn parse_entry_signal_specs(value: &str) -> Result<Vec<EntrySignalSpec>> {
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let parts = item.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["legacy"] | ["legacy_rsi"] => specs.push(default_entry_spec(
                "entry_legacy".to_string(),
                EntrySignalMode::LegacyRsi,
            )),
            ["sweep", lookback, wick_ratio, rsi_min]
            | ["resistance_sweep_rejection", lookback, wick_ratio, rsi_min] => {
                let mut spec = default_entry_spec(
                    format!(
                        "entry_sweep_lb{}_wick{}_rsi{}",
                        lookback, wick_ratio, rsi_min
                    ),
                    EntrySignalMode::ResistanceSweepRejection,
                );
                spec.resistance_lookback_bars = parse_spec_value(lookback, "sweep lookback")?;
                spec.upper_wick_min_ratio = parse_spec_value(wick_ratio, "sweep wick ratio")?;
                spec.rsi_min = parse_spec_value(rsi_min, "sweep rsi min")?;
                specs.push(spec);
            }
            ["pullback", lookback, max_bars, tolerance_atr]
            | ["breakdown_pullback", lookback, max_bars, tolerance_atr] => {
                let mut spec = default_entry_spec(
                    format!(
                        "entry_pullback_lb{}_bars{}_tol{}",
                        lookback, max_bars, tolerance_atr
                    ),
                    EntrySignalMode::BreakdownPullback,
                );
                spec.breakdown_lookback_bars = parse_spec_value(lookback, "pullback lookback")?;
                spec.pullback_max_bars = parse_spec_value(max_bars, "pullback max bars")?;
                spec.pullback_tolerance_atr =
                    parse_spec_value(tolerance_atr, "pullback tolerance atr")?;
                specs.push(spec);
            }
            ["vol", body_atr, range_atr, close_near_low, volume_multiplier]
            | ["bearish_volatility_expansion", body_atr, range_atr, close_near_low, volume_multiplier] =>
            {
                let mut spec = default_entry_spec(
                    format!(
                        "entry_vol_body{}_range{}_close{}_vol{}",
                        body_atr, range_atr, close_near_low, volume_multiplier
                    ),
                    EntrySignalMode::BearishVolatilityExpansion,
                );
                spec.body_atr_min = parse_spec_value(body_atr, "vol body atr")?;
                spec.range_atr_min = parse_spec_value(range_atr, "vol range atr")?;
                spec.close_near_low_ratio = parse_spec_value(close_near_low, "vol close near low")?;
                spec.volume_multiplier = parse_spec_value(volume_multiplier, "vol multiplier")?;
                specs.push(spec);
            }
            _ => {
                return Err(anyhow!(
                    "invalid entry_signal_specs item {}; use legacy, sweep:<lookback>:<wick>:<rsi>, pullback:<lookback>:<bars>:<tol_atr>, or vol:<body_atr>:<range_atr>:<close_low>:<vol_mult>",
                    item
                ));
            }
        }
    }
    if specs.is_empty() {
        return Err(anyhow!("entry_signal_specs must not be empty"));
    }
    Ok(specs)
}

fn default_entry_spec(label: String, mode: EntrySignalMode) -> EntrySignalSpec {
    EntrySignalSpec {
        label,
        mode,
        rsi_min: 52.0,
        resistance_lookback_bars: 24,
        upper_wick_min_ratio: 0.35,
        breakdown_lookback_bars: 24,
        pullback_max_bars: 6,
        pullback_tolerance_atr: 0.3,
        body_atr_min: 0.6,
        range_atr_min: 1.2,
        close_near_low_ratio: 0.25,
        volume_sma_bars: 24,
        volume_multiplier: 1.2,
    }
}

fn parse_spec_value<T>(value: &str, name: &str) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse::<T>()
        .map_err(|err| anyhow!("invalid {name} value {}: {}", value, err))
}

fn parse_bps_vector(value: &str) -> Result<Vec<f64>> {
    let values = value
        .split('-')
        .map(|item| {
            item.trim()
                .parse::<f64>()
                .map_err(|err| anyhow!("invalid layer min loss bps {}: {}", item.trim(), err))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("layer min loss bps must not be empty"));
    }
    Ok(values)
}

fn parse_weight_vector(value: &str) -> Result<Vec<f64>> {
    let values = value
        .split('-')
        .map(|item| {
            item.trim()
                .parse::<f64>()
                .map_err(|err| anyhow!("invalid layer weight {}: {}", item.trim(), err))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("layer weights must not be empty"));
    }
    Ok(values)
}

fn parse_optional_list<T>(value: Option<&str>, name: &str) -> Result<Vec<Option<T>>>
where
    T: FromStr,
    T::Err: Display,
{
    let Some(value) = value else {
        return Ok(vec![None]);
    };
    let mut parsed = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        parsed
            .push(Some(item.parse::<T>().map_err(|err| {
                anyhow!("invalid {name} value {}: {}", item, err)
            })?));
    }
    if parsed.is_empty() {
        return Err(anyhow!("{name} must not be empty"));
    }
    Ok(parsed)
}

fn parse_optional_weight_specs(value: Option<&str>) -> Result<Vec<Option<Vec<f64>>>> {
    let Some(value) = value else {
        return Ok(vec![None]);
    };
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        specs.push(Some(parse_weight_vector(item)?));
    }
    if specs.is_empty() {
        return Err(anyhow!("layer_weight_specs must not be empty"));
    }
    Ok(specs)
}

fn parse_core_risk_specs(
    initial_notional_specs: Option<&str>,
    layer_weight_specs: Option<&str>,
    stop_loss_atr_specs: Option<&str>,
    stop_loss_min_layers_specs: Option<&str>,
) -> Result<Vec<CoreRiskSpec>> {
    let notionals = parse_optional_list::<f64>(initial_notional_specs, "initial_notional_specs")?;
    let weights = parse_optional_weight_specs(layer_weight_specs)?;
    let stop_atrs = parse_optional_list::<f64>(stop_loss_atr_specs, "stop_loss_atr_specs")?;
    let stop_layers =
        parse_optional_list::<usize>(stop_loss_min_layers_specs, "stop_loss_min_layers_specs")?;
    let mut specs = Vec::new();
    for initial_notional in &notionals {
        for layer_weights in &weights {
            for stop_loss_atr in &stop_atrs {
                for stop_loss_min_layers in &stop_layers {
                    let mut parts = Vec::new();
                    if let Some(value) = initial_notional {
                        parts.push(format!("l1{value:.0}"));
                    }
                    if let Some(values) = layer_weights {
                        parts.push(format!(
                            "w{}",
                            values
                                .iter()
                                .map(|value| format!("{value:.3}"))
                                .collect::<Vec<_>>()
                                .join("-")
                        ));
                    }
                    if let Some(value) = stop_loss_atr {
                        parts.push(format!("sl{value:.2}"));
                    }
                    if let Some(value) = stop_loss_min_layers {
                        parts.push(format!("slmin{value}"));
                    }
                    specs.push(CoreRiskSpec {
                        label: if parts.is_empty() {
                            "risk_base".to_string()
                        } else {
                            parts.join("_")
                        },
                        initial_notional: *initial_notional,
                        layer_weights: layer_weights.clone(),
                        stop_loss_atr: *stop_loss_atr,
                        stop_loss_min_layers: *stop_loss_min_layers,
                    });
                }
            }
        }
    }
    Ok(specs)
}

fn parse_entry_range_position_specs(value: Option<&str>) -> Result<Vec<EntryRangePositionSpec>> {
    let Some(value) = value else {
        return Ok(vec![EntryRangePositionSpec {
            label: "pos_base".to_string(),
            lookback_bars: 0,
            min: 0.0,
            max: 1.0,
        }]);
    };
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        match item {
            "off" | "base" => specs.push(EntryRangePositionSpec {
                label: "pos_base".to_string(),
                lookback_bars: 0,
                min: 0.0,
                max: 1.0,
            }),
            _ => {
                let parts = item.split(':').collect::<Vec<_>>();
                match parts.as_slice() {
                    ["pos", lookback, min, max] | ["golden", lookback, min, max] => {
                        let lookback_bars = parse_spec_value(lookback, "entry range lookback")?;
                        let min = parse_spec_value(min, "entry range min")?;
                        let max = parse_spec_value(max, "entry range max")?;
                        specs.push(EntryRangePositionSpec {
                            label: format!("pos_lb{}_{}-{}", lookback, min, max),
                            lookback_bars,
                            min,
                            max,
                        });
                    }
                    _ => {
                        return Err(anyhow!(
                            "invalid entry_range_position_specs item {}; use off or pos:<lookback>:<min>:<max>",
                            item
                        ));
                    }
                }
            }
        }
    }
    if specs.is_empty() {
        return Err(anyhow!("entry_range_position_specs must not be empty"));
    }
    Ok(specs)
}

fn parse_final_layer_specs(value: Option<&str>) -> Result<Vec<FinalLayerSpec>> {
    let Some(value) = value else {
        return Ok(vec![FinalLayerSpec {
            label: "l4_base".to_string(),
            spacing_atr: None,
            lookback_bars: None,
            buffer_atr: None,
        }]);
    };
    let mut specs = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        match item {
            "base" | "off" => specs.push(FinalLayerSpec {
                label: "l4_base".to_string(),
                spacing_atr: None,
                lookback_bars: None,
                buffer_atr: None,
            }),
            _ => {
                let parts = item.split(':').collect::<Vec<_>>();
                match parts.as_slice() {
                    ["l4", spacing_atr, lookback_bars, buffer_atr] => {
                        specs.push(FinalLayerSpec {
                            label: format!(
                                "l4_sp{}_lb{}_buf{}",
                                spacing_atr, lookback_bars, buffer_atr
                            ),
                            spacing_atr: Some(parse_spec_value(
                                spacing_atr,
                                "final layer spacing atr",
                            )?),
                            lookback_bars: Some(parse_spec_value(
                                lookback_bars,
                                "final layer lookback bars",
                            )?),
                            buffer_atr: Some(parse_spec_value(
                                buffer_atr,
                                "final layer buffer atr",
                            )?),
                        });
                    }
                    _ => {
                        return Err(anyhow!(
                            "invalid final_layer_specs item {}; use base or l4:<spacing_atr>:<lookback_bars>:<buffer_atr>",
                            item
                        ));
                    }
                }
            }
        }
    }
    if specs.is_empty() {
        return Err(anyhow!("final_layer_specs must not be empty"));
    }
    Ok(specs)
}

fn apply_take_profit_spec(config: &mut ShortLadderConfig, spec: &TakeProfitSpec) {
    config.take_profit_mode = spec.mode;
    if matches!(spec.mode, TakeProfitMode::FixedAtr) {
        config.take_profit_atr = spec.fixed_atr;
    } else {
        config.trailing_take_profit_activation_atr = spec.activation_atr;
        config.trailing_take_profit_distance_atr = spec.distance_atr;
    }
}

fn apply_core_risk_spec(config: &mut ShortLadderConfig, spec: &CoreRiskSpec) {
    if let Some(initial_notional) = spec.initial_notional {
        config.initial_notional = initial_notional;
    }
    if let Some(layer_weights) = &spec.layer_weights {
        config.layer_weights = layer_weights.clone();
    }
    if let Some(stop_loss_atr) = spec.stop_loss_atr {
        config.stop_loss_atr = stop_loss_atr;
    }
    if let Some(stop_loss_min_layers) = spec.stop_loss_min_layers {
        config.stop_loss_min_layers = stop_loss_min_layers;
    }
}

fn apply_entry_range_position_spec(config: &mut ShortLadderConfig, spec: &EntryRangePositionSpec) {
    config.entry_range_position_lookback_bars = spec.lookback_bars;
    config.entry_range_position_min = spec.min;
    config.entry_range_position_max = spec.max;
}

fn apply_final_layer_spec(config: &mut ShortLadderConfig, spec: &FinalLayerSpec) {
    config.final_layer_spacing_atr = spec.spacing_atr;
    config.final_layer_resistance_lookback_bars = spec.lookback_bars;
    config.final_layer_resistance_buffer_atr = spec.buffer_atr;
}

fn apply_layer_price_spec(config: &mut ShortLadderConfig, spec: &LayerPriceSpec) {
    config.layer_price_mode = spec.mode;
    if matches!(spec.mode, LayerPriceMode::AtrResistanceLoss) {
        config.layer_resistance_lookback_bars = spec.resistance_lookback_bars;
        config.layer_resistance_buffer_atr = spec.resistance_buffer_atr;
        config.layer_min_loss_bps = spec.min_loss_bps.clone();
    }
}

fn apply_entry_signal_spec(config: &mut ShortLadderConfig, spec: &EntrySignalSpec) {
    config.entry_signal_mode = spec.mode;
    config.entry_rsi_min = spec.rsi_min;
    config.entry_resistance_lookback_bars = spec.resistance_lookback_bars;
    config.entry_upper_wick_min_ratio = spec.upper_wick_min_ratio;
    config.entry_breakdown_lookback_bars = spec.breakdown_lookback_bars;
    config.entry_pullback_max_bars = spec.pullback_max_bars;
    config.entry_pullback_tolerance_atr = spec.pullback_tolerance_atr;
    config.entry_body_atr_min = spec.body_atr_min;
    config.entry_range_atr_min = spec.range_atr_min;
    config.entry_close_near_low_ratio = spec.close_near_low_ratio;
    config.entry_volume_sma_bars = spec.volume_sma_bars;
    config.entry_volume_multiplier = spec.volume_multiplier;
}

fn parse_symbols(value: &str) -> Result<Vec<String>> {
    value
        .split(',')
        .map(|item| normalize_symbol(item.trim()))
        .collect()
}

fn normalize_symbol(value: &str) -> Result<String> {
    let symbol = value.trim().to_ascii_uppercase();
    if symbol.contains('/') {
        return Ok(symbol);
    }
    for quote in ["USDC", "USDT", "BUSD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok(format!("{base}/{quote}"));
            }
        }
    }
    Err(anyhow!("unsupported symbol format {value}"))
}

fn parse_list<T>(value: &str, name: &str) -> Result<Vec<T>>
where
    T: FromStr,
    T::Err: Display,
{
    let parsed = value
        .split(',')
        .map(|item| {
            item.trim()
                .parse::<T>()
                .map_err(|err| anyhow!("invalid {name} value {}: {}", item.trim(), err))
        })
        .collect::<Result<Vec<_>>>()?;
    if parsed.is_empty() {
        return Err(anyhow!("{name} must not be empty"));
    }
    Ok(parsed)
}

fn filter_klines(klines: &[Kline], start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Kline> {
    klines
        .iter()
        .filter(|bar| bar.open_time >= start && bar.open_time < end)
        .cloned()
        .collect()
}

fn summarize_report(
    symbol: String,
    rows_1m: usize,
    report_path: PathBuf,
    report: &ShortLadderMtfExecutionRunReport,
) -> GridSummaryRow {
    let take_profit_count = count_exit_reason(&report.trades, "take_profit");
    let trailing_take_profit_count = count_exit_reason(&report.trades, "trailing_take_profit");
    let stop_loss_count = count_exit_reason(&report.trades, "stop_loss");
    let time_stop_count = count_exit_reason(&report.trades, "time_stop");
    let session_end_count = count_exit_reason(&report.trades, "session_end");
    let l4_trades = report
        .trades
        .iter()
        .filter(|trade| trade.layers_filled >= 4)
        .count();
    let l4_stop_loss_count = report
        .trades
        .iter()
        .filter(|trade| trade.layers_filled >= 4 && trade.exit_reason == "stop_loss")
        .count();
    let l4_net_pnl = report
        .trades
        .iter()
        .filter(|trade| trade.layers_filled >= 4)
        .map(|trade| trade.net_pnl)
        .sum();
    let fast_l4_trades = report
        .trades
        .iter()
        .filter(|trade| minutes_to_l4(trade).is_some_and(|minutes| minutes <= 25))
        .collect::<Vec<_>>();
    let fast_l4_25_count = fast_l4_trades.len();
    let fast_l4_25_stop_loss_count = fast_l4_trades
        .iter()
        .filter(|trade| trade.exit_reason == "stop_loss")
        .count();
    let fast_l4_25_net_pnl = fast_l4_trades.iter().map(|trade| trade.net_pnl).sum();
    let l4_minutes = report
        .trades
        .iter()
        .filter_map(minutes_to_l4)
        .collect::<Vec<_>>();
    let avg_minutes_to_l4 = if l4_minutes.is_empty() {
        0.0
    } else {
        l4_minutes.iter().sum::<i64>() as f64 / l4_minutes.len() as f64
    };
    let worst_trade_net_pnl = report
        .trades
        .iter()
        .map(|trade| trade.net_pnl)
        .fold(0.0, f64::min);
    let max_unrealized_loss = report
        .trades
        .iter()
        .map(|trade| trade.max_unrealized_loss)
        .fold(0.0, f64::max);

    GridSummaryRow {
        symbol,
        order_cooldown_secs: report.execution_config.order_cooldown_secs,
        min_minutes_to_l4: report.execution_config.min_minutes_to_l4,
        max_layers_per_hour: report.execution_config.max_layers_per_hour,
        entry_signal_label: entry_signal_label(&report.config),
        entry_signal_mode: report.config.entry_signal_mode,
        layer_price_label: layer_price_label(&report.config),
        layer_price_mode: report.config.layer_price_mode,
        layer_resistance_lookback_bars: report.config.layer_resistance_lookback_bars,
        layer_resistance_buffer_atr: report.config.layer_resistance_buffer_atr,
        layer_min_loss_bps: report
            .config
            .layer_min_loss_bps
            .iter()
            .map(|value| format!("{value:.0}"))
            .collect::<Vec<_>>()
            .join("-"),
        take_profit_label: take_profit_label(&report.config),
        take_profit_mode: report.config.take_profit_mode,
        take_profit_atr: report.config.take_profit_atr,
        trailing_activation_atr: report.config.trailing_take_profit_activation_atr,
        trailing_distance_atr: report.config.trailing_take_profit_distance_atr,
        annualized_return_pct: report.summary.annualized_return_pct,
        rows_1m,
        bars_1m: report.summary.bars,
        trades: report.summary.trades,
        wins: report.summary.wins,
        losses: report.summary.losses,
        win_rate_pct: report.summary.win_rate_pct,
        net_pnl: report.summary.net_pnl,
        roi_pct: report.summary.roi_pct,
        profit_factor: report.summary.profit_factor,
        max_drawdown_pct: report.summary.max_drawdown_pct,
        max_layers_filled: report.summary.max_layers_filled,
        max_position_notional: report.summary.max_position_notional,
        total_fees_paid: report.summary.total_fees_paid,
        take_profit_count,
        trailing_take_profit_count,
        stop_loss_count,
        time_stop_count,
        session_end_count,
        l4_trades,
        l4_stop_loss_count,
        l4_net_pnl,
        fast_l4_25_count,
        fast_l4_25_stop_loss_count,
        fast_l4_25_net_pnl,
        avg_minutes_to_l4,
        worst_trade_net_pnl,
        max_unrealized_loss,
        report_path,
    }
}

fn count_exit_reason(trades: &[ShortLadderTrade], reason: &str) -> usize {
    trades
        .iter()
        .filter(|trade| trade.exit_reason == reason)
        .count()
}

fn minutes_to_l4(trade: &ShortLadderTrade) -> Option<i64> {
    trade
        .entry_times
        .get(3)
        .zip(trade.entry_times.first())
        .map(|(l4_time, entry_time)| {
            l4_time
                .signed_duration_since(*entry_time)
                .num_minutes()
                .max(0)
        })
}

fn write_summary_csv(path: &Path, rows: &[GridSummaryRow]) -> Result<()> {
    let headers = [
        "symbol",
        "order_cooldown_secs",
        "min_minutes_to_l4",
        "max_layers_per_hour",
        "entry_signal_label",
        "entry_signal_mode",
        "layer_price_label",
        "layer_price_mode",
        "layer_resistance_lookback_bars",
        "layer_resistance_buffer_atr",
        "layer_min_loss_bps",
        "take_profit_label",
        "take_profit_mode",
        "take_profit_atr",
        "trailing_activation_atr",
        "trailing_distance_atr",
        "rows_1m",
        "bars_1m",
        "trades",
        "wins",
        "losses",
        "win_rate_pct",
        "net_pnl",
        "roi_pct",
        "annualized_return_pct",
        "profit_factor",
        "max_drawdown_pct",
        "max_layers_filled",
        "max_position_notional",
        "total_fees_paid",
        "take_profit_count",
        "trailing_take_profit_count",
        "stop_loss_count",
        "time_stop_count",
        "session_end_count",
        "l4_trades",
        "l4_stop_loss_count",
        "l4_net_pnl",
        "fast_l4_25_count",
        "fast_l4_25_stop_loss_count",
        "fast_l4_25_net_pnl",
        "avg_minutes_to_l4",
        "worst_trade_net_pnl",
        "max_unrealized_loss",
        "report_path",
    ];
    let mut output = String::new();
    output.push_str(&headers.join(","));
    output.push('\n');
    for row in rows {
        output.push_str(&csv_line(&[
            row.symbol.clone(),
            row.order_cooldown_secs.to_string(),
            row.min_minutes_to_l4.to_string(),
            row.max_layers_per_hour.to_string(),
            row.entry_signal_label.clone(),
            format!("{:?}", row.entry_signal_mode),
            row.layer_price_label.clone(),
            format!("{:?}", row.layer_price_mode),
            row.layer_resistance_lookback_bars.to_string(),
            fmt_f64(row.layer_resistance_buffer_atr),
            row.layer_min_loss_bps.clone(),
            row.take_profit_label.clone(),
            format!("{:?}", row.take_profit_mode),
            fmt_f64(row.take_profit_atr),
            fmt_f64(row.trailing_activation_atr),
            fmt_f64(row.trailing_distance_atr),
            row.rows_1m.to_string(),
            row.bars_1m.to_string(),
            row.trades.to_string(),
            row.wins.to_string(),
            row.losses.to_string(),
            fmt_f64(row.win_rate_pct),
            fmt_f64(row.net_pnl),
            fmt_f64(row.roi_pct),
            fmt_f64(row.annualized_return_pct),
            fmt_f64(row.profit_factor),
            fmt_f64(row.max_drawdown_pct),
            row.max_layers_filled.to_string(),
            fmt_f64(row.max_position_notional),
            fmt_f64(row.total_fees_paid),
            row.take_profit_count.to_string(),
            row.trailing_take_profit_count.to_string(),
            row.stop_loss_count.to_string(),
            row.time_stop_count.to_string(),
            row.session_end_count.to_string(),
            row.l4_trades.to_string(),
            row.l4_stop_loss_count.to_string(),
            fmt_f64(row.l4_net_pnl),
            row.fast_l4_25_count.to_string(),
            row.fast_l4_25_stop_loss_count.to_string(),
            fmt_f64(row.fast_l4_25_net_pnl),
            fmt_f64(row.avg_minutes_to_l4),
            fmt_f64(row.worst_trade_net_pnl),
            fmt_f64(row.max_unrealized_loss),
            row.report_path.display().to_string(),
        ]));
        output.push('\n');
    }
    fs::write(path, output)?;
    Ok(())
}

fn write_report_markdown(path: &Path, args: &Args, rows: &[GridSummaryRow]) -> Result<()> {
    let mut output = String::new();
    output.push_str("# Short Ladder MTF Execution Grid Report\n\n");
    output.push_str(&format!("- Dataset: `{}`\n", args.dataset.display()));
    output.push_str(&format!("- Config: `{}`\n", args.config.display()));
    output.push_str(&format!("- Window: `{}` ~ `{}`\n", args.start, args.end));
    output.push_str("- Signal/execution: `5m signal + 1m execution`\n");
    output.push_str(&format!(
        "- Take profit specs: `{}`\n",
        args.take_profit_specs
    ));
    output.push_str(&format!(
        "- Layer price specs: `{}`\n",
        args.layer_price_specs
    ));
    output.push_str(&format!(
        "- Entry signal specs: `{}`\n",
        args.entry_signal_specs
    ));
    output.push_str("- Fast L4 definition: L1 to L4 fill time <= 25 minutes\n\n");

    output.push_str("## Best By Symbol (ROI)\n\n");
    output.push_str("| symbol | entry | layer | tp | cooldown | min_to_l4 | max_layers/h | trades | roi% | annual% | pf | mdd% | stop | L4 | fastL4 | fastL4 pnl | fees |\n");
    output.push_str(
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for row in best_by_symbol(rows) {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            row.symbol,
            row.entry_signal_label,
            row.layer_price_label,
            row.take_profit_label,
            row.order_cooldown_secs,
            row.min_minutes_to_l4,
            row.max_layers_per_hour,
            row.trades,
            fmt_f64(row.roi_pct),
            fmt_f64(row.annualized_return_pct),
            fmt_f64(row.profit_factor),
            fmt_f64(row.max_drawdown_pct),
            row.stop_loss_count,
            row.l4_trades,
            row.fast_l4_25_count,
            fmt_f64(row.fast_l4_25_net_pnl),
            fmt_f64(row.total_fees_paid)
        ));
    }

    output.push_str("\n## Fast L4 Worst Cases\n\n");
    output.push_str("| symbol | cooldown | min_to_l4 | max_layers/h | fastL4 | fastL4 stops | fastL4 pnl | roi% | annual% | pf |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    let mut fast_rows = rows
        .iter()
        .filter(|row| row.fast_l4_25_count > 0)
        .collect::<Vec<_>>();
    fast_rows.sort_by(|left, right| left.fast_l4_25_net_pnl.total_cmp(&right.fast_l4_25_net_pnl));
    for row in fast_rows.into_iter().take(20) {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            row.symbol,
            row.order_cooldown_secs,
            row.min_minutes_to_l4,
            row.max_layers_per_hour,
            row.fast_l4_25_count,
            row.fast_l4_25_stop_loss_count,
            fmt_f64(row.fast_l4_25_net_pnl),
            fmt_f64(row.roi_pct),
            fmt_f64(row.annualized_return_pct),
            fmt_f64(row.profit_factor)
        ));
    }

    output.push_str("\n## Top 20 Overall (ROI)\n\n");
    output.push_str("| symbol | entry | layer | tp | cooldown | min_to_l4 | max_layers/h | trades | roi% | annual% | pf | mdd% | net pnl | fees |\n");
    output.push_str("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    let mut top_rows = rows.iter().collect::<Vec<_>>();
    top_rows.sort_by(|left, right| right.roi_pct.total_cmp(&left.roi_pct));
    for row in top_rows.into_iter().take(20) {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            row.symbol,
            row.entry_signal_label,
            row.layer_price_label,
            row.take_profit_label,
            row.order_cooldown_secs,
            row.min_minutes_to_l4,
            row.max_layers_per_hour,
            row.trades,
            fmt_f64(row.roi_pct),
            fmt_f64(row.annualized_return_pct),
            fmt_f64(row.profit_factor),
            fmt_f64(row.max_drawdown_pct),
            fmt_f64(row.net_pnl),
            fmt_f64(row.total_fees_paid)
        ));
    }

    fs::write(path, output)?;
    Ok(())
}

fn best_by_symbol(rows: &[GridSummaryRow]) -> Vec<&GridSummaryRow> {
    let mut symbols = rows
        .iter()
        .map(|row| row.symbol.clone())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    symbols
        .iter()
        .filter_map(|symbol| {
            rows.iter()
                .filter(|row| &row.symbol == symbol)
                .max_by(|left, right| left.roi_pct.total_cmp(&right.roi_pct))
        })
        .collect()
}

fn take_profit_label(config: &ShortLadderConfig) -> String {
    match config.take_profit_mode {
        TakeProfitMode::FixedAtr => format!("fixed_{:.2}atr", config.take_profit_atr),
        TakeProfitMode::AtrTrailing => format!(
            "trail_a{:.2}_d{:.2}",
            config.trailing_take_profit_activation_atr, config.trailing_take_profit_distance_atr
        ),
    }
}

fn layer_price_label(config: &ShortLadderConfig) -> String {
    match config.layer_price_mode {
        LayerPriceMode::AtrSpacing => "layer_atr".to_string(),
        LayerPriceMode::AtrResistanceLoss => format!(
            "layer_struct_lb{}_buf{:.2}_loss{}",
            config.layer_resistance_lookback_bars,
            config.layer_resistance_buffer_atr,
            config
                .layer_min_loss_bps
                .iter()
                .map(|value| format!("{value:.0}"))
                .collect::<Vec<_>>()
                .join("-")
        ),
    }
}

fn entry_signal_label(config: &ShortLadderConfig) -> String {
    match config.entry_signal_mode {
        EntrySignalMode::LegacyRsi => "entry_legacy".to_string(),
        EntrySignalMode::ResistanceSweepRejection => format!(
            "entry_sweep_lb{}_wick{:.2}_rsi{:.0}",
            config.entry_resistance_lookback_bars,
            config.entry_upper_wick_min_ratio,
            config.entry_rsi_min
        ),
        EntrySignalMode::BreakdownPullback => format!(
            "entry_pullback_lb{}_bars{}_tol{:.2}",
            config.entry_breakdown_lookback_bars,
            config.entry_pullback_max_bars,
            config.entry_pullback_tolerance_atr
        ),
        EntrySignalMode::BearishVolatilityExpansion => format!(
            "entry_vol_body{:.2}_range{:.2}_close{:.2}_vol{:.2}",
            config.entry_body_atr_min,
            config.entry_range_atr_min,
            config.entry_close_near_low_ratio,
            config.entry_volume_multiplier
        ),
    }
}

fn csv_line(fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| csv_escape(field))
        .collect::<Vec<_>>()
        .join(",")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn fmt_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.6}")
    } else if value.is_sign_positive() {
        "inf".to_string()
    } else {
        "-inf".to_string()
    }
}

fn sanitize_filename(symbol: &str) -> String {
    symbol
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
