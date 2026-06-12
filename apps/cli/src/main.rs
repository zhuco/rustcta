use anyhow::{bail, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use rustcta_event_ledger::{JsonlLedger, LedgerEvent, LedgerReader};
use rustcta_supervisor::{
    build_legacy_process_spec, JsonFileProcessRegistryStore, LegacyProcessSpecOptions,
    LegacyProcessTemplate, ProcessRegistry, StrategyProcessSpec,
};
use rustcta_tools_ops::{
    print_lines, run_account_position_render, run_gateio_bitget_spot_symbols,
    smart_money_binance_collector_summary, smart_money_hyperliquid_wallet_ingestion_summary,
    smart_money_portfolio_service_summary, AccountPositionRenderArgs, GateioBitgetSpotSymbolsArgs,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(name = "rustcta-industrial")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Doctor,
    CrossArb {
        #[command(subcommand)]
        command: CrossArbCommand,
    },
    Migration {
        #[command(subcommand)]
        command: MigrationCommand,
    },
    Ledger {
        #[command(subcommand)]
        command: LedgerCommand,
    },
    Supervisor {
        #[command(subcommand)]
        command: SupervisorCommand,
    },
    Ops {
        #[command(subcommand)]
        command: OpsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CrossArbCommand {
    Preflight(CrossArbPreflightArgs),
    Observe(CrossArbObserveArgs),
}

#[derive(Debug, Parser)]
struct CrossArbPreflightArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: String,
    #[arg(long, alias = "private-readonly", default_value_t = false)]
    private: bool,
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 8)]
    private_symbol_sample: usize,
    #[arg(long, default_value_t = 24)]
    public_orderbook_sample: usize,
    #[arg(long, default_value_t = false)]
    full_symbol_checks: bool,
}

#[derive(Debug, Parser)]
struct CrossArbObserveArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: String,
    #[arg(long, default_value_t = 5_000)]
    request_timeout_ms: u64,
    #[arg(long)]
    max_symbols: Option<usize>,
}

#[derive(Debug, Serialize)]
struct CrossArbPreflightBridgePlan {
    command: &'static str,
    legacy_binary: &'static str,
    legacy_args: Vec<String>,
    config: String,
    private_readonly: bool,
    timeout_ms: u64,
    private_symbol_sample: usize,
    public_orderbook_sample: usize,
    full_symbol_checks: bool,
    network_access: &'static str,
    live_order_access: &'static str,
    boundary: &'static str,
}

#[derive(Debug, Serialize)]
struct CrossArbObserveBridgePlan {
    command: &'static str,
    legacy_binary: &'static str,
    legacy_args: Vec<String>,
    config: String,
    request_timeout_ms: u64,
    max_symbols: Option<usize>,
    network_access: &'static str,
    live_order_access: &'static str,
    boundary: &'static str,
    output_fields_preserved: Vec<&'static str>,
    summary: CrossArbObserveOfflineSummary,
}

#[derive(Debug, Serialize)]
struct CrossArbObserveOfflineSummary {
    config_path: String,
    mode: &'static str,
    configured_mode: String,
    generated_at: &'static str,
    symbols: Vec<String>,
    exchanges_seen: Vec<String>,
    books_loaded: usize,
    funding_loaded: usize,
    opportunities: Vec<serde_json::Value>,
    errors: Vec<CrossArbObserveOfflineError>,
}

#[derive(Debug, Serialize)]
struct CrossArbObserveOfflineError {
    exchange: &'static str,
    symbol: Option<String>,
    kind: &'static str,
    message: String,
}

#[derive(Debug, Subcommand)]
enum MigrationCommand {
    VerifyRetiredSrc {
        #[arg(long, default_value = "src")]
        src_dir: String,
    },
}

#[derive(Debug, Subcommand)]
enum LedgerCommand {
    Validate(LedgerArgs),
    Summary(LedgerArgs),
}

#[derive(Debug, Parser)]
struct LedgerArgs {
    #[arg(long)]
    path: String,
    #[arg(long)]
    from_sequence: Option<u64>,
}

#[derive(Debug, Subcommand)]
enum SupervisorCommand {
    PrintLegacySpec(PrintLegacySpecArgs),
    PrintCrossArbShardSpecs(PrintCrossArbShardSpecsArgs),
    Readiness(SupervisorReadinessArgs),
    ValidateRegistry(ValidateRegistryArgs),
    ValidateSpec(ValidateSpecArgs),
}

#[derive(Debug, Subcommand)]
enum OpsCommand {
    SmartMoney {
        #[command(subcommand)]
        command: OpsSmartMoneyCommand,
    },
    Reporter {
        #[command(subcommand)]
        command: OpsReporterCommand,
    },
    Symbols {
        #[command(subcommand)]
        command: OpsSymbolsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum OpsSmartMoneyCommand {
    BinanceCollector(OpsConfigArgs),
    HyperliquidWalletIngestion(OpsConfigArgs),
    PortfolioService(OpsConfigArgs),
}

#[derive(Debug, Subcommand)]
enum OpsReporterCommand {
    AccountPosition {
        #[command(subcommand)]
        command: OpsAccountPositionCommand,
    },
}

#[derive(Debug, Subcommand)]
enum OpsAccountPositionCommand {
    Render(AccountPositionRenderArgs),
}

#[derive(Debug, Subcommand)]
enum OpsSymbolsCommand {
    GateioBitgetSpot(GateioBitgetSpotSymbolsArgs),
}

#[derive(Debug, ClapArgs)]
struct OpsConfigArgs {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Parser)]
struct PrintLegacySpecArgs {
    #[arg(long)]
    template: LegacyProcessTemplate,
    #[arg(long)]
    strategy_id: Option<String>,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    tenant_id: Option<String>,
    #[arg(long)]
    config: Option<String>,
    #[arg(long)]
    working_dir: Option<String>,
    #[arg(long)]
    log_dir: Option<String>,
    #[arg(long)]
    restart_backoff_ms: Option<u64>,
}

#[derive(Debug, Parser)]
struct PrintCrossArbShardSpecsArgs {
    #[arg(long, default_value_t = 1)]
    shard_count: usize,
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: String,
    #[arg(long, default_value = "cross_arb_live")]
    strategy_id_prefix: String,
    #[arg(long, default_value = "local")]
    run_id_prefix: String,
    #[arg(long, default_value = "local")]
    tenant_id: String,
    #[arg(long, default_value = "cross_arb_3venues")]
    account_id: String,
    #[arg(long)]
    working_dir: Option<String>,
    #[arg(long, default_value = "logs/supervisor")]
    log_dir: String,
    #[arg(long, default_value = "logs/cross_exchange_arbitrage")]
    runtime_dir: String,
    #[arg(long)]
    restart_backoff_ms: Option<u64>,
    #[arg(long, default_value_t = 5000)]
    dashboard_refresh_ms: u64,
    #[arg(long, default_value_t = false)]
    enable_live_trading: bool,
}

#[derive(Debug, Parser)]
struct ValidateSpecArgs {
    #[arg(long)]
    path: String,
}

#[derive(Debug, Parser)]
struct SupervisorReadinessArgs {
    #[arg(long, default_value = "config/supervisor")]
    spec_dir: PathBuf,
}

#[derive(Debug, Parser)]
struct ValidateRegistryArgs {
    #[arg(long)]
    path: Option<String>,
}

#[derive(Debug, Serialize)]
struct LedgerReplaySummary {
    path: String,
    from_sequence: Option<u64>,
    event_count: usize,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    schema_version: Option<String>,
    kind_counts: BTreeMap<String, usize>,
    secret_free: bool,
}

#[derive(Debug, Serialize)]
struct LedgerValidationReport {
    valid: bool,
    path: String,
    from_sequence: Option<u64>,
    event_count: usize,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    secret_free: bool,
}

#[derive(Debug, Serialize)]
struct SupervisorReadinessReport {
    valid: bool,
    spec_dir: String,
    spec_count: usize,
    recommended_start_order: Vec<String>,
    specs: Vec<SupervisorReadinessEntry>,
}

#[derive(Debug, Serialize)]
struct SupervisorReadinessEntry {
    spec_path: String,
    strategy_id: String,
    strategy_kind: String,
    config_path: String,
    valid: bool,
    config_exists: bool,
    recommended_start_rank: usize,
    risk_level: &'static str,
    operator_gated: bool,
    first_run: bool,
    safety_notes: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command.unwrap_or(Command::Doctor) {
        Command::Doctor => {
            println!("industrial workspace scaffold is installed");
        }
        Command::CrossArb { command } => run_cross_arb(command)?,
        Command::Migration { command } => run_migration(command)?,
        Command::Ledger { command } => run_ledger(command).await?,
        Command::Supervisor { command } => run_supervisor(command)?,
        Command::Ops { command } => run_ops(command).await?,
    }
    Ok(())
}

fn run_cross_arb(command: CrossArbCommand) -> Result<()> {
    match command {
        CrossArbCommand::Preflight(args) => run_cross_arb_preflight_bridge(args)?,
        CrossArbCommand::Observe(args) => run_cross_arb_observe_bridge(args)?,
    }
    Ok(())
}

fn run_cross_arb_preflight_bridge(args: CrossArbPreflightArgs) -> Result<()> {
    let plan = cross_arb_preflight_bridge_plan(args);
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

fn cross_arb_preflight_bridge_plan(args: CrossArbPreflightArgs) -> CrossArbPreflightBridgePlan {
    let legacy_args = legacy_cross_arb_preflight_args(&args);
    CrossArbPreflightBridgePlan {
        command: "cross-arb preflight",
        legacy_binary: "cross_arb_preflight",
        legacy_args,
        config: args.config,
        private_readonly: args.private,
        timeout_ms: args.timeout_ms,
        private_symbol_sample: args.private_symbol_sample,
        public_orderbook_sample: args.public_orderbook_sample,
        full_symbol_checks: args.full_symbol_checks,
        network_access: "disabled",
        live_order_access: "disabled",
        boundary:
            "industrial CLI reports the legacy preflight invocation only; run cross_arb_preflight directly for networked read-only checks",
    }
}

fn legacy_cross_arb_preflight_args(args: &CrossArbPreflightArgs) -> Vec<String> {
    let mut forwarded = vec![
        "--config".to_string(),
        args.config.clone(),
        "--timeout-ms".to_string(),
        args.timeout_ms.to_string(),
        "--private-symbol-sample".to_string(),
        args.private_symbol_sample.to_string(),
        "--public-orderbook-sample".to_string(),
        args.public_orderbook_sample.to_string(),
    ];
    if args.private {
        forwarded.push("--private".to_string());
    }
    if args.full_symbol_checks {
        forwarded.push("--full-symbol-checks".to_string());
    }
    forwarded
}

fn run_cross_arb_observe_bridge(args: CrossArbObserveArgs) -> Result<()> {
    let plan = cross_arb_observe_bridge_plan(args);
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

fn cross_arb_observe_bridge_plan(args: CrossArbObserveArgs) -> CrossArbObserveBridgePlan {
    let legacy_args = legacy_cross_arb_observe_args(&args);
    let (configured_mode, symbols, errors) = cross_arb_observe_config_preview(&args);
    CrossArbObserveBridgePlan {
        command: "cross-arb observe",
        legacy_binary: "cross_arb_observe",
        legacy_args,
        config: args.config.clone(),
        request_timeout_ms: args.request_timeout_ms,
        max_symbols: args.max_symbols,
        network_access: "disabled",
        live_order_access: "disabled",
        boundary:
            "industrial CLI preserves the observe command contract only; supervised strategy runtime owns live market data",
        output_fields_preserved: vec![
            "config_path",
            "mode",
            "configured_mode",
            "generated_at",
            "symbols",
            "exchanges_seen",
            "books_loaded",
            "funding_loaded",
            "opportunities",
            "errors",
        ],
        summary: CrossArbObserveOfflineSummary {
            config_path: args.config,
            mode: "observe",
            configured_mode,
            generated_at: "offline_plan",
            symbols,
            exchanges_seen: Vec::new(),
            books_loaded: 0,
            funding_loaded: 0,
            opportunities: Vec::new(),
            errors,
        },
    }
}

fn legacy_cross_arb_observe_args(args: &CrossArbObserveArgs) -> Vec<String> {
    let mut forwarded = vec![
        "--config".to_string(),
        args.config.clone(),
        "--request-timeout-ms".to_string(),
        args.request_timeout_ms.to_string(),
    ];
    if let Some(max_symbols) = args.max_symbols {
        forwarded.push("--max-symbols".to_string());
        forwarded.push(max_symbols.to_string());
    }
    forwarded
}

fn cross_arb_observe_config_preview(
    args: &CrossArbObserveArgs,
) -> (String, Vec<String>, Vec<CrossArbObserveOfflineError>) {
    let mut errors = Vec::new();
    let raw = match std::fs::read_to_string(&args.config) {
        Ok(raw) => raw,
        Err(error) => {
            errors.push(CrossArbObserveOfflineError {
                exchange: "config",
                symbol: None,
                kind: "read",
                message: error.to_string(),
            });
            return ("unknown".to_string(), Vec::new(), errors);
        }
    };
    let value = match serde_yaml::from_str::<serde_yaml::Value>(&raw) {
        Ok(value) => value,
        Err(error) => {
            errors.push(CrossArbObserveOfflineError {
                exchange: "config",
                symbol: None,
                kind: "parse",
                message: error.to_string(),
            });
            return ("unknown".to_string(), Vec::new(), errors);
        }
    };

    let configured_mode = yaml_path_string(&value, &["mode"]).unwrap_or_else(|| "unknown".into());
    let mut symbols = yaml_path_sequence_strings(&value, &["universe", "symbols"]);
    if let Some(max_symbols) = args.max_symbols {
        symbols.truncate(max_symbols);
    }
    (configured_mode, symbols, errors)
}

fn yaml_path_string(value: &serde_yaml::Value, path: &[&str]) -> Option<String> {
    yaml_path(value, path).and_then(|value| match value {
        serde_yaml::Value::String(text) => Some(text.clone()),
        serde_yaml::Value::Bool(value) => Some(value.to_string()),
        serde_yaml::Value::Number(value) => Some(value.to_string()),
        _ => None,
    })
}

fn yaml_path_sequence_strings(value: &serde_yaml::Value, path: &[&str]) -> Vec<String> {
    yaml_path(value, path)
        .and_then(serde_yaml::Value::as_sequence)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn yaml_path<'a>(value: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(value, |current, key| {
        current
            .as_mapping()
            .and_then(|mapping| mapping.get(serde_yaml::Value::String((*key).to_string())))
    })
}

fn run_migration(command: MigrationCommand) -> Result<()> {
    match command {
        MigrationCommand::VerifyRetiredSrc { src_dir } => verify_retired_src(&src_dir)?,
    }
    Ok(())
}

fn verify_retired_src(src_dir: &str) -> Result<()> {
    let exists = Path::new(src_dir).exists();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "src_dir": src_dir,
            "exists": exists,
            "retired": !exists,
        }))?
    );
    if exists {
        bail!("legacy root source directory still exists: {src_dir}");
    }
    Ok(())
}

async fn run_ledger(command: LedgerCommand) -> Result<()> {
    match command {
        LedgerCommand::Validate(args) => {
            let ledger = JsonlLedger::new(&args.path);
            let events = ledger.replay(args.from_sequence).await?;
            let report = validate_ledger(&args.path, args.from_sequence, &events);
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        LedgerCommand::Summary(args) => {
            let ledger = JsonlLedger::new(&args.path);
            let events = ledger.replay(args.from_sequence).await?;
            let summary = summarize_ledger(&args.path, args.from_sequence, &events);
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }
    Ok(())
}

fn run_supervisor(command: SupervisorCommand) -> Result<()> {
    match command {
        SupervisorCommand::PrintLegacySpec(args) => {
            let mut options = LegacyProcessSpecOptions::new(args.template);
            options.strategy_id = args.strategy_id;
            options.run_id = args.run_id;
            options.tenant_id = args.tenant_id;
            options.config_path = args.config;
            options.working_dir = args.working_dir;
            options.log_dir = args.log_dir;
            options.restart_backoff_ms = args.restart_backoff_ms;
            let spec = build_legacy_process_spec(options);
            spec.validate()?;
            println!("{}", serde_json::to_string_pretty(&spec)?);
        }
        SupervisorCommand::PrintCrossArbShardSpecs(args) => {
            let specs = build_cross_arb_shard_specs(&args)?;
            println!("{}", serde_json::to_string_pretty(&specs)?);
        }
        SupervisorCommand::Readiness(args) => {
            let report = supervisor_readiness_report(&args.spec_dir)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.valid {
                bail!("supervisor readiness failed");
            }
        }
        SupervisorCommand::ValidateRegistry(args) => {
            let registry = load_supervisor_registry(args.path.as_deref())?;
            let report = registry.validation_report();
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.valid {
                bail!("supervisor registry validation failed");
            }
        }
        SupervisorCommand::ValidateSpec(args) => {
            let spec = load_supervisor_spec(&args.path)?;
            spec.validate()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&SupervisorSpecValidation::from_spec(
                    &args.path, &spec,
                ))?
            );
        }
    }
    Ok(())
}

fn build_cross_arb_shard_specs(
    args: &PrintCrossArbShardSpecsArgs,
) -> Result<Vec<StrategyProcessSpec>> {
    if args.shard_count == 0 || args.shard_count > 1024 {
        bail!("--shard-count must be between 1 and 1024");
    }
    let mut specs = Vec::with_capacity(args.shard_count);
    for shard_id in 0..args.shard_count {
        let strategy_id = format!("{}_shard_{shard_id}", args.strategy_id_prefix);
        let run_id = format!("{}_shard_{shard_id}", args.run_id_prefix);
        let mut options = LegacyProcessSpecOptions::new(LegacyProcessTemplate::CrossArbLive);
        options.strategy_id = Some(strategy_id.clone());
        options.run_id = Some(run_id.clone());
        options.tenant_id = Some(args.tenant_id.clone());
        options.config_path = Some(args.config.clone());
        options.working_dir = args.working_dir.clone();
        options.log_dir = Some(args.log_dir.clone());
        options.restart_backoff_ms = args.restart_backoff_ms;
        let mut spec = build_legacy_process_spec(options);
        replace_arg_value(&mut spec.args, "--strategy-id", &strategy_id);
        replace_arg_value(&mut spec.args, "--run-id", &run_id);
        replace_arg_value(&mut spec.args, "--tenant-id", &args.tenant_id);
        replace_arg_value(&mut spec.args, "--account-id", &args.account_id);
        replace_arg_value(
            &mut spec.args,
            "--lock-file",
            &format!("{}/cross_arb_live_shard_{shard_id}.lock", args.runtime_dir),
        );
        replace_arg_value(
            &mut spec.args,
            "--dashboard-snapshot-path",
            &format!(
                "{}/cross_arb_live_dashboard_shard_{shard_id}.json",
                args.runtime_dir
            ),
        );
        replace_arg_value(
            &mut spec.args,
            "--trade-ledger-path",
            &format!("{}/trade_events_shard_{shard_id}.jsonl", args.runtime_dir),
        );
        replace_arg_value(
            &mut spec.args,
            "--dashboard-refresh-ms",
            &args.dashboard_refresh_ms.to_string(),
        );
        append_arg_pair(&mut spec.args, "--shard-id", &shard_id.to_string());
        append_arg_pair(
            &mut spec.args,
            "--shard-count",
            &args.shard_count.to_string(),
        );
        if args.enable_live_trading && !spec.args.iter().any(|arg| arg == "--enable-live-trading") {
            spec.args.push("--enable-live-trading".to_string());
        }
        spec.validate()?;
        specs.push(spec);
    }
    Ok(specs)
}

fn replace_arg_value(args: &mut Vec<String>, flag: &str, value: &str) {
    if let Some(index) = args.iter().position(|arg| arg == flag) {
        if let Some(existing) = args.get_mut(index + 1) {
            *existing = value.to_string();
            return;
        }
    }
    append_arg_pair(args, flag, value);
}

fn append_arg_pair(args: &mut Vec<String>, flag: &str, value: &str) {
    args.push(flag.to_string());
    args.push(value.to_string());
}

async fn run_ops(command: OpsCommand) -> Result<()> {
    match command {
        OpsCommand::SmartMoney { command } => run_ops_smart_money(command)?,
        OpsCommand::Reporter { command } => run_ops_reporter(command)?,
        OpsCommand::Symbols { command } => run_ops_symbols(command).await?,
    }
    Ok(())
}

fn run_ops_smart_money(command: OpsSmartMoneyCommand) -> Result<()> {
    let lines = match command {
        OpsSmartMoneyCommand::BinanceCollector(args) => {
            smart_money_binance_collector_summary(args.config)?
        }
        OpsSmartMoneyCommand::HyperliquidWalletIngestion(args) => {
            smart_money_hyperliquid_wallet_ingestion_summary(args.config)?
        }
        OpsSmartMoneyCommand::PortfolioService(args) => {
            smart_money_portfolio_service_summary(args.config)?
        }
    };
    print_lines(&lines);
    Ok(())
}

fn run_ops_reporter(command: OpsReporterCommand) -> Result<()> {
    match command {
        OpsReporterCommand::AccountPosition { command } => match command {
            OpsAccountPositionCommand::Render(args) => {
                println!("{}", run_account_position_render(args)?);
            }
        },
    }
    Ok(())
}

async fn run_ops_symbols(command: OpsSymbolsCommand) -> Result<()> {
    let lines = match command {
        OpsSymbolsCommand::GateioBitgetSpot(args) => run_gateio_bitget_spot_symbols(args).await?,
    };
    print_lines(&lines);
    Ok(())
}

const SUPERVISOR_READINESS_ORDER: &[(&str, usize, &str, bool, bool)] = &[
    ("trend_report.spec.json", 1, "reporter", false, true),
    (
        "account_position_reporter.spec.json",
        2,
        "reporter",
        false,
        true,
    ),
    ("cross_arb_live.spec.json", 3, "live_smoke", true, false),
    (
        "funding_arb_live.spec.json",
        4,
        "secondary_live",
        true,
        false,
    ),
    (
        "spot_spot_live_dry_run.spec.json",
        5,
        "live_dry_run",
        true,
        false,
    ),
];

fn supervisor_readiness_report(spec_dir: &Path) -> Result<SupervisorReadinessReport> {
    let workspace_root = spec_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."));
    let mut specs = Vec::new();
    for (spec_name, rank, risk_level, operator_gated, first_run) in SUPERVISOR_READINESS_ORDER {
        let spec_path = spec_dir.join(spec_name);
        let spec = load_supervisor_spec_path(&spec_path)?;
        let valid = spec.validate().is_ok();
        let config_path = workspace_root.join(&spec.config_path);
        let config_exists = config_path.exists();
        let mut safety_notes = vec![
            format!("command={}", spec.command),
            format!("arg_count={}", spec.args.len()),
        ];
        if *operator_gated {
            safety_notes.push("operator_gated=true".to_string());
        }
        if spec.strategy_id == "spot_spot_live_dry_run" {
            safety_notes.extend(spot_spot_live_dry_run_safety_notes(&config_path)?);
        }

        specs.push(SupervisorReadinessEntry {
            spec_path: spec_path.to_string_lossy().to_string(),
            strategy_id: spec.strategy_id,
            strategy_kind: spec.strategy_kind,
            config_path: spec.config_path,
            valid,
            config_exists,
            recommended_start_rank: *rank,
            risk_level,
            operator_gated: *operator_gated,
            first_run: *first_run,
            safety_notes,
        });
    }

    let valid = specs.iter().all(|entry| entry.valid && entry.config_exists);
    let recommended_start_order = specs
        .iter()
        .map(|entry| entry.strategy_id.clone())
        .collect::<Vec<_>>();

    Ok(SupervisorReadinessReport {
        valid,
        spec_dir: spec_dir.to_string_lossy().to_string(),
        spec_count: specs.len(),
        recommended_start_order,
        specs,
    })
}

fn spot_spot_live_dry_run_safety_notes(config_path: &Path) -> Result<Vec<String>> {
    let config = std::fs::read_to_string(config_path)?;
    let checks = [
        ("trading_mode=live", config.contains("trading_mode: live\n")),
        (
            "live_dry_run.enabled=true",
            config.contains("live_dry_run:\n  enabled: true\n"),
        ),
        (
            "live_dry_run.build_order_requests=true",
            config.contains("\n  build_order_requests: true\n"),
        ),
        (
            "live_dry_run.submit_orders=false",
            config.contains("\n  submit_orders: false\n"),
        ),
        (
            "small_live_gate.explicit_live_confirmation=true",
            config.contains("\n  explicit_live_confirmation: true\n"),
        ),
        (
            "live_preflight.max_live_notional_per_trade=3.6",
            config.contains("\n  max_live_notional_per_trade: 3.6\n"),
        ),
        (
            "live_preflight.max_total_live_notional=50",
            config.contains("\n  max_total_live_notional: 50\n"),
        ),
        (
            "monitoring.http_enabled=false",
            config.contains("\n  http_enabled: false\n"),
        ),
        (
            "monitoring.expose_publicly=false",
            config.contains("\n  expose_publicly: false\n"),
        ),
        (
            "spot_symbol_control.require_write_auth=true",
            config.contains("\n  require_write_auth: true\n"),
        ),
        (
            "live_runtime_path=true",
            config.contains("\nlive_trading_enabled: true\n"),
        ),
        (
            "global_dry_run=false",
            config.contains("\ndry_run: false\n"),
        ),
        (
            "requires_trade_permission=true",
            config.contains("\n  require_api_key_trade_permission: true\n"),
        ),
        (
            "live_preflight.require_withdraw_permission_absent=true",
            config.contains("\n  require_withdraw_permission_absent: true\n"),
        ),
        (
            "kill_switch.allow_live_orders=true",
            config.contains("\n  allow_live_orders: true\n"),
        ),
        (
            "inventory_rebalance.allow_market_rebalance=true",
            config.contains("\n  allow_market_rebalance: true\n"),
        ),
        (
            "inventory_rebalance.allow_lossy_rebalance_when_blocked=true",
            config.contains("\n  allow_lossy_rebalance_when_blocked: true\n"),
        ),
        (
            "live_preflight.require_private_stream=false",
            config.contains("\n  require_private_stream: false\n"),
        ),
        (
            "live_preflight.allow_rest_order_polling_fallback=true",
            config.contains("\n  allow_rest_order_polling_fallback: true\n"),
        ),
        (
            "inline_credentials=false",
            !config.contains("\napi_key:")
                && !config.contains("\n  api_key:")
                && !config.contains("\napi_secret:")
                && !config.contains("\n  api_secret:")
                && !config.contains("\npassphrase:")
                && !config.contains("\n  passphrase:"),
        ),
        (
            "local_secret_paths=false",
            !config.contains(".env") && !config.contains("~/"),
        ),
    ];

    Ok(checks
        .into_iter()
        .map(|(name, passed)| format!("{name}:{passed}"))
        .collect())
}

fn load_supervisor_spec(path: &str) -> Result<StrategyProcessSpec> {
    load_supervisor_spec_path(Path::new(path))
}

fn load_supervisor_spec_path(path: &Path) -> Result<StrategyProcessSpec> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn load_supervisor_registry(path: Option<&str>) -> Result<ProcessRegistry> {
    match path {
        Some(path) => Ok(JsonFileProcessRegistryStore::new(path).load()?),
        None => Ok(ProcessRegistry::default()),
    }
}

#[derive(Debug, Serialize)]
struct SupervisorSpecValidation {
    path: String,
    valid: bool,
    schema_version: String,
    strategy_id: String,
    strategy_kind: String,
    run_id: String,
    tenant_id: String,
    command: String,
    arg_count: usize,
    working_dir_configured: bool,
    log_configured: bool,
    restart_backoff_ms: u64,
}

impl SupervisorSpecValidation {
    fn from_spec(path: &str, spec: &StrategyProcessSpec) -> Self {
        Self {
            path: path.to_string(),
            valid: true,
            schema_version: spec.schema_version.to_string(),
            strategy_id: spec.strategy_id.clone(),
            strategy_kind: spec.strategy_kind.clone(),
            run_id: spec.run_id.clone(),
            tenant_id: spec.tenant_id.clone(),
            command: spec.command.clone(),
            arg_count: spec.args.len(),
            working_dir_configured: spec.working_dir.is_some(),
            log_configured: spec.log_path.is_some(),
            restart_backoff_ms: spec.restart_backoff_ms,
        }
    }
}

fn summarize_ledger(
    path: &str,
    from_sequence: Option<u64>,
    events: &[LedgerEvent],
) -> LedgerReplaySummary {
    let mut kind_counts = BTreeMap::new();
    for event in events {
        let kind = serde_json::to_value(event.kind)
            .ok()
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_else(|| format!("{:?}", event.kind));
        *kind_counts.entry(kind).or_insert(0) += 1;
    }

    LedgerReplaySummary {
        path: path.to_string(),
        from_sequence,
        event_count: events.len(),
        first_sequence: events.first().map(|event| event.sequence),
        last_sequence: events.last().map(|event| event.sequence),
        schema_version: events
            .first()
            .map(|event| format!("{:?}", event.schema_version)),
        kind_counts,
        secret_free: true,
    }
}

fn validate_ledger(
    path: &str,
    from_sequence: Option<u64>,
    events: &[LedgerEvent],
) -> LedgerValidationReport {
    LedgerValidationReport {
        valid: true,
        path: path.to_string(),
        from_sequence,
        event_count: events.len(),
        first_sequence: events.first().map(|event| event.sequence),
        last_sequence: events.last().map(|event| event.sequence),
        secret_free: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_validation_report_should_have_validation_shape() {
        let report = validate_ledger("/tmp/events.jsonl", Some(7), &[]);
        let value = serde_json::to_value(report).expect("serialize validation report");

        assert_eq!(value["valid"], true);
        assert_eq!(value["path"], "/tmp/events.jsonl");
        assert_eq!(value["from_sequence"], 7);
        assert_eq!(value["event_count"], 0);
        assert_eq!(value["secret_free"], true);
        assert!(value.get("kind_counts").is_none());
        assert!(value.get("schema_version").is_none());
    }

    #[test]
    fn ledger_summary_should_keep_replay_statistics_shape() {
        let summary = summarize_ledger("/tmp/events.jsonl", Some(1), &[]);
        let value = serde_json::to_value(summary).expect("serialize summary");

        assert_eq!(value["path"], "/tmp/events.jsonl");
        assert_eq!(value["from_sequence"], 1);
        assert_eq!(value["event_count"], 0);
        assert_eq!(
            value["kind_counts"].as_object().expect("kind counts").len(),
            0
        );
        assert_eq!(value["secret_free"], true);
        assert!(value.get("valid").is_none());
    }

    #[test]
    fn ops_should_parse_only_safe_aggregated_commands() {
        let args = Args::try_parse_from([
            "rustcta-industrial",
            "ops",
            "smart-money",
            "binance-collector",
            "--config",
            "config/smart_money.yml",
        ])
        .expect("parse safe ops command");

        assert!(matches!(
            args.command,
            Some(Command::Ops {
                command: OpsCommand::SmartMoney {
                    command: OpsSmartMoneyCommand::BinanceCollector(_)
                }
            })
        ));

        assert!(
            Args::try_parse_from(["rustcta-industrial", "ops", "canary", "exchange-order"])
                .is_err()
        );
        assert!(
            Args::try_parse_from(["rustcta-industrial", "ops", "admin", "cancel-order"]).is_err()
        );
    }

    #[test]
    fn supervisor_cross_arb_shard_specs_should_emit_distinct_shard_args() {
        let args = PrintCrossArbShardSpecsArgs {
            shard_count: 3,
            config: "config/cross_exchange_arbitrage_usdt.yml".to_string(),
            strategy_id_prefix: "cross_arb_live".to_string(),
            run_id_prefix: "run".to_string(),
            tenant_id: "tenant-a".to_string(),
            account_id: "acct-a".to_string(),
            working_dir: Some(".".to_string()),
            log_dir: "logs/supervisor".to_string(),
            runtime_dir: "logs/cross_exchange_arbitrage".to_string(),
            restart_backoff_ms: Some(1000),
            dashboard_refresh_ms: 7000,
            enable_live_trading: false,
        };

        let specs = build_cross_arb_shard_specs(&args).expect("shard specs");

        assert_eq!(specs.len(), 3);
        for (shard_id, spec) in specs.iter().enumerate() {
            assert_eq!(spec.strategy_id, format!("cross_arb_live_shard_{shard_id}"));
            assert!(spec
                .args
                .windows(2)
                .any(|pair| pair[0] == "--shard-id" && pair[1] == shard_id.to_string()));
            assert!(spec
                .args
                .windows(2)
                .any(|pair| pair[0] == "--shard-count" && pair[1] == "3"));
            assert!(spec
                .args
                .windows(2)
                .any(|pair| pair[0] == "--dashboard-refresh-ms" && pair[1] == "7000"));
            assert!(spec
                .args
                .windows(2)
                .any(|pair| pair[0] == "--lock-file"
                    && pair[1].contains(&format!("shard_{shard_id}"))));
            assert!(spec
                .args
                .windows(2)
                .any(|pair| pair[0] == "--dashboard-snapshot-path"
                    && pair[1].contains(&format!("shard_{shard_id}"))));
        }
    }

    #[test]
    fn cross_arb_preflight_bridge_should_not_spawn_legacy_network_command() {
        let args = CrossArbPreflightArgs {
            config: "config/cross_exchange_arbitrage_usdt.yml".to_string(),
            private: true,
            timeout_ms: 123,
            private_symbol_sample: 2,
            public_orderbook_sample: 3,
            full_symbol_checks: true,
        };

        let plan = cross_arb_preflight_bridge_plan(args);
        let value = serde_json::to_value(plan).expect("serialize preflight bridge plan");

        assert_eq!(value["command"], "cross-arb preflight");
        assert_eq!(value["legacy_binary"], "cross_arb_preflight");
        assert_eq!(value["network_access"], "disabled");
        assert_eq!(value["live_order_access"], "disabled");
        assert_eq!(value["private_readonly"], true);
        assert_eq!(value["legacy_args"][0], "--config");
        assert!(value["legacy_args"]
            .as_array()
            .expect("legacy args")
            .iter()
            .any(|arg| arg == "--private"));
        assert!(value["legacy_args"]
            .as_array()
            .expect("legacy args")
            .iter()
            .any(|arg| arg == "--full-symbol-checks"));
    }

    #[test]
    fn supervisor_readiness_report_should_rank_small_runtime_specs() {
        let spec_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .join("config/supervisor");
        let report = supervisor_readiness_report(&spec_dir).expect("readiness report");
        let value = serde_json::to_value(report).expect("serialize readiness report");

        assert_eq!(value["valid"], true);
        assert_eq!(value["spec_count"], 5);
        assert_eq!(value["recommended_start_order"][0], "trend_report");
        assert_eq!(
            value["recommended_start_order"][4],
            "spot_spot_live_dry_run"
        );
        let spot_spot = value["specs"]
            .as_array()
            .expect("specs")
            .iter()
            .find(|entry| entry["strategy_id"] == "spot_spot_live_dry_run")
            .expect("spot_spot readiness entry");
        assert_eq!(spot_spot["operator_gated"], true);
        assert_eq!(spot_spot["first_run"], false);
        assert_eq!(spot_spot["risk_level"], "live_dry_run");
        assert!(spot_spot["safety_notes"]
            .as_array()
            .expect("safety notes")
            .iter()
            .any(|note| note == "live_dry_run.submit_orders=false:true"));
        assert!(spot_spot["safety_notes"]
            .as_array()
            .expect("safety notes")
            .iter()
            .any(|note| note == "small_live_gate.explicit_live_confirmation=true:true"));
        assert!(spot_spot["safety_notes"]
            .as_array()
            .expect("safety notes")
            .iter()
            .any(|note| note == "kill_switch.allow_live_orders=true:true"));
        assert!(
            spot_spot["safety_notes"]
                .as_array()
                .expect("safety notes")
                .iter()
                .any(|note| note
                    == "inventory_rebalance.allow_lossy_rebalance_when_blocked=true:true")
        );
    }
}
