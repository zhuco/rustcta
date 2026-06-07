use anyhow::{bail, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use rustcta_event_ledger::{JsonlLedger, LedgerEvent, LedgerReader};
use rustcta_supervisor::{
    build_legacy_process_spec, JsonFileProcessRegistryStore, LegacyProcessSpecOptions,
    LegacyProcessTemplate, ProcessRegistry, StrategyProcessSpec,
};
use rustcta_tools_ops::{
    migrations_by_target, print_lines, run_account_position_render, run_gateio_bitget_spot_symbols,
    smart_money_binance_collector_summary, smart_money_hyperliquid_wallet_ingestion_summary,
    smart_money_portfolio_service_summary, verify_legacy_bin_migrations, AccountPositionRenderArgs,
    GateioBitgetSpotSymbolsArgs, LegacyBinTarget, LEGACY_BIN_MIGRATIONS,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

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

#[derive(Debug, Subcommand)]
enum MigrationCommand {
    LegacyBinPlan {
        #[arg(long)]
        target: Option<LegacyBinTarget>,
    },
    VerifyLegacyBins {
        #[arg(long, default_value = "src/bin")]
        src_bin_dir: String,
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
        CrossArbCommand::Preflight(args) => run_legacy_cross_arb_preflight(args)?,
    }
    Ok(())
}

fn run_legacy_cross_arb_preflight(args: CrossArbPreflightArgs) -> Result<()> {
    let mut forwarded = vec![
        OsString::from("--config"),
        OsString::from(args.config),
        OsString::from("--timeout-ms"),
        OsString::from(args.timeout_ms.to_string()),
        OsString::from("--private-symbol-sample"),
        OsString::from(args.private_symbol_sample.to_string()),
        OsString::from("--public-orderbook-sample"),
        OsString::from(args.public_orderbook_sample.to_string()),
    ];
    if args.private {
        forwarded.push(OsString::from("--private"));
    }
    if args.full_symbol_checks {
        forwarded.push(OsString::from("--full-symbol-checks"));
    }

    let status = match ProcessCommand::new("cross_arb_preflight")
        .args(&forwarded)
        .status()
    {
        Ok(status) => status,
        Err(binary_error) => ProcessCommand::new("cargo")
            .arg("run")
            .arg("-q")
            .arg("--bin")
            .arg("cross_arb_preflight")
            .arg("--")
            .args(forwarded.iter().map(OsStr::new))
            .status()
            .map_err(|cargo_error| {
                anyhow::anyhow!(
                    "failed to run cross_arb_preflight directly ({binary_error}); cargo fallback also failed: {cargo_error}"
                )
            })?,
    };

    if status.success() {
        Ok(())
    } else {
        bail!("cross_arb_preflight exited with status {status}")
    }
}

fn run_migration(command: MigrationCommand) -> Result<()> {
    match command {
        MigrationCommand::LegacyBinPlan { target } => {
            let migrations = target.map_or_else(
                || LEGACY_BIN_MIGRATIONS.iter().collect::<Vec<_>>(),
                migrations_by_target,
            );
            println!("{}", serde_json::to_string_pretty(&migrations)?);
        }
        MigrationCommand::VerifyLegacyBins { src_bin_dir } => {
            let verification = verify_legacy_bin_migrations(&src_bin_dir)?;
            println!("{}", serde_json::to_string_pretty(&verification)?);
            if !verification.is_clean() {
                bail!(
                    "legacy bin migration matrix is out of sync: {} unclassified, {} stale",
                    verification.unclassified_bins.len(),
                    verification.stale_migrations.len()
                );
            }
        }
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
