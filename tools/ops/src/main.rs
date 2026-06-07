use anyhow::{bail, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use rustcta_tools_ops::{
    migrations_by_target, print_lines, run_account_position_render, run_gateio_bitget_spot_symbols,
    run_trend_report, smart_money_binance_collector_summary,
    smart_money_hyperliquid_wallet_ingestion_summary, smart_money_portfolio_service_summary,
    verify_legacy_bin_migrations, AccountPositionRenderArgs, GateioBitgetSpotSymbolsArgs,
    LegacyBinTarget, TrendReportArgs, WsProxyProbeArgs, LEGACY_BIN_MIGRATIONS,
};

#[derive(Debug, Parser)]
#[command(name = "rustcta-tools-ops")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    LegacyBinPlan {
        #[arg(long)]
        target: Option<LegacyBinTarget>,
    },
    VerifyLegacyBins {
        #[arg(long, default_value = "src/bin")]
        src_bin_dir: String,
    },
    SmartMoney {
        #[command(subcommand)]
        command: SmartMoneyCommand,
    },
    Symbols {
        #[command(subcommand)]
        command: SymbolsCommand,
    },
    Reporter {
        #[command(subcommand)]
        command: ReporterCommand,
    },
    Probe {
        #[command(subcommand)]
        command: ProbeCommand,
    },
    WsProxyProbe(WsProxyProbeArgs),
}

#[derive(Debug, Subcommand)]
enum SmartMoneyCommand {
    BinanceCollector(ConfigArgs),
    HyperliquidWalletIngestion(ConfigArgs),
    PortfolioService(ConfigArgs),
}

#[derive(Debug, Subcommand)]
enum SymbolsCommand {
    GateioBitgetSpot(GateioBitgetSpotSymbolsArgs),
}

#[derive(Debug, Subcommand)]
enum ReporterCommand {
    AccountPosition {
        #[command(subcommand)]
        command: AccountPositionCommand,
    },
    Trend(TrendReportArgs),
}

#[derive(Debug, Subcommand)]
enum ProbeCommand {
    WsProxy(WsProxyProbeArgs),
}

#[derive(Debug, Subcommand)]
enum AccountPositionCommand {
    Render(AccountPositionRenderArgs),
}

#[derive(Debug, ClapArgs)]
struct ConfigArgs {
    #[arg(long)]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args
        .command
        .unwrap_or(Command::LegacyBinPlan { target: None })
    {
        Command::LegacyBinPlan { target } => print_legacy_bin_plan(target)?,
        Command::VerifyLegacyBins { src_bin_dir } => verify_legacy_bins(&src_bin_dir)?,
        Command::SmartMoney { command } => run_smart_money(command)?,
        Command::Symbols { command } => run_symbols(command).await?,
        Command::Reporter { command } => run_reporter(command).await?,
        Command::Probe { command } => run_probe(command).await?,
        Command::WsProxyProbe(args) => run_ws_proxy_probe(args).await?,
    }
    Ok(())
}

fn run_smart_money(command: SmartMoneyCommand) -> Result<()> {
    let lines = match command {
        SmartMoneyCommand::BinanceCollector(args) => {
            smart_money_binance_collector_summary(args.config)?
        }
        SmartMoneyCommand::HyperliquidWalletIngestion(args) => {
            smart_money_hyperliquid_wallet_ingestion_summary(args.config)?
        }
        SmartMoneyCommand::PortfolioService(args) => {
            smart_money_portfolio_service_summary(args.config)?
        }
    };
    print_lines(&lines);
    Ok(())
}

async fn run_symbols(command: SymbolsCommand) -> Result<()> {
    let lines = match command {
        SymbolsCommand::GateioBitgetSpot(args) => run_gateio_bitget_spot_symbols(args).await?,
    };
    print_lines(&lines);
    Ok(())
}

async fn run_reporter(command: ReporterCommand) -> Result<()> {
    match command {
        ReporterCommand::AccountPosition { command } => match command {
            AccountPositionCommand::Render(args) => {
                println!("{}", run_account_position_render(args)?);
            }
        },
        ReporterCommand::Trend(args) => run_trend_report(args).await?,
    }
    Ok(())
}

async fn run_probe(command: ProbeCommand) -> Result<()> {
    match command {
        ProbeCommand::WsProxy(args) => run_ws_proxy_probe(args).await?,
    }
    Ok(())
}

async fn run_ws_proxy_probe(args: WsProxyProbeArgs) -> Result<()> {
    let lines = rustcta_tools_ops::run_ws_proxy_probe(args).await?;
    print_lines(&lines);
    Ok(())
}

fn print_legacy_bin_plan(target: Option<LegacyBinTarget>) -> Result<()> {
    let migrations = target.map_or_else(
        || LEGACY_BIN_MIGRATIONS.iter().collect::<Vec<_>>(),
        migrations_by_target,
    );
    println!("{}", serde_json::to_string_pretty(&migrations)?);
    for target in LegacyBinTarget::ALL {
        eprintln!("{target:?}: {}", migrations_by_target(target).len());
    }
    Ok(())
}

fn verify_legacy_bins(src_bin_dir: &str) -> Result<()> {
    let verification = verify_legacy_bin_migrations(src_bin_dir)?;
    println!("{}", serde_json::to_string_pretty(&verification)?);
    if !verification.is_clean() {
        bail!(
            "legacy bin migration matrix is out of sync: {} unclassified, {} stale",
            verification.unclassified_bins.len(),
            verification.stale_migrations.len()
        );
    }
    Ok(())
}
