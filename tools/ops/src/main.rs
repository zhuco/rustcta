use anyhow::{bail, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use rustcta_tools_ops::{
    bitget_perp_order_canary_safety_plan, bitget_spot_order_canary_safety_plan,
    cross_arb_account_audit_safety_plan, cross_arb_fee_audit_safety_plan,
    cross_arb_order_admin_safety_plan, cross_arb_ws_opportunity_probe_safety_plan,
    exchange_order_canary_safety_plan, funding_arb_observe_safety_plan,
    hyperliquid_self_test_safety_plan, print_lines, run_account_position_render,
    run_gateio_bitget_spot_symbols, run_trend_report, smart_money_binance_collector_summary,
    smart_money_hyperliquid_wallet_ingestion_summary, smart_money_portfolio_service_summary,
    AccountPositionRenderArgs, BitgetPerpOrderCanarySafetyArgs, BitgetSpotOrderCanarySafetyArgs,
    CrossArbAccountAuditSafetyArgs, CrossArbFeeAuditSafetyArgs, CrossArbOrderAdminSafetyArgs,
    CrossArbWsOpportunityProbeSafetyArgs, ExchangeOrderCanarySafetyArgs,
    FundingArbObserveSafetyArgs, GateioBitgetSpotSymbolsArgs, HyperliquidSelfTestPlanArgs,
    TrendReportArgs, WsProxyProbeArgs,
};

#[derive(Debug, Parser)]
#[command(name = "rustcta-tools-ops")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    VerifyRetiredSrc {
        #[arg(long, default_value = "src")]
        src_dir: String,
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
    Canary {
        #[command(subcommand)]
        command: CanaryCommand,
    },
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
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
    HyperliquidSelfTest(HyperliquidSelfTestPlanArgs),
    FundingArbObserve(FundingArbObserveSafetyArgs),
    CrossArbWsOpportunity(CrossArbWsOpportunityProbeSafetyArgs),
}

#[derive(Debug, Subcommand)]
enum CanaryCommand {
    #[command(name = "exchange-order")]
    Exchange(ExchangeOrderCanarySafetyArgs),
    #[command(name = "bitget-perp-order")]
    BitgetPerp(BitgetPerpOrderCanarySafetyArgs),
    #[command(name = "bitget-spot-order")]
    BitgetSpot(BitgetSpotOrderCanarySafetyArgs),
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
    CrossArbAccount(CrossArbAccountAuditSafetyArgs),
    CrossArbFee(CrossArbFeeAuditSafetyArgs),
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    CrossArbOrder(CrossArbOrderAdminSafetyArgs),
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
    match args.command.unwrap_or(Command::VerifyRetiredSrc {
        src_dir: "src".to_string(),
    }) {
        Command::VerifyRetiredSrc { src_dir } => verify_retired_src(&src_dir)?,
        Command::SmartMoney { command } => run_smart_money(command)?,
        Command::Symbols { command } => run_symbols(command).await?,
        Command::Reporter { command } => run_reporter(command).await?,
        Command::Probe { command } => run_probe(command).await?,
        Command::Canary { command } => run_canary(command)?,
        Command::Audit { command } => run_audit(command)?,
        Command::Admin { command } => run_admin(command)?,
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
        ProbeCommand::HyperliquidSelfTest(args) => {
            println!("{}", hyperliquid_self_test_safety_plan(args)?);
        }
        ProbeCommand::FundingArbObserve(args) => {
            println!("{}", funding_arb_observe_safety_plan(args)?);
        }
        ProbeCommand::CrossArbWsOpportunity(args) => {
            println!("{}", cross_arb_ws_opportunity_probe_safety_plan(args)?);
        }
    }
    Ok(())
}

fn run_canary(command: CanaryCommand) -> Result<()> {
    let report = match command {
        CanaryCommand::Exchange(args) => exchange_order_canary_safety_plan(args)?,
        CanaryCommand::BitgetPerp(args) => bitget_perp_order_canary_safety_plan(args)?,
        CanaryCommand::BitgetSpot(args) => bitget_spot_order_canary_safety_plan(args)?,
    };
    println!("{report}");
    Ok(())
}

fn run_audit(command: AuditCommand) -> Result<()> {
    let report = match command {
        AuditCommand::CrossArbAccount(args) => cross_arb_account_audit_safety_plan(args)?,
        AuditCommand::CrossArbFee(args) => cross_arb_fee_audit_safety_plan(args)?,
    };
    println!("{report}");
    Ok(())
}

fn run_admin(command: AdminCommand) -> Result<()> {
    let report = match command {
        AdminCommand::CrossArbOrder(args) => cross_arb_order_admin_safety_plan(args)?,
    };
    println!("{report}");
    Ok(())
}

async fn run_ws_proxy_probe(args: WsProxyProbeArgs) -> Result<()> {
    let lines = rustcta_tools_ops::run_ws_proxy_probe(args).await?;
    print_lines(&lines);
    Ok(())
}

fn verify_retired_src(src_dir: &str) -> Result<()> {
    let exists = std::path::Path::new(src_dir).exists();
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
