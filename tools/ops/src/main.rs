use std::collections::BTreeMap;
use std::time::Duration;

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
    ContractReadonlyCanaryArgs, CrossArbAccountAuditSafetyArgs, CrossArbFeeAuditSafetyArgs,
    CrossArbOrderAdminSafetyArgs, CrossArbWsOpportunityProbeSafetyArgs,
    ExchangeOrderCanarySafetyArgs, FundingArbObserveSafetyArgs, GateioBitgetSpotSymbolsArgs,
    HyperliquidSelfTestPlanArgs, PrivateWsObserveConfig, PrivateWsObserveEvent, PrivateWsProbeArgs,
    TrendReportArgs, WsConfigProbeArgs, WsProxyProbeArgs,
};
use serde_json::{json, Value};

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
    WsConfig(WsConfigProbeArgs),
    PrivateWs(PrivateWsProbeArgs),
    PrivateWsObserve(PrivateWsObserveCliArgs),
    ContractReadonlyCanary(ContractReadonlyCanaryArgs),
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

#[derive(Debug, ClapArgs)]
struct PrivateWsObserveCliArgs {
    #[arg(long, value_delimiter = ',', required = true)]
    exchanges: Vec<String>,
    #[arg(long, default_value_t = 30_000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    reconnect_delay_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    duration_ms: u64,
    #[arg(long)]
    gateio_user_id: Option<String>,
    #[arg(long, default_value_t = 200)]
    max_events: usize,
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
        ProbeCommand::WsConfig(args) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&rustcta_tools_ops::run_ws_config_probe(args).await?)?
            );
        }
        ProbeCommand::PrivateWs(args) => {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &rustcta_tools_ops::run_private_ws_probe(args).await?
                )?
            );
        }
        ProbeCommand::PrivateWsObserve(args) => run_private_ws_observe_cli(args).await?,
        ProbeCommand::ContractReadonlyCanary(args) => {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &rustcta_tools_ops::run_contract_readonly_canary(args).await?
                )?
            );
        }
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

async fn run_private_ws_observe_cli(args: PrivateWsObserveCliArgs) -> Result<()> {
    let exchanges = args
        .exchanges
        .iter()
        .map(|exchange| exchange.trim().to_ascii_lowercase())
        .filter(|exchange| !exchange.is_empty())
        .collect::<Vec<_>>();
    if exchanges.is_empty() {
        bail!("--exchanges must include at least one exchange");
    }

    let duration_ms = args.duration_ms.max(1_000);
    let max_events = args.max_events.max(1);
    let config = PrivateWsObserveConfig {
        timeout_ms: args.timeout_ms,
        reconnect_delay_ms: args.reconnect_delay_ms,
        gateio_user_id: args.gateio_user_id,
    };
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PrivateWsObserveEvent>(1024);
    let observed_exchanges = exchanges.clone();
    let handle = tokio::spawn(async move {
        rustcta_tools_ops::run_private_ws_observe_once(&observed_exchanges, config, tx).await;
    });

    let deadline = tokio::time::sleep(Duration::from_millis(duration_ms));
    tokio::pin!(deadline);
    let mut statuses = BTreeMap::<String, Value>::new();
    let mut private_events = Vec::<Value>::new();
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                match event {
                    PrivateWsObserveEvent::Status { exchange, row } => {
                        statuses.insert(exchange, row);
                    }
                    PrivateWsObserveEvent::PrivateEvent(row) => {
                        private_events.push(row);
                        if private_events.len() > max_events {
                            let overflow = private_events.len() - max_events;
                            private_events.drain(0..overflow);
                        }
                    }
                }
            }
        }
    }
    handle.abort();

    let ready = exchanges.iter().all(|exchange| {
        statuses.get(exchange).is_some_and(|row| {
            row.get("connected")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && row.get("login_ok").and_then(Value::as_bool).unwrap_or(true)
        })
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "tool": "rustcta-tools-ops probe private-ws-observe",
            "read_only": true,
            "exchanges": exchanges,
            "duration_ms": duration_ms,
            "private_ws_ready": ready,
            "statuses": statuses,
            "private_events_sample": private_events,
        }))?
    );
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
