use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;
use rustcta::exchanges::registry::gateway_capabilities;
use rustcta::strategies::funding_rate_arbitrage::{
    require_live_mode, run_funding_live_once, run_funding_live_scheduler, FundingLiveRunOptions,
    FundingRateArbitrageConfig,
};

#[derive(Parser, Debug)]
#[command(
    name = "funding_arb_live",
    version,
    about = "Run funding-rate arbitrage live: one candidate per exchange, market open before settlement, market close after settlement"
)]
struct Args {
    #[arg(long, default_value = "config/funding_rate_arbitrage_live_usdt.yml")]
    config: PathBuf,
    #[arg(long, default_value_t = false)]
    confirm_live_order: bool,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, default_value_t = false)]
    no_notify: bool,
    #[arg(long, default_value_t = 15_000)]
    request_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    rustcta::utils::init_tracing_logger("info");

    let args = Args::parse();
    if !args.confirm_live_order {
        bail!("refusing to run live orders without --confirm-live-order");
    }

    let mut config = load_config(&args.config)?;
    config
        .validate()
        .context("invalid funding-rate arbitrage live config")?;
    require_live_mode(&config)?;
    if args.no_notify {
        config.notifications.enabled = false;
    }

    log_gateway_status(&config);
    let options = FundingLiveRunOptions {
        request_timeout_ms: Some(args.request_timeout_ms),
        notify: config.notifications.enabled,
    };

    if args.once {
        run_funding_live_once(config, options).await?;
    } else {
        run_funding_live_scheduler(config, options).await?;
    }
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<FundingRateArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<FundingRateArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn log_gateway_status(config: &FundingRateArbitrageConfig) {
    for exchange in &config.universe.enabled_exchanges {
        match gateway_capabilities(exchange) {
            Some(capabilities) => log::info!(
                "{} gateway private_rest={} dual_side={} trading_registered={}",
                exchange,
                capabilities.supports_private_rest,
                capabilities.supports_dual_side_position,
                capabilities.trading.is_some()
            ),
            None => log::warn!("{} gateway is not registered", exchange),
        }
    }
}
