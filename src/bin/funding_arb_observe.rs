use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rustcta::exchanges::registry::market_adapter;
use rustcta::market::MarketDataAdapter;
use rustcta::strategies::funding_rate_arbitrage::{
    require_observe_mode, scan_funding_opportunities_with_timeout, send_startup_notification,
    FundingRateArbitrageConfig,
};

#[derive(Parser, Debug)]
#[command(
    name = "funding_arb_observe",
    version,
    about = "Observe funding-rate arbitrage candidates and notify WeCom without placing orders"
)]
struct Args {
    #[arg(long, default_value = "config/funding_rate_arbitrage_usdt.yml")]
    config: PathBuf,
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
    let mut config = load_config(&args.config)?;
    config
        .validate()
        .context("invalid funding-rate arbitrage config")?;
    require_observe_mode(&config)?;

    if args.no_notify {
        config.notifications.enabled = false;
    }

    log_private_adapter_status(&config);

    let adapters = configured_adapters(&config);
    let report = scan_funding_opportunities_with_timeout(
        &adapters,
        &config,
        Utc::now(),
        Some(args.request_timeout_ms),
    )
    .await;

    println!("{}", serde_json::to_string_pretty(&report)?);

    send_startup_notification(&config, &report)
        .await
        .context("send funding-rate arbitrage startup notification")?;

    Ok(())
}

fn load_config(path: &PathBuf) -> Result<FundingRateArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<FundingRateArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn configured_adapters(
    config: &FundingRateArbitrageConfig,
) -> Vec<Box<dyn MarketDataAdapter + Send + Sync>> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter_map(|exchange| {
            let adapter = market_adapter(exchange);
            if adapter.is_none() {
                log::warn!("{} is not enabled for funding_arb_observe", exchange);
            }
            adapter
        })
        .collect()
}

fn log_private_adapter_status(config: &FundingRateArbitrageConfig) {
    for exchange in &config.universe.enabled_exchanges {
        let support = rustcta::exchanges::trading_adapters::private_trading_support_for(exchange);
        if support.private_trading_enabled {
            log::info!(
                "{} private trading adapter is wired, but funding_arb_observe will not place orders",
                exchange
            );
        } else {
            log::info!(
                "{} private trading disabled: {}",
                exchange,
                support
                    .disabled_reason
                    .unwrap_or("not registered for private trading")
            );
        }
    }
}
