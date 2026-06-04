use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use rustcta::smart_money::SmartMoneyServiceConfig;

#[derive(Debug, Parser)]
#[command(
    name = "smart_money_hyperliquid_wallet_ingestion",
    version,
    about = "Dry-run skeleton for smart-money Hyperliquid wallet ingestion"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let cfg = SmartMoneyServiceConfig::load_yaml(&args.config)?;
    let ingestion = &cfg.hyperliquid_wallet_ingestion;
    let enabled_wallets = ingestion
        .wallets
        .iter()
        .filter(|wallet| wallet.enabled)
        .count();

    log::info!(
        "loaded smart-money Hyperliquid ingestion config from {}",
        args.config.display()
    );
    println!(
        "smart_money_hyperliquid_wallet_ingestion dry-run: enabled={}, wallets={}, enabled_wallets={}, positions={}, fills={}, lookback_days={}, poll_interval_secs={}",
        ingestion.enabled,
        ingestion.wallets.len(),
        enabled_wallets,
        ingestion.ingest_positions,
        ingestion.ingest_fills,
        ingestion.lookback_days,
        ingestion.poll_interval_secs
    );
    println!("no Hyperliquid network connections were opened");

    Ok(())
}
