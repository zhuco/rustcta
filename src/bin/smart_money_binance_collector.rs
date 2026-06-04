use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use rustcta::smart_money::SmartMoneyServiceConfig;

#[derive(Debug, Parser)]
#[command(
    name = "smart_money_binance_collector",
    version,
    about = "Dry-run skeleton for smart-money Binance market data collection"
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
    let collector = &cfg.binance_collector;

    log::info!(
        "loaded smart-money Binance collector config from {}",
        args.config.display()
    );
    println!(
        "smart_money_binance_collector dry-run: enabled={}, symbols={}, intervals={}, trades={}, orderbook={}, depth={}, poll_interval_secs={}",
        collector.enabled,
        collector.symbols.len(),
        collector.intervals.len(),
        collector.collect_trades,
        collector.collect_orderbook,
        collector.orderbook_depth,
        collector.poll_interval_secs
    );
    println!("no Binance network connections were opened");

    Ok(())
}
