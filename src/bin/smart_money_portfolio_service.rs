use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use rustcta::smart_money::SmartMoneyServiceConfig;

#[derive(Debug, Parser)]
#[command(
    name = "smart_money_portfolio_service",
    version,
    about = "Dry-run skeleton for smart-money portfolio construction service"
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
    let portfolio = &cfg.portfolio;
    let constraints = &portfolio.constraints;

    log::info!(
        "loaded smart-money portfolio service config from {}",
        args.config.display()
    );
    println!(
        "smart_money_portfolio_service dry-run: enabled={}, dry_run={}, close_only={}, rebalance_interval_secs={}, alpha_stale_after_secs={}",
        portfolio.enabled,
        portfolio.dry_run,
        portfolio.close_only,
        portfolio.rebalance_interval_secs,
        portfolio.alpha_stale_after_secs
    );
    println!(
        "portfolio constraints: initial_capital_usdt={}, standard_entry_notional_usdt={}, max_leverage={}, max_gross_notional_usdt={}, max_single_asset_gross_share={}, symbol_overrides={}",
        constraints.initial_capital_usdt,
        constraints.standard_entry_notional_usdt,
        constraints.max_leverage,
        constraints.max_gross_notional_usdt,
        constraints.max_single_asset_gross_share,
        portfolio.symbol_overrides.len()
    );
    println!("no portfolio orders or network connections were created");

    Ok(())
}
