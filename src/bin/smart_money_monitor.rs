use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use rustcta::smart_money::{serve_smart_money_monitor, SmartMoneyServiceConfig};

#[derive(Debug, Parser)]
#[command(
    name = "smart_money_monitor",
    version,
    about = "Bilingual smart-money wallet monitor with SQLite persistence and REST order book simulation"
)]
struct Args {
    #[arg(long, default_value = "config/smart_money.yml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let cfg = SmartMoneyServiceConfig::load_yaml(&args.config)?;
    serve_smart_money_monitor(cfg.monitor).await
}
