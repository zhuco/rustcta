#![allow(clippy::all)]
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use reqwest::Client;
use rustcta::core::config::Config;
use rustcta::core::types::MarketType;
use rustcta::cta::account_manager::{AccountConfig, AccountManager};
use rustcta_reporting::{
    build_account_position_markdown_report, collect_account_position_report,
    send_account_position_markdown_webhook, AccountBalanceInput, AccountPositionInput,
    AccountPositionMarketType, AccountPositionProvider, AccountPositionReport,
    AccountPositionReporterConfig,
};
use std::sync::Arc;
use tokio::time::{Duration, MissedTickBehavior};

const DEFAULT_CONFIG_PATH: &str = "config/account_position_reporter.yml";

#[derive(Debug, Clone, Parser)]
#[command(
    name = "account-position-reporter",
    version,
    about = "Standalone account position reporter"
)]
pub struct AccountPositionReporterArgs {
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    pub config: String,
    #[arg(long)]
    pub once: bool,
}

type ReporterConfig = AccountPositionReporterConfig;
type AccountReport = AccountPositionReport;
type LegacyExchange = Arc<Box<dyn rustcta::Exchange>>;

pub async fn run_account_position_reporter(args: AccountPositionReporterArgs) -> Result<()> {
    dotenv::dotenv().ok();
    rustcta::utils::init_tracing_logger("info");

    let config = ReporterConfig::from_file(&args.config)?;
    let market_type = to_legacy_market_type(config.market_type()?);

    let account_manager = build_account_manager(&config).await?;
    let account = account_manager
        .get_account(&config.account_id)
        .ok_or_else(|| anyhow!("account not loaded: {}", config.account_id))?;
    let provider = LegacyExchangeAccountPositionProvider {
        exchange: account.exchange.clone(),
    };

    let client = rustcta::core::http2_fix::shared_http_client();

    log::info!(
        "starting reporter account_id={} market_type={:?} interval_secs={}",
        config.account_id,
        market_type,
        config.interval_secs
    );

    if config.send_on_start || args.once {
        run_once(&client, &config, &provider).await?;
    }

    if args.once {
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_secs(config.interval_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        if let Err(err) = run_once(&client, &config, &provider).await {
            log::error!("report cycle failed: {err:#}");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run_account_position_reporter(AccountPositionReporterArgs::parse()).await
}

async fn run_once(
    client: &Client,
    config: &ReporterConfig,
    provider: &LegacyExchangeAccountPositionProvider,
) -> Result<()> {
    let report = collect_report(config, provider).await?;
    let markdown = build_account_position_markdown_report(config, &report);
    send_markdown(client, &config.webhook_url, markdown).await?;
    log::info!(
        "position report sent account_id={} positions={}",
        report.account_id,
        report.exposures.len()
    );
    Ok(())
}

async fn collect_report(
    config: &ReporterConfig,
    provider: &LegacyExchangeAccountPositionProvider,
) -> Result<AccountReport> {
    collect_account_position_report(config, provider).await
}

async fn send_markdown(client: &Client, webhook_url: &str, content: String) -> Result<()> {
    send_account_position_markdown_webhook(client, webhook_url, content).await
}

fn to_legacy_market_type(market_type: AccountPositionMarketType) -> MarketType {
    match market_type {
        AccountPositionMarketType::Spot => MarketType::Spot,
        AccountPositionMarketType::Futures => MarketType::Futures,
    }
}

#[derive(Clone)]
struct LegacyExchangeAccountPositionProvider {
    exchange: LegacyExchange,
}

#[async_trait]
impl AccountPositionProvider for LegacyExchangeAccountPositionProvider {
    async fn get_balances(
        &self,
        market_type: AccountPositionMarketType,
    ) -> Result<Vec<AccountBalanceInput>> {
        let balances = self
            .exchange
            .get_balance(to_legacy_market_type(market_type))
            .await
            .context("failed to fetch balances")?;
        Ok(balances
            .into_iter()
            .map(|balance| AccountBalanceInput {
                currency: balance.currency,
                total: balance.total,
            })
            .collect())
    }

    async fn get_positions(&self) -> Result<Vec<AccountPositionInput>> {
        let positions = self
            .exchange
            .get_positions(None)
            .await
            .context("failed to fetch positions")?;
        Ok(positions
            .into_iter()
            .map(|position| AccountPositionInput {
                symbol: position.symbol,
                side: position.side,
                contracts: position.contracts,
                contract_size: position.contract_size,
                mark_price: position.mark_price,
                unrealized_pnl: position.unrealized_pnl,
                size: position.size,
                amount: position.amount,
            })
            .collect())
    }

    async fn get_last_price(
        &self,
        symbol: &str,
        market_type: AccountPositionMarketType,
    ) -> Result<f64> {
        let ticker = self
            .exchange
            .get_ticker(symbol, to_legacy_market_type(market_type))
            .await?;
        Ok(ticker.last)
    }
}

async fn build_account_manager(config: &ReporterConfig) -> Result<AccountManager> {
    let exchange_config = Config {
        name: "Binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443/ws".to_string(),
        ws_futures_url: "wss://fstream.binance.com/ws".to_string(),
    };

    let account_config = AccountConfig {
        id: config.account_id.clone(),
        exchange: config.exchange.clone(),
        api_key_env: config.env_prefix.clone(),
        position_mode: None,
        enabled: true,
        max_positions: 10,
        max_orders_per_symbol: 20,
    };

    let mut account_manager = AccountManager::new(exchange_config);
    account_manager
        .add_account(account_config)
        .await
        .with_context(|| format!("failed to add account {}", config.account_id))?;

    Ok(account_manager)
}
