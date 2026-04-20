use std::collections::HashMap;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use clap::Parser;
use reqwest::Client;
use rustcta::core::config::Config;
use rustcta::core::types::{Balance, MarketType, Position};
use rustcta::cta::account_manager::{AccountConfig, AccountManager};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, MissedTickBehavior};

const DEFAULT_CONFIG_PATH: &str = "config/account_position_reporter.yml";

#[derive(Parser, Debug)]
#[command(
    name = "account-position-reporter",
    version,
    about = "Standalone account position reporter"
)]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config: String,
    #[arg(long)]
    once: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ReporterConfig {
    account_id: String,
    #[serde(default = "default_exchange")]
    exchange: String,
    env_prefix: String,
    #[serde(default = "default_market_type")]
    market_type: String,
    #[serde(default = "default_interval_secs")]
    interval_secs: u64,
    webhook_url: String,
    #[serde(default = "default_report_title")]
    report_title: String,
    #[serde(default = "default_true")]
    send_on_start: bool,
    #[serde(default = "default_min_position_value_usdt")]
    min_position_value_usdt: f64,
    #[serde(default = "default_stablecoins")]
    stablecoins: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WecomMessage {
    msgtype: String,
    markdown: WecomMarkdown,
}

#[derive(Debug, Serialize)]
struct WecomMarkdown {
    content: String,
}

#[derive(Debug, Deserialize)]
struct WecomResponse {
    errcode: i64,
    errmsg: String,
}

#[derive(Debug, Clone)]
struct SymbolExposure {
    symbol: String,
    long_value_usdt: f64,
    short_value_usdt: f64,
    long_qty: f64,
    short_qty: f64,
    unrealized_pnl_usdt: f64,
}

#[derive(Debug, Clone)]
struct AccountReport {
    account_id: String,
    total_equity_usdt: f64,
    long_total_usdt: f64,
    short_total_usdt: f64,
    net_exposure_usdt: f64,
    usdt_balance_value: f64,
    total_unrealized_pnl_usdt: f64,
    exposures: Vec<SymbolExposure>,
}

fn default_exchange() -> String {
    "binance".to_string()
}

fn default_market_type() -> String {
    "futures".to_string()
}

fn default_interval_secs() -> u64 {
    600
}

fn default_report_title() -> String {
    "自动仓位报告".to_string()
}

fn default_true() -> bool {
    true
}

fn default_min_position_value_usdt() -> f64 {
    1.0
}

fn default_stablecoins() -> Vec<String> {
    vec![
        "USDT".to_string(),
        "USDC".to_string(),
        "BUSD".to_string(),
        "FDUSD".to_string(),
        "TUSD".to_string(),
        "USDS".to_string(),
    ]
}

impl ReporterConfig {
    fn from_file(path: &str) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
        let config: Self =
            serde_yaml::from_str(&raw).with_context(|| format!("failed to parse {path}"))?;
        Ok(config)
    }

    fn market_type(&self) -> Result<MarketType> {
        match self.market_type.trim().to_ascii_lowercase().as_str() {
            "spot" => Ok(MarketType::Spot),
            "futures" | "future" | "perp" | "perpetual" => Ok(MarketType::Futures),
            other => Err(anyhow!("unsupported market_type: {other}")),
        }
    }

    fn stablecoins_uppercase(&self) -> Vec<String> {
        self.stablecoins
            .iter()
            .map(|item| item.trim().to_ascii_uppercase())
            .collect()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let config = ReporterConfig::from_file(&args.config)?;
    let market_type = config.market_type()?;

    let account_manager = build_account_manager(&config).await?;
    let account = account_manager
        .get_account(&config.account_id)
        .ok_or_else(|| anyhow!("account not loaded: {}", config.account_id))?;

    let client = Client::builder()
        .timeout(StdDuration::from_secs(10))
        .build()
        .context("failed to build webhook client")?;

    log::info!(
        "starting reporter account_id={} market_type={:?} interval_secs={}",
        config.account_id,
        market_type,
        config.interval_secs
    );

    if config.send_on_start || args.once {
        run_once(&client, &config, market_type, &account.exchange).await?;
    }

    if args.once {
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_secs(config.interval_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        if let Err(err) = run_once(&client, &config, market_type, &account.exchange).await {
            log::error!("report cycle failed: {err:#}");
        }
    }
}

async fn run_once(
    client: &Client,
    config: &ReporterConfig,
    market_type: MarketType,
    exchange: &std::sync::Arc<Box<dyn rustcta::Exchange>>,
) -> Result<()> {
    let report = collect_report(config, market_type, exchange).await?;
    let markdown = build_markdown_report(config, &report);
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
    market_type: MarketType,
    exchange: &std::sync::Arc<Box<dyn rustcta::Exchange>>,
) -> Result<AccountReport> {
    let balances = exchange
        .get_balance(market_type)
        .await
        .context("failed to fetch balances")?;
    let positions = exchange
        .get_positions(None)
        .await
        .context("failed to fetch positions")?;

    let stablecoins = config.stablecoins_uppercase();
    let total_equity_usdt =
        estimate_balances_usdt_value(exchange, &balances, market_type, &stablecoins).await?;
    let usdt_balance_value = balances
        .iter()
        .filter(|balance| balance.currency.eq_ignore_ascii_case("USDT"))
        .map(|balance| balance.total.max(0.0))
        .sum::<f64>();

    let mut by_symbol: HashMap<String, SymbolExposure> = HashMap::new();
    let mut long_total_usdt = 0.0;
    let mut short_total_usdt = 0.0;
    let mut total_unrealized_pnl_usdt = 0.0;

    for position in positions {
        let qty = position_qty(&position);
        if qty <= 0.0 {
            continue;
        }

        let notional_usdt = qty * position.mark_price * normalized_contract_size(&position);
        if notional_usdt < config.min_position_value_usdt {
            continue;
        }

        total_unrealized_pnl_usdt += position.unrealized_pnl;

        let entry = by_symbol
            .entry(position.symbol.clone())
            .or_insert_with(|| SymbolExposure {
                symbol: position.symbol.clone(),
                long_value_usdt: 0.0,
                short_value_usdt: 0.0,
                long_qty: 0.0,
                short_qty: 0.0,
                unrealized_pnl_usdt: 0.0,
            });

        entry.unrealized_pnl_usdt += position.unrealized_pnl;

        if is_long_position(&position) {
            entry.long_value_usdt += notional_usdt;
            entry.long_qty += qty;
            long_total_usdt += notional_usdt;
        } else {
            entry.short_value_usdt += notional_usdt;
            entry.short_qty += qty;
            short_total_usdt += notional_usdt;
        }
    }

    let mut exposures: Vec<SymbolExposure> = by_symbol.into_values().collect();
    exposures.sort_by(|left, right| {
        gross_exposure(right)
            .partial_cmp(&gross_exposure(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(AccountReport {
        account_id: config.account_id.clone(),
        total_equity_usdt,
        long_total_usdt,
        short_total_usdt,
        net_exposure_usdt: long_total_usdt - short_total_usdt,
        usdt_balance_value,
        total_unrealized_pnl_usdt,
        exposures,
    })
}

async fn estimate_balances_usdt_value(
    exchange: &std::sync::Arc<Box<dyn rustcta::Exchange>>,
    balances: &[Balance],
    market_type: MarketType,
    stablecoins: &[String],
) -> Result<f64> {
    let mut total_usdt_value = 0.0;
    let mut price_cache: HashMap<String, f64> = HashMap::new();

    for balance in balances {
        if balance.total <= 0.0 {
            continue;
        }

        let asset = balance.currency.trim().to_ascii_uppercase();
        let price = if is_stable_valued_asset(&asset, stablecoins) {
            1.0
        } else if let Some(price) = price_cache.get(&asset) {
            *price
        } else {
            let resolved = resolve_asset_price_usdt(exchange, &asset, market_type).await?;
            price_cache.insert(asset.clone(), resolved);
            resolved
        };

        total_usdt_value += balance.total * price;
    }

    Ok(total_usdt_value)
}

async fn resolve_asset_price_usdt(
    exchange: &std::sync::Arc<Box<dyn rustcta::Exchange>>,
    asset: &str,
    market_type: MarketType,
) -> Result<f64> {
    let candidates = [
        (format!("{asset}/USDT"), MarketType::Spot),
        (format!("{asset}/USDT"), market_type),
        (format!("{asset}/USDC"), MarketType::Spot),
        (format!("{asset}/USDC"), market_type),
    ];

    for (symbol, query_market_type) in candidates {
        if let Ok(ticker) = exchange.get_ticker(&symbol, query_market_type).await {
            if ticker.last > 0.0 {
                return Ok(ticker.last);
            }
        }
    }

    Err(anyhow!("unable to price asset in USDT: {asset}"))
}

fn build_markdown_report(config: &ReporterConfig, report: &AccountReport) -> String {
    let generated_at = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
    let pnl_color = color_for_value(report.total_unrealized_pnl_usdt);
    let net_color = color_for_value(report.net_exposure_usdt);

    let mut content = String::new();
    content.push_str(&format!("## {}\n\n", config.report_title));
    content.push_str(&format!(
        "> 账户: `{}`\n> 时间: `{}`\n> 总权益: <font color=\"info\">{:.2} USDT</font>\n> 多头合计: `{:.2} USDT`\n> 空头合计: `{:.2} USDT`\n> 净敞口: <font color=\"{}\">{:+.2} USDT</font>\n> USDT价值: `{:.2} USDT`\n> 未实现PNL: <font color=\"{}\">{:+.2} USDT</font>\n\n",
        report.account_id,
        generated_at,
        report.total_equity_usdt,
        report.long_total_usdt,
        report.short_total_usdt,
        net_color,
        report.net_exposure_usdt,
        report.usdt_balance_value,
        pnl_color,
        report.total_unrealized_pnl_usdt,
    ));

    if report.exposures.is_empty() {
        content.push_str("### 持仓明细\n\n当前无持仓。\n");
        return content;
    }

    content.push_str("### 持仓明细\n\n");

    for (index, exposure) in report.exposures.iter().enumerate() {
        let net_value = exposure.long_value_usdt - exposure.short_value_usdt;
        let gross_value = exposure.long_value_usdt + exposure.short_value_usdt;
        let net_color = color_for_value(net_value);
        let pnl_color = color_for_value(exposure.unrealized_pnl_usdt);

        content.push_str(&format!(
            "{}. `{}` 总持仓 `{:.2}U` | 多头 `{:.2}U` | 空头 `{:.2}U` | 净敞口 <font color=\"{}\">{:+.2}U</font> | PNL <font color=\"{}\">{:+.2}U</font> | 数量 `多 {:.4} / 空 {:.4}`\n",
            index + 1,
            exposure.symbol,
            gross_value,
            exposure.long_value_usdt,
            exposure.short_value_usdt,
            net_color,
            net_value,
            pnl_color,
            exposure.unrealized_pnl_usdt,
            exposure.long_qty,
            exposure.short_qty,
        ));
    }

    content
}

async fn send_markdown(client: &Client, webhook_url: &str, content: String) -> Result<()> {
    let payload = WecomMessage {
        msgtype: "markdown".to_string(),
        markdown: WecomMarkdown { content },
    };

    let response = client
        .post(webhook_url)
        .json(&payload)
        .send()
        .await
        .context("failed to send webhook request")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read webhook response body")?;

    if !status.is_success() {
        return Err(anyhow!("webhook http status={} body={}", status, body));
    }

    let parsed: WecomResponse =
        serde_json::from_str(&body).context("failed to parse webhook response json")?;
    if parsed.errcode != 0 {
        return Err(anyhow!(
            "webhook returned errcode={} errmsg={}",
            parsed.errcode,
            parsed.errmsg
        ));
    }

    Ok(())
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

fn is_long_position(position: &Position) -> bool {
    let side = position.side.trim().to_ascii_uppercase();
    if side == "LONG" {
        return true;
    }
    if side == "SHORT" {
        return false;
    }
    position.amount >= 0.0
}

fn normalized_contract_size(position: &Position) -> f64 {
    if position.contract_size > 0.0 {
        position.contract_size
    } else {
        1.0
    }
}

fn position_qty(position: &Position) -> f64 {
    if position.amount.abs() > 0.0 {
        position.amount.abs()
    } else if position.contracts > 0.0 {
        position.contracts.abs()
    } else {
        position.size.abs()
    }
}

fn gross_exposure(exposure: &SymbolExposure) -> f64 {
    exposure.long_value_usdt + exposure.short_value_usdt
}

fn color_for_value(value: f64) -> &'static str {
    if value > 0.0 {
        "warning"
    } else if value < 0.0 {
        "info"
    } else {
        "comment"
    }
}

fn is_stable_valued_asset(asset: &str, stablecoins: &[String]) -> bool {
    stablecoins
        .iter()
        .any(|stablecoin| asset == stablecoin || asset.ends_with(stablecoin))
}
