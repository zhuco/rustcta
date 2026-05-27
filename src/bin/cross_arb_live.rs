use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::adapters::{BinanceMarketAdapter, BitgetMarketAdapter, GateMarketAdapter};
use rustcta::market::{exchange_symbol_for, ExchangeId, InstrumentMeta, MarketDataAdapter};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_cross_arb_live_runtime_parts, live_enabled_exchanges, CrossArbRuntime,
    CrossExchangeArbitrageConfig, PrivateRestAuditPlan, PrivateRuntimeSync,
};
use serde::Serialize;
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_live",
    version,
    about = "Safe live-small runner bootstrap for cross-exchange USDT perpetual arbitrage"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/cross_exchange_arbitrage_usdt.live-small.example.yml"
    )]
    config: PathBuf,
    #[arg(long, default_value_t = false)]
    execute: bool,
    #[arg(long, default_value_t = false)]
    skip_private_audit: bool,
}

#[derive(Debug, Serialize)]
struct LiveBootstrapReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    execute_requested: bool,
    dry_run: bool,
    live_ready: bool,
    blocking_reasons: Vec<String>,
    enabled_exchanges: Vec<String>,
    symbols: Vec<String>,
    instruments_loaded: usize,
    router_adapters: Vec<String>,
    private_ws_specs: Vec<PrivateWsSpecSummary>,
    rest_audits: Vec<RestAuditSummary>,
}

#[derive(Debug, Serialize)]
struct PrivateWsSpecSummary {
    exchange: String,
    symbols: usize,
    account_id_configured: bool,
}

#[derive(Debug, Serialize)]
struct RestAuditSummary {
    exchange: String,
    ok: bool,
    balances: usize,
    positions: usize,
    open_orders: usize,
    fills: usize,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let config = load_config(&args.config)?;
    config
        .validate()
        .context("invalid cross arbitrage live config")?;
    if !config.mode.allows_live_orders() {
        return Err(anyhow!(
            "cross_arb_live requires live-capable mode; got {:?}",
            config.mode
        ));
    }
    if args.execute && config.execution.dry_run {
        return Err(anyhow!(
            "--execute was requested but execution.dry_run=true; switch dry_run=false only after preflight passes"
        ));
    }

    let report = bootstrap_live(args.config, config, args.execute, args.skip_private_audit).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.live_ready {
        return Err(anyhow!(
            "live bootstrap is not ready; inspect blocking_reasons"
        ));
    }
    Ok(())
}

async fn bootstrap_live(
    config_path: PathBuf,
    config: CrossExchangeArbitrageConfig,
    execute_requested: bool,
    skip_private_audit: bool,
) -> Result<LiveBootstrapReport> {
    let enabled_exchanges = live_enabled_exchanges(&config);
    let instruments = load_live_instruments(&config, &enabled_exchanges).await?;
    let parts = build_cross_arb_live_runtime_parts(&config, instruments.clone())?;
    let runtime = Arc::new(RwLock::new(CrossArbRuntime::new(
        config.clone(),
        Utc::now(),
    )));
    let private_sync = PrivateRuntimeSync::new(runtime, parts.adapters.clone());

    let rest_audits = if skip_private_audit {
        Vec::new()
    } else {
        run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await
    };
    let mut blocking_reasons = Vec::new();
    if !skip_private_audit {
        for audit in &rest_audits {
            if !audit.ok {
                blocking_reasons.push(format!("{} private REST audit failed", audit.exchange));
            }
        }
    }
    for exchange in &enabled_exchanges {
        if !parts.router.has_adapter(exchange) {
            blocking_reasons.push(format!("missing router adapter for {}", exchange.as_str()));
        }
    }
    if execute_requested && config.execution.dry_run {
        blocking_reasons.push("--execute requested while dry_run=true".to_string());
    }

    Ok(LiveBootstrapReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        execute_requested,
        dry_run: config.execution.dry_run,
        live_ready: blocking_reasons.is_empty(),
        blocking_reasons,
        enabled_exchanges: enabled_exchanges
            .iter()
            .map(|exchange| exchange.as_str().to_string())
            .collect(),
        symbols: config
            .universe
            .symbols
            .iter()
            .map(ToString::to_string)
            .collect(),
        instruments_loaded: instruments.len(),
        router_adapters: parts
            .adapters
            .iter()
            .map(|adapter| adapter.exchange().as_str().to_string())
            .collect(),
        private_ws_specs: parts
            .private_ws_specs
            .iter()
            .map(|spec| PrivateWsSpecSummary {
                exchange: spec.exchange.exchange_id().as_str().to_string(),
                symbols: spec.symbols.len(),
                account_id_configured: spec.auth.account_id.is_some(),
            })
            .collect(),
        rest_audits,
    })
}

async fn load_live_instruments(
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
) -> Result<Vec<InstrumentMeta>> {
    let mut loaded = Vec::new();
    let wanted = config
        .universe
        .symbols
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for exchange in exchanges {
        let Some(adapter) = market_adapter(exchange) else {
            continue;
        };
        let instruments = adapter
            .load_instruments()
            .await
            .with_context(|| format!("load {} instruments", exchange.as_str()))?;
        loaded.extend(
            instruments
                .into_iter()
                .filter(|instrument| {
                    instrument.is_tradeable_usdt_perpetual()
                        && wanted.contains(&instrument.canonical_symbol)
                })
                .collect::<Vec<_>>(),
        );
    }
    Ok(loaded)
}

async fn run_initial_rest_audits(
    private_sync: &PrivateRuntimeSync,
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
) -> Vec<RestAuditSummary> {
    let mut summaries = Vec::new();
    for exchange in exchanges {
        let symbols = config
            .universe
            .symbols
            .iter()
            .map(|symbol| exchange_symbol_for(exchange, symbol))
            .collect::<Vec<_>>();
        let plan = PrivateRestAuditPlan {
            exchange: exchange.clone(),
            symbols,
            include_recent_fills: true,
        };
        match private_sync.run_rest_audit(&plan).await {
            Ok(snapshot) => summaries.push(RestAuditSummary {
                exchange: exchange.as_str().to_string(),
                ok: true,
                balances: snapshot.balances.len(),
                positions: snapshot.positions.len(),
                open_orders: snapshot.open_orders.len(),
                fills: snapshot.fills.len(),
                error: None,
            }),
            Err(err) => summaries.push(RestAuditSummary {
                exchange: exchange.as_str().to_string(),
                ok: false,
                balances: 0,
                positions: 0,
                open_orders: 0,
                fills: 0,
                error: Some(err.to_string()),
            }),
        }
    }
    summaries
}

fn market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(BinanceMarketAdapter)),
        ExchangeId::Bitget => Some(Box::new(BitgetMarketAdapter)),
        ExchangeId::Gate => Some(Box::new(GateMarketAdapter)),
        ExchangeId::Okx | ExchangeId::Other(_) => None,
    }
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcta::market::RuntimeMode;

    #[test]
    fn live_runner_market_adapter_should_exclude_okx_for_current_live_plan() {
        assert!(market_adapter(&ExchangeId::Binance).is_some());
        assert!(market_adapter(&ExchangeId::Bitget).is_some());
        assert!(market_adapter(&ExchangeId::Gate).is_some());
        assert!(market_adapter(&ExchangeId::Okx).is_none());
    }

    #[test]
    fn live_runner_example_config_should_parse_as_live_small_dry_run() {
        let config = load_config(&PathBuf::from(
            "config/cross_exchange_arbitrage_usdt.live-small.example.yml",
        ))
        .unwrap();

        assert_eq!(config.mode, RuntimeMode::LiveSmall);
        assert!(config.execution.dry_run);
        assert_eq!(
            live_enabled_exchanges(&config),
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate]
        );
    }
}
