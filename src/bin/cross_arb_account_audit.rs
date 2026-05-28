use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use rustcta::execution::{crossarb_order_mode, is_crossarb_client_order_id};
use rustcta::market::ExchangeId;
use rustcta::strategies::cross_exchange_arbitrage::{
    build_trading_adapter_for_exchange, live_enabled_exchanges, CrossExchangeArbitrageConfig,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_account_audit",
    version,
    about = "Read-only private account snapshot for cross-exchange arbitrage venues"
)]
struct Args {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: PathBuf,
}

#[derive(Debug, Serialize)]
struct AuditReport {
    exchanges: Vec<ExchangeAudit>,
}

#[derive(Debug, Serialize)]
struct ExchangeAudit {
    exchange: ExchangeId,
    balances: usize,
    positions: Vec<PositionAudit>,
    open_orders: Vec<OrderAudit>,
    strategy_open_orders: Vec<OrderAudit>,
    external_open_orders: Vec<OrderAudit>,
}

#[derive(Debug, Serialize)]
struct PositionAudit {
    symbol: String,
    side: String,
    quantity: f64,
    entry_price: Option<f64>,
    unrealized_pnl: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct OrderAudit {
    symbol: String,
    side: String,
    position_side: String,
    quantity: f64,
    filled_quantity: f64,
    status: String,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    strategy_owned: bool,
    strategy_mode: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read config {}", args.config.display()))?;
    let config: CrossExchangeArbitrageConfig = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse config {}", args.config.display()))?;
    config
        .validate()
        .context("invalid cross arbitrage config")?;

    let mut exchanges = Vec::new();
    for exchange in live_enabled_exchanges(&config) {
        let adapter = build_trading_adapter_for_exchange(&config, &exchange)
            .with_context(|| format!("build {} trading adapter", exchange.as_str()))?;
        let balances = adapter
            .get_balances()
            .await
            .with_context(|| format!("read {} balances", exchange.as_str()))?;
        let positions = adapter
            .get_positions(None)
            .await
            .with_context(|| format!("read {} positions", exchange.as_str()))?
            .into_iter()
            .filter(|position| position.quantity.abs() > config.reconciliation.quantity_tolerance)
            .map(|position| PositionAudit {
                symbol: position.canonical_symbol.to_string(),
                side: format!("{:?}", position.position_side),
                quantity: position.quantity,
                entry_price: position.entry_price,
                unrealized_pnl: position.unrealized_pnl,
            })
            .collect::<Vec<_>>();
        let open_orders = adapter
            .get_open_orders(None)
            .await
            .with_context(|| format!("read {} open orders", exchange.as_str()))?
            .into_iter()
            .map(|order| OrderAudit {
                strategy_owned: order
                    .client_order_id
                    .as_deref()
                    .map(is_crossarb_client_order_id)
                    .unwrap_or(false),
                strategy_mode: order
                    .client_order_id
                    .as_deref()
                    .and_then(crossarb_order_mode)
                    .map(ToOwned::to_owned),
                symbol: order.canonical_symbol.to_string(),
                side: format!("{:?}", order.side),
                position_side: format!("{:?}", order.position_side),
                quantity: order.quantity,
                filled_quantity: order.filled_quantity,
                status: format!("{:?}", order.status),
                client_order_id: order.client_order_id,
                exchange_order_id: order.exchange_order_id,
            })
            .collect::<Vec<_>>();
        let strategy_open_orders = open_orders
            .iter()
            .filter(|order| order.strategy_owned)
            .cloned()
            .collect::<Vec<_>>();
        let external_open_orders = open_orders
            .iter()
            .filter(|order| !order.strategy_owned)
            .cloned()
            .collect::<Vec<_>>();

        exchanges.push(ExchangeAudit {
            exchange,
            balances: balances.len(),
            positions,
            open_orders,
            strategy_open_orders,
            external_open_orders,
        });
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&AuditReport { exchanges })?
    );
    Ok(())
}
