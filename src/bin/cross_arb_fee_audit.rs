#![allow(clippy::all)]
use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use rustcta::execution::{FillEvent, FillQuery};
use rustcta::market::{exchange_symbol_for, CanonicalSymbol, ExchangeId};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_trading_adapter_for_exchange, live_enabled_exchanges, CrossExchangeArbitrageConfig,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_fee_audit",
    version,
    about = "Read-only recent fill fee audit for cross-exchange arbitrage venues"
)]
struct Args {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: PathBuf,
    #[arg(long, value_delimiter = ',', default_value = "ALLO/USDT")]
    symbols: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exchanges: Vec<String>,
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long, default_value_t = 24)]
    since_hours: i64,
    #[arg(long, default_value_t = 0.20)]
    binance_rebate: f64,
    #[arg(long, default_value_t = 0.60)]
    other_rebate: f64,
}

#[derive(Debug, Serialize)]
struct FeeAuditReport {
    generated_at: DateTime<Utc>,
    config: String,
    symbols: Vec<String>,
    since: DateTime<Utc>,
    limit_per_symbol: u32,
    rebate: BTreeMap<String, f64>,
    summaries: Vec<ExchangeSymbolFeeSummary>,
}

#[derive(Debug, Serialize)]
struct ExchangeSymbolFeeSummary {
    exchange: ExchangeId,
    symbol: CanonicalSymbol,
    fills: usize,
    notional_usdt: f64,
    reported_fee_usdt: f64,
    net_fee_after_rebate_usdt: f64,
    reported_fee_rate: Option<f64>,
    net_fee_rate_after_rebate: Option<f64>,
    fills_detail: Vec<FillFeeRow>,
}

#[derive(Debug, Serialize)]
struct FillFeeRow {
    filled_at: DateTime<Utc>,
    trade_id: String,
    exchange_order_id: Option<String>,
    side: String,
    position_side: String,
    liquidity: String,
    price: f64,
    quantity: f64,
    notional_usdt: f64,
    fee: Option<f64>,
    fee_asset: Option<String>,
    fee_rate: Option<f64>,
    net_fee_after_rebate_usdt: Option<f64>,
    effective_reported_fee_rate: Option<f64>,
    effective_net_fee_rate_after_rebate: Option<f64>,
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

    let symbols = args
        .symbols
        .iter()
        .map(|value| {
            CanonicalSymbol::parse(value)
                .with_context(|| format!("invalid canonical symbol {value:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let requested_exchanges = if args.exchanges.is_empty() {
        live_enabled_exchanges(&config)
    } else {
        args.exchanges
            .iter()
            .map(|exchange| ExchangeId::from(exchange.as_str()))
            .collect()
    };
    let since = Utc::now() - Duration::hours(args.since_hours);

    let mut summaries = Vec::new();
    for exchange in requested_exchanges {
        let adapter = build_trading_adapter_for_exchange(&config, &exchange)
            .with_context(|| format!("build {} trading adapter", exchange.as_str()))?;
        for symbol in &symbols {
            let exchange_symbol = exchange_symbol_for(&exchange, symbol);
            let mut query =
                FillQuery::for_symbol(exchange.clone(), symbol.clone(), exchange_symbol);
            query.limit = Some(args.limit);
            query.start_time = Some(since);
            let mut fills = adapter
                .get_fills(query)
                .await
                .with_context(|| format!("read {} {} fills", exchange.as_str(), symbol))?;
            fills.retain(|fill| fill.filled_at >= since);
            fills.sort_by_key(|fill| fill.filled_at);
            summaries.push(summarize_fills(
                exchange.clone(),
                symbol.clone(),
                fills,
                rebate_for(&exchange, args.binance_rebate, args.other_rebate),
            ));
        }
    }

    let mut rebate = BTreeMap::new();
    rebate.insert("binance".to_string(), args.binance_rebate);
    rebate.insert("bitget".to_string(), args.other_rebate);
    rebate.insert("gate".to_string(), args.other_rebate);

    println!(
        "{}",
        serde_json::to_string_pretty(&FeeAuditReport {
            generated_at: Utc::now(),
            config: args.config.display().to_string(),
            symbols: symbols.iter().map(ToString::to_string).collect(),
            since,
            limit_per_symbol: args.limit,
            rebate,
            summaries,
        })?
    );
    Ok(())
}

fn summarize_fills(
    exchange: ExchangeId,
    symbol: CanonicalSymbol,
    fills: Vec<FillEvent>,
    rebate_rate: f64,
) -> ExchangeSymbolFeeSummary {
    let fills_detail = fills
        .iter()
        .map(|fill| fill_row(fill, rebate_rate))
        .collect::<Vec<_>>();
    let notional_usdt = fills_detail
        .iter()
        .map(|fill| fill.notional_usdt)
        .sum::<f64>();
    let reported_fee_usdt = fills_detail.iter().filter_map(|fill| fill.fee).sum::<f64>();
    let net_fee_after_rebate_usdt = reported_fee_usdt * (1.0 - rebate_rate);

    ExchangeSymbolFeeSummary {
        exchange,
        symbol,
        fills: fills_detail.len(),
        notional_usdt,
        reported_fee_usdt,
        net_fee_after_rebate_usdt,
        reported_fee_rate: nonzero_rate(reported_fee_usdt, notional_usdt),
        net_fee_rate_after_rebate: nonzero_rate(net_fee_after_rebate_usdt, notional_usdt),
        fills_detail,
    }
}

fn fill_row(fill: &FillEvent, rebate_rate: f64) -> FillFeeRow {
    let notional = fill.notional();
    let fee = fill.fee.map(f64::abs);
    let net_fee = fee.map(|value| value * (1.0 - rebate_rate));
    FillFeeRow {
        filled_at: fill.filled_at,
        trade_id: fill.trade_id.clone(),
        exchange_order_id: fill.exchange_order_id.clone(),
        side: format!("{:?}", fill.side),
        position_side: format!("{:?}", fill.position_side),
        liquidity: format!("{:?}", fill.liquidity),
        price: fill.price,
        quantity: fill.quantity,
        notional_usdt: notional,
        fee,
        fee_asset: fill.fee_asset.clone(),
        fee_rate: fill.fee_rate,
        net_fee_after_rebate_usdt: net_fee,
        effective_reported_fee_rate: fee.and_then(|value| nonzero_rate(value, notional)),
        effective_net_fee_rate_after_rebate: net_fee
            .and_then(|value| nonzero_rate(value, notional)),
    }
}

fn nonzero_rate(fee: f64, notional: f64) -> Option<f64> {
    (notional > 0.0).then_some(fee / notional)
}

fn rebate_for(exchange: &ExchangeId, binance_rebate: f64, other_rebate: f64) -> f64 {
    if *exchange == ExchangeId::Binance {
        binance_rebate
    } else {
        other_rebate
    }
}
