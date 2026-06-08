#![allow(clippy::all)]
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

#[path = "cross_arb_server/ws.rs"]
mod cross_arb_server_ws;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::registry::market_adapter;
use rustcta::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, MarketDataAdapter, MarketStateCache,
    MarketSymbolSnapshot, PublicBookProfileKind, RouteStatus, RuntimeMode,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    scan_opportunities, CrossExchangeArbitrageConfig, MarketSnapshot, OpenExecutionStyle,
};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Duration};

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_ws_opportunity_probe",
    version,
    about = "Receive public WS books locally and print cross-exchange arbitrage opportunities"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/cross_exchange_arbitrage_three_venues_hcr_10u_405symbols.live-small.yml"
    )]
    config: PathBuf,
    #[arg(long, default_value_t = 220)]
    max_symbols: usize,
    #[arg(long, default_value_t = 120)]
    seconds: u64,
    #[arg(long, default_value_t = 0.004)]
    min_raw_spread: f64,
    #[arg(long, default_value_t = 0.05)]
    max_raw_spread: f64,
    #[arg(long, default_value_t = -1.0)]
    min_net_edge: f64,
    #[arg(long, default_value_t = 80)]
    ws_batch_size: usize,
    #[arg(long, default_value_t = false)]
    all_updates: bool,
}

#[derive(Debug, Default)]
struct WsStats {
    connected: usize,
    books: usize,
    usable_books: usize,
    errors: usize,
    last_book_at: Option<DateTime<Utc>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let mut config = load_config(&args.config)?;
    force_observe_ws_probe_config(&mut config, &args);
    config.universe.symbols.truncate(args.max_symbols);

    println!(
        "ws_probe config={} exchanges={:?} symbols={} profile={} raw_spread=[{:.4}%, {:.2}%] seconds={}",
        args.config.display(),
        config.universe.enabled_exchanges,
        config.universe.symbols.len(),
        config
            .market
            .public_book_trigger_profile_kind()
            .unwrap_or(PublicBookProfileKind::FastestL1),
        args.min_raw_spread * 100.0,
        args.max_raw_spread * 100.0,
        args.seconds
    );

    run_probe(config, &args).await
}

async fn run_probe(config: CrossExchangeArbitrageConfig, args: &Args) -> Result<()> {
    let mut adapters: Vec<(ExchangeId, Arc<dyn MarketDataAdapter + Send + Sync>)> = Vec::new();
    for exchange in &config.universe.enabled_exchanges {
        if let Some(adapter) = market_adapter(exchange) {
            adapters.push((exchange.clone(), Arc::from(adapter)));
        }
    }

    let (tx, mut rx) = mpsc::channel(16_384);
    let profile = config
        .market
        .public_book_trigger_profile_kind()
        .unwrap_or(PublicBookProfileKind::FastestL1);

    for (exchange, adapter) in &adapters {
        let symbols = config
            .universe
            .symbols
            .iter()
            .map(|symbol| exchange_symbol_for(exchange, symbol))
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            continue;
        }
        tokio::spawn(cross_arb_server_ws::run_public_ws_adapter(
            adapter.clone(),
            symbols,
            cross_arb_server_ws::PublicWsConfig {
                max_symbols_per_connection: config.ws_batch_size_for(exchange, args.ws_batch_size),
                profile,
                ..Default::default()
            },
            tx.clone(),
        ));
    }
    drop(tx);

    let mut cache = MarketStateCache::new(config.market.stale_quote_ms);
    let mut stats: BTreeMap<String, WsStats> = BTreeMap::new();
    let mut seen_opportunities: HashSet<String> = HashSet::new();
    let mut progress = interval(Duration::from_secs(10));
    let deadline = Duration::from_secs(args.seconds.max(1));

    timeout(deadline, async {
        loop {
            tokio::select! {
                _ = progress.tick() => {
                    print_progress(&stats, &cache, &config);
                }
                maybe_update = rx.recv() => {
                    let Some(update) = maybe_update else {
                        break;
                    };
                    match update {
                        cross_arb_server_ws::PublicWsUpdate::Connected { exchange, symbol_count, route, .. } => {
                            let entry = stats.entry(exchange.as_str().to_string()).or_default();
                            entry.connected += 1;
                            println!(
                                "ws_connected exchange={} symbols={} route={}",
                                exchange, symbol_count, route
                            );
                        }
                        cross_arb_server_ws::PublicWsUpdate::Error(error) => {
                            let entry = stats.entry(error.exchange.as_str().to_string()).or_default();
                            entry.errors += 1;
                            println!(
                                "ws_error exchange={} kind={} message={}",
                                error.exchange, error.kind, error.message
                            );
                        }
                        cross_arb_server_ws::PublicWsUpdate::OrderBook(book) => {
                            let now = Utc::now();
                            let exchange = book.exchange.as_str().to_string();
                            let canonical = book.canonical_symbol.clone();
                            let entry = stats.entry(exchange).or_default();
                            entry.books += 1;
                            entry.last_book_at = Some(now);
                            let updated = cache.upsert_orderbook_at(book, now);
                            if updated.is_usable() {
                                entry.usable_books += 1;
                            }
                            print_ws_book(args, &updated);
                            print_opportunities_for_symbol(
                                &cache,
                                &config,
                                &canonical,
                                now,
                                &mut seen_opportunities,
                            );
                        }
                    }
                }
            }
        }
    }).await.ok();

    print_progress(&stats, &cache, &config);
    Ok(())
}

fn print_ws_book(args: &Args, book: &rustcta::market::OrderBook5) {
    if !args.all_updates {
        return;
    }
    let bid = book.best_bid().map(|level| level.price).unwrap_or_default();
    let ask = book.best_ask().map(|level| level.price).unwrap_or_default();
    println!(
        "ws_book exchange={} symbol={} bid={} ask={} usable={} route={}",
        book.exchange,
        book.canonical_symbol,
        bid,
        ask,
        book.is_usable(),
        book.source_route.as_deref().unwrap_or("")
    );
}

fn print_opportunities_for_symbol(
    cache: &MarketStateCache,
    config: &CrossExchangeArbitrageConfig,
    canonical: &CanonicalSymbol,
    now: DateTime<Utc>,
    seen: &mut HashSet<String>,
) {
    let snapshots = market_symbol_snapshots_to_runtime_snapshots(
        &cache.snapshots_for_symbol(canonical),
        config,
        now,
    );
    if snapshots.len() < config.market.min_common_exchanges {
        return;
    }

    let mut opportunities = scan_opportunities(canonical, &snapshots, config, now)
        .into_iter()
        .filter(|opportunity| {
            opportunity.raw_open_spread >= config.thresholds.min_open_raw_spread
                && opportunity.raw_open_spread <= config.thresholds.max_open_raw_spread
        })
        .collect::<Vec<_>>();
    opportunities.sort_by(|left, right| {
        right
            .maker_taker_net_edge
            .partial_cmp(&left.maker_taker_net_edge)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for opportunity in opportunities.into_iter().take(3) {
        let key = format!(
            "{}:{}>{}:{:.5}",
            opportunity.canonical_symbol,
            opportunity.long_exchange,
            opportunity.short_exchange,
            opportunity.raw_open_spread
        );
        if !seen.insert(key) {
            continue;
        }
        println!(
            "opportunity symbol={} long={} short={} raw={:.4}% net={:.4}% can_open={} reject={:?} long_px={:?} short_px={:?}",
            opportunity.canonical_symbol,
            opportunity.long_exchange,
            opportunity.short_exchange,
            opportunity.raw_open_spread * 100.0,
            opportunity.maker_taker_net_edge * 100.0,
            opportunity.can_open,
            opportunity.reject_reasons,
            opportunity.long_limit_price,
            opportunity.short_limit_price
        );
    }
}

fn market_symbol_snapshots_to_runtime_snapshots(
    snapshots: &[MarketSymbolSnapshot],
    config: &CrossExchangeArbitrageConfig,
    now: DateTime<Utc>,
) -> Vec<MarketSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| {
            config
                .universe
                .enabled_exchanges
                .contains(&snapshot.exchange)
        })
        .filter_map(|snapshot| {
            let mut book = snapshot.orderbook.clone()?;
            book.quality.stale = now.signed_duration_since(book.recv_ts).num_milliseconds()
                > config.market.stale_quote_ms;
            if !book.is_usable() {
                return None;
            }
            Some(MarketSnapshot {
                book,
                instrument: snapshot.instrument.clone(),
                route_status: RouteStatus::Healthy,
                funding_rate: 0.0,
                next_funding_time: None,
                trigger_book_profile: config.market.public_book_trigger_profile_kind(),
                validation_book_profile: config.market.public_book_validation_profile_kind(),
            })
        })
        .collect()
}

fn print_progress(
    stats: &BTreeMap<String, WsStats>,
    cache: &MarketStateCache,
    config: &CrossExchangeArbitrageConfig,
) {
    let mut covered_symbols = 0usize;
    for symbol in &config.universe.symbols {
        if cache.snapshots_for_symbol(symbol).len() >= config.market.min_common_exchanges {
            covered_symbols += 1;
        }
    }
    let stats_text = stats
        .iter()
        .map(|(exchange, stats)| {
            format!(
                "{}:connected={} books={} usable={} errors={}",
                exchange, stats.connected, stats.books, stats.usable_books, stats.errors
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    println!(
        "ws_progress covered_symbols={}/{} {}",
        covered_symbols,
        config.universe.symbols.len(),
        stats_text
    );
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn force_observe_ws_probe_config(config: &mut CrossExchangeArbitrageConfig, args: &Args) {
    config.mode = RuntimeMode::Observe;
    config.trading_mode = Default::default();
    config.enable_live_trading = false;
    config.execution.dry_run = true;
    config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
    config.thresholds.min_display_raw_spread = args.min_raw_spread;
    config.thresholds.min_open_raw_spread = args.min_raw_spread;
    config.thresholds.max_open_raw_spread = args.max_raw_spread;
    config.thresholds.min_open_maker_taker_net_edge = args.min_net_edge;
    config.thresholds.route_overrides.clear();
    config.market.public_book_trigger_profile = "fastest_l1".to_string();
    config.market.allow_top_of_book_only = true;
    config.market.min_common_exchanges = config.market.min_common_exchanges.max(2);
    config.persistence.enabled = false;
}
