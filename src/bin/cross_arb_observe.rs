use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::adapters::{
    BinanceMarketAdapter, BitgetMarketAdapter, GateMarketAdapter, MarketAdapterInfo,
    OkxMarketAdapter,
};
use rustcta::market::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta, MarketDataAdapter,
    MarketStateCache, RouteStatus, RuntimeMode,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    scan_opportunities, CrossExchangeArbitrageConfig, MarketSnapshot, Opportunity,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_observe",
    version,
    about = "Run one safe public-REST cross-exchange arbitrage observation pass"
)]
struct Args {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: PathBuf,
}

#[derive(Debug, Serialize)]
struct ObserveSummary {
    config_path: String,
    mode: RuntimeMode,
    configured_mode: RuntimeMode,
    generated_at: DateTime<Utc>,
    symbols: Vec<String>,
    exchanges_seen: Vec<String>,
    books_loaded: usize,
    funding_loaded: usize,
    opportunities: Vec<Opportunity>,
    errors: Vec<ObserveError>,
}

#[derive(Debug, Serialize)]
struct ObserveError {
    exchange: String,
    symbol: Option<String>,
    kind: &'static str,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = load_config(&args.config)?;
    config
        .validate()
        .context("invalid cross arbitrage config")?;

    let mut summary = observe_once(args.config, config).await?;
    summary.opportunities.sort_by(|left, right| {
        right
            .maker_taker_net_edge
            .partial_cmp(&left.maker_taker_net_edge)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn observe_once(
    config_path: PathBuf,
    config: CrossExchangeArbitrageConfig,
) -> Result<ObserveSummary> {
    let adapters = configured_adapters(&config);
    let now = Utc::now();
    let mut cache = MarketStateCache::new(config.risk.stale_quote_ms);
    let mut errors = Vec::new();
    let mut exchanges_seen = BTreeSet::new();
    let mut books_loaded = 0usize;
    let mut funding_loaded = 0usize;
    let mut coverage_by_symbol: HashMap<CanonicalSymbol, HashSet<ExchangeId>> = HashMap::new();
    let mut instruments_by_exchange_symbol: HashMap<(ExchangeId, CanonicalSymbol), InstrumentMeta> =
        HashMap::new();

    for adapter in &adapters {
        let exchange = adapter.exchange();
        let supported_symbols = match adapter.load_instruments().await {
            Ok(instruments) => {
                let mut supported = HashSet::new();
                for instrument in instruments
                    .into_iter()
                    .filter(|instrument| instrument.is_tradeable_usdt_perpetual())
                {
                    supported.insert(instrument.canonical_symbol.clone());
                    instruments_by_exchange_symbol.insert(
                        (exchange.clone(), instrument.canonical_symbol.clone()),
                        instrument,
                    );
                }
                supported
            }
            Err(err) => {
                errors.push(ObserveError {
                    exchange: exchange.as_str().to_string(),
                    symbol: None,
                    kind: "instruments",
                    message: err.to_string(),
                });
                HashSet::new()
            }
        };
        let symbols_for_exchange = config
            .universe
            .symbols
            .iter()
            .filter(|symbol| supported_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        for canonical in &symbols_for_exchange {
            coverage_by_symbol
                .entry(canonical.clone())
                .or_default()
                .insert(exchange.clone());
            let symbol = exchange_symbol_for(&exchange, canonical);

            match adapter.fetch_orderbook_snapshot(&symbol, 5).await {
                Ok(book) => {
                    cache.upsert_orderbook_at(book, now);
                    books_loaded += 1;
                    exchanges_seen.insert(exchange.as_str().to_string());
                }
                Err(err) => errors.push(ObserveError {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(canonical.to_string()),
                    kind: "orderbook",
                    message: err.to_string(),
                }),
            }
        }

        match adapter.load_funding(&symbols_for_exchange).await {
            Ok(snapshots) => {
                for snapshot in snapshots {
                    cache.upsert_funding(snapshot);
                    funding_loaded += 1;
                    exchanges_seen.insert(exchange.as_str().to_string());
                }
            }
            Err(err) => errors.push(ObserveError {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                kind: "funding",
                message: err.to_string(),
            }),
        }
    }

    for canonical in &config.universe.symbols {
        let venues = coverage_by_symbol
            .get(canonical)
            .map(HashSet::len)
            .unwrap_or_default();
        if venues < config.market.min_common_exchanges {
            errors.push(ObserveError {
                exchange: "universe".to_string(),
                symbol: Some(canonical.to_string()),
                kind: "coverage",
                message: format!(
                    "available on {venues} exchange(s), requires at least {}",
                    config.market.min_common_exchanges
                ),
            });
        }
    }

    let mut opportunities = Vec::new();
    for canonical in &config.universe.symbols {
        let snapshots = cache
            .snapshots_for_symbol(canonical)
            .into_iter()
            .filter_map(|snapshot| {
                let book = snapshot.orderbook?;
                let funding = snapshot.funding;
                let instrument = instruments_by_exchange_symbol
                    .get(&(book.exchange.clone(), canonical.clone()))
                    .cloned();
                Some(MarketSnapshot {
                    book,
                    instrument,
                    route_status: RouteStatus::Healthy,
                    funding_rate: funding
                        .as_ref()
                        .map(|funding| funding.funding_rate)
                        .unwrap_or(0.0),
                    next_funding_time: funding.and_then(|funding| funding.next_funding_time),
                })
            })
            .collect::<Vec<_>>();

        if snapshots.len() >= 2 {
            opportunities.extend(scan_opportunities(canonical, &snapshots, &config, now));
        }
    }

    Ok(ObserveSummary {
        config_path: config_path.display().to_string(),
        mode: RuntimeMode::Observe,
        configured_mode: config.mode,
        generated_at: now,
        symbols: config
            .universe
            .symbols
            .iter()
            .map(ToString::to_string)
            .collect(),
        exchanges_seen: exchanges_seen.into_iter().collect(),
        books_loaded,
        funding_loaded,
        opportunities,
        errors,
    })
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn configured_adapters(
    config: &CrossExchangeArbitrageConfig,
) -> Vec<Box<dyn MarketDataAdapter + Send + Sync>> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter_map(|exchange| match exchange {
            ExchangeId::Binance => {
                Some(Box::new(BinanceMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Okx => {
                Some(Box::new(OkxMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Bitget => {
                Some(Box::new(BitgetMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Gate => {
                Some(Box::new(GateMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Other(_) => None,
        })
        .collect()
}

fn exchange_symbol_for(exchange: &ExchangeId, canonical: &CanonicalSymbol) -> ExchangeSymbol {
    match exchange {
        ExchangeId::Binance => BinanceMarketAdapter.to_exchange_symbol(canonical),
        ExchangeId::Okx => OkxMarketAdapter.to_exchange_symbol(canonical),
        ExchangeId::Bitget => BitgetMarketAdapter.to_exchange_symbol(canonical),
        ExchangeId::Gate => GateMarketAdapter.to_exchange_symbol(canonical),
        ExchangeId::Other(name) => {
            ExchangeSymbol::new(ExchangeId::Other(name.clone()), canonical.to_string())
        }
    }
}
