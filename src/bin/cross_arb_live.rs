use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::adapters::{
    BinanceMarketAdapter, BitgetMarketAdapter, BybitMarketAdapter, GateMarketAdapter,
    HtxMarketAdapter, MexcMarketAdapter,
};
use rustcta::execution::{
    ExecutionEngine, ExecutionRouter, LeverageCommand, PositionMode, PositionModeCommand,
    TradingAdapter,
};
use rustcta::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, InstrumentMeta, MarketDataAdapter,
    MarketFundingSnapshot, RouteStatus,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_cross_arb_live_runtime_parts, live_enabled_exchanges, ArbSignalAction,
    CrossArbExecutionCoordinator, CrossArbRuntime, CrossExchangeArbitrageConfig, MarketSnapshot,
    PrivateRestAuditPlan, PrivateRuntimeSync, SimulatedBundleStatus,
};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration as TokioDuration};

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
    run: bool,
    #[arg(long, default_value_t = false)]
    skip_private_audit: bool,
}

#[derive(Debug, Serialize)]
struct LiveBootstrapReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    execute_requested: bool,
    run_requested: bool,
    dry_run: bool,
    live_ready: bool,
    blocking_reasons: Vec<String>,
    enabled_exchanges: Vec<String>,
    symbols: Vec<String>,
    instruments_loaded: usize,
    router_adapters: Vec<String>,
    private_ws_specs: Vec<PrivateWsSpecSummary>,
    binance_private_ws_specs: usize,
    account_preparation: Vec<AccountPrepSummary>,
    rest_audits: Vec<RestAuditSummary>,
}

#[derive(Debug, Serialize)]
struct PrivateWsSpecSummary {
    exchange: String,
    symbols: usize,
    account_id_configured: bool,
    url: String,
    demo_trading: bool,
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

#[derive(Debug, Serialize)]
struct AccountPrepSummary {
    exchange: String,
    symbol: Option<String>,
    action: String,
    ok: bool,
    dry_run: bool,
    message: Option<String>,
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
    if !args.execute && !config.execution.dry_run {
        return Err(anyhow!(
            "execution.dry_run=false requires --execute so account writes and orders are explicit"
        ));
    }

    let report = bootstrap_live(
        args.config,
        config.clone(),
        args.execute,
        args.run,
        args.skip_private_audit,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.live_ready {
        return Err(anyhow!(
            "live bootstrap is not ready; inspect blocking_reasons"
        ));
    }
    if args.run {
        run_live(config, args.skip_private_audit).await?;
    }
    Ok(())
}

async fn bootstrap_live(
    config_path: PathBuf,
    config: CrossExchangeArbitrageConfig,
    execute_requested: bool,
    run_requested: bool,
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
    let account_preparation =
        prepare_live_accounts(&parts.router, &parts.adapters, &config, &instruments).await;

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
    for prep in &account_preparation {
        if !prep.ok {
            blocking_reasons.push(format!(
                "{} {} account preparation failed",
                prep.exchange, prep.action
            ));
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
        run_requested,
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
                url: spec.url.clone(),
                demo_trading: spec.auth.demo_trading,
            })
            .collect(),
        binance_private_ws_specs: parts.binance_private_ws_specs.len(),
        account_preparation,
        rest_audits,
    })
}

async fn run_live(config: CrossExchangeArbitrageConfig, skip_private_audit: bool) -> Result<()> {
    let enabled_exchanges = live_enabled_exchanges(&config);
    let instruments = load_live_instruments(&config, &enabled_exchanges).await?;
    let parts = build_cross_arb_live_runtime_parts(&config, instruments.clone())?;
    let account_preparation =
        prepare_live_accounts(&parts.router, &parts.adapters, &config, &instruments).await;
    let failed_prep = account_preparation
        .iter()
        .filter(|summary| !summary.ok)
        .map(|summary| format!("{} {}", summary.exchange, summary.action))
        .collect::<Vec<_>>();
    if !failed_prep.is_empty() {
        return Err(anyhow!(
            "account preparation failed before live loop: {}",
            failed_prep.join(", ")
        ));
    }

    let runtime = Arc::new(RwLock::new(CrossArbRuntime::new(
        config.clone(),
        Utc::now(),
    )));
    let execution = Arc::new(RwLock::new(CrossArbExecutionCoordinator::new(
        ExecutionEngine::new(parts.router),
    )));
    let private_sync = Arc::new(PrivateRuntimeSync::with_execution(
        runtime.clone(),
        parts.adapters.clone(),
        Some(execution.clone()),
    ));

    if !skip_private_audit {
        let audits = run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await;
        let failures = audits
            .iter()
            .filter(|summary| !summary.ok)
            .map(|summary| summary.exchange.clone())
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(anyhow!(
                "initial private REST audit failed before live loop: {}",
                failures.join(", ")
            ));
        }
    }

    let mut perp_rx = private_sync.start_private_ws_supervisors(parts.private_ws_specs, 256);
    let private_sync_for_perp = private_sync.clone();
    tokio::spawn(async move {
        private_sync_for_perp
            .pump_private_events(&mut perp_rx)
            .await;
    });

    let mut binance_rx =
        private_sync.start_binance_private_ws_supervisors(parts.binance_private_ws_specs, 256);
    let private_sync_for_binance = private_sync.clone();
    tokio::spawn(async move {
        private_sync_for_binance
            .pump_private_events(&mut binance_rx)
            .await;
    });

    let private_sync_for_audit = private_sync.clone();
    tokio::spawn(periodic_rest_audit_loop(
        private_sync_for_audit,
        config.clone(),
        enabled_exchanges.clone(),
    ));

    tokio::spawn(rest_market_execution_loop(
        runtime.clone(),
        execution.clone(),
        config.clone(),
        enabled_exchanges,
        instruments,
    ));

    futures_util::future::pending::<()>().await;
    Ok(())
}

async fn periodic_rest_audit_loop(
    private_sync: Arc<PrivateRuntimeSync>,
    config: CrossExchangeArbitrageConfig,
    exchanges: Vec<ExchangeId>,
) {
    let mut ticker = interval(TokioDuration::from_secs(
        config.reconciliation.full_audit_interval_secs.max(1),
    ));
    loop {
        ticker.tick().await;
        let _ = run_initial_rest_audits(&private_sync, &config, &exchanges).await;
    }
}

async fn rest_market_execution_loop(
    runtime: Arc<RwLock<CrossArbRuntime>>,
    execution: Arc<RwLock<CrossArbExecutionCoordinator>>,
    config: CrossExchangeArbitrageConfig,
    exchanges: Vec<ExchangeId>,
    instruments: Vec<InstrumentMeta>,
) {
    let mut adapters = Vec::new();
    for exchange in &exchanges {
        if let Some(adapter) = market_adapter(exchange) {
            adapters.push((exchange.clone(), adapter));
        }
    }
    let instruments_by_symbol = instruments
        .into_iter()
        .map(|instrument| {
            (
                (
                    instrument.exchange.clone(),
                    instrument.canonical_symbol.clone(),
                ),
                instrument,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut ticker = interval(TokioDuration::from_millis(
        (config.market.stale_quote_ms / 2).max(1_000) as u64,
    ));

    loop {
        ticker.tick().await;
        let now = Utc::now();
        let funding = fetch_live_funding(&adapters, &config.universe.symbols).await;
        let snapshots_by_symbol =
            fetch_live_orderbooks(&adapters, &config, &funding, &instruments_by_symbol, now).await;

        let mut runtime_guard = runtime.write().await;
        let mut execution_guard = execution.write().await;
        let _ = execution_guard
            .cancel_expired_maker_orders(&mut runtime_guard, now)
            .await;
        if has_active_bundle(&runtime_guard) || execution_guard.pending_maker_count() > 0 {
            continue;
        }

        for symbol in &config.universe.symbols {
            let Some(snapshots) = snapshots_by_symbol.get(symbol) else {
                continue;
            };
            if snapshots.len() < config.market.min_common_exchanges {
                continue;
            }
            let signals = runtime_guard.on_market_snapshots(symbol, snapshots, now);
            for signal in signals
                .iter()
                .filter(|signal| signal.action == ArbSignalAction::Open)
                .take(1)
            {
                let signal_canonical_symbol = signal.opportunity_id.as_ref().and_then(|id| {
                    runtime_guard
                        .state
                        .opportunities
                        .iter()
                        .find(|opportunity| opportunity.opportunity_id == *id)
                        .map(|opportunity| opportunity.canonical_symbol.clone())
                });
                match execution_guard
                    .execute_open_signal(&mut runtime_guard, signal)
                    .await
                {
                    Ok(Some(_)) | Ok(None) => {}
                    Err(error) => runtime_guard
                        .state
                        .risk_events
                        .push(rustcta::strategies::cross_exchange_arbitrage::RiskEventReadModel {
                            event_id: format!("live-open-exec-{}", now.timestamp_millis()),
                            canonical_symbol: signal_canonical_symbol,
                            exchange: None,
                            reason: rustcta::strategies::cross_exchange_arbitrage::RejectReason::RouteUnhealthy,
                            message: error.to_string(),
                            created_at: now,
                        }),
                }
                break;
            }
            if execution_guard.pending_maker_count() > 0 || has_active_bundle(&runtime_guard) {
                break;
            }
        }
    }
}

async fn fetch_live_funding(
    adapters: &[(ExchangeId, Box<dyn MarketDataAdapter + Send + Sync>)],
    symbols: &[CanonicalSymbol],
) -> HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot> {
    let mut snapshots = HashMap::new();
    for (exchange, adapter) in adapters {
        if !adapter.capabilities().supports_funding {
            continue;
        }
        match adapter.load_funding(symbols).await {
            Ok(items) => {
                for item in items {
                    snapshots.insert((exchange.clone(), item.canonical_symbol.clone()), item);
                }
            }
            Err(error) => {
                log::warn!(
                    "cross_arb_live funding fetch failed exchange={} error={}",
                    exchange,
                    error
                );
            }
        }
    }
    snapshots
}

async fn fetch_live_orderbooks(
    adapters: &[(ExchangeId, Box<dyn MarketDataAdapter + Send + Sync>)],
    config: &CrossExchangeArbitrageConfig,
    funding: &HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
    instruments: &HashMap<(ExchangeId, CanonicalSymbol), InstrumentMeta>,
    now: DateTime<Utc>,
) -> HashMap<CanonicalSymbol, Vec<MarketSnapshot>> {
    let mut snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>> = HashMap::new();
    for (exchange, adapter) in adapters {
        if !adapter.capabilities().supports_rest_snapshot {
            continue;
        }
        for symbol in &config.universe.symbols {
            let exchange_symbol = exchange_symbol_for(exchange, symbol);
            match adapter
                .fetch_orderbook_snapshot(&exchange_symbol, config.market.depth_levels)
                .await
            {
                Ok(book) => {
                    let funding = funding.get(&(exchange.clone(), symbol.clone()));
                    snapshots_by_symbol
                        .entry(symbol.clone())
                        .or_default()
                        .push(MarketSnapshot {
                            book,
                            instrument: instruments
                                .get(&(exchange.clone(), symbol.clone()))
                                .cloned(),
                            route_status: RouteStatus::Healthy,
                            funding_rate: funding
                                .map(|snapshot| snapshot.funding_rate)
                                .unwrap_or(0.0),
                            next_funding_time: funding
                                .and_then(|snapshot| snapshot.next_funding_time),
                        });
                }
                Err(error) => {
                    log::warn!(
                        "cross_arb_live orderbook fetch failed exchange={} symbol={} error={}",
                        exchange,
                        symbol,
                        error
                    );
                }
            }
        }
    }
    for snapshots in snapshots_by_symbol.values_mut() {
        for snapshot in snapshots {
            snapshot.book.quality.stale = now
                .signed_duration_since(snapshot.book.recv_ts)
                .num_milliseconds()
                > config.market.stale_quote_ms;
        }
    }
    snapshots_by_symbol
}

fn has_active_bundle(runtime: &CrossArbRuntime) -> bool {
    runtime.state.open_bundles.values().any(|bundle| {
        matches!(
            bundle.status,
            SimulatedBundleStatus::MakerPending
                | SimulatedBundleStatus::MakerFilled
                | SimulatedBundleStatus::Hedging
                | SimulatedBundleStatus::OpenSimulated
                | SimulatedBundleStatus::ClosingSimulated
                | SimulatedBundleStatus::OrphanLeg
        )
    })
}

async fn prepare_live_accounts(
    router: &ExecutionRouter,
    adapters: &[Arc<dyn TradingAdapter>],
    config: &CrossExchangeArbitrageConfig,
    instruments: &[InstrumentMeta],
) -> Vec<AccountPrepSummary> {
    let mut summaries = Vec::new();
    let leverage = config.sizing.leverage.round().max(1.0) as u32;
    for adapter in adapters {
        let exchange = adapter.exchange();
        let capabilities = adapter.capabilities();
        let mode = configured_position_mode_for_config(config, &exchange);
        if capabilities.supports_position_mode_change {
            let action = "set_position_mode".to_string();
            match router
                .route_set_position_mode(PositionModeCommand {
                    exchange: exchange.clone(),
                    mode,
                    requested_at: Utc::now(),
                })
                .await
            {
                Ok(ack) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: None,
                    action,
                    ok: config.execution.dry_run || ack.accepted,
                    dry_run: config.execution.dry_run,
                    message: ack.message,
                }),
                Err(err) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: None,
                    action,
                    ok: false,
                    dry_run: config.execution.dry_run,
                    message: Some(err.to_string()),
                }),
            }
        } else if mode.is_hedge() {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_position_mode".to_string(),
                ok: false,
                dry_run: config.execution.dry_run,
                message: Some(format!(
                    "{exchange} does not support position mode changes but hedge was requested"
                )),
            });
        } else {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_position_mode".to_string(),
                ok: true,
                dry_run: config.execution.dry_run,
                message: Some(
                    "skipped: one-way mode and adapter does not change account mode".to_string(),
                ),
            });
        }

        if !capabilities.supports_leverage {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_leverage".to_string(),
                ok: false,
                dry_run: config.execution.dry_run,
                message: Some(format!("{exchange} does not support leverage changes")),
            });
            continue;
        }

        for instrument in instruments.iter().filter(|item| item.exchange == exchange) {
            let command = LeverageCommand {
                exchange: exchange.clone(),
                canonical_symbol: instrument.canonical_symbol.clone(),
                exchange_symbol: instrument.exchange_symbol.clone(),
                leverage,
                requested_at: Utc::now(),
            };
            match router.route_set_leverage(command).await {
                Ok(ack) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(ack.canonical_symbol.to_string()),
                    action: "set_leverage".to_string(),
                    ok: config.execution.dry_run || ack.accepted,
                    dry_run: config.execution.dry_run,
                    message: ack.message,
                }),
                Err(err) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(instrument.canonical_symbol.to_string()),
                    action: "set_leverage".to_string(),
                    ok: false,
                    dry_run: config.execution.dry_run,
                    message: Some(err.to_string()),
                }),
            }
        }

        for instrument in instruments.iter().filter(|item| item.exchange == exchange) {
            match adapter.get_trade_fee(&instrument.exchange_symbol).await {
                Ok(fee) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(instrument.canonical_symbol.to_string()),
                    action: "read_trade_fee".to_string(),
                    ok: fee.maker.is_finite() && fee.taker.is_finite(),
                    dry_run: false,
                    message: Some(format!("maker={} taker={}", fee.maker, fee.taker)),
                }),
                Err(err) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(instrument.canonical_symbol.to_string()),
                    action: "read_trade_fee".to_string(),
                    ok: false,
                    dry_run: false,
                    message: Some(err.to_string()),
                }),
            }

            match adapter
                .get_symbol_account_config(&instrument.exchange_symbol)
                .await
            {
                Ok(readback) => {
                    let mode_ok = readback
                        .position_mode
                        .map(|readback_mode| {
                            readback_mode == mode || !capabilities.supports_position_mode_change
                        })
                        .unwrap_or(true);
                    let leverage_ok = readback
                        .leverage
                        .map(|readback_leverage| readback_leverage == leverage)
                        .unwrap_or(true);
                    summaries.push(AccountPrepSummary {
                        exchange: exchange.as_str().to_string(),
                        symbol: Some(instrument.canonical_symbol.to_string()),
                        action: "read_symbol_account_config".to_string(),
                        ok: mode_ok && leverage_ok,
                        dry_run: false,
                        message: Some(format!(
                            "position_mode={:?} margin_mode={:?} leverage={:?} expected_leverage={} max_leverage={:?}",
                            readback.position_mode,
                            readback.margin_mode,
                            readback.leverage,
                            leverage,
                            readback.max_leverage
                        )),
                    })
                }
                Err(err) => summaries.push(AccountPrepSummary {
                    exchange: exchange.as_str().to_string(),
                    symbol: Some(instrument.canonical_symbol.to_string()),
                    action: "read_symbol_account_config".to_string(),
                    ok: false,
                    dry_run: false,
                    message: Some(err.to_string()),
                }),
            }
        }
    }
    summaries
}

fn configured_position_mode_for_config(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> PositionMode {
    config
        .exchanges
        .get(exchange)
        .and_then(|runtime| runtime.position_mode.as_deref())
        .map(|mode| {
            if matches!(
                mode.to_ascii_lowercase().as_str(),
                "hedge" | "hedged" | "long_short"
            ) {
                PositionMode::Hedge
            } else {
                PositionMode::OneWay
            }
        })
        .unwrap_or(PositionMode::OneWay)
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
        ExchangeId::Bybit => Some(Box::new(BybitMarketAdapter)),
        ExchangeId::Mexc => Some(Box::new(MexcMarketAdapter)),
        ExchangeId::Htx => Some(Box::new(HtxMarketAdapter)),
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
        assert!(market_adapter(&ExchangeId::Bybit).is_some());
        assert!(market_adapter(&ExchangeId::Mexc).is_some());
        assert!(market_adapter(&ExchangeId::Htx).is_some());
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
        for exchange in live_enabled_exchanges(&config) {
            assert!(config
                .exchanges
                .get(&exchange)
                .is_some_and(|runtime| runtime.private_ws_enabled));
        }
    }
}
