use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

#[path = "cross_arb_server/ws.rs"]
mod cross_arb_server_ws;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::registry as exchange_registry;
use rustcta::execution::{
    is_crossarb_client_order_id, BundleLeg, CancelCommand, ExecutionEngine, ExecutionRouter,
    OrderCommand, OrderIntent, OrderSide, OrderType, PositionMode, PositionSide, TimeInForce,
    TradeFeeSnapshot, TradingAdapter,
};
use rustcta::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta,
    MarketDataAdapter, MarketFundingSnapshot, MarketStateCache, RoundingMode,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_cross_arb_live_runtime_parts, exchange_route_status, live_close_candidate_for_bundle,
    live_close_metrics_for_bundle, live_enabled_exchanges, order_ack_is_live, ArbSignal,
    ArbSignalAction, BundleCloseMetricReadModel, CrossArbExecutionCoordinator, CrossArbRuntime,
    CrossExchangeArbitrageConfig, ExchangeFeeRates, HedgeRepairTaskReadModel, MarketSnapshot,
    PrivateRestAuditPlan, PrivateRuntimeSync, RejectReason, RiskEventReadModel,
    SimulatedBundleStatus,
};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration as TokioDuration};

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
    #[arg(long)]
    max_symbols: Option<usize>,
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
    account_fee_overrides: Vec<AccountFeeOverrideSummary>,
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
    nonzero_positions: usize,
    open_orders: usize,
    strategy_open_orders: usize,
    external_open_orders: usize,
    fills: usize,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct StartupStrategyOrderCancelAttempt {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    accepted: bool,
    message: Option<String>,
}

const STARTUP_ORDER_CANCEL_SETTLE_SECS: u64 = 3;

#[derive(Debug, Clone, Serialize)]
struct AccountPrepSummary {
    exchange: String,
    symbol: Option<String>,
    action: String,
    ok: bool,
    dry_run: bool,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct AccountFeeOverrideSummary {
    exchange: String,
    maker: f64,
    taker: f64,
    symbols: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    rustcta::utils::init_tracing_logger("info");
    let args = Args::parse();
    let mut config = load_config(&args.config)?;
    if let Some(max_symbols) = args.max_symbols {
        config.universe.symbols.truncate(max_symbols);
    }
    config
        .validate()
        .context("invalid cross arbitrage live config")?;
    validate_cross_arb_live_admission(
        config.mode,
        config.execution.dry_run,
        args.execute,
        args.skip_private_audit,
    )?;

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
        if args.run && report.only_blocks_on_external_exposure() {
            log::warn!(
                "live bootstrap found external exposure/open orders; starting runtime in close-only mode without taking over external positions: {}",
                report.blocking_reasons.join("; ")
            );
        } else {
            return Err(anyhow!(
                "live bootstrap is not ready; inspect blocking_reasons"
            ));
        }
    }
    if args.run {
        run_live(config, args.skip_private_audit).await?;
    }
    Ok(())
}

fn validate_cross_arb_live_admission(
    mode: rustcta::market::RuntimeMode,
    dry_run: bool,
    execute_requested: bool,
    skip_private_audit: bool,
) -> Result<()> {
    if !mode.allows_live_orders() {
        if execute_requested {
            return Err(anyhow!(
                "--execute requires live-capable mode; got {mode:?}"
            ));
        }
        if !dry_run {
            return Err(anyhow!(
                "non-live cross_arb_live runs are public discovery only and require execution.dry_run=true"
            ));
        }
        if !skip_private_audit {
            return Err(anyhow!(
                "non-live public discovery requires --skip-private-audit so API keys are not required"
            ));
        }
        return Ok(());
    }
    if execute_requested && dry_run {
        return Err(anyhow!(
            "--execute was requested but execution.dry_run=true; switch dry_run=false only after preflight passes"
        ));
    }
    if !execute_requested && !dry_run {
        return Err(anyhow!(
            "execution.dry_run=false requires --execute so account writes and orders are explicit"
        ));
    }
    Ok(())
}

impl LiveBootstrapReport {
    fn only_blocks_on_external_exposure(&self) -> bool {
        !self.blocking_reasons.is_empty()
            && self
                .blocking_reasons
                .iter()
                .all(|reason| reason.contains("startup audit found nonzero_positions="))
    }
}

async fn bootstrap_live(
    config_path: PathBuf,
    mut config: CrossExchangeArbitrageConfig,
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
    let account_fee_overrides = apply_account_fee_overrides(&mut config, &account_preparation);

    let mut rest_audits = if skip_private_audit {
        Vec::new()
    } else {
        run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await
    };
    if execute_requested
        && run_requested
        && !skip_private_audit
        && !config.execution.dry_run
        && rest_audits
            .iter()
            .any(|audit| audit.strategy_open_orders > 0)
    {
        let startup_cancel_attempts =
            cancel_startup_strategy_open_orders(&private_sync, &parts.adapters, &enabled_exchanges)
                .await;
        log_startup_strategy_order_cancel_attempts(&startup_cancel_attempts);
        if !startup_cancel_attempts.is_empty() {
            sleep(TokioDuration::from_secs(STARTUP_ORDER_CANCEL_SETTLE_SECS)).await;
            rest_audits = run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await;
        }
    }
    let mut blocking_reasons = Vec::new();
    if !skip_private_audit {
        for audit in &rest_audits {
            if !audit.ok {
                blocking_reasons.push(format!("{} private REST audit failed", audit.exchange));
            }
            if audit.strategy_open_orders > 0 {
                blocking_reasons.push(format!(
                    "{} startup audit found {} crossarb strategy open order(s); restart in close-only/reconcile mode or cancel/settle them before allowing new entries",
                    audit.exchange, audit.strategy_open_orders
                ));
            }
            if config.risk.block_on_external_account_exposure
                && (audit.nonzero_positions > 0 || audit.open_orders > 0)
            {
                blocking_reasons.push(format!(
                    "{} startup audit found nonzero_positions={} open_orders={}; runtime may start close-only but new entries stay blocked until external exposure is cleared",
                    audit.exchange, audit.nonzero_positions, audit.open_orders
                ));
            } else if audit.nonzero_positions > 0 || audit.open_orders > 0 {
                log::warn!(
                    "cross_arb_live startup audit observed external exposure exchange={} nonzero_positions={} open_orders={} but block_on_external_account_exposure=false",
                    audit.exchange,
                    audit.nonzero_positions,
                    audit.open_orders
                );
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
    if !config.execution.dry_run {
        for exchange in &enabled_exchanges {
            if !parts.router.has_adapter(exchange) {
                blocking_reasons.push(format!("missing router adapter for {}", exchange.as_str()));
            }
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
        account_fee_overrides,
    })
}

async fn run_live(
    mut config: CrossExchangeArbitrageConfig,
    skip_private_audit: bool,
) -> Result<()> {
    let enabled_exchanges = live_enabled_exchanges(&config);
    let instruments = load_live_instruments(&config, &enabled_exchanges).await?;
    let parts = build_cross_arb_live_runtime_parts(&config, instruments.clone())?;
    let runtime = Arc::new(RwLock::new(CrossArbRuntime::new(
        config.clone(),
        Utc::now(),
    )));
    {
        let mut guard = runtime.write().await;
        let (records, tasks) = guard.state.import_persisted_hedge_state(Utc::now());
        if records > 0 || tasks > 0 {
            log::info!(
                "cross_arb_live imported persisted hedge state records={} repair_tasks={}",
                records,
                tasks
            );
        }
    }
    let private_sync = Arc::new(PrivateRuntimeSync::new(
        runtime.clone(),
        parts.adapters.clone(),
    ));

    if !skip_private_audit {
        let mut audits = run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await;
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
        let startup_cancel_attempts =
            cancel_startup_strategy_open_orders(&private_sync, &parts.adapters, &enabled_exchanges)
                .await;
        if !startup_cancel_attempts.is_empty() {
            log_startup_strategy_order_cancel_attempts(&startup_cancel_attempts);
            sleep(TokioDuration::from_secs(STARTUP_ORDER_CANCEL_SETTLE_SECS)).await;
            audits = run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await;
        }
        let dirty_exchanges = audits
            .iter()
            .filter(|summary| summary.nonzero_positions > 0 || summary.open_orders > 0)
            .map(|summary| {
                format!(
                    "{} nonzero_positions={} open_orders={}",
                    summary.exchange, summary.nonzero_positions, summary.open_orders
                )
            })
            .collect::<Vec<_>>();
        let strategy_order_exchanges = audits
            .iter()
            .filter(|summary| summary.strategy_open_orders > 0)
            .map(|summary| {
                format!(
                    "{} strategy_open_orders={}",
                    summary.exchange, summary.strategy_open_orders
                )
            })
            .collect::<Vec<_>>();
        let remaining_unpaired = {
            let guard = runtime.read().await;
            guard.state.account_unpaired_exchange_positions()
        };
        if remaining_unpaired.is_empty() {
            log::info!("cross_arb_live startup audit found no unmanaged account positions");
        } else {
            let details = remaining_unpaired
                .iter()
                .map(|position| {
                    format!(
                        "{} {} {:?} qty={}",
                        position.exchange,
                        position.canonical_symbol,
                        position.position_side,
                        position.quantity
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            if config.risk.orphan_exposure_blocks_new_entries {
                let mut guard = runtime.write().await;
                guard.state.set_close_only();
                log::error!(
                    "cross_arb_live startup found {} unmanaged account position(s); keeping close-only and leaving them untouched: {}",
                    remaining_unpaired.len(),
                    details
                );
            } else {
                log::warn!(
                    "cross_arb_live startup found {} unmanaged account position(s); continuing because orphan_exposure_blocks_new_entries=false and leaving them untouched: {}",
                    remaining_unpaired.len(),
                    details
                );
            }
        }
        if !strategy_order_exchanges.is_empty() {
            let mut guard = runtime.write().await;
            guard.state.set_close_only();
            return Err(anyhow!(
                "initial private REST audit found existing crossarb open orders without restored in-memory state: {}; cancel/reconcile them before allowing new entries",
                strategy_order_exchanges.join(", ")
            ));
        }
        if config.risk.block_on_external_account_exposure
            && !dirty_exchanges.is_empty()
            && config
                .exchanges
                .values()
                .all(|runtime| runtime.allows_new_entries())
        {
            let mut guard = runtime.write().await;
            guard.state.set_close_only();
            log::error!(
                "initial private REST audit found existing exposure/open orders without restored state: {}; starting close-only and blocking new entries",
                dirty_exchanges.join(", ")
            );
        } else if !dirty_exchanges.is_empty() {
            log::warn!(
                "cross_arb_live initial audit observed external account exposure but continuing because block_on_external_account_exposure=false: {}",
                dirty_exchanges.join(", ")
            );
        }
    }

    let account_preparation =
        prepare_live_accounts(&parts.router, &parts.adapters, &config, &instruments).await;
    let fee_overrides = apply_account_fee_overrides(&mut config, &account_preparation);
    if !fee_overrides.is_empty() {
        log::info!(
            "cross_arb_live applied {} account fee override(s) to runtime opportunity model",
            fee_overrides.len()
        );
    }
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

    let execution = Arc::new(RwLock::new(CrossArbExecutionCoordinator::new(
        ExecutionEngine::new(parts.router),
    )));
    let close_metrics = Arc::new(RwLock::new(HashMap::new()));
    let private_sync = Arc::new(PrivateRuntimeSync::with_execution(
        runtime.clone(),
        parts.adapters.clone(),
        Some(execution.clone()),
    ));

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

    if !skip_private_audit && !config.execution.dry_run {
        let private_sync_for_audit = private_sync.clone();
        tokio::spawn(periodic_rest_audit_loop(
            private_sync_for_audit,
            config.clone(),
            enabled_exchanges.clone(),
        ));
    }

    tokio::spawn(ws_market_execution_loop(
        runtime.clone(),
        execution.clone(),
        close_metrics.clone(),
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

async fn cancel_startup_strategy_open_orders(
    private_sync: &PrivateRuntimeSync,
    adapters: &[Arc<dyn TradingAdapter>],
    exchanges: &[ExchangeId],
) -> Vec<StartupStrategyOrderCancelAttempt> {
    let adapters_by_exchange = adapters
        .iter()
        .cloned()
        .map(|adapter| (adapter.exchange(), adapter))
        .collect::<HashMap<_, _>>();
    let mut attempts = Vec::new();

    for exchange in exchanges {
        let plan = PrivateRestAuditPlan {
            exchange: exchange.clone(),
            symbols: Vec::new(),
            include_recent_fills: false,
        };
        let snapshot = match private_sync.run_rest_audit(&plan).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::error!(
                    "cross_arb_live startup strategy order audit failed exchange={} error={}",
                    exchange,
                    error
                );
                continue;
            }
        };
        let Some(adapter) = adapters_by_exchange.get(exchange) else {
            continue;
        };

        for order in snapshot.open_orders.into_iter().filter(|order| {
            order
                .client_order_id
                .as_deref()
                .map(is_crossarb_client_order_id)
                .unwrap_or(false)
        }) {
            let command = CancelCommand {
                exchange: order.exchange.clone(),
                canonical_symbol: order.canonical_symbol.clone(),
                exchange_symbol: order.exchange_symbol.clone(),
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone(),
                reason: Some("startup cleanup of leftover crossarb strategy order".to_string()),
                requested_at: Utc::now(),
            };
            let result = adapter.cancel_order(command).await;
            match result {
                Ok(ack) => attempts.push(StartupStrategyOrderCancelAttempt {
                    exchange: order.exchange,
                    canonical_symbol: order.canonical_symbol,
                    client_order_id: ack.client_order_id,
                    exchange_order_id: ack.exchange_order_id,
                    accepted: ack.accepted,
                    message: ack.message,
                }),
                Err(error) => attempts.push(StartupStrategyOrderCancelAttempt {
                    exchange: order.exchange,
                    canonical_symbol: order.canonical_symbol,
                    client_order_id: order.client_order_id,
                    exchange_order_id: order.exchange_order_id,
                    accepted: false,
                    message: Some(error.to_string()),
                }),
            }
        }
    }

    attempts
}

fn log_startup_strategy_order_cancel_attempts(attempts: &[StartupStrategyOrderCancelAttempt]) {
    for attempt in attempts {
        if attempt.accepted {
            log::warn!(
                "cross_arb_live startup strategy order cancel accepted exchange={} symbol={} client_order_id={} exchange_order_id={}",
                attempt.exchange,
                attempt.canonical_symbol,
                attempt.client_order_id.as_deref().unwrap_or("-"),
                attempt.exchange_order_id.as_deref().unwrap_or("-")
            );
        } else {
            log::error!(
                "cross_arb_live startup strategy order cancel failed exchange={} symbol={} client_order_id={} exchange_order_id={} message={}",
                attempt.exchange,
                attempt.canonical_symbol,
                attempt.client_order_id.as_deref().unwrap_or("-"),
                attempt.exchange_order_id.as_deref().unwrap_or("-"),
                attempt.message.as_deref().unwrap_or("unknown")
            );
        }
    }
}

async fn ws_market_execution_loop(
    runtime: Arc<RwLock<CrossArbRuntime>>,
    execution: Arc<RwLock<CrossArbExecutionCoordinator>>,
    close_metrics: Arc<RwLock<HashMap<String, BundleCloseMetricReadModel>>>,
    config: CrossExchangeArbitrageConfig,
    exchanges: Vec<ExchangeId>,
    instruments: Vec<InstrumentMeta>,
) {
    let mut adapters: Vec<(ExchangeId, Arc<dyn MarketDataAdapter + Send + Sync>)> = Vec::new();
    for exchange in &exchanges {
        if let Some(adapter) = market_adapter(exchange) {
            adapters.push((exchange.clone(), Arc::from(adapter)));
        }
    }

    let market_cache = Arc::new(RwLock::new(MarketStateCache::new(
        config.market.stale_quote_ms,
    )));
    {
        let mut cache = market_cache.write().await;
        let mut runtime_guard = runtime.write().await;
        let now = Utc::now();
        for instrument in instruments {
            cache.upsert_instrument(instrument.clone());
            runtime_guard.record_instrument(instrument, now);
        }
    }

    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(16_384);
    for (exchange, adapter) in &adapters {
        let symbols = config
            .universe
            .symbols
            .iter()
            .map(|symbol| exchange_symbol_for(exchange, symbol))
            .collect::<Vec<_>>();
        let batch_size = config.ws_batch_size_for(exchange, 80);
        tokio::spawn(cross_arb_server_ws::run_public_ws_adapter(
            adapter.clone(),
            symbols,
            cross_arb_server_ws::PublicWsConfig {
                max_symbols_per_connection: batch_size,
                ..Default::default()
            },
            ws_tx.clone(),
        ));
    }
    drop(ws_tx);

    let cache_for_ws = market_cache.clone();
    tokio::spawn(async move {
        while let Some(update) = ws_rx.recv().await {
            match update {
                cross_arb_server_ws::PublicWsUpdate::OrderBook(book) => {
                    cache_for_ws
                        .write()
                        .await
                        .upsert_orderbook_at(book, Utc::now());
                }
                cross_arb_server_ws::PublicWsUpdate::Connected {
                    exchange,
                    route,
                    symbol_count,
                    ..
                } => {
                    log::info!(
                        "cross_arb_live public ws connected exchange={} symbols={} route={}",
                        exchange,
                        symbol_count,
                        route
                    );
                }
                cross_arb_server_ws::PublicWsUpdate::Error(error) => {
                    log::warn!(
                        "cross_arb_live public ws error exchange={} kind={} message={}",
                        error.exchange,
                        error.kind,
                        error.message
                    );
                }
            }
        }
    });

    let mut ticker = interval(TokioDuration::from_millis(
        (config.market.stale_quote_ms / 2).max(1_000) as u64,
    ));
    let mut funding_cache: HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot> =
        HashMap::new();
    let mut last_funding_refresh: Option<DateTime<Utc>> = None;
    let mut last_close_metric_persist: Option<DateTime<Utc>> = None;

    loop {
        ticker.tick().await;
        let now = Utc::now();
        if config.funding.enabled
            && last_funding_refresh
                .map(|last| {
                    now.signed_duration_since(last).num_seconds()
                        >= config.funding.refresh_secs.max(1) as i64
                })
                .unwrap_or(true)
        {
            funding_cache = fetch_live_funding(&adapters, &config.universe.symbols).await;
            let mut cache = market_cache.write().await;
            for funding in funding_cache.values().cloned() {
                cache.upsert_funding(funding);
            }
            last_funding_refresh = Some(now);
        }
        let snapshots_by_symbol =
            snapshots_from_market_cache(&market_cache, &config, &funding_cache, now).await;

        let mut close_metric_items = {
            let runtime_guard = runtime.read().await;
            close_metrics_from_runtime(&runtime_guard, &snapshots_by_symbol, now)
        };
        let previous_metrics = close_metrics.read().await.clone();
        for (bundle_id, metric) in previous_metrics {
            close_metric_items.entry(bundle_id).or_insert(metric);
        }
        *close_metrics.write().await = close_metric_items;
        let should_persist_close_metrics = last_close_metric_persist
            .map(|last| now.signed_duration_since(last).num_seconds() >= 1)
            .unwrap_or(true);

        let mut runtime_guard = runtime.write().await;
        if should_persist_close_metrics {
            let latest_close_metrics = close_metrics.read().await.clone();
            for metric in latest_close_metrics.into_values() {
                runtime_guard.persist_close_metric(metric, now);
            }
            last_close_metric_persist = Some(now);
        }
        {
            let mut execution_guard = execution.write().await;
            let _ = execution_guard
                .cancel_expired_maker_orders(&mut runtime_guard, now)
                .await;
            execute_hedge_repair_tasks(
                &mut runtime_guard,
                &mut execution_guard,
                &snapshots_by_symbol,
                now,
            )
            .await;
            let closed_or_attempted = execute_live_close_candidates(
                &mut runtime_guard,
                &mut execution_guard,
                &snapshots_by_symbol,
                now,
            )
            .await;
            if closed_or_attempted {
                continue;
            }
            execute_live_open_candidates(
                &mut runtime_guard,
                &mut execution_guard,
                &snapshots_by_symbol,
                now,
            )
            .await;
        }
    }
}

async fn execute_live_open_candidates(
    runtime: &mut CrossArbRuntime,
    execution: &mut CrossArbExecutionCoordinator,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    now: DateTime<Utc>,
) {
    let configured_limit = runtime
        .state
        .config
        .execution
        .max_concurrent_maker_orders
        .max(1);
    let available_maker_slots = configured_limit.saturating_sub(execution.pending_maker_count());
    let available_bundle_slots = runtime
        .state
        .config
        .risk
        .max_open_bundles
        .saturating_sub(runtime.state.open_bundles.len());
    let max_to_submit = available_maker_slots.min(available_bundle_slots);
    if max_to_submit == 0 {
        return;
    }

    let mut candidates = Vec::new();
    let mut blocked_private_stream_exchanges: HashSet<ExchangeId> = HashSet::new();
    for symbol in runtime.state.config.universe.symbols.clone() {
        if has_active_bundle_for_symbol(runtime, &symbol) {
            continue;
        }
        let Some(snapshots) = snapshots_by_symbol.get(&symbol) else {
            continue;
        };
        if snapshots.len() < runtime.state.config.market.min_common_exchanges {
            continue;
        }
        let signals = runtime.on_market_snapshots(&symbol, snapshots, now);
        if let Some(signal) = signals
            .iter()
            .find(|signal| signal.action == ArbSignalAction::Open)
            .cloned()
        {
            let (signal_canonical_symbol, signal_exchange) =
                signal_opportunity_context(runtime, &signal);
            if runtime.state.config.execution.open_execution_style
                == rustcta::strategies::cross_exchange_arbitrage::OpenExecutionStyle::MakerTaker
                && signal_exchange
                    .as_ref()
                    .is_some_and(|exchange| !runtime.state.is_private_stream_ready(exchange))
            {
                if let Some(exchange) = signal_exchange {
                    if blocked_private_stream_exchanges.insert(exchange.clone()) {
                        runtime.state.risk_events.push(RiskEventReadModel {
                            event_id: format!(
                                "live-open-private-stream-blocked-{}-{}",
                                exchange.as_str(),
                                now.timestamp_millis()
                            ),
                            canonical_symbol: signal_canonical_symbol,
                            exchange: Some(exchange.clone()),
                            reason: RejectReason::RouteUnhealthy,
                            message: format!(
                                "maker-taker open skipped because maker exchange {} private stream is not ready",
                                exchange
                            ),
                            created_at: now,
                        });
                    }
                }
                continue;
            }
            if let Some(opportunity) = signal.opportunity_id.as_ref().and_then(|id| {
                runtime
                    .state
                    .opportunities
                    .iter()
                    .find(|opportunity| opportunity.opportunity_id == *id)
            }) {
                if execution
                    .maker_cooldown_until_for_opportunity(opportunity)
                    .is_some_and(|cooldown_until| cooldown_until > now)
                {
                    continue;
                }
            }
            let rank = signal_rank(runtime, &signal);
            candidates.push((rank, signal));
        }
    }
    candidates.sort_by(|(left_rank, _), (right_rank, _)| {
        right_rank
            .0
            .partial_cmp(&left_rank.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right_rank
                    .1
                    .partial_cmp(&left_rank.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let mut submitted = 0usize;
    for (_, signal) in candidates {
        if submitted >= max_to_submit {
            break;
        }
        let (signal_canonical_symbol, signal_exchange) =
            signal_opportunity_context(runtime, &signal);
        if signal_canonical_symbol
            .as_ref()
            .is_some_and(|symbol| has_active_bundle_for_symbol(runtime, symbol))
        {
            continue;
        }
        match execution.execute_open_signal(runtime, &signal).await {
            Ok(Some(decision)) => {
                if decision.blocked_reason.is_none() && !decision.submitted_orders.is_empty() {
                    let live_orders = decision
                        .submitted_orders
                        .iter()
                        .filter(|ack| order_ack_is_live(ack))
                        .count();
                    if live_orders == 0 {
                        runtime.state.risk_events.push(RiskEventReadModel {
                            event_id: format!(
                                "live-open-ack-not-live-{}-{}",
                                now.timestamp_millis(),
                                signal.signal_id
                            ),
                            canonical_symbol: signal_canonical_symbol,
                            exchange: signal_exchange,
                            reason: RejectReason::RouteUnhealthy,
                            message: "maker order ack was not live; not counting as submitted"
                                .to_string(),
                            created_at: now,
                        });
                        continue;
                    }
                    submitted += 1;
                    log::info!(
                        "cross_arb_live open maker submitted symbol={} pending_makers={}/{}",
                        signal_canonical_symbol
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "unknown".to_string()),
                        execution.pending_maker_count(),
                        configured_limit
                    );
                }
            }
            Ok(None) => {}
            Err(error) => runtime.state.risk_events.push(RiskEventReadModel {
                event_id: format!(
                    "live-open-exec-{}-{}",
                    now.timestamp_millis(),
                    signal.signal_id
                ),
                canonical_symbol: signal_canonical_symbol,
                exchange: signal_exchange,
                reason: RejectReason::RouteUnhealthy,
                message: error.to_string(),
                created_at: now,
            }),
        }
    }
}

async fn execute_hedge_repair_tasks(
    runtime: &mut CrossArbRuntime,
    execution: &mut CrossArbExecutionCoordinator,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    now: DateTime<Utc>,
) {
    let tasks = runtime.state.due_hedge_repair_tasks(now);
    for task in tasks.into_iter().take(2) {
        let Some(command) = repair_command_from_task(runtime, &task, snapshots_by_symbol, now)
        else {
            runtime.state.record_hedge_repair_failed(
                &task.task_id,
                "no alternate taker route available for hedge repair",
                now,
            );
            if !task.reduce_only {
                runtime.state.set_close_only();
            }
            runtime.persist_hedge_repair_task(&task.task_id, now);
            runtime.persist_hedge_record(&task.bundle_id, now);
            continue;
        };
        let attempt_exchange = command.exchange.clone();
        if !command.reduce_only
            && runtime
                .state
                .update_bundle_open_leg_route(
                    &command.bundle_id,
                    command.position_side,
                    command.exchange.clone(),
                    command.exchange_symbol.clone(),
                    command.quantity,
                    now,
                )
                .is_err()
        {
            runtime.state.record_hedge_repair_failed(
                &task.task_id,
                "could not update open hedge repair leg route before submitting order",
                now,
            );
            runtime.state.set_close_only();
            runtime.persist_hedge_repair_task(&task.task_id, now);
            runtime.persist_hedge_record(&task.bundle_id, now);
            continue;
        }
        let decision = execution
            .execute_repair_command(runtime.state.config.mode, command, now)
            .await;
        let submitted = decision.plan.commands.first().is_some_and(|command| {
            decision
                .submitted_orders
                .iter()
                .any(|ack| ack.client_order_id == command.client_order_id && order_ack_is_live(ack))
        });
        if decision.blocked_reason.is_none() && submitted {
            runtime.state.record_hedge_repair_submitted(
                &task.task_id,
                attempt_exchange.clone(),
                now,
            );
            runtime.persist_hedge_repair_task(&task.task_id, now);
            runtime.persist_hedge_record(&task.bundle_id, now);
            execution
                .verify_submitted_hedge_order(runtime, &decision, now)
                .await;
            log::warn!(
                "cross_arb_live hedge repair submitted task={} bundle={} exchange={} qty={}",
                task.task_id,
                task.bundle_id,
                attempt_exchange,
                task.quantity
            );
        } else {
            runtime.state.record_hedge_repair_failed(
                &task.task_id,
                decision
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "hedge repair order was not submitted".to_string()),
                now,
            );
            if !task.reduce_only {
                runtime.state.set_close_only();
            }
            runtime.persist_hedge_repair_task(&task.task_id, now);
            runtime.persist_hedge_record(&task.bundle_id, now);
        }
    }
}

fn repair_command_from_task(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    now: DateTime<Utc>,
) -> Option<OrderCommand> {
    let snapshots = snapshots_by_symbol.get(&task.canonical_symbol)?;
    let selection = best_repair_taker_snapshot(runtime, task, snapshots)
        .or_else(|| fallback_repair_taker_snapshot(runtime, task, snapshots))?;
    let quantity = repair_quantity_from_position(runtime, task)?;
    let leg = if task.reduce_only {
        match task.position_side {
            PositionSide::Long => BundleLeg::CloseLong,
            PositionSide::Short => BundleLeg::CloseShort,
            PositionSide::Net => BundleLeg::Hedge,
        }
    } else if matches!(task.position_side, PositionSide::Long) {
        BundleLeg::Long
    } else if matches!(task.position_side, PositionSide::Short) {
        BundleLeg::Short
    } else {
        BundleLeg::Hedge
    };
    let intent = match (task.reduce_only, task.position_side) {
        (false, PositionSide::Long) => OrderIntent::HedgeLongTaker,
        (false, PositionSide::Short) => OrderIntent::HedgeShortTaker,
        (true, PositionSide::Long) => OrderIntent::CloseLongTaker,
        (true, PositionSide::Short) => OrderIntent::CloseShortTaker,
        (_, PositionSide::Net) => OrderIntent::HedgeLongTaker,
    };
    let attempt = task.attempts.saturating_add(2);
    Some(OrderCommand::new(
        runtime.state.config.mode,
        task.bundle_id.clone(),
        leg,
        attempt,
        selection.exchange,
        task.canonical_symbol.clone(),
        selection.exchange_symbol,
        intent,
        task.side,
        task.position_side,
        selection.order_type,
        quantity,
        selection.price,
        TimeInForce::Ioc,
        false,
        task.reduce_only,
        Some(runtime.state.config.execution.taker_ioc_slippage_limit_pct),
        now,
    ))
}

struct RepairTakerSelection {
    exchange: ExchangeId,
    exchange_symbol: ExchangeSymbol,
    price: Option<f64>,
    order_type: OrderType,
}

fn repair_quantity_from_position(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
) -> Option<f64> {
    let position = runtime.state.position_manager.bundle(&task.bundle_id)?;
    let tolerance = runtime.state.config.reconciliation.quantity_tolerance;
    let quantity = if task.reduce_only {
        match task.position_side {
            PositionSide::Long => position.long_leg.available_to_close_qty(),
            PositionSide::Short => position.short_leg.available_to_close_qty(),
            PositionSide::Net => 0.0,
        }
        .min(task.quantity)
    } else {
        match task.position_side {
            PositionSide::Long => {
                (position.short_leg.filled_qty - position.long_leg.filled_qty).max(0.0)
            }
            PositionSide::Short => {
                (position.long_leg.filled_qty - position.short_leg.filled_qty).max(0.0)
            }
            PositionSide::Net => 0.0,
        }
        .min(task.quantity)
    };
    (quantity > tolerance && quantity.is_finite()).then_some(quantity)
}

fn best_repair_taker_snapshot(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
    snapshots: &[MarketSnapshot],
) -> Option<RepairTakerSelection> {
    if let Some(selection) = planned_repair_leg_snapshot(runtime, task, snapshots) {
        return Some(selection);
    }
    snapshots
        .iter()
        .filter(|snapshot| snapshot.book.is_usable())
        .filter(|snapshot| snapshot.book.exchange != task.failed_exchange)
        .filter(|snapshot| open_repair_exchange_is_allowed(runtime, task, &snapshot.book.exchange))
        .filter(|snapshot| {
            runtime
                .state
                .is_private_stream_ready(&snapshot.book.exchange)
        })
        .filter_map(|snapshot| {
            let price = match task.side {
                OrderSide::Sell => snapshot.book.best_bid()?.price,
                OrderSide::Buy => snapshot.book.best_ask()?.price,
            };
            let price = snapshot
                .instrument
                .as_ref()
                .map(|instrument| {
                    let mode = match task.side {
                        OrderSide::Sell => RoundingMode::Floor,
                        OrderSide::Buy => RoundingMode::Ceil,
                    };
                    instrument.quantize_price(price, mode)
                })
                .unwrap_or(price);
            (price.is_finite() && price > 0.0).then_some((snapshot, price))
        })
        .max_by(|(_, left_price), (_, right_price)| match task.side {
            OrderSide::Sell => left_price
                .partial_cmp(right_price)
                .unwrap_or(std::cmp::Ordering::Equal),
            OrderSide::Buy => right_price
                .partial_cmp(left_price)
                .unwrap_or(std::cmp::Ordering::Equal),
        })
        .map(|(snapshot, price)| RepairTakerSelection {
            exchange: snapshot.book.exchange.clone(),
            exchange_symbol: snapshot.book.exchange_symbol.clone(),
            price: Some(price),
            order_type: OrderType::Limit,
        })
}

fn fallback_repair_taker_snapshot(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
    snapshots: &[MarketSnapshot],
) -> Option<RepairTakerSelection> {
    snapshots
        .iter()
        .find(|snapshot| {
            snapshot.book.is_usable()
                && snapshot.book.exchange == task.failed_exchange
                && open_repair_exchange_is_allowed(runtime, task, &snapshot.book.exchange)
                && runtime
                    .state
                    .is_private_stream_ready(&snapshot.book.exchange)
        })
        .map(|snapshot| RepairTakerSelection {
            exchange: snapshot.book.exchange.clone(),
            exchange_symbol: snapshot.book.exchange_symbol.clone(),
            price: None,
            order_type: OrderType::Market,
        })
}

fn planned_repair_leg_snapshot(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
    snapshots: &[MarketSnapshot],
) -> Option<RepairTakerSelection> {
    if task.reduce_only {
        return None;
    }
    let position = runtime.state.position_manager.bundle(&task.bundle_id)?;
    let leg = match task.position_side {
        PositionSide::Long => &position.long_leg,
        PositionSide::Short => &position.short_leg,
        PositionSide::Net => return None,
    };
    let snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.book.is_usable() && snapshot.book.exchange == leg.exchange)?;
    if !runtime
        .state
        .is_private_stream_ready(&snapshot.book.exchange)
    {
        return None;
    }
    let price = match task.side {
        OrderSide::Sell => snapshot.book.best_bid()?.price,
        OrderSide::Buy => snapshot.book.best_ask()?.price,
    };
    let price = snapshot
        .instrument
        .as_ref()
        .map(|instrument| {
            let mode = match task.side {
                OrderSide::Sell => RoundingMode::Floor,
                OrderSide::Buy => RoundingMode::Ceil,
            };
            instrument.quantize_price(price, mode)
        })
        .unwrap_or(price);
    Some(RepairTakerSelection {
        exchange: snapshot.book.exchange.clone(),
        exchange_symbol: snapshot.book.exchange_symbol.clone(),
        price: Some(price),
        order_type: OrderType::Limit,
    })
}

fn open_repair_exchange_is_allowed(
    runtime: &CrossArbRuntime,
    task: &HedgeRepairTaskReadModel,
    exchange: &ExchangeId,
) -> bool {
    if task.reduce_only {
        return true;
    }
    let Some(position) = runtime.state.position_manager.bundle(&task.bundle_id) else {
        return false;
    };
    let tolerance = runtime.state.config.reconciliation.quantity_tolerance;
    match task.position_side {
        PositionSide::Long => {
            *exchange != position.short_leg.exchange || position.short_leg.filled_qty <= tolerance
        }
        PositionSide::Short => {
            *exchange != position.long_leg.exchange || position.long_leg.filled_qty <= tolerance
        }
        PositionSide::Net => false,
    }
}

fn signal_opportunity_context(
    runtime: &CrossArbRuntime,
    signal: &ArbSignal,
) -> (Option<CanonicalSymbol>, Option<ExchangeId>) {
    signal
        .opportunity_id
        .as_ref()
        .and_then(|id| {
            runtime
                .state
                .opportunities
                .iter()
                .find(|opportunity| opportunity.opportunity_id == *id)
        })
        .map(|opportunity| {
            (
                Some(opportunity.canonical_symbol.clone()),
                Some(opportunity.maker_exchange.clone()),
            )
        })
        .unwrap_or((None, None))
}

fn signal_rank(runtime: &CrossArbRuntime, signal: &ArbSignal) -> (f64, f64) {
    signal
        .opportunity_id
        .as_ref()
        .and_then(|id| {
            runtime
                .state
                .opportunities
                .iter()
                .find(|opportunity| opportunity.opportunity_id == *id)
        })
        .map(|opportunity| {
            (
                opportunity.maker_taker_net_edge,
                opportunity.maker_book_spread_pct,
            )
        })
        .unwrap_or((signal.expected_edge, 0.0))
}

async fn execute_live_close_candidates(
    runtime: &mut CrossArbRuntime,
    execution: &mut CrossArbExecutionCoordinator,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    now: DateTime<Utc>,
) -> bool {
    let bundle_ids = runtime
        .state
        .open_bundles
        .iter()
        .filter(|(_, bundle)| bundle.status == SimulatedBundleStatus::OpenSimulated)
        .map(|(bundle_id, _)| bundle_id.clone())
        .collect::<Vec<_>>();

    for bundle_id in bundle_ids {
        let Some(position) = runtime.state.position_manager.bundle(&bundle_id) else {
            continue;
        };
        let Some(snapshots) = snapshots_by_symbol.get(&position.canonical_symbol) else {
            continue;
        };
        let Some(candidate) = live_close_candidate_for_bundle(runtime, &bundle_id, snapshots, now)
        else {
            continue;
        };
        runtime.state.record_close_candidate_metrics(
            &candidate.bundle_id,
            candidate.close_spread_pct,
            candidate.close_profit_pct,
            now,
        );
        runtime.persist_hedge_record(&candidate.bundle_id, now);
        match execution.execute_close_bundle(runtime, &candidate).await {
            Ok(decision) if decision.blocked_reason.is_none() => {
                log::info!(
                    "cross_arb_live close submitted bundle={} qty={} profit_pct={:.6}",
                    candidate.bundle_id,
                    candidate.quantity,
                    candidate.close_profit_pct
                );
            }
            Ok(decision) => {
                log::warn!(
                    "cross_arb_live close blocked bundle={} reason={:?}",
                    candidate.bundle_id,
                    decision.blocked_reason
                );
            }
            Err(error) => {
                runtime
                    .state
                    .risk_events
                    .push(rustcta::strategies::cross_exchange_arbitrage::RiskEventReadModel {
                        event_id: format!("live-close-exec-{}", now.timestamp_millis()),
                        canonical_symbol: Some(candidate.canonical_symbol.clone()),
                        exchange: None,
                        reason: rustcta::strategies::cross_exchange_arbitrage::RejectReason::RouteUnhealthy,
                        message: error.to_string(),
                        created_at: now,
                    });
                log::warn!(
                    "cross_arb_live close failed bundle={} error={}",
                    candidate.bundle_id,
                    error
                );
            }
        }
        return true;
    }

    false
}

fn close_metrics_from_runtime(
    runtime: &CrossArbRuntime,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    now: DateTime<Utc>,
) -> HashMap<String, BundleCloseMetricReadModel> {
    let active_ids = runtime
        .state
        .open_bundles
        .iter()
        .filter(|(_, bundle)| {
            matches!(
                bundle.status,
                SimulatedBundleStatus::OpenSimulated
                    | SimulatedBundleStatus::ClosingSimulated
                    | SimulatedBundleStatus::OrphanLeg
            )
        })
        .map(|(bundle_id, _)| bundle_id.clone())
        .collect::<Vec<_>>();

    let mut next = HashMap::new();
    for bundle_id in active_ids {
        let Some(position) = runtime.state.position_manager.bundle(&bundle_id) else {
            continue;
        };
        let Some(snapshots) = snapshots_by_symbol.get(&position.canonical_symbol) else {
            continue;
        };
        let Some(metric) = live_close_metrics_for_bundle(runtime, &bundle_id, snapshots, now)
        else {
            continue;
        };
        next.insert(
            bundle_id.clone(),
            BundleCloseMetricReadModel {
                bundle_id,
                symbol: metric.canonical_symbol.to_string(),
                long_exchange: metric.long_exchange.as_str().to_string(),
                short_exchange: metric.short_exchange.as_str().to_string(),
                quantity: metric.quantity,
                target_notional_usdt: metric.target_notional_usdt,
                entry_edge_pct: Some(position.entry_edge_pct),
                open_fee_paid_usdt: metric.open_fee_paid_usdt,
                close_fee_est_usdt: metric.close_fee_est_usdt,
                gross_spread_pnl_usdt: metric.gross_spread_pnl_usdt,
                realized_funding_pnl_usdt: metric.realized_funding_pnl_usdt,
                close_spread_pct: metric.close_spread_pct,
                close_profit_pct: metric.close_profit_pct,
                close_threshold_pct: runtime.state.config.thresholds.lock_profit_dual_taker_pct,
                close_maker_exchange: metric.maker_close_exchange.as_str().to_string(),
                close_maker_side: format!("{:?}", metric.maker_close_side),
                close_maker_price: metric.maker_close_price,
                close_maker_book_spread_pct: metric.maker_close_book_spread_pct,
                closeable: metric.close_profit_pct
                    >= runtime.state.config.thresholds.lock_profit_dual_taker_pct
                    && metric.close_spread_pct
                        <= runtime.state.config.thresholds.max_close_spread_pct,
                updated_at: now,
            },
        );
    }
    next
}

async fn fetch_live_funding(
    adapters: &[(ExchangeId, Arc<dyn MarketDataAdapter + Send + Sync>)],
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

async fn snapshots_from_market_cache(
    market_cache: &Arc<RwLock<MarketStateCache>>,
    config: &CrossExchangeArbitrageConfig,
    funding: &HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
    now: DateTime<Utc>,
) -> HashMap<CanonicalSymbol, Vec<MarketSnapshot>> {
    let mut snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>> = HashMap::new();
    let cache = market_cache.read().await;
    for symbol in &config.universe.symbols {
        for exchange in &config.universe.enabled_exchanges {
            let Some(book) = cache.orderbook(exchange, symbol).cloned() else {
                continue;
            };
            let mut book = book;
            book.quality.stale = now.signed_duration_since(book.recv_ts).num_milliseconds()
                > config.market.stale_quote_ms;
            if book.quality.stale {
                continue;
            }
            let funding = funding
                .get(&(exchange.clone(), symbol.clone()))
                .cloned()
                .or_else(|| cache.funding(exchange, symbol).cloned());
            snapshots_by_symbol
                .entry(symbol.clone())
                .or_default()
                .push(MarketSnapshot {
                    book,
                    instrument: cache.instrument(exchange, symbol).cloned(),
                    route_status: exchange_route_status(config, exchange),
                    funding_rate: funding
                        .as_ref()
                        .map(|snapshot| snapshot.funding_rate)
                        .unwrap_or(0.0),
                    next_funding_time: funding.and_then(|snapshot| snapshot.next_funding_time),
                });
        }
    }
    snapshots_by_symbol
}

fn has_active_bundle_for_symbol(runtime: &CrossArbRuntime, symbol: &CanonicalSymbol) -> bool {
    runtime.state.open_bundles.values().any(|bundle| {
        bundle.route.long_exchange != bundle.route.short_exchange
            && matches!(
                bundle.status,
                SimulatedBundleStatus::MakerPending
                    | SimulatedBundleStatus::MakerFilled
                    | SimulatedBundleStatus::Hedging
                    | SimulatedBundleStatus::OpenSimulated
                    | SimulatedBundleStatus::ClosingSimulated
                    | SimulatedBundleStatus::OrphanLeg
            )
            && runtime
                .state
                .position_manager
                .bundle(&bundle.bundle_id)
                .map(|position| &position.canonical_symbol == symbol)
                .unwrap_or_else(|| {
                    runtime
                        .state
                        .opportunities
                        .iter()
                        .find(|opportunity| opportunity.opportunity_id == bundle.opportunity_id)
                        .map(|opportunity| &opportunity.canonical_symbol == symbol)
                        .unwrap_or(false)
                })
    })
}

async fn prepare_live_accounts(
    _router: &ExecutionRouter,
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
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_position_mode".to_string(),
                ok: true,
                dry_run: config.execution.dry_run,
                message: Some(format!(
                    "skipped during live bootstrap to avoid account-mode writes; expected_position_mode={mode:?}"
                )),
            });
        } else if mode.is_hedge() {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_position_mode".to_string(),
                ok: true,
                dry_run: config.execution.dry_run,
                message: Some(format!(
                    "skipped during live bootstrap: adapter does not write account mode; configured_position_mode={mode:?}, verify/read back in preflight"
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

        if config.execution.dry_run {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_leverage".to_string(),
                ok: true,
                dry_run: true,
                message: Some(format!(
                    "skipped in dry-run pre-live bootstrap; startup does not write leverage, configured_reference_leverage={leverage}"
                )),
            });
        } else {
            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: None,
                action: "set_leverage".to_string(),
                ok: true,
                dry_run: false,
                message: Some(format!(
                    "skipped during live bootstrap to avoid per-symbol REST writes; using exchange current/default leverage, configured_reference_leverage={leverage}"
                )),
            });
        }

        for instrument in instruments
            .iter()
            .filter(|item| item.exchange == exchange)
            .take(3)
        {
            if config.fees.use_account_fee_api {
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
            }

            summaries.push(AccountPrepSummary {
                exchange: exchange.as_str().to_string(),
                symbol: Some(instrument.canonical_symbol.to_string()),
                action: "read_symbol_account_config".to_string(),
                ok: true,
                dry_run: false,
                message: Some(
                    "skipped as non-blocking pre-live check; precision/min-notional comes from public instrument metadata"
                        .to_string(),
                ),
            });
        }
    }
    summaries
}

fn apply_account_fee_overrides(
    config: &mut CrossExchangeArbitrageConfig,
    account_preparation: &[AccountPrepSummary],
) -> Vec<AccountFeeOverrideSummary> {
    if !config.fees.use_account_fee_api {
        return Vec::new();
    }

    let mut parsed: HashMap<ExchangeId, Vec<TradeFeeSnapshot>> = HashMap::new();
    for summary in account_preparation
        .iter()
        .filter(|summary| summary.action == "read_trade_fee" && summary.ok)
    {
        let Some(message) = summary.message.as_deref() else {
            continue;
        };
        let Some((maker, taker)) = parse_trade_fee_message(message) else {
            continue;
        };
        let exchange = ExchangeId::from(summary.exchange.as_str());
        let canonical_symbol = summary
            .symbol
            .as_deref()
            .and_then(CanonicalSymbol::parse)
            .unwrap_or_else(|| CanonicalSymbol::new("UNKNOWN", "USDT"));
        parsed
            .entry(exchange.clone())
            .or_default()
            .push(TradeFeeSnapshot {
                exchange: exchange.clone(),
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: exchange_symbol_for(&exchange, &canonical_symbol),
                maker,
                taker,
                source: "account_preparation".to_string(),
                updated_at: Utc::now(),
            });
    }

    let mut overrides = Vec::new();
    for (exchange, fees) in parsed {
        let maker = fees
            .iter()
            .map(|fee| fee.maker.max(0.0))
            .filter(|fee| fee.is_finite())
            .fold(config.fees.default_maker_fee_rate, f64::max);
        let taker = fees
            .iter()
            .map(|fee| fee.taker.max(0.0))
            .filter(|fee| fee.is_finite())
            .fold(config.fees.default_taker_fee_rate, f64::max);
        config
            .fees
            .per_exchange
            .insert(exchange.clone(), ExchangeFeeRates { maker, taker });
        overrides.push(AccountFeeOverrideSummary {
            exchange: exchange.as_str().to_string(),
            maker,
            taker,
            symbols: fees.len(),
        });
    }
    overrides.sort_by(|left, right| left.exchange.cmp(&right.exchange));
    overrides
}

fn parse_trade_fee_message(message: &str) -> Option<(f64, f64)> {
    let mut maker = None;
    let mut taker = None;
    for part in message.split_whitespace() {
        if let Some(value) = part.strip_prefix("maker=") {
            maker = value.parse::<f64>().ok();
        } else if let Some(value) = part.strip_prefix("taker=") {
            taker = value.parse::<f64>().ok();
        }
    }
    match (maker, taker) {
        (Some(maker), Some(taker)) if maker.is_finite() && taker.is_finite() => {
            Some((maker, taker))
        }
        _ => None,
    }
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
        let mut last_error = None;
        let mut instruments = None;
        for attempt in 1..=3 {
            match adapter.load_instruments().await {
                Ok(loaded) => {
                    instruments = Some(loaded);
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 3 {
                        sleep(TokioDuration::from_millis(500 * attempt)).await;
                    }
                }
            }
        }
        let instruments = instruments
            .ok_or_else(|| {
                last_error.unwrap_or_else(|| anyhow!("load instruments returned no result"))
            })
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
        let plan = PrivateRestAuditPlan {
            exchange: exchange.clone(),
            symbols: Vec::new(),
            include_recent_fills: false,
        };
        match private_sync.run_rest_audit(&plan).await {
            Ok(snapshot) => {
                let nonzero_positions = snapshot
                    .positions
                    .iter()
                    .filter(|position| {
                        position.quantity.abs() > config.reconciliation.quantity_tolerance
                    })
                    .count();
                let strategy_open_orders = snapshot
                    .open_orders
                    .iter()
                    .filter(|order| {
                        order
                            .client_order_id
                            .as_deref()
                            .map(is_crossarb_client_order_id)
                            .unwrap_or(false)
                    })
                    .count();
                summaries.push(RestAuditSummary {
                    exchange: exchange.as_str().to_string(),
                    ok: true,
                    balances: snapshot.balances.len(),
                    positions: snapshot.positions.len(),
                    nonzero_positions,
                    open_orders: snapshot.open_orders.len(),
                    strategy_open_orders,
                    external_open_orders: snapshot
                        .open_orders
                        .len()
                        .saturating_sub(strategy_open_orders),
                    fills: snapshot.fills.len(),
                    error: None,
                })
            }
            Err(err) => summaries.push(RestAuditSummary {
                exchange: exchange.as_str().to_string(),
                ok: false,
                balances: 0,
                positions: 0,
                nonzero_positions: 0,
                open_orders: 0,
                strategy_open_orders: 0,
                external_open_orders: 0,
                fills: 0,
                error: Some(err.to_string()),
            }),
        }
    }
    private_sync
        .reconcile_unpaired_positions_from_account_state(Utc::now())
        .await;
    summaries
}

fn market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    match exchange {
        ExchangeId::Okx | ExchangeId::Other(_) => None,
        _ => exchange_registry::market_adapter(exchange),
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
    use rustcta::market::{BookLevel, OrderBook5, RuntimeMode};

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
    fn live_runner_example_config_should_parse_as_disabled_live_small_dry_run_template() {
        let config = load_config(&PathBuf::from(
            "config/cross_exchange_arbitrage_usdt.live-small.example.yml",
        ))
        .unwrap();

        assert_eq!(config.mode, RuntimeMode::LiveSmall);
        assert!(config.execution.dry_run);
        assert!(live_enabled_exchanges(&config).is_empty());
        assert!(config
            .exchanges
            .values()
            .all(|runtime| runtime.is_disabled() && !runtime.private_ws_enabled));
    }

    #[test]
    fn live_runner_should_admit_non_live_public_discovery_dry_run() {
        validate_cross_arb_live_admission(RuntimeMode::Simulation, true, false, true).unwrap();
    }

    #[test]
    fn live_runner_should_reject_non_live_execute_request() {
        let error = validate_cross_arb_live_admission(RuntimeMode::Simulation, true, true, true)
            .unwrap_err();

        assert!(error.to_string().contains("--execute requires"));
    }

    #[test]
    fn live_runner_should_require_skip_private_audit_for_non_live_discovery() {
        let error = validate_cross_arb_live_admission(RuntimeMode::Simulation, true, false, false)
            .unwrap_err();

        assert!(error.to_string().contains("--skip-private-audit"));
    }

    #[test]
    fn live_runner_should_reject_execute_with_live_dry_run_config() {
        let error = validate_cross_arb_live_admission(RuntimeMode::LiveSmall, true, true, false)
            .unwrap_err();

        assert!(error.to_string().contains("dry_run=true"));
    }

    #[test]
    fn live_runner_repair_should_not_use_existing_opposite_leg_exchange() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("SPCX", "USDT");
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);
        runtime
            .state
            .update_private_stream_health(ExchangeId::Gate, now, false);
        runtime
            .state
            .update_private_stream_health(ExchangeId::Binance, now, false);
        let mut bundle = rustcta::execution::ArbitrageBundle::new(
            "bundle-spcx-repair",
            RuntimeMode::LiveSmall,
            symbol.clone(),
            ExchangeId::Gate,
            ExchangeId::Binance,
            ExchangeId::Gate,
            ExchangeId::Binance,
            6.0,
            now,
        );
        bundle.status = rustcta::execution::BundleStatus::OrphanLeg;
        runtime
            .state
            .register_bundle_position(&bundle, 0.03, 0.02, now);
        runtime
            .state
            .position_manager
            .record_leg_fill(
                "bundle-spcx-repair",
                PositionSide::Long,
                0.03,
                199.0,
                0.0,
                now,
            )
            .unwrap();
        let task = HedgeRepairTaskReadModel {
            task_id: "repair-open-bundle-spcx-repair".to_string(),
            record_id: "bundle-spcx-repair".to_string(),
            bundle_id: "bundle-spcx-repair".to_string(),
            canonical_symbol: symbol.clone(),
            failed_exchange: ExchangeId::Binance,
            last_attempt_exchange: None,
            side: OrderSide::Sell,
            position_side: PositionSide::Short,
            quantity: 0.03,
            reduce_only: false,
            status: rustcta::strategies::cross_exchange_arbitrage::HedgeRepairTaskStatus::Pending,
            attempts: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
        };
        let snapshots = vec![
            MarketSnapshot::healthy(test_book(ExchangeId::Gate, symbol.clone(), 199.0, 200.0)),
            MarketSnapshot::healthy(test_book(ExchangeId::Binance, symbol.clone(), 198.0, 199.0)),
        ];

        let selection = best_repair_taker_snapshot(&runtime, &task, &snapshots).unwrap();

        assert_eq!(selection.exchange, ExchangeId::Binance);
        assert_ne!(selection.exchange, ExchangeId::Gate);
    }

    fn test_book(exchange: ExchangeId, symbol: CanonicalSymbol, bid: f64, ask: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            symbol.clone(),
            exchange_symbol_for(&exchange, &symbol),
            vec![BookLevel::new(bid, 1.0)],
            vec![BookLevel::new(ask, 1.0)],
            Utc::now(),
            Utc::now(),
            Some(1),
            None,
        )
    }
}
