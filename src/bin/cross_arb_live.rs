use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[path = "cross_arb_server/ws.rs"]
mod cross_arb_server_ws;

use anyhow::{anyhow, Context, Result};
use axum::extract::State as AxumState;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::registry as exchange_registry;
use rustcta::execution::{
    is_crossarb_client_order_id, BundleLeg, CancelCommand, ClosePositionCommand, ExchangeBalance,
    ExchangePosition, ExecutionEngine, ExecutionRouter, OrderCommand, OrderIntent, OrderSide,
    OrderSnapshot, OrderType, PositionMode, PositionSide, PositionSnapshot, TimeInForce,
    TradeFeeSnapshot, TradingAdapter,
};
use rustcta::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta,
    MarketDataAdapter, MarketFundingSnapshot, MarketStateCache, RoundingMode,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_cross_arb_live_runtime_parts, exchange_route_status, live_close_candidate_for_bundle,
    live_close_metrics_for_bundle, live_enabled_exchanges, order_ack_is_live, ArbSignal,
    ArbSignalAction, BundleReadModel, CrossArbDashboardStatus, CrossArbExecutionCoordinator,
    CrossArbRuntime, CrossExchangeArbitrageConfig, ExchangeFeeRates, HedgeRecordReadModel,
    HedgeRepairTaskReadModel, MarketSnapshot, OpportunityReadModel, PrivateRestAuditPlan,
    PrivateRuntimeSync, RejectReason, RiskEventReadModel, SimulatedBundleStatus,
};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration as TokioDuration};
use tower_http::cors::{Any, CorsLayer};

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
struct StartupOrphanCloseAttempt {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    position_side: PositionSide,
    quantity: f64,
    client_order_id: String,
    accepted: bool,
    message: Option<String>,
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

const STARTUP_ORPHAN_CLOSE_MAX_ATTEMPTS: usize = 3;
const STARTUP_ORPHAN_CLOSE_SETTLE_SECS: u64 = 3;

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

#[derive(Clone)]
struct LiveDashboardState {
    runtime: Arc<RwLock<CrossArbRuntime>>,
    execution: Arc<RwLock<CrossArbExecutionCoordinator>>,
    close_metrics: Arc<RwLock<HashMap<String, BundleCloseMetricReadModel>>>,
    started_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct LiveDashboardPayload {
    generated_at: DateTime<Utc>,
    started_at: DateTime<Utc>,
    status: CrossArbDashboardStatus,
    symbols: Vec<SymbolCoverageReadModel>,
    opportunities: Vec<BestOpportunityReadModel>,
    open_bundles: Vec<BundleReadModel>,
    active_bundles: Vec<ActiveBundleReadModel>,
    hedge_records: Vec<HedgeRecordReadModel>,
    hedge_repair_tasks: Vec<HedgeRepairTaskReadModel>,
    history: Vec<BundleReadModel>,
    risk_event_summary: Vec<RiskEventSummaryReadModel>,
    risk_events: Vec<RiskEventReadModel>,
    private_positions: Vec<PositionSnapshot>,
    private_open_orders: Vec<OrderSnapshot>,
    strategy_open_orders: Vec<OrderSnapshot>,
    external_open_orders: Vec<OrderSnapshot>,
    balances: Vec<ExchangeBalance>,
    controls: LiveControlState,
    execution: LiveExecutionSummary,
    maker_execution_stats:
        Vec<rustcta::strategies::cross_exchange_arbitrage::MakerExecutionStatsReadModel>,
}

#[derive(Debug, Clone, Serialize)]
struct BundleCloseMetricReadModel {
    bundle_id: String,
    symbol: String,
    long_exchange: String,
    short_exchange: String,
    quantity: f64,
    target_notional_usdt: f64,
    entry_edge_pct: Option<f64>,
    open_fee_paid_usdt: f64,
    close_fee_est_usdt: f64,
    gross_spread_pnl_usdt: f64,
    realized_funding_pnl_usdt: f64,
    close_spread_pct: f64,
    close_profit_pct: f64,
    close_threshold_pct: f64,
    close_maker_exchange: String,
    close_maker_side: String,
    close_maker_price: f64,
    close_maker_book_spread_pct: f64,
    closeable: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct ActiveBundleReadModel {
    bundle_id: String,
    opportunity_id: String,
    symbol: Option<String>,
    long_exchange: Option<String>,
    short_exchange: Option<String>,
    status: SimulatedBundleStatus,
    target_notional_usdt: f64,
    entry_edge_pct: Option<f64>,
    close_spread_pct: Option<f64>,
    close_profit_pct: Option<f64>,
    close_threshold_pct: f64,
    close_maker_exchange: Option<String>,
    close_maker_side: Option<String>,
    close_maker_price: Option<f64>,
    close_maker_book_spread_pct: Option<f64>,
    closeable: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct RiskEventSummaryReadModel {
    key: String,
    exchange: Option<String>,
    symbol: Option<String>,
    reason: String,
    message: String,
    count: usize,
    first_at: DateTime<Utc>,
    last_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct SymbolCoverageReadModel {
    symbol: String,
    configured_exchanges: Vec<String>,
    exchanges: Vec<String>,
    opportunities: usize,
    openable_opportunities: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BestOpportunityReadModel {
    symbol: String,
    best_route: String,
    long_exchange: String,
    short_exchange: String,
    maker_exchange: String,
    maker_side: String,
    maker_price: f64,
    maker_book_spread_pct: f64,
    open_spread_pct: f64,
    expected_net_edge_pct: f64,
    notional_usdt: f64,
    can_open: bool,
    reject_reasons: Vec<String>,
    book_age_ms: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct LiveControlState {
    paused_new_entries: bool,
    close_only: bool,
    kill_switch: bool,
}

#[derive(Debug, Serialize)]
struct LiveExecutionSummary {
    dry_run: bool,
    open_execution_style: String,
    maker_order_ttl_ms: u64,
    max_concurrent_maker_orders: usize,
    pending_maker_orders: usize,
    maker_price_offset_ticks: u32,
    maker_aggressive_after_cancels: u32,
    maker_aggressive_step_ticks: u32,
    maker_aggressive_max_ticks: u32,
    maker_cooldown_after_cancels: u32,
    maker_cooldown_ms: u64,
    target_notional_usdt: f64,
    max_notional_usdt: f64,
    min_open_maker_taker_net_edge: f64,
    close_profit_threshold_pct: f64,
    max_open_bundles: usize,
    max_positions_per_exchange: usize,
    enabled_exchanges: Vec<String>,
    enabled_symbols: usize,
    strategy_open_orders: usize,
    external_open_orders: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
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
            sleep(TokioDuration::from_secs(STARTUP_ORPHAN_CLOSE_SETTLE_SECS)).await;
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
                    "{} startup audit found nonzero_positions={} open_orders={}; restart in close-only/reconcile mode before allowing new entries",
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
            sleep(TokioDuration::from_secs(STARTUP_ORPHAN_CLOSE_SETTLE_SECS)).await;
            audits = run_initial_rest_audits(&private_sync, &config, &enabled_exchanges).await;
        }
        let startup_close_attempts = close_startup_unpaired_positions(
            runtime.clone(),
            &private_sync,
            &parts.adapters,
            &config,
            &enabled_exchanges,
        )
        .await;
        if !startup_close_attempts.is_empty() {
            for attempt in &startup_close_attempts {
                if attempt.accepted {
                    log::warn!(
                        "cross_arb_live startup orphan close submitted exchange={} symbol={} side={:?} qty={} client_order_id={}",
                        attempt.exchange,
                        attempt.canonical_symbol,
                        attempt.position_side,
                        attempt.quantity,
                        attempt.client_order_id
                    );
                } else {
                    log::error!(
                        "cross_arb_live startup orphan close failed exchange={} symbol={} side={:?} qty={} client_order_id={} message={}",
                        attempt.exchange,
                        attempt.canonical_symbol,
                        attempt.position_side,
                        attempt.quantity,
                        attempt.client_order_id,
                        attempt.message.as_deref().unwrap_or("unknown")
                    );
                }
            }
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
            let mut guard = runtime.write().await;
            if !startup_close_attempts.is_empty()
                && guard.state.close_only
                && !guard.state.kill_switch
                && !config.controls.start_close_only
                && guard.state.open_bundles.is_empty()
            {
                guard.state.clear_close_only();
            }
        } else {
            let mut guard = runtime.write().await;
            guard.state.set_close_only();
            log::error!(
                "cross_arb_live startup still has {} unpaired position(s) after cleanup; keeping close-only: {}",
                remaining_unpaired.len(),
                remaining_unpaired
                    .iter()
                    .map(|position| format!(
                        "{} {} {:?} qty={}",
                        position.exchange,
                        position.canonical_symbol,
                        position.position_side,
                        position.quantity
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
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
            return Err(anyhow!(
                "initial private REST audit found existing exposure/open orders without restored state: {}; new entries blocked. Configure affected exchange operating_mode=close_only or reconcile state before live run",
                dirty_exchanges.join(", ")
            ));
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

    let private_sync_for_audit = private_sync.clone();
    tokio::spawn(periodic_rest_audit_loop(
        private_sync_for_audit,
        config.clone(),
        enabled_exchanges.clone(),
    ));

    tokio::spawn(ws_market_execution_loop(
        runtime.clone(),
        execution.clone(),
        close_metrics.clone(),
        config.clone(),
        enabled_exchanges,
        instruments,
    ));

    start_live_dashboard(runtime.clone(), execution.clone(), close_metrics, &config).await?;

    futures_util::future::pending::<()>().await;
    Ok(())
}

async fn start_live_dashboard(
    runtime: Arc<RwLock<CrossArbRuntime>>,
    execution: Arc<RwLock<CrossArbExecutionCoordinator>>,
    close_metrics: Arc<RwLock<HashMap<String, BundleCloseMetricReadModel>>>,
    config: &CrossExchangeArbitrageConfig,
) -> Result<()> {
    if !config.dashboard.enabled {
        return Ok(());
    }
    let addr: SocketAddr = format!("{}:{}", config.dashboard.bind_host, config.dashboard.port)
        .parse()
        .with_context(|| "failed to parse cross_arb_live dashboard bind address")?;
    let state = LiveDashboardState {
        runtime,
        execution,
        close_metrics,
        started_at: Utc::now(),
    };
    let router = live_dashboard_router(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind cross_arb_live dashboard on {addr}"))?;
    log::info!("cross_arb_live dashboard listening on http://{addr}");
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            log::error!("cross_arb_live dashboard stopped: {error}");
        }
    });
    Ok(())
}

fn live_dashboard_router(state: LiveDashboardState) -> Router {
    Router::new()
        .route("/", get(live_dashboard_index))
        .route("/healthz", get(live_dashboard_healthz))
        .route("/api/cross-arb/live/dashboard", get(live_dashboard_payload))
        .route("/api/cross-arb/dashboard", get(live_dashboard_payload))
        .route(
            "/api/cross-arb/opportunities",
            get(live_dashboard_opportunities),
        )
        .route(
            "/api/cross-arb/bundles/open",
            get(live_dashboard_open_bundles),
        )
        .route("/api/cross-arb/positions", get(live_dashboard_positions))
        .route(
            "/api/cross-arb/open-orders",
            get(live_dashboard_open_orders),
        )
        .route("/api/cross-arb/balances", get(live_dashboard_balances))
        .route(
            "/api/cross-arb/risk-events",
            get(live_dashboard_risk_events),
        )
        .route(
            "/api/cross-arb/control/pause-new-entries",
            post(live_dashboard_pause_new_entries),
        )
        .route(
            "/api/cross-arb/control/resume-new-entries",
            post(live_dashboard_resume_new_entries),
        )
        .route(
            "/api/cross-arb/control/close-only",
            post(live_dashboard_close_only),
        )
        .route(
            "/api/cross-arb/control/kill-switch",
            post(live_dashboard_kill_switch),
        )
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

async fn live_dashboard_index() -> Html<&'static str> {
    Html(LIVE_DASHBOARD_HTML)
}

async fn live_dashboard_healthz() -> &'static str {
    "ok"
}

async fn live_dashboard_payload(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<LiveDashboardPayload> {
    Json(build_live_dashboard_payload(&state).await)
}

async fn live_dashboard_opportunities(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<OpportunityReadModel>> {
    let guard = state.runtime.read().await;
    Json(guard.state.opportunity_read_models())
}

async fn live_dashboard_open_bundles(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<BundleReadModel>> {
    let guard = state.runtime.read().await;
    Json(guard.state.open_bundle_read_models())
}

async fn live_dashboard_positions(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<PositionSnapshot>> {
    let guard = state.runtime.read().await;
    Json(private_positions_from_runtime(&guard))
}

async fn live_dashboard_open_orders(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<OrderSnapshot>> {
    let guard = state.runtime.read().await;
    Json(private_open_orders_from_runtime(&guard))
}

async fn live_dashboard_balances(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<ExchangeBalance>> {
    let guard = state.runtime.read().await;
    Json(balances_from_runtime(&guard))
}

async fn live_dashboard_risk_events(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<Vec<RiskEventReadModel>> {
    let guard = state.runtime.read().await;
    Json(guard.state.risk_events.clone())
}

async fn live_dashboard_pause_new_entries(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<LiveControlState> {
    let mut guard = state.runtime.write().await;
    guard.state.pause_new_entries();
    Json(control_state_from_runtime(&guard))
}

async fn live_dashboard_resume_new_entries(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<LiveControlState> {
    let mut guard = state.runtime.write().await;
    guard.state.resume_new_entries();
    Json(control_state_from_runtime(&guard))
}

async fn live_dashboard_close_only(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<LiveControlState> {
    let mut guard = state.runtime.write().await;
    guard.state.set_close_only();
    Json(control_state_from_runtime(&guard))
}

async fn live_dashboard_kill_switch(
    AxumState(state): AxumState<LiveDashboardState>,
) -> Json<LiveControlState> {
    let mut guard = state.runtime.write().await;
    guard.state.kill_switch();
    Json(control_state_from_runtime(&guard))
}

async fn build_live_dashboard_payload(state: &LiveDashboardState) -> LiveDashboardPayload {
    let guard = state.runtime.read().await;
    let execution_guard = state.execution.read().await;
    let close_metrics = state.close_metrics.read().await;
    let status = guard.state.dashboard_status();
    let private_open_orders = private_open_orders_from_runtime(&guard);
    let strategy_open_orders = private_open_orders
        .iter()
        .filter(|order| order_is_crossarb(order))
        .cloned()
        .collect::<Vec<_>>();
    let external_open_orders = private_open_orders
        .iter()
        .filter(|order| !order_is_crossarb(order))
        .cloned()
        .collect::<Vec<_>>();
    LiveDashboardPayload {
        generated_at: Utc::now(),
        started_at: state.started_at,
        status,
        symbols: symbol_coverage_from_runtime(&guard),
        opportunities: filtered_opportunity_read_models(&guard),
        open_bundles: guard.state.open_bundle_read_models(),
        active_bundles: active_bundle_read_models(&guard, &close_metrics),
        hedge_records: guard.state.hedge_record_read_models(),
        hedge_repair_tasks: guard.state.hedge_repair_task_read_models(),
        history: guard.state.history_read_models(),
        risk_event_summary: summarize_risk_events(&guard.state.risk_events),
        risk_events: guard.state.risk_events.clone(),
        private_positions: private_positions_from_runtime(&guard),
        private_open_orders,
        strategy_open_orders,
        external_open_orders,
        balances: balances_from_runtime(&guard),
        controls: control_state_from_runtime(&guard),
        execution: execution_summary_from_runtime(&guard, execution_guard.pending_maker_count()),
        maker_execution_stats: execution_guard.maker_execution_stats(&guard.state.config),
    }
}

fn filtered_opportunity_read_models(runtime: &CrossArbRuntime) -> Vec<BestOpportunityReadModel> {
    let active_symbols = active_position_symbols_from_runtime(runtime);
    let mut best_by_symbol: HashMap<String, OpportunityReadModel> = HashMap::new();
    for opportunity in runtime
        .state
        .opportunity_read_models()
        .into_iter()
        .filter(|opportunity| !active_symbols.contains(&opportunity.canonical_symbol.to_string()))
    {
        let symbol = opportunity.canonical_symbol.to_string();
        let replace = best_by_symbol
            .get(&symbol)
            .map(|current| opportunity_rank_cmp(&opportunity, current).is_gt())
            .unwrap_or(true);
        if replace {
            best_by_symbol.insert(symbol, opportunity);
        }
    }
    let mut opportunities = best_by_symbol
        .into_values()
        .map(|opportunity| BestOpportunityReadModel {
            symbol: opportunity.canonical_symbol.to_string(),
            best_route: format!(
                "多 {} / 空 {}",
                opportunity.long_exchange.as_str(),
                opportunity.short_exchange.as_str()
            ),
            long_exchange: opportunity.long_exchange.as_str().to_string(),
            short_exchange: opportunity.short_exchange.as_str().to_string(),
            maker_exchange: opportunity.maker_exchange.as_str().to_string(),
            maker_side: format!("{:?}", opportunity.maker_side),
            maker_price: opportunity.maker_price,
            maker_book_spread_pct: opportunity.maker_book_spread_pct,
            open_spread_pct: opportunity.raw_open_spread,
            expected_net_edge_pct: opportunity.maker_taker_net_edge,
            notional_usdt: opportunity.executable_notional_usdt,
            can_open: opportunity.can_open,
            reject_reasons: opportunity
                .reject_reasons
                .iter()
                .map(|reason| chinese_reject_reason(*reason).to_string())
                .collect(),
            book_age_ms: opportunity.book_age_ms,
            updated_at: opportunity.created_at,
        })
        .collect::<Vec<_>>();
    opportunities.sort_by(|left, right| {
        right.can_open.cmp(&left.can_open).then_with(|| {
            right
                .expected_net_edge_pct
                .partial_cmp(&left.expected_net_edge_pct)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    opportunities.into_iter().take(200).collect()
}

fn opportunity_rank_cmp(
    left: &OpportunityReadModel,
    right: &OpportunityReadModel,
) -> std::cmp::Ordering {
    left.can_open.cmp(&right.can_open).then_with(|| {
        left.maker_taker_net_edge
            .partial_cmp(&right.maker_taker_net_edge)
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

fn active_bundle_read_models(
    runtime: &CrossArbRuntime,
    close_metrics: &HashMap<String, BundleCloseMetricReadModel>,
) -> Vec<ActiveBundleReadModel> {
    let mut by_pair: HashMap<String, ActiveBundleReadModel> = HashMap::new();
    for item in runtime
        .state
        .open_bundle_read_models()
        .into_iter()
        .filter(|bundle| {
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
        .map(|bundle| {
            let metric = close_metrics.get(&bundle.bundle_id);
            let position = runtime.state.position_manager.bundle(&bundle.bundle_id);
            ActiveBundleReadModel {
                bundle_id: bundle.bundle_id,
                opportunity_id: bundle.opportunity_id,
                symbol: position
                    .map(|position| position.canonical_symbol.to_string())
                    .or_else(|| metric.map(|metric| metric.symbol.clone())),
                long_exchange: position
                    .map(|position| position.long_leg.exchange.as_str().to_string())
                    .or_else(|| metric.map(|metric| metric.long_exchange.clone())),
                short_exchange: position
                    .map(|position| position.short_leg.exchange.as_str().to_string())
                    .or_else(|| metric.map(|metric| metric.short_exchange.clone())),
                status: bundle.status,
                target_notional_usdt: bundle.target_notional_usdt,
                entry_edge_pct: position.map(|position| position.entry_edge_pct),
                close_spread_pct: metric.map(|metric| metric.close_spread_pct),
                close_profit_pct: metric.map(|metric| metric.close_profit_pct),
                close_threshold_pct: metric
                    .map(|metric| metric.close_threshold_pct)
                    .unwrap_or(runtime.state.config.thresholds.lock_profit_dual_taker_pct),
                close_maker_exchange: metric.map(|metric| metric.close_maker_exchange.clone()),
                close_maker_side: metric.map(|metric| metric.close_maker_side.clone()),
                close_maker_price: metric.map(|metric| metric.close_maker_price),
                close_maker_book_spread_pct: metric
                    .map(|metric| metric.close_maker_book_spread_pct),
                closeable: metric.map(|metric| metric.closeable).unwrap_or(false),
                updated_at: bundle.updated_at,
            }
        })
    {
        let key = match (&item.symbol, &item.long_exchange, &item.short_exchange) {
            (Some(symbol), Some(long), Some(short)) => format!("{symbol}|{long}|{short}"),
            _ => format!("bundle|{}", item.bundle_id),
        };
        let replace = by_pair
            .get(&key)
            .map(|current| should_replace_active_bundle(current, &item))
            .unwrap_or(true);
        if replace {
            by_pair.insert(key, item);
        }
    }
    let mut items = by_pair.into_values().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.symbol
            .cmp(&right.symbol)
            .then_with(|| left.long_exchange.cmp(&right.long_exchange))
            .then_with(|| left.short_exchange.cmp(&right.short_exchange))
            .then_with(|| left.bundle_id.cmp(&right.bundle_id))
    });
    items
}

fn should_replace_active_bundle(
    current: &ActiveBundleReadModel,
    candidate: &ActiveBundleReadModel,
) -> bool {
    let current_restored = current.bundle_id.starts_with("restored-");
    let candidate_restored = candidate.bundle_id.starts_with("restored-");
    if current_restored != candidate_restored {
        return current_restored && !candidate_restored;
    }
    if current.close_profit_pct.is_none() != candidate.close_profit_pct.is_none() {
        return current.close_profit_pct.is_none() && candidate.close_profit_pct.is_some();
    }
    candidate.updated_at > current.updated_at
}

fn summarize_risk_events(events: &[RiskEventReadModel]) -> Vec<RiskEventSummaryReadModel> {
    let mut summaries: HashMap<String, RiskEventSummaryReadModel> = HashMap::new();
    for event in events {
        let symbol = event.canonical_symbol.as_ref().map(ToString::to_string);
        let exchange = event
            .exchange
            .as_ref()
            .map(|exchange| exchange.as_str().to_string());
        let message = normalize_risk_message(&event.message);
        let key = format!(
            "{}|{}|{:?}|{}",
            exchange.as_deref().unwrap_or(""),
            symbol.as_deref().unwrap_or(""),
            event.reason,
            message
        );
        summaries
            .entry(key.clone())
            .and_modify(|summary| {
                summary.count += 1;
                summary.last_at = event.created_at;
            })
            .or_insert_with(|| RiskEventSummaryReadModel {
                key,
                exchange,
                symbol,
                reason: chinese_reject_reason(event.reason).to_string(),
                message,
                count: 1,
                first_at: event.created_at,
                last_at: event.created_at,
            });
    }
    let mut values = summaries.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| right.last_at.cmp(&left.last_at));
    values
}

fn normalize_risk_message(message: &str) -> String {
    if message.contains("cancelled after TTL") {
        "Maker 挂单超过 TTL 未成交，已自动撤单".to_string()
    } else if message.contains("TTL cancel failed") {
        "Maker 挂单超过 TTL，自动撤单失败".to_string()
    } else if message.contains("private stream event") {
        "私有 WebSocket 事件异常".to_string()
    } else {
        message.to_string()
    }
}

fn chinese_reject_reason(reason: RejectReason) -> &'static str {
    match reason {
        RejectReason::RawSpreadTooSmall => "原始价差不足",
        RejectReason::NetEdgeTooSmall => "净价差不足",
        RejectReason::StaleBook => "盘口过期",
        RejectReason::RouteUnhealthy => "路由/交易所状态异常",
        RejectReason::DepthInsufficient => "深度不足",
        RejectReason::FundingDangerous => "资金费率风险",
        RejectReason::FundingWindowTooClose => "临近资金费结算",
        RejectReason::NotionalOverLimit => "名义金额超限",
        RejectReason::SlippageTooHigh => "滑点过高",
        RejectReason::BadOrderBook => "盘口异常",
        RejectReason::PrecisionInvalid => "精度不合法",
        RejectReason::AbnormalCrossExchangeSpread => "跨所价差异常",
        RejectReason::ExchangeCapacityExceeded => "交易所容量超限",
        RejectReason::ExchangePositionLimitExceeded => "交易所仓位数超限",
        RejectReason::UnpairedExchangePosition => "未配对交易所仓位",
    }
}

fn symbol_coverage_from_runtime(runtime: &CrossArbRuntime) -> Vec<SymbolCoverageReadModel> {
    runtime
        .state
        .config
        .universe
        .symbols
        .iter()
        .map(|symbol| {
            let mut exchanges = runtime
                .state
                .opportunities
                .iter()
                .filter(|opportunity| opportunity.canonical_symbol == *symbol)
                .flat_map(|opportunity| {
                    [
                        opportunity.long_exchange.as_str().to_string(),
                        opportunity.short_exchange.as_str().to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            exchanges.sort();
            exchanges.dedup();
            let opportunities = runtime
                .state
                .opportunities
                .iter()
                .filter(|opportunity| opportunity.canonical_symbol == *symbol)
                .count();
            let openable_opportunities = runtime
                .state
                .opportunities
                .iter()
                .filter(|opportunity| {
                    opportunity.canonical_symbol == *symbol && opportunity.can_open
                })
                .count();
            SymbolCoverageReadModel {
                symbol: symbol.to_string(),
                configured_exchanges: runtime
                    .state
                    .config
                    .universe
                    .enabled_exchanges
                    .iter()
                    .map(|exchange| exchange.as_str().to_string())
                    .collect(),
                exchanges,
                opportunities,
                openable_opportunities,
            }
        })
        .collect()
}

fn active_position_symbols_from_runtime(runtime: &CrossArbRuntime) -> HashSet<String> {
    let mut symbols = runtime
        .state
        .position_manager
        .bundles()
        .filter(|position| {
            position.closeable_qty(runtime.state.config.reconciliation.quantity_tolerance) > 0.0
                || position.unhedged_qty() > runtime.state.config.reconciliation.quantity_tolerance
        })
        .map(|position| position.canonical_symbol.to_string())
        .collect::<HashSet<_>>();
    for position in private_positions_from_runtime(runtime) {
        if position.quantity.abs() > runtime.state.config.reconciliation.quantity_tolerance {
            symbols.insert(position.canonical_symbol.to_string());
        }
    }
    symbols
}

fn private_positions_from_runtime(runtime: &CrossArbRuntime) -> Vec<PositionSnapshot> {
    let mut positions = runtime
        .state
        .account_sync
        .values()
        .flat_map(|state| state.position_snapshots())
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        left.exchange
            .as_str()
            .cmp(right.exchange.as_str())
            .then_with(|| {
                left.canonical_symbol
                    .to_string()
                    .cmp(&right.canonical_symbol.to_string())
            })
            .then_with(|| {
                format!("{:?}", left.position_side).cmp(&format!("{:?}", right.position_side))
            })
    });
    positions
}

fn private_open_orders_from_runtime(runtime: &CrossArbRuntime) -> Vec<OrderSnapshot> {
    let mut orders = runtime
        .state
        .account_sync
        .values()
        .flat_map(|state| state.order_snapshots())
        .collect::<Vec<_>>();
    orders.sort_by(|left, right| {
        left.exchange
            .as_str()
            .cmp(right.exchange.as_str())
            .then_with(|| {
                left.canonical_symbol
                    .to_string()
                    .cmp(&right.canonical_symbol.to_string())
            })
            .then_with(|| left.updated_at.cmp(&right.updated_at))
    });
    orders
}

fn order_is_crossarb(order: &OrderSnapshot) -> bool {
    order
        .client_order_id
        .as_deref()
        .map(is_crossarb_client_order_id)
        .unwrap_or(false)
}

fn balances_from_runtime(runtime: &CrossArbRuntime) -> Vec<ExchangeBalance> {
    let mut balances = runtime
        .state
        .account_sync
        .values()
        .flat_map(|state| state.account_snapshot())
        .collect::<Vec<_>>();
    balances.sort_by(|left, right| {
        left.exchange
            .as_str()
            .cmp(right.exchange.as_str())
            .then_with(|| left.asset.cmp(&right.asset))
    });
    balances
}

fn control_state_from_runtime(runtime: &CrossArbRuntime) -> LiveControlState {
    LiveControlState {
        paused_new_entries: runtime.state.paused_new_entries,
        close_only: runtime.state.close_only,
        kill_switch: runtime.state.kill_switch,
    }
}

fn execution_summary_from_runtime(
    runtime: &CrossArbRuntime,
    pending_maker_orders: usize,
) -> LiveExecutionSummary {
    let private_open_orders = private_open_orders_from_runtime(runtime);
    let strategy_open_orders = private_open_orders
        .iter()
        .filter(|order| order_is_crossarb(order))
        .count();
    LiveExecutionSummary {
        dry_run: runtime.state.config.execution.dry_run,
        open_execution_style: format!("{:?}", runtime.state.config.execution.open_execution_style),
        maker_order_ttl_ms: runtime.state.config.execution.maker_order_ttl_ms,
        max_concurrent_maker_orders: runtime.state.config.execution.max_concurrent_maker_orders,
        pending_maker_orders,
        maker_price_offset_ticks: runtime.state.config.execution.maker_price_offset_ticks,
        maker_aggressive_after_cancels: runtime
            .state
            .config
            .execution
            .maker_aggressive_after_cancels,
        maker_aggressive_step_ticks: runtime.state.config.execution.maker_aggressive_step_ticks,
        maker_aggressive_max_ticks: runtime.state.config.execution.maker_aggressive_max_ticks,
        maker_cooldown_after_cancels: runtime.state.config.execution.maker_cooldown_after_cancels,
        maker_cooldown_ms: runtime.state.config.execution.maker_cooldown_ms,
        target_notional_usdt: runtime.state.config.sizing.target_notional_usdt,
        max_notional_usdt: runtime.state.config.sizing.max_notional_usdt,
        min_open_maker_taker_net_edge: runtime
            .state
            .config
            .thresholds
            .min_open_maker_taker_net_edge,
        close_profit_threshold_pct: runtime.state.config.thresholds.lock_profit_dual_taker_pct,
        max_open_bundles: runtime.state.config.risk.max_open_bundles,
        max_positions_per_exchange: runtime.state.config.sizing.max_positions_per_exchange,
        enabled_exchanges: runtime
            .state
            .config
            .universe
            .enabled_exchanges
            .iter()
            .map(|exchange| exchange.as_str().to_string())
            .collect(),
        enabled_symbols: runtime.state.config.universe.symbols.len(),
        strategy_open_orders,
        external_open_orders: private_open_orders
            .len()
            .saturating_sub(strategy_open_orders),
    }
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

async fn close_startup_unpaired_positions(
    runtime: Arc<RwLock<CrossArbRuntime>>,
    private_sync: &PrivateRuntimeSync,
    adapters: &[Arc<dyn TradingAdapter>],
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
) -> Vec<StartupOrphanCloseAttempt> {
    if config.execution.dry_run {
        return Vec::new();
    }

    let adapters_by_exchange = adapters
        .iter()
        .cloned()
        .map(|adapter| (adapter.exchange(), adapter))
        .collect::<HashMap<_, _>>();
    let mut attempts = Vec::new();

    for attempt_index in 1..=STARTUP_ORPHAN_CLOSE_MAX_ATTEMPTS {
        let unpaired = {
            let guard = runtime.read().await;
            dedupe_startup_unpaired_positions(guard.state.account_unpaired_exchange_positions())
        };
        if unpaired.is_empty() {
            break;
        }

        log::warn!(
            "cross_arb_live startup cleanup attempt {} closing {} unpaired position(s)",
            attempt_index,
            unpaired.len()
        );

        for position in unpaired {
            let Some(adapter) = adapters_by_exchange.get(&position.exchange) else {
                attempts.push(StartupOrphanCloseAttempt {
                    exchange: position.exchange,
                    canonical_symbol: position.canonical_symbol,
                    position_side: position.position_side,
                    quantity: position.quantity.abs(),
                    client_order_id: String::new(),
                    accepted: false,
                    message: Some("missing trading adapter for startup orphan close".to_string()),
                });
                continue;
            };
            let client_order_id =
                startup_orphan_close_client_id(attempt_index, &position.exchange, &position);
            let command = ClosePositionCommand::market(
                position.exchange.clone(),
                position.canonical_symbol.clone(),
                position.exchange_symbol.clone(),
                position.position_side,
                position.quantity.abs(),
                client_order_id.clone(),
                Utc::now(),
            );
            match adapter.close_position(command).await {
                Ok(ack) => attempts.push(StartupOrphanCloseAttempt {
                    exchange: position.exchange,
                    canonical_symbol: position.canonical_symbol,
                    position_side: position.position_side,
                    quantity: position.quantity.abs(),
                    client_order_id,
                    accepted: ack.accepted,
                    message: ack.message,
                }),
                Err(err) => attempts.push(StartupOrphanCloseAttempt {
                    exchange: position.exchange,
                    canonical_symbol: position.canonical_symbol,
                    position_side: position.position_side,
                    quantity: position.quantity.abs(),
                    client_order_id,
                    accepted: false,
                    message: Some(err.to_string()),
                }),
            }
        }

        sleep(TokioDuration::from_secs(STARTUP_ORPHAN_CLOSE_SETTLE_SECS)).await;
        let audits = run_initial_rest_audits(private_sync, config, exchanges).await;
        let failures = audits
            .iter()
            .filter(|summary| !summary.ok)
            .map(|summary| summary.exchange.clone())
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            log::error!(
                "cross_arb_live startup cleanup REST verification failed after attempt {}: {}",
                attempt_index,
                failures.join(", ")
            );
            break;
        }
    }

    attempts
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

fn dedupe_startup_unpaired_positions(positions: Vec<ExchangePosition>) -> Vec<ExchangePosition> {
    let mut deduped: HashMap<(ExchangeId, CanonicalSymbol, PositionSide), ExchangePosition> =
        HashMap::new();
    for position in positions {
        let key = (
            position.exchange.clone(),
            position.canonical_symbol.clone(),
            position.position_side,
        );
        match deduped.get(&key) {
            Some(existing) if existing.quantity.abs() >= position.quantity.abs() => {}
            _ => {
                deduped.insert(key, position);
            }
        }
    }
    deduped.into_values().collect()
}

fn startup_orphan_close_client_id(
    attempt_index: usize,
    exchange: &ExchangeId,
    position: &ExchangePosition,
) -> String {
    let symbol = compact_order_id_symbol(&position.canonical_symbol);
    let side = match position.position_side {
        PositionSide::Long => "l",
        PositionSide::Short => "s",
        PositionSide::Net => "n",
    };
    format!(
        "xsc{}{}{}{}",
        exchange_order_id_code(exchange),
        side,
        attempt_index,
        symbol
    )
    .chars()
    .take(32)
    .collect()
}

fn exchange_order_id_code(exchange: &ExchangeId) -> &'static str {
    match exchange {
        ExchangeId::Binance => "bn",
        ExchangeId::Bitget => "bg",
        ExchangeId::Gate => "gt",
        ExchangeId::Bybit => "bb",
        ExchangeId::Htx => "ht",
        ExchangeId::Mexc => "mx",
        ExchangeId::Okx => "ok",
        ExchangeId::Other(_) => "ot",
    }
}

fn compact_order_id_symbol(symbol: &CanonicalSymbol) -> String {
    symbol
        .to_string()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .take(18)
        .collect()
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
        for instrument in instruments {
            cache.upsert_instrument(instrument);
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

        let mut runtime_guard = runtime.write().await;
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
            runtime.persist_hedge_repair_task(&task.task_id, now);
            runtime.persist_hedge_record(&task.bundle_id, now);
            continue;
        };
        let attempt_exchange = command.exchange.clone();
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
    if let Some(selection) = existing_repair_leg_snapshot(runtime, task, snapshots) {
        return Some(selection);
    }
    snapshots
        .iter()
        .filter(|snapshot| snapshot.book.is_usable())
        .filter(|snapshot| snapshot.book.exchange != task.failed_exchange)
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

fn existing_repair_leg_snapshot(
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
    if leg.filled_qty <= runtime.state.config.reconciliation.quantity_tolerance {
        return None;
    }
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
                    >= runtime.state.config.thresholds.lock_profit_dual_taker_pct,
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

const LIVE_DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>跨所套利实盘监控</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0f1318;
      --panel: #171d24;
      --panel-2: #1f2730;
      --line: #2d3844;
      --text: #eef3f7;
      --muted: #9ba8b5;
      --ok: #44c776;
      --warn: #f0b84a;
      --bad: #ef6461;
      --accent: #58a6ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 2;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 20px;
      background: rgba(15, 19, 24, 0.94);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(10px);
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: 0;
    }
    main {
      display: grid;
      gap: 14px;
      padding: 16px 20px 28px;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }
    button {
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel-2);
      color: var(--text);
      padding: 8px 10px;
      cursor: pointer;
      font-weight: 600;
    }
    button:hover { border-color: var(--accent); }
    button.danger { color: #ffd9d8; border-color: #6f3333; background: #321b1e; }
    .muted { color: var(--muted); }
    .grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
    }
    .metric, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .metric {
      min-height: 76px;
      padding: 12px;
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .metric .value {
      margin-top: 6px;
      font-size: 22px;
      font-weight: 750;
    }
    section {
      overflow: hidden;
    }
    section h2 {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin: 0;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      background: rgba(255, 255, 255, 0.02);
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
      text-align: left;
      white-space: nowrap;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    tbody tr:hover { background: rgba(255, 255, 255, 0.035); }
    .scroll {
      overflow: auto;
      max-height: 440px;
    }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .two {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 14px;
    }
    @media (max-width: 1100px) {
      .grid { grid-template-columns: repeat(3, minmax(120px, 1fr)); }
      .two { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      header { align-items: flex-start; flex-direction: column; }
      main { padding: 12px; }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .metric .value { font-size: 18px; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>跨所套利实盘监控</h1>
      <div id="subtitle" class="muted">加载中</div>
    </div>
    <div class="toolbar">
      <button onclick="postControl('/api/cross-arb/control/pause-new-entries')">暂停开仓</button>
      <button onclick="postControl('/api/cross-arb/control/resume-new-entries')">恢复开仓</button>
      <button onclick="postControl('/api/cross-arb/control/close-only')">只平仓</button>
      <button class="danger" onclick="postControl('/api/cross-arb/control/kill-switch')">熔断</button>
    </div>
  </header>
  <main>
    <div id="metrics" class="grid"></div>
    <section>
      <h2>交易对覆盖 <span id="symbols-count" class="muted"></span></h2>
      <div class="scroll"><table id="symbols"></table></div>
    </section>
    <div class="two">
      <section>
        <h2>私有仓位 <span id="positions-count" class="muted"></span></h2>
        <div class="scroll"><table id="positions"></table></div>
      </section>
      <section>
        <h2>未成交订单 <span id="orders-count" class="muted"></span></h2>
        <div class="scroll"><table id="orders"></table></div>
      </section>
    </div>
    <section>
      <h2>机会扫描 <span id="opportunities-count" class="muted"></span></h2>
      <div class="scroll"><table id="opportunities"></table></div>
    </section>
    <section>
      <h2>对冲记录 <span id="hedge-records-count" class="muted"></span></h2>
      <div class="scroll"><table id="hedge-records"></table></div>
    </section>
    <section>
      <h2>补单任务 <span id="hedge-repair-count" class="muted"></span></h2>
      <div class="scroll"><table id="hedge-repair"></table></div>
    </section>
    <section>
      <h2>Maker成交统计 <span id="maker-stats-count" class="muted"></span></h2>
      <div class="scroll"><table id="maker-stats"></table></div>
    </section>
    <section>
      <h2>账户余额 <span id="balances-count" class="muted"></span></h2>
      <div class="scroll"><table id="balances"></table></div>
    </section>
    <section>
      <h2>风控事件 <span id="risk-count" class="muted"></span></h2>
      <div class="scroll"><table id="risk"></table></div>
    </section>
  </main>
  <script>
    const fmt = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 6 });
    const money = new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 });
    const beijingTime = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
    let data = null;

    async function load() {
      data = await fetch('/api/cross-arb/live/dashboard', { cache: 'no-store' }).then(r => r.json());
      render();
    }

    async function postControl(path) {
      await fetch(path, { method: 'POST' });
      await load();
    }

    function render() {
      const d = data;
      const dry = d.execution.dry_run ? 'DRY RUN' : 'LIVE ORDERS';
      const mode = d.status.risk_state.mode;
      document.getElementById('subtitle').textContent =
        `${dry} | ${mode} | 交易所 ${d.execution.enabled_exchanges.join(', ')} | 交易对 ${d.execution.enabled_symbols} | 更新 ${time(d.status.updated_at)}`;
      metrics(d);
      rows('symbols', (d.symbols || []).slice(0, 500), [
        ['交易对', r => r.symbol],
        ['配置交易所', r => (r.configured_exchanges || []).join(', ')],
        ['已扫描交易所', r => (r.exchanges || []).join(', ')],
        ['机会数', r => r.opportunities],
        ['可开仓机会', r => r.openable_opportunities],
      ]);
      text('symbols-count', `${(d.symbols || []).length} 个配置交易对`);
      rows('positions', d.private_positions, [
        ['交易所', r => r.exchange],
        ['交易对', r => sym(r.canonical_symbol)],
        ['方向', r => r.position_side],
        ['数量', r => num(r.quantity)],
        ['开仓价', r => num(r.entry_price)],
        ['未实现盈亏', r => num(r.unrealized_pnl)],
        ['更新时间', r => time(r.updated_at)],
      ]);
      text('positions-count', d.private_positions.length);
      rows('orders', d.private_open_orders, [
        ['交易所', r => r.exchange],
        ['交易对', r => sym(r.canonical_symbol)],
        ['归属', r => orderOwner(r.client_order_id)],
        ['模式', r => orderMode(r.client_order_id)],
        ['状态', r => r.status],
        ['数量', r => num(r.quantity)],
        ['已成交', r => num(r.filled_quantity)],
        ['客户端ID', r => r.client_order_id || ''],
        ['更新时间', r => time(r.updated_at)],
      ]);
      text('orders-count', `${d.strategy_open_orders.length} 策略 / ${d.external_open_orders.length} 外部 / ${d.private_open_orders.length} 总计`);
      rows('opportunities', d.opportunities.slice(0, 200), [
        ['交易对', r => r.symbol],
        ['最佳组合', r => r.best_route],
        ['Maker交易所', r => r.maker_exchange],
        ['Maker方向', r => r.maker_side],
        ['Maker价格', r => num(r.maker_price)],
        ['Maker本所价差', r => pct(r.maker_book_spread_pct)],
        ['开仓价差', r => pct(r.open_spread_pct)],
        ['预计净收益', r => cls(pct(r.expected_net_edge_pct), r.can_open ? 'ok' : 'warn')],
        ['名义金额', r => money.format(r.notional_usdt || 0)],
        ['可开仓', r => r.can_open ? cls('是', 'ok') : cls('否', 'bad')],
        ['拒绝原因', r => (r.reject_reasons || []).join(', ')],
        ['盘口年龄', r => `${r.book_age_ms}ms`],
      ]);
      text('opportunities-count', d.opportunities.length);
      const hedgeRecords = (d.hedge_records || []).filter(r => r.hedge_order_submitted_at || r.is_closed);
      rows('hedge-records', hedgeRecords, [
        ['开仓时间', r => time(r.opened_at || r.updated_at)],
        ['交易对', r => sym(r.canonical_symbol)],
        ['做多交易所', r => r.long_exchange || ''],
        ['做空交易所', r => r.short_exchange || ''],
        ['状态', r => r.status],
        ['入场净价差', r => pct(r.entry_net_edge_pct)],
        ['平仓价差', r => pct(r.close_spread_pct)],
        ['平仓净利润', r => pct(r.close_net_profit_pct)],
        ['是否平仓', r => r.is_closed ? cls('是', 'ok') : '否'],
        ['平仓时间', r => time(r.closed_at)],
        ['更新时间', r => time(r.updated_at)],
      ]);
      text('hedge-records-count', `${hedgeRecords.length} 条 / ${d.open_bundles.length} 个组合`);
      rows('hedge-repair', d.hedge_repair_tasks || [], [
        ['任务ID', r => r.task_id],
        ['交易对', r => sym(r.canonical_symbol)],
        ['失败交易所', r => r.failed_exchange],
        ['尝试交易所', r => r.last_attempt_exchange || ''],
        ['方向', r => `${r.side}/${r.position_side}`],
        ['数量', r => num(r.quantity)],
        ['状态', r => r.status],
        ['尝试次数', r => r.attempts],
        ['错误', r => r.last_error || ''],
        ['更新时间', r => time(r.updated_at)],
      ]);
      text('hedge-repair-count', `${(d.hedge_repair_tasks || []).length} 个任务`);
      rows('maker-stats', d.maker_execution_stats || [], [
        ['交易对', r => sym(r.canonical_symbol)],
        ['交易所', r => r.exchange],
        ['方向', r => r.side],
        ['连续撤单', r => r.consecutive_ttl_cancels],
        ['累计撤单', r => r.total_ttl_cancels],
        ['累计成交', r => r.total_fills],
        ['当前激进', r => `${r.current_aggressive_ticks} tick`],
        ['冷却到', r => time(r.cooldown_until)],
        ['最近事件', r => time(r.last_event_at)],
      ]);
      text('maker-stats-count', `${(d.maker_execution_stats || []).length} 条`);
      rows('balances', d.balances, [
        ['交易所', r => r.exchange],
        ['资产', r => r.asset],
        ['总额', r => num(r.total)],
        ['可用', r => num(r.available)],
        ['冻结', r => num(r.locked)],
        ['更新时间', r => time(r.updated_at)],
      ]);
      text('balances-count', d.balances.length);
      rows('risk', d.risk_event_summary.slice(0, 200), [
        ['最近时间', r => time(r.last_at)],
        ['交易所', r => r.exchange || ''],
        ['交易对', r => r.symbol || ''],
        ['原因', r => r.reason],
        ['次数', r => r.count],
        ['消息', r => r.message],
      ]);
      text('risk-count', `${d.risk_event_summary.length} 类 / ${d.risk_events.length} 条`);
    }

    function metrics(d) {
      const s = d.status;
      const e = d.execution;
      const controls = d.controls;
      document.getElementById('metrics').innerHTML = [
        metric('订单模式', e.dry_run ? 'DRY' : 'LIVE', e.dry_run ? 'warn' : 'bad'),
        metric('风控模式', s.risk_state.mode, s.risk_state.allow_new_entries ? 'ok' : 'warn'),
        metric('开仓允许', s.risk_state.allow_new_entries ? 'YES' : 'NO', s.risk_state.allow_new_entries ? 'ok' : 'bad'),
        metric('只平仓', controls.close_only ? 'YES' : 'NO', controls.close_only ? 'warn' : 'ok'),
        metric('熔断', controls.kill_switch ? 'ON' : 'OFF', controls.kill_switch ? 'bad' : 'ok'),
        metric('开仓组合', s.open_bundles),
        metric('每单', money.format(e.target_notional_usdt)),
        metric('开仓阈值', pct(e.min_open_maker_taker_net_edge)),
        metric('平仓净利阈值', pct(e.close_profit_threshold_pct)),
        metric('开仓方式', e.open_execution_style),
        metric('Maker超时', `${e.maker_order_ttl_ms}ms`),
        metric('Maker并发', `${e.pending_maker_orders}/${e.max_concurrent_maker_orders}`),
        metric('挂价偏移', `${e.maker_price_offset_ticks} tick`),
        metric('撤单加价', `每${e.maker_aggressive_after_cancels}次 +${e.maker_aggressive_step_ticks}tick / max ${e.maker_aggressive_max_ticks}`),
        metric('撤单冷却', `每${e.maker_cooldown_after_cancels}次 / ${e.maker_cooldown_ms}ms`),
        metric('最多仓位', `${s.open_bundles}/${e.max_open_bundles}`),
        metric('私有流', s.private_stream_health.length),
        metric('仓位净风险', num(s.position_summary.net_unhedged_qty || 0)),
        metric('交易所', e.enabled_exchanges.join(', ')),
        metric('更新时间', time(s.updated_at)),
      ].join('');
    }

    function metric(label, value, klass='') {
      return `<div class="metric"><div class="label">${esc(label)}</div><div class="value ${klass}">${esc(value)}</div></div>`;
    }

    function rows(id, items, cols) {
      const head = `<thead><tr>${cols.map(([h]) => `<th>${esc(h)}</th>`).join('')}</tr></thead>`;
      const body = `<tbody>${items.map(item => `<tr>${cols.map(([, f]) => `<td>${renderCell(f(item))}</td>`).join('')}</tr>`).join('')}</tbody>`;
      document.getElementById(id).innerHTML = head + body;
    }

    function renderCell(value) {
      if (value && value.__html) return value.__html;
      return esc(value == null ? '' : value);
    }
    function cls(value, klass) { return { __html: `<span class="${klass}">${esc(value)}</span>` }; }
    function text(id, value) { document.getElementById(id).textContent = value; }
    function sym(value) {
      if (!value) return '';
      if (typeof value === 'string') return value;
      return `${value.base || ''}/${value.quote || ''}`.replace(/^\/|\/$/g, '');
    }
    function num(value) { return value == null ? '' : fmt.format(Number(value)); }
    function pct(value) { return value == null ? '' : `${fmt.format(Number(value) * 100)}%`; }
    function time(value) {
      if (!value) return '';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return String(value);
      return `${beijingTime.format(date)} 北京时间`;
    }
    function orderOwner(id) {
      const v = (id || '').replace(/^t-/, '').toLowerCase();
      return v.startsWith('crossarb-') ? 'crossarb' : 'external';
    }
    function orderMode(id) {
      const v = (id || '').replace(/^t-/, '').toLowerCase();
      if (v.startsWith('crossarb-live-small-')) return 'live-small';
      if (v.startsWith('crossarb-live-scaled-')) return 'live-scaled';
      if (v.startsWith('crossarb-shadow-')) return 'shadow';
      if (v.startsWith('crossarb-simulation-')) return 'simulation';
      if (v.startsWith('crossarb-observe-')) return 'observe';
      return 'external';
    }
    function esc(value) {
      return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    load();
    setInterval(load, 3000);
  </script>
</body>
</html>"#;

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
