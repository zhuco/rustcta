#![allow(clippy::all)]
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "cross_arb_server/ws.rs"]
mod cross_arb_server_ws;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::exchanges::registry as exchange_registry;
use rustcta::execution::EngineDecision;
use rustcta::execution::{
    is_crossarb_client_order_id, BundleLeg, CancelCommand, ExecutionEngine, ExecutionRouter,
    FillEvent, FillLiquidity, OrderAck, OrderCommand, OrderIntent, OrderSide, OrderType,
    PositionMode, PositionSide, PrivateEvent, TimeInForce, TradeFeeSnapshot, TradingAdapter,
};
use rustcta::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta,
    MarketDataAdapter, MarketFundingSnapshot, MarketStateCache, MarketSymbolSnapshot, RoundingMode,
};
use rustcta::strategies::cross_exchange_arbitrage::TradingMode;
use rustcta::strategies::cross_exchange_arbitrage::{
    build_cross_arb_live_runtime_parts, exchange_route_status, live_close_candidate_for_bundle,
    live_close_metrics_for_bundle, live_enabled_exchanges, order_ack_is_live, ArbSignal,
    ArbSignalAction, BundleCloseMetricReadModel, CrossArbExecutionCoordinator, CrossArbRuntime,
    CrossExchangeArbitrageConfig, ExchangeFeeRates, HedgeRepairTaskReadModel, MarketSnapshot,
    OpenExecutionStyle, PrivateRestAuditPlan, PrivateRuntimeSync, RejectReason, RiskEventReadModel,
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
    #[arg(
        long,
        help = "Stop the run loop after this many open maker submissions are accepted"
    )]
    max_open_submissions: Option<usize>,
    #[arg(
        long,
        default_value_t = 30_000,
        help = "After max open submissions is reached, keep managing maker orders until pending makers settle or this timeout elapses"
    )]
    max_open_submission_settle_ms: u64,
    #[arg(
        long,
        help = "Stop the run loop if no open maker submission is accepted within this many milliseconds"
    )]
    max_open_wait_ms: Option<u64>,
    #[arg(
        long,
        default_value_t = false,
        help = "Required for non-dry-run --execute --run because it can place live orders"
    )]
    confirm_live_order: bool,
    #[arg(
        long,
        help = "Append compact JSONL live run summary and final audit reports to this file"
    )]
    run_report_path: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Dry-run only: after a maker-taker open is planned, synthesize a maker fill and execute the taker hedge path"
    )]
    dry_run_simulate_maker_fill: bool,
}

#[derive(Debug, Serialize)]
struct LiveBootstrapReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    execute_requested: bool,
    run_requested: bool,
    dry_run: bool,
    live_order_confirmed: bool,
    max_open_submissions: Option<usize>,
    max_open_wait_ms: Option<u64>,
    run_report_path: Option<String>,
    live_ready: bool,
    blocking_reasons: Vec<String>,
    enabled_exchanges: Vec<String>,
    symbols: Vec<String>,
    instruments_loaded: usize,
    instrument_precision_coverage: InstrumentPrecisionCoverageSummary,
    router_adapters: Vec<String>,
    private_ws_specs: Vec<PrivateWsSpecSummary>,
    binance_private_ws_specs: usize,
    account_preparation: Vec<AccountPrepSummary>,
    rest_audits: Vec<RestAuditSummary>,
    account_fee_overrides: Vec<AccountFeeOverrideSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct InstrumentPrecisionCoverageSummary {
    expected_exchange_symbol_pairs: usize,
    covered_exchange_symbol_pairs: usize,
    missing_exchange_symbol_pairs: usize,
    invalid_precision_pairs: usize,
    min_common_exchange_symbols: usize,
    symbols_with_insufficient_precision_coverage: usize,
    missing_pair_samples: Vec<String>,
    invalid_precision_pair_samples: Vec<String>,
    insufficient_symbol_samples: Vec<String>,
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

#[derive(Debug, Serialize)]
struct FinalRestAuditReport {
    generated_at: DateTime<Utc>,
    report_type: &'static str,
    dry_run: bool,
    run_exit: LiveRunExit,
    rest_audits: Vec<RestAuditSummary>,
    blocking_reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LiveRunSummaryReport {
    generated_at: DateTime<Utc>,
    report_type: &'static str,
    dry_run: bool,
    run_exit: LiveRunExit,
}

#[derive(Debug, Clone, Serialize)]
struct LiveRunExit {
    reason: LiveRunExitReason,
    submitted_open_bundles: usize,
    hedged_open_bundles: usize,
    hedge_submitted_orders: usize,
    dry_run_simulated_maker_fills: usize,
    pending_makers: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum LiveRunExitReason {
    MaxOpenSubmissionsSettled,
    DryRunSubmissionSettleTimeout,
    MaxOpenWaitElapsed,
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

#[derive(Debug, Clone)]
struct LiveRunOptions {
    max_open_submissions: Option<usize>,
    max_open_submission_settle_ms: u64,
    max_open_wait_ms: Option<u64>,
    run_report_path: Option<PathBuf>,
    dry_run_simulate_maker_fill: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct OpenExecutionStats {
    submitted_open_bundles: usize,
    hedged_open_bundles: usize,
    hedge_submitted_orders: usize,
    dry_run_simulated_maker_fills: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct DryRunMakerFillSimulationResult {
    simulated_maker_fills: usize,
    hedge_submitted_orders: usize,
}

#[derive(Debug, Clone)]
struct SymbolEvaluationEvent {
    canonical_symbol: CanonicalSymbol,
    updated_book_usable: bool,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolEvaluationAdmission {
    Accepted,
    Debounced,
    InFlight,
}

#[derive(Debug, Clone)]
struct SymbolEvaluationGuard {
    debounce_ms: i64,
    last_started_at: HashMap<CanonicalSymbol, DateTime<Utc>>,
    in_flight: HashSet<CanonicalSymbol>,
}

impl SymbolEvaluationGuard {
    fn new(stale_quote_ms: i64) -> Self {
        Self {
            debounce_ms: (stale_quote_ms / 10).clamp(25, 250),
            last_started_at: HashMap::new(),
            in_flight: HashSet::new(),
        }
    }

    fn try_start(
        &mut self,
        symbol: &CanonicalSymbol,
        now: DateTime<Utc>,
    ) -> SymbolEvaluationAdmission {
        if self.in_flight.contains(symbol) {
            return SymbolEvaluationAdmission::InFlight;
        }
        if self.last_started_at.get(symbol).is_some_and(|last| {
            now.signed_duration_since(*last).num_milliseconds() < self.debounce_ms
        }) {
            return SymbolEvaluationAdmission::Debounced;
        }
        self.last_started_at.insert(symbol.clone(), now);
        self.in_flight.insert(symbol.clone());
        SymbolEvaluationAdmission::Accepted
    }

    fn finish(&mut self, symbol: &CanonicalSymbol) {
        self.in_flight.remove(symbol);
    }
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
        config.trading_mode,
        config.enable_live_trading,
        config.max_live_notional_per_trade,
        config.enabled_symbols.is_empty(),
        config.enabled_exchanges.is_empty(),
        config.execution.dry_run,
        config.execution.open_execution_style,
        config.execution.close_execution_style,
        config.execution.taker_ioc_slippage_limit_pct,
        args.execute,
        args.run,
        args.skip_private_audit,
        args.confirm_live_order,
        args.max_open_submissions,
        args.max_open_wait_ms,
        args.run_report_path.is_some(),
    )?;

    let report = bootstrap_live(
        args.config,
        config.clone(),
        args.execute,
        args.run,
        args.skip_private_audit,
        args.confirm_live_order,
        args.max_open_submissions,
        args.max_open_wait_ms,
        args.run_report_path.clone(),
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
        run_live(
            config,
            args.skip_private_audit,
            LiveRunOptions {
                max_open_submissions: args.max_open_submissions,
                max_open_submission_settle_ms: args.max_open_submission_settle_ms,
                max_open_wait_ms: args.max_open_wait_ms,
                run_report_path: args.run_report_path.clone(),
                dry_run_simulate_maker_fill: args.dry_run_simulate_maker_fill,
            },
        )
        .await?;
    }
    Ok(())
}

fn validate_cross_arb_live_admission(
    mode: rustcta::market::RuntimeMode,
    trading_mode: TradingMode,
    enable_live_trading: bool,
    max_live_notional_per_trade: Option<f64>,
    enabled_symbols_empty: bool,
    enabled_exchanges_empty: bool,
    dry_run: bool,
    open_execution_style: OpenExecutionStyle,
    close_execution_style: OpenExecutionStyle,
    taker_ioc_slippage_limit_pct: f64,
    execute_requested: bool,
    run_requested: bool,
    skip_private_audit: bool,
    confirm_live_order: bool,
    max_open_submissions: Option<usize>,
    max_open_wait_ms: Option<u64>,
    run_report_path_configured: bool,
) -> Result<()> {
    if max_open_submissions.is_some_and(|value| value == 0) {
        return Err(anyhow!("--max-open-submissions must be greater than zero"));
    }
    if max_open_wait_ms.is_some_and(|value| value == 0) {
        return Err(anyhow!("--max-open-wait-ms must be greater than zero"));
    }
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
    if execute_requested || !dry_run {
        if open_execution_style != OpenExecutionStyle::DualTaker {
            return Err(anyhow!(
                "--execute or execution.dry_run=false requires execution.open_execution_style=dual_taker"
            ));
        }
        if close_execution_style != OpenExecutionStyle::DualTaker {
            return Err(anyhow!(
                "--execute or execution.dry_run=false requires execution.close_execution_style=dual_taker"
            ));
        }
        if taker_ioc_slippage_limit_pct <= 0.0 || !taker_ioc_slippage_limit_pct.is_finite() {
            return Err(anyhow!(
                "--execute or execution.dry_run=false requires taker_ioc_slippage_limit_pct > 0"
            ));
        }
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
    if !dry_run {
        if trading_mode != TradingMode::Live {
            return Err(anyhow!(
                "execution.dry_run=false requires trading_mode=live"
            ));
        }
        if !enable_live_trading {
            return Err(anyhow!(
                "execution.dry_run=false requires enable_live_trading=true"
            ));
        }
        if max_live_notional_per_trade
            .filter(|value| value.is_finite() && *value > 0.0)
            .is_none()
        {
            return Err(anyhow!(
                "execution.dry_run=false requires max_live_notional_per_trade > 0"
            ));
        }
        if enabled_symbols_empty {
            return Err(anyhow!(
                "execution.dry_run=false requires explicit enabled_symbols"
            ));
        }
        if enabled_exchanges_empty {
            return Err(anyhow!(
                "execution.dry_run=false requires explicit enabled_exchanges"
            ));
        }
    }
    if execute_requested && run_requested && !dry_run && !confirm_live_order {
        return Err(anyhow!(
            "--execute --run with execution.dry_run=false requires --confirm-live-order"
        ));
    }
    if execute_requested && run_requested && !dry_run && max_open_submissions.is_none() {
        return Err(anyhow!(
            "--execute --run with execution.dry_run=false requires --max-open-submissions"
        ));
    }
    if execute_requested && run_requested && !dry_run && max_open_wait_ms.is_none() {
        return Err(anyhow!(
            "--execute --run with execution.dry_run=false requires --max-open-wait-ms"
        ));
    }
    if execute_requested && run_requested && !dry_run && !run_report_path_configured {
        return Err(anyhow!(
            "--execute --run with execution.dry_run=false requires --run-report-path"
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
    live_order_confirmed: bool,
    max_open_submissions: Option<usize>,
    max_open_wait_ms: Option<u64>,
    run_report_path: Option<PathBuf>,
) -> Result<LiveBootstrapReport> {
    let enabled_exchanges = live_enabled_exchanges(&config);
    let instruments = load_live_instruments(&config, &enabled_exchanges).await?;
    let instrument_precision_coverage =
        summarize_instrument_precision_coverage(&config, &enabled_exchanges, &instruments);
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
    if instrument_precision_coverage.invalid_precision_pairs > 0 {
        blocking_reasons.push(format!(
            "instrument precision metadata invalid for {} configured exchange-symbol pair(s)",
            instrument_precision_coverage.invalid_precision_pairs
        ));
    }
    if instrument_precision_coverage.symbols_with_insufficient_precision_coverage > 0 {
        blocking_reasons.push(format!(
            "{} symbol(s) have precision metadata on fewer than {} exchange(s)",
            instrument_precision_coverage.symbols_with_insufficient_precision_coverage,
            config.market.min_common_exchanges
        ));
    }

    Ok(LiveBootstrapReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        execute_requested,
        run_requested,
        dry_run: config.execution.dry_run,
        live_order_confirmed,
        max_open_submissions,
        max_open_wait_ms,
        run_report_path: run_report_path
            .as_ref()
            .map(|path| path.display().to_string()),
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
        instrument_precision_coverage,
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
    options: LiveRunOptions,
) -> Result<()> {
    let enabled_exchanges = live_enabled_exchanges(&config);
    let instruments = load_live_instruments(&config, &enabled_exchanges).await?;
    validate_instrument_precision_coverage(&config, &enabled_exchanges, &instruments)?;
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

    let run_report_path = options.run_report_path.clone();
    let run_exit = ws_market_execution_loop(
        runtime.clone(),
        execution.clone(),
        close_metrics.clone(),
        config.clone(),
        enabled_exchanges.clone(),
        instruments,
        options,
    )
    .await;
    print_live_run_summary(&config, run_exit.clone(), run_report_path.as_deref())?;
    if !skip_private_audit && !config.execution.dry_run {
        run_final_rest_audit_after_live_loop(
            &private_sync,
            &config,
            &enabled_exchanges,
            run_exit,
            run_report_path.as_deref(),
        )
        .await?;
    }
    Ok(())
}

fn print_live_run_summary(
    config: &CrossExchangeArbitrageConfig,
    run_exit: LiveRunExit,
    run_report_path: Option<&Path>,
) -> Result<()> {
    let report = LiveRunSummaryReport {
        generated_at: Utc::now(),
        report_type: "cross_arb_live_run_summary",
        dry_run: config.execution.dry_run,
        run_exit,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(path) = run_report_path {
        append_jsonl_report(path, &report)?;
    }
    Ok(())
}

async fn run_final_rest_audit_after_live_loop(
    private_sync: &PrivateRuntimeSync,
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
    run_exit: LiveRunExit,
    run_report_path: Option<&Path>,
) -> Result<()> {
    let rest_audits = run_initial_rest_audits(private_sync, config, exchanges).await;
    let mut blocking_reasons = Vec::new();
    for audit in &rest_audits {
        if !audit.ok {
            blocking_reasons.push(format!(
                "{} final private REST audit failed",
                audit.exchange
            ));
        }
        if audit.strategy_open_orders > 0 {
            blocking_reasons.push(format!(
                "{} final audit found {} crossarb strategy open order(s)",
                audit.exchange, audit.strategy_open_orders
            ));
        }
    }

    let report = FinalRestAuditReport {
        generated_at: Utc::now(),
        report_type: "cross_arb_live_final_rest_audit",
        dry_run: config.execution.dry_run,
        run_exit,
        rest_audits,
        blocking_reasons,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(path) = run_report_path {
        append_jsonl_report(path, &report)?;
    }
    if !report.blocking_reasons.is_empty() {
        return Err(anyhow!(
            "final private REST audit is not clean: {}",
            report.blocking_reasons.join("; ")
        ));
    }
    Ok(())
}

fn append_jsonl_report<T: Serialize>(path: &Path, report: &T) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("failed to create run report directory {}", parent.display())
        })?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open run report {}", path.display()))?;
    serde_json::to_writer(&mut file, report)
        .with_context(|| format!("failed to serialize run report {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to append newline to run report {}", path.display()))?;
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
    options: LiveRunOptions,
) -> LiveRunExit {
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
        for instrument in &instruments {
            cache.upsert_instrument(instrument.clone());
            runtime_guard.record_instrument(instrument.clone(), now);
        }
    }

    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(16_384);
    let (symbol_eval_tx, mut symbol_eval_rx) = tokio::sync::mpsc::channel(16_384);
    let available_symbols_by_exchange = instruments
        .iter()
        .filter(|instrument| instrument.has_valid_lot_rules())
        .fold(
            HashMap::<ExchangeId, Vec<ExchangeSymbol>>::new(),
            |mut acc, instrument| {
                acc.entry(instrument.exchange.clone())
                    .or_default()
                    .push(instrument.exchange_symbol.clone());
                acc
            },
        );
    for (exchange, adapter) in &adapters {
        let symbols = available_symbols_by_exchange
            .get(exchange)
            .cloned()
            .unwrap_or_else(|| {
                config
                    .universe
                    .symbols
                    .iter()
                    .map(|symbol| exchange_symbol_for(exchange, symbol))
                    .collect::<Vec<_>>()
            });
        if symbols.is_empty() {
            log::warn!(
                "cross_arb_live public ws skipped exchange={} because no configured symbols have valid instrument precision metadata",
                exchange
            );
            continue;
        }
        let batch_size = config.ws_batch_size_for(exchange, 80);
        let profile = config
            .market
            .public_book_trigger_profile_kind()
            .unwrap_or(rustcta::market::PublicBookProfileKind::FastestL1);
        tokio::spawn(cross_arb_server_ws::run_public_ws_adapter(
            adapter.clone(),
            symbols,
            cross_arb_server_ws::PublicWsConfig {
                max_symbols_per_connection: batch_size,
                profile,
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
                    let now = Utc::now();
                    let (canonical_symbol, updated_book_usable) = {
                        let mut cache = cache_for_ws.write().await;
                        let updated = cache.upsert_orderbook_at(book, now);
                        (updated.canonical_symbol.clone(), updated.is_usable())
                    };
                    match symbol_eval_tx.try_send(SymbolEvaluationEvent {
                        canonical_symbol,
                        updated_book_usable,
                        received_at: now,
                    }) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                            log::warn!(
                                "cross_arb_live symbol evaluation queue full; dropped book event symbol={}",
                                event.canonical_symbol
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    }
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
    let run_started_at = Utc::now();
    let mut open_submission_count = 0usize;
    let mut hedged_open_bundle_count = 0usize;
    let mut hedge_submitted_order_count = 0usize;
    let mut dry_run_simulated_maker_fill_count = 0usize;
    let mut settling_after_open_limit_since: Option<DateTime<Utc>> = None;
    let mut last_open_limit_settle_timeout_log: Option<DateTime<Utc>> = None;
    let mut symbol_eval_guard = SymbolEvaluationGuard::new(config.market.stale_quote_ms);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
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
                    if let Some(settling_since) = settling_after_open_limit_since {
                        if execution_guard.pending_maker_count() == 0 {
                            log::info!(
                                "cross_arb_live max open submission limit settled submitted={} limit={}",
                                open_submission_count,
                                options.max_open_submissions.unwrap_or_default()
                            );
                            return LiveRunExit {
                                reason: LiveRunExitReason::MaxOpenSubmissionsSettled,
                                submitted_open_bundles: open_submission_count,
                                hedged_open_bundles: hedged_open_bundle_count,
                                hedge_submitted_orders: hedge_submitted_order_count,
                                dry_run_simulated_maker_fills: dry_run_simulated_maker_fill_count,
                                pending_makers: 0,
                                elapsed_ms: now
                                    .signed_duration_since(run_started_at)
                                    .num_milliseconds()
                                    .max(0) as u64,
                            };
                        }
                        let settle_elapsed_ms = now
                            .signed_duration_since(settling_since)
                            .num_milliseconds()
                            .max(0) as u64;
                        if settle_elapsed_ms >= options.max_open_submission_settle_ms.max(1) {
                            if config.execution.dry_run {
                                log::warn!(
                                    "cross_arb_live max open submission settle timeout submitted={} limit={} pending_makers={} elapsed_ms={} timeout_ms={} dry_run=true",
                                    open_submission_count,
                                    options.max_open_submissions.unwrap_or_default(),
                                    execution_guard.pending_maker_count(),
                                    settle_elapsed_ms,
                                    options.max_open_submission_settle_ms.max(1)
                                );
                                return LiveRunExit {
                                    reason: LiveRunExitReason::DryRunSubmissionSettleTimeout,
                                    submitted_open_bundles: open_submission_count,
                                    hedged_open_bundles: hedged_open_bundle_count,
                                    hedge_submitted_orders: hedge_submitted_order_count,
                                    dry_run_simulated_maker_fills: dry_run_simulated_maker_fill_count,
                                    pending_makers: execution_guard.pending_maker_count(),
                                    elapsed_ms: now
                                        .signed_duration_since(run_started_at)
                                        .num_milliseconds()
                                        .max(0) as u64,
                                };
                            }
                            let should_log_timeout = last_open_limit_settle_timeout_log
                                .map(|last| now.signed_duration_since(last).num_seconds() >= 5)
                                .unwrap_or(true);
                            if should_log_timeout {
                                log::error!(
                                    "cross_arb_live max open submission settle timeout submitted={} limit={} pending_makers={} elapsed_ms={} timeout_ms={} dry_run=false; continuing until pending makers settle",
                                    open_submission_count,
                                    options.max_open_submissions.unwrap_or_default(),
                                    execution_guard.pending_maker_count(),
                                    settle_elapsed_ms,
                                    options.max_open_submission_settle_ms.max(1)
                                );
                                last_open_limit_settle_timeout_log = Some(now);
                            }
                        }
                    } else if open_submission_count == 0 {
                        if let Some(max_open_wait_ms) = options.max_open_wait_ms {
                            let open_wait_elapsed_ms = now
                                .signed_duration_since(run_started_at)
                                .num_milliseconds()
                                .max(0) as u64;
                            if open_wait_elapsed_ms >= max_open_wait_ms.max(1) {
                                log::warn!(
                                    "cross_arb_live max open wait elapsed without submission elapsed_ms={} limit_ms={} dry_run={}",
                                    open_wait_elapsed_ms,
                                    max_open_wait_ms.max(1),
                                    config.execution.dry_run
                                );
                                return LiveRunExit {
                                    reason: LiveRunExitReason::MaxOpenWaitElapsed,
                                    submitted_open_bundles: open_submission_count,
                                    hedged_open_bundles: hedged_open_bundle_count,
                                    hedge_submitted_orders: hedge_submitted_order_count,
                                    dry_run_simulated_maker_fills: dry_run_simulated_maker_fill_count,
                                    pending_makers: execution_guard.pending_maker_count(),
                                    elapsed_ms: open_wait_elapsed_ms,
                                };
                            }
                        }
                    }
                }
            }
            event = symbol_eval_rx.recv() => {
                let Some(event) = event else {
                    continue;
                };
                if !event.updated_book_usable || settling_after_open_limit_since.is_some() {
                    continue;
                }
                if options
                    .max_open_submissions
                    .is_some_and(|limit| open_submission_count >= limit)
                {
                    continue;
                }
                let now = Utc::now();
                if !config.universe.symbols.contains(&event.canonical_symbol) {
                    continue;
                }
                match symbol_eval_guard.try_start(&event.canonical_symbol, event.received_at) {
                    SymbolEvaluationAdmission::Accepted => {}
                    SymbolEvaluationAdmission::Debounced | SymbolEvaluationAdmission::InFlight => {
                        continue;
                    }
                }
                {
                    let snapshots = snapshots_for_market_cache_symbol(
                        &market_cache,
                        &config,
                        &event.canonical_symbol,
                        &funding_cache,
                        now,
                    )
                    .await;
                    if snapshots.len() >= config.market.min_common_exchanges {
                        let mut snapshots_by_symbol = HashMap::new();
                        snapshots_by_symbol.insert(event.canonical_symbol.clone(), snapshots);
                        let mut runtime_guard = runtime.write().await;
                        let mut execution_guard = execution.write().await;
                        let stats = execute_live_open_candidates_for_symbols(
                            &mut runtime_guard,
                            &mut execution_guard,
                            &snapshots_by_symbol,
                            std::slice::from_ref(&event.canonical_symbol),
                            now,
                            options.dry_run_simulate_maker_fill,
                        )
                        .await;
                        open_submission_count += stats.submitted_open_bundles;
                        hedged_open_bundle_count += stats.hedged_open_bundles;
                        hedge_submitted_order_count += stats.hedge_submitted_orders;
                        dry_run_simulated_maker_fill_count += stats.dry_run_simulated_maker_fills;
                        if stats.submitted_open_bundles > 0
                            && options
                                .max_open_submissions
                                .is_some_and(|limit| open_submission_count >= limit)
                        {
                            settling_after_open_limit_since = Some(now);
                            log::info!(
                                "cross_arb_live max open submission limit reached submitted={} limit={} pending_makers={}; waiting for settlement before exit",
                                open_submission_count,
                                options.max_open_submissions.unwrap_or_default(),
                                execution_guard.pending_maker_count()
                            );
                        }
                    }
                }
                symbol_eval_guard.finish(&event.canonical_symbol);
            }
        }
    }
}

async fn execute_live_open_candidates_for_symbols(
    runtime: &mut CrossArbRuntime,
    execution: &mut CrossArbExecutionCoordinator,
    snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    symbols: &[CanonicalSymbol],
    now: DateTime<Utc>,
    dry_run_simulate_maker_fill: bool,
) -> OpenExecutionStats {
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
        return OpenExecutionStats::default();
    }

    let mut candidates = Vec::new();
    let mut blocked_private_stream_exchanges: HashSet<ExchangeId> = HashSet::new();
    for symbol in symbols.iter().cloned() {
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
                && !runtime.state.config.execution.dry_run
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

    let mut stats = OpenExecutionStats::default();
    for (_, signal) in candidates {
        if stats.submitted_open_bundles >= max_to_submit {
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
        let pending_before = execution.pending_maker_count();
        match execution.execute_open_signal(runtime, &signal).await {
            Ok(Some(decision)) => {
                if decision.blocked_reason.is_none() {
                    let pending_after = execution.pending_maker_count();
                    let dry_run = runtime.state.config.execution.dry_run;
                    let mut submitted_order_count = if dry_run {
                        decision
                            .submitted_orders
                            .len()
                            .max(pending_after.saturating_sub(pending_before))
                    } else {
                        let live_ack_count = decision
                            .submitted_orders
                            .iter()
                            .filter(|ack| order_ack_is_live(ack))
                            .count();
                        live_ack_count.max(pending_after.saturating_sub(pending_before))
                    };
                    if submitted_order_count == 0 && !runtime.state.config.execution.dry_run {
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
                    if submitted_order_count == 0 {
                        continue;
                    }
                    let mut simulation_result = DryRunMakerFillSimulationResult::default();
                    if dry_run_simulate_maker_fill
                        && dry_run
                        && runtime.state.config.execution.open_execution_style
                            == OpenExecutionStyle::MakerTaker
                    {
                        simulation_result = simulate_dry_run_maker_fill_and_hedge(
                            runtime, execution, &decision, now,
                        )
                        .await;
                        submitted_order_count += simulation_result.hedge_submitted_orders;
                    }
                    stats.submitted_open_bundles += 1;
                    stats.dry_run_simulated_maker_fills += simulation_result.simulated_maker_fills;
                    stats.hedge_submitted_orders += simulation_result.hedge_submitted_orders;
                    if simulation_result.hedge_submitted_orders > 0 {
                        stats.hedged_open_bundles += 1;
                    }
                    let execution_style =
                        format!("{:?}", runtime.state.config.execution.open_execution_style);
                    log::info!(
                        "cross_arb_live open submitted symbol={} style={} submitted_orders={} pending_makers={}/{} dry_run={}",
                        signal_canonical_symbol
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "unknown".to_string()),
                        execution_style,
                        submitted_order_count,
                        execution.pending_maker_count(),
                        configured_limit,
                        dry_run
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
    stats
}

async fn simulate_dry_run_maker_fill_and_hedge(
    runtime: &mut CrossArbRuntime,
    execution: &mut CrossArbExecutionCoordinator,
    decision: &EngineDecision,
    now: DateTime<Utc>,
) -> DryRunMakerFillSimulationResult {
    let Some(maker_command) = decision
        .plan
        .commands
        .iter()
        .find(|command| command.post_only)
        .cloned()
    else {
        return DryRunMakerFillSimulationResult::default();
    };
    let fill = FillEvent {
        exchange: maker_command.exchange.clone(),
        canonical_symbol: maker_command.canonical_symbol.clone(),
        exchange_symbol: maker_command.exchange_symbol.clone(),
        trade_id: format!(
            "dry-run-maker-fill-{}-{}",
            maker_command.bundle_id,
            now.timestamp_millis()
        ),
        client_order_id: Some(maker_command.client_order_id.clone()),
        exchange_order_id: decision
            .submitted_orders
            .iter()
            .find(|ack| ack.client_order_id == maker_command.client_order_id)
            .and_then(|ack| ack.exchange_order_id.clone()),
        side: maker_command.side,
        position_side: maker_command.position_side,
        liquidity: FillLiquidity::Maker,
        price: maker_command.price.unwrap_or_default(),
        quantity: maker_command.quantity,
        quote_quantity: maker_command.price.unwrap_or_default() * maker_command.quantity,
        fee: Some(0.0),
        fee_asset: Some(maker_command.canonical_symbol.quote().to_string()),
        fee_rate: Some(0.0),
        realized_pnl: None,
        reduce_only: Some(maker_command.reduce_only),
        filled_at: now,
        received_at: now,
    };
    let event = PrivateEvent::fill(fill, now);
    runtime.on_private_event(event.clone());
    let Some(hedge_fill) = execution.hedge_candidate_from_private_event(runtime, &event) else {
        runtime.state.risk_events.push(RiskEventReadModel {
            event_id: format!("dry-run-maker-fill-no-hedge-{}", now.timestamp_millis()),
            canonical_symbol: Some(maker_command.canonical_symbol),
            exchange: Some(maker_command.exchange),
            reason: RejectReason::RouteUnhealthy,
            message: "dry-run simulated maker fill did not produce hedge candidate".to_string(),
            created_at: now,
        });
        return DryRunMakerFillSimulationResult {
            simulated_maker_fills: 1,
            hedge_submitted_orders: 0,
        };
    };
    let mut hedge_decision = execution.execute_hedge_for_maker_fill(hedge_fill).await;
    if runtime.state.config.execution.dry_run
        && hedge_decision.blocked_reason.is_none()
        && !hedge_decision.requires_reconcile
        && hedge_decision.submitted_orders.is_empty()
    {
        hedge_decision.submitted_orders.extend(
            hedge_decision
                .plan
                .commands
                .iter()
                .map(|command| OrderAck::dry_run(command, hedge_decision.plan.created_at)),
        );
    }
    let hedge_submitted_count = hedge_decision.submitted_orders.len();
    if hedge_decision.blocked_reason.is_some()
        || hedge_decision.requires_reconcile
        || hedge_submitted_count == 0
    {
        if let Some(command) = hedge_decision.plan.commands.first() {
            runtime.state.record_hedge_repair_required(
                command,
                hedge_decision.blocked_reason.clone().unwrap_or_else(|| {
                    "dry-run simulated hedge order was not submitted".to_string()
                }),
                hedge_decision.plan.created_at,
            );
            runtime.persist_hedge_repair_task(
                &rustcta::strategies::cross_exchange_arbitrage::hedge_repair_task_id(
                    &command.bundle_id,
                    command.reduce_only,
                ),
                hedge_decision.plan.created_at,
            );
            runtime.persist_hedge_record(&command.bundle_id, hedge_decision.plan.created_at);
        }
        return DryRunMakerFillSimulationResult {
            simulated_maker_fills: 1,
            hedge_submitted_orders: 0,
        };
    }
    if let Some(command) = hedge_decision.plan.commands.first() {
        runtime
            .state
            .record_hedge_order_submitted(command, hedge_decision.plan.created_at);
        runtime.persist_hedge_record(&command.bundle_id, hedge_decision.plan.created_at);
        log::info!(
            "cross_arb_live dry-run simulated maker fill hedged bundle={} exchange={} qty={} submitted_orders={}",
            command.bundle_id,
            command.exchange,
            command.quantity,
            hedge_submitted_count
        );
    }
    DryRunMakerFillSimulationResult {
        simulated_maker_fills: 1,
        hedge_submitted_orders: hedge_submitted_count,
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
    for symbol in &config.universe.symbols {
        let snapshots =
            snapshots_for_market_cache_symbol(market_cache, config, symbol, funding, now).await;
        if !snapshots.is_empty() {
            snapshots_by_symbol.insert(symbol.clone(), snapshots);
        }
    }
    snapshots_by_symbol
}

async fn snapshots_for_market_cache_symbol(
    market_cache: &Arc<RwLock<MarketStateCache>>,
    config: &CrossExchangeArbitrageConfig,
    symbol: &CanonicalSymbol,
    funding: &HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
    now: DateTime<Utc>,
) -> Vec<MarketSnapshot> {
    let cache = market_cache.read().await;
    market_symbol_snapshots_to_runtime_snapshots(
        &cache.snapshots_for_symbol(symbol),
        config,
        funding,
        now,
    )
}

fn market_symbol_snapshots_to_runtime_snapshots(
    snapshots: &[MarketSymbolSnapshot],
    config: &CrossExchangeArbitrageConfig,
    funding: &HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
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
            let funding = funding
                .get(&(snapshot.exchange.clone(), snapshot.canonical_symbol.clone()))
                .cloned()
                .or_else(|| snapshot.funding.clone());
            Some(MarketSnapshot {
                book,
                instrument: snapshot.instrument.clone(),
                route_status: exchange_route_status(config, &snapshot.exchange),
                funding_rate: funding
                    .as_ref()
                    .map(|snapshot| snapshot.funding_rate)
                    .unwrap_or(0.0),
                next_funding_time: funding.and_then(|snapshot| snapshot.next_funding_time),
                trigger_book_profile: config.market.public_book_trigger_profile_kind(),
                validation_book_profile: config.market.public_book_validation_profile_kind(),
            })
        })
        .collect()
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

fn validate_instrument_precision_coverage(
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
    instruments: &[InstrumentMeta],
) -> Result<()> {
    let coverage = summarize_instrument_precision_coverage(config, exchanges, instruments);
    if coverage.invalid_precision_pairs > 0
        || coverage.symbols_with_insufficient_precision_coverage > 0
    {
        return Err(anyhow!(
            "instrument precision coverage is incomplete: expected_pairs={} covered_pairs={} missing_pairs={} invalid_precision_pairs={} insufficient_symbols={} missing_samples=[{}] invalid_samples=[{}] insufficient_symbol_samples=[{}]",
            coverage.expected_exchange_symbol_pairs,
            coverage.covered_exchange_symbol_pairs,
            coverage.missing_exchange_symbol_pairs,
            coverage.invalid_precision_pairs,
            coverage.symbols_with_insufficient_precision_coverage,
            coverage.missing_pair_samples.join(", "),
            coverage.invalid_precision_pair_samples.join(", "),
            coverage.insufficient_symbol_samples.join(", "),
        ));
    }
    Ok(())
}

fn summarize_instrument_precision_coverage(
    config: &CrossExchangeArbitrageConfig,
    exchanges: &[ExchangeId],
    instruments: &[InstrumentMeta],
) -> InstrumentPrecisionCoverageSummary {
    const SAMPLE_LIMIT: usize = 12;

    let by_pair = instruments
        .iter()
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
    let mut covered_exchange_symbol_pairs = 0;
    let mut missing_pair_samples = Vec::new();
    let mut invalid_precision_pair_samples = Vec::new();
    let mut valid_precision_counts_by_symbol = HashMap::<CanonicalSymbol, usize>::new();
    let mut missing_exchange_symbol_pairs = 0;
    let mut invalid_precision_pairs = 0;

    for symbol in &config.universe.symbols {
        for exchange in exchanges {
            match by_pair.get(&(exchange.clone(), symbol.clone())) {
                Some(instrument) => {
                    covered_exchange_symbol_pairs += 1;
                    if instrument.has_valid_lot_rules() {
                        *valid_precision_counts_by_symbol
                            .entry(symbol.clone())
                            .or_default() += 1;
                    } else {
                        invalid_precision_pairs += 1;
                        if invalid_precision_pair_samples.len() < SAMPLE_LIMIT {
                            invalid_precision_pair_samples.push(format!(
                                "{}:{} price_tick={} quantity_step={} contract_size={} min_qty={} min_notional={}",
                                exchange,
                                symbol,
                                instrument.price_tick,
                                instrument.quantity_step,
                                instrument.contract_size,
                                instrument.min_qty,
                                instrument.min_notional
                            ));
                        }
                    }
                }
                None => {
                    missing_exchange_symbol_pairs += 1;
                    if missing_pair_samples.len() < SAMPLE_LIMIT {
                        missing_pair_samples.push(format!("{}:{}", exchange, symbol));
                    }
                }
            }
        }
    }

    let insufficient_symbol_samples = config
        .universe
        .symbols
        .iter()
        .filter_map(|symbol| {
            let valid_count = valid_precision_counts_by_symbol
                .get(symbol)
                .copied()
                .unwrap_or_default();
            (valid_count < config.market.min_common_exchanges).then(|| {
                format!(
                    "{}:{}/{}",
                    symbol, valid_count, config.market.min_common_exchanges
                )
            })
        })
        .take(SAMPLE_LIMIT)
        .collect::<Vec<_>>();

    InstrumentPrecisionCoverageSummary {
        expected_exchange_symbol_pairs: exchanges.len() * config.universe.symbols.len(),
        covered_exchange_symbol_pairs,
        missing_exchange_symbol_pairs,
        invalid_precision_pairs,
        min_common_exchange_symbols: config.market.min_common_exchanges,
        symbols_with_insufficient_precision_coverage: config
            .universe
            .symbols
            .iter()
            .filter(|symbol| {
                valid_precision_counts_by_symbol
                    .get(*symbol)
                    .copied()
                    .unwrap_or_default()
                    < config.market.min_common_exchanges
            })
            .count(),
        missing_pair_samples,
        invalid_precision_pair_samples,
        insufficient_symbol_samples,
    }
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

    #[derive(Debug, Clone)]
    struct AdmissionCase {
        mode: RuntimeMode,
        trading_mode: TradingMode,
        enable_live_trading: bool,
        max_live_notional_per_trade: Option<f64>,
        enabled_symbols_empty: bool,
        enabled_exchanges_empty: bool,
        dry_run: bool,
        open_execution_style: OpenExecutionStyle,
        close_execution_style: OpenExecutionStyle,
        taker_ioc_slippage_limit_pct: f64,
        execute_requested: bool,
        run_requested: bool,
        skip_private_audit: bool,
        confirm_live_order: bool,
        max_open_submissions: Option<usize>,
        max_open_wait_ms: Option<u64>,
        run_report_path_configured: bool,
    }

    impl Default for AdmissionCase {
        fn default() -> Self {
            Self {
                mode: RuntimeMode::LiveSmall,
                trading_mode: TradingMode::Live,
                enable_live_trading: true,
                max_live_notional_per_trade: Some(10.0),
                enabled_symbols_empty: false,
                enabled_exchanges_empty: false,
                dry_run: true,
                open_execution_style: OpenExecutionStyle::DualTaker,
                close_execution_style: OpenExecutionStyle::DualTaker,
                taker_ioc_slippage_limit_pct: 0.003,
                execute_requested: false,
                run_requested: false,
                skip_private_audit: false,
                confirm_live_order: false,
                max_open_submissions: None,
                max_open_wait_ms: None,
                run_report_path_configured: false,
            }
        }
    }

    fn validate_admission(case: AdmissionCase) -> Result<()> {
        validate_cross_arb_live_admission(
            case.mode,
            case.trading_mode,
            case.enable_live_trading,
            case.max_live_notional_per_trade,
            case.enabled_symbols_empty,
            case.enabled_exchanges_empty,
            case.dry_run,
            case.open_execution_style,
            case.close_execution_style,
            case.taker_ioc_slippage_limit_pct,
            case.execute_requested,
            case.run_requested,
            case.skip_private_audit,
            case.confirm_live_order,
            case.max_open_submissions,
            case.max_open_wait_ms,
            case.run_report_path_configured,
        )
    }

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
        assert_eq!(config.market.public_book_trigger_profile, "fastest_l1");
        assert_eq!(
            config.market.public_book_validation_profile,
            "fastest_depth"
        );
        assert_eq!(
            config.execution.open_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert_eq!(
            config.execution.close_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert!(live_enabled_exchanges(&config).is_empty());
        assert!(config
            .exchanges
            .values()
            .all(|runtime| runtime.is_disabled() && !runtime.private_ws_enabled));
    }

    #[test]
    fn live_runner_should_admit_non_live_public_discovery_dry_run() {
        validate_admission(AdmissionCase {
            mode: RuntimeMode::Simulation,
            trading_mode: TradingMode::Paper,
            dry_run: true,
            execute_requested: false,
            run_requested: true,
            skip_private_audit: true,
            ..AdmissionCase::default()
        })
        .unwrap();
    }

    #[test]
    fn live_runner_should_reject_non_live_execute_request() {
        let error = validate_admission(AdmissionCase {
            mode: RuntimeMode::Simulation,
            trading_mode: TradingMode::Paper,
            dry_run: true,
            execute_requested: true,
            skip_private_audit: true,
            confirm_live_order: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--execute requires"));
    }

    #[test]
    fn live_runner_should_require_skip_private_audit_for_non_live_discovery() {
        let error = validate_admission(AdmissionCase {
            mode: RuntimeMode::Simulation,
            trading_mode: TradingMode::Paper,
            dry_run: true,
            execute_requested: false,
            run_requested: true,
            skip_private_audit: false,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--skip-private-audit"));
    }

    #[test]
    fn live_runner_should_reject_execute_with_live_dry_run_config() {
        let error = validate_admission(AdmissionCase {
            dry_run: true,
            execute_requested: true,
            run_requested: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("dry_run=true"));
    }

    #[test]
    fn cross_arb_live_should_reject_non_dual_taker_open_for_live_execute() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            open_execution_style: OpenExecutionStyle::MakerTaker,
            close_execution_style: OpenExecutionStyle::DualTaker,
            execute_requested: true,
            confirm_live_order: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("execution.open_execution_style=dual_taker"));
    }

    #[test]
    fn cross_arb_live_should_reject_non_dual_taker_close_for_live_execute() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            open_execution_style: OpenExecutionStyle::DualTaker,
            close_execution_style: OpenExecutionStyle::MakerTaker,
            execute_requested: true,
            confirm_live_order: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("execution.close_execution_style=dual_taker"));
    }

    #[test]
    fn cross_arb_live_should_reject_missing_taker_slippage_for_live_execute() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            taker_ioc_slippage_limit_pct: 0.0,
            execute_requested: true,
            confirm_live_order: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("taker_ioc_slippage_limit_pct > 0"));
    }

    #[test]
    fn cross_arb_live_should_admit_dual_taker_live_execute_without_run() {
        validate_admission(AdmissionCase {
            dry_run: false,
            open_execution_style: OpenExecutionStyle::DualTaker,
            close_execution_style: OpenExecutionStyle::DualTaker,
            execute_requested: true,
            run_requested: false,
            confirm_live_order: true,
            ..AdmissionCase::default()
        })
        .unwrap();
    }

    #[test]
    fn live_runner_should_require_confirmation_for_live_execute_run() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            execute_requested: true,
            run_requested: true,
            confirm_live_order: false,
            max_open_submissions: Some(1),
            max_open_wait_ms: Some(60_000),
            run_report_path_configured: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--confirm-live-order"));
        validate_admission(AdmissionCase {
            dry_run: false,
            execute_requested: true,
            run_requested: true,
            confirm_live_order: true,
            max_open_submissions: Some(1),
            max_open_wait_ms: Some(60_000),
            run_report_path_configured: true,
            ..AdmissionCase::default()
        })
        .unwrap();
    }

    #[test]
    fn live_runner_should_require_live_config_for_non_dry_run() {
        let error = validate_admission(AdmissionCase {
            trading_mode: TradingMode::Paper,
            dry_run: false,
            execute_requested: true,
            run_requested: false,
            confirm_live_order: true,
            max_open_submissions: Some(1),
            max_open_wait_ms: Some(60_000),
            run_report_path_configured: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("trading_mode=live"));
    }

    #[test]
    fn live_runner_should_require_submission_limit_for_live_execute_run() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            execute_requested: true,
            run_requested: true,
            confirm_live_order: true,
            max_open_submissions: None,
            max_open_wait_ms: Some(60_000),
            run_report_path_configured: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--max-open-submissions"));
    }

    #[test]
    fn live_runner_should_require_open_wait_limit_for_live_execute_run() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            execute_requested: true,
            run_requested: true,
            confirm_live_order: true,
            max_open_submissions: Some(1),
            max_open_wait_ms: None,
            run_report_path_configured: true,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--max-open-wait-ms"));
    }

    #[test]
    fn live_runner_should_require_run_report_path_for_live_execute_run() {
        let error = validate_admission(AdmissionCase {
            dry_run: false,
            execute_requested: true,
            run_requested: true,
            confirm_live_order: true,
            max_open_submissions: Some(1),
            max_open_wait_ms: Some(60_000),
            run_report_path_configured: false,
            ..AdmissionCase::default()
        })
        .unwrap_err();

        assert!(error.to_string().contains("--run-report-path"));
    }

    #[test]
    fn cross_arb_live_event_guard_should_debounce_and_block_reentrant_symbol() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut guard = SymbolEvaluationGuard::new(1_000);

        assert_eq!(
            guard.try_start(&symbol, now),
            SymbolEvaluationAdmission::Accepted
        );
        assert_eq!(
            guard.try_start(&symbol, now + chrono::Duration::milliseconds(10)),
            SymbolEvaluationAdmission::InFlight
        );
        guard.finish(&symbol);
        assert_eq!(
            guard.try_start(&symbol, now + chrono::Duration::milliseconds(10)),
            SymbolEvaluationAdmission::Debounced
        );
        assert_eq!(
            guard.try_start(&symbol, now + chrono::Duration::milliseconds(200)),
            SymbolEvaluationAdmission::Accepted
        );
    }

    #[test]
    fn cross_arb_live_event_should_drop_stale_or_gap_book_before_open_snapshot() {
        let now = Utc::now();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.symbols = vec![symbol.clone()];
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
        config.market.stale_quote_ms = 500;

        let mut stale = test_book(ExchangeId::Binance, symbol.clone(), 100.0, 101.0);
        stale.recv_ts = now - chrono::Duration::seconds(2);
        let mut sequence_gap = test_book(ExchangeId::Okx, symbol.clone(), 104.0, 105.0);
        sequence_gap.quality.sequence_gap = true;
        let snapshots = vec![
            MarketSymbolSnapshot {
                exchange: ExchangeId::Binance,
                canonical_symbol: symbol.clone(),
                orderbook: Some(stale),
                funding: None,
                instrument: None,
                route_health: Vec::new(),
                usable_for_entries: false,
            },
            MarketSymbolSnapshot {
                exchange: ExchangeId::Okx,
                canonical_symbol: symbol,
                orderbook: Some(sequence_gap),
                funding: None,
                instrument: None,
                route_health: Vec::new(),
                usable_for_entries: false,
            },
        ];

        let runtime_snapshots =
            market_symbol_snapshots_to_runtime_snapshots(&snapshots, &config, &HashMap::new(), now);

        assert!(runtime_snapshots.is_empty());
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
