use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, ExchangeClientCapabilities, FeesRequest, FundingRatesRequest,
    OrderBookRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, RequestContext,
    SymbolRules, SymbolRulesRequest, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{AdapterBackedGateway, GatewayClient, InProcessGatewayClient};
use rustcta_exchange_gateway::{GetCapabilitiesRequest, GATEWAY_PROTOCOL_SCHEMA_VERSION};
use rustcta_execution_api::{CancelCommand, CancellationIds, MutationIdentity, OrderCommand};
use rustcta_execution_router::{ExecutionRouter, ExecutionRouterConfig};
use rustcta_gateway_app::GatewayAppConfig;
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyContext, StrategyExecutionClient,
    StrategyInstanceId, StrategyRuntime, StrategySdkError,
};
use rustcta_strategy_spot_futures_arbitrage::{
    evaluate_spot_futures_opportunity, spot_futures_order_draft_to_execution_command,
    CanonicalSymbol as StrategyCanonicalSymbol, FeeRates, FundingSnapshot, InstrumentKey,
    OrderBookTop, SpotFuturesArbitrageConfig, SpotFuturesArbitrageRuntime, SpotFuturesBundle,
    SpotFuturesBundleEvent, SpotFuturesMarketType, SpotFuturesOpportunity, SpotFuturesRoute,
    SymbolPrecision, STRATEGY_KIND,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    PositionSide, RunId, StrategyId, TenantId, TimeInForce,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

type LocalGatewayClient = InProcessGatewayClient<AdapterBackedGateway>;
type LocalExecutionRouter = ExecutionRouter<LocalGatewayClient>;

#[derive(Debug)]
struct Args {
    config: PathBuf,
    strategy_id: String,
    run_id: String,
    tenant_id: String,
    account_id: String,
    once: bool,
    snapshot_interval_ms: u64,
    dashboard_snapshot_path: Option<PathBuf>,
    execution_mode: ExecutionMode,
    quiet_stdout: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Noop,
    RouterDryRun,
    RouterLive,
}

impl ExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::RouterDryRun => "router-dry-run",
            Self::RouterLive => "router-live",
        }
    }
}

#[derive(Debug, Serialize)]
struct RuntimeReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    root_free_runtime: bool,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    execution_mode: &'static str,
    gateway_adapters: Vec<String>,
    preflight: Option<SpotFuturesPreflightReport>,
    reconcile: Option<SpotFuturesReconcileReport>,
    market_scan: Option<MarketScanReport>,
    execution_cycle: SpotFuturesExecutionCycleReport,
    snapshot: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct SpotFuturesPreflightReport {
    checked_at: chrono::DateTime<Utc>,
    exchange_count: usize,
    ready_for_live: bool,
    blocks_live: bool,
    missing_requirements: Vec<String>,
    capabilities: Vec<Value>,
    errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MarketScanReport {
    scanned_at: chrono::DateTime<Utc>,
    route_count: usize,
    accepted_count: usize,
    rejected_count: usize,
    error_count: usize,
    reference_prices: Vec<Value>,
    opportunities: Vec<Value>,
    audits: Vec<Value>,
    errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SpotFuturesReconcileReport {
    checked_at: chrono::DateTime<Utc>,
    checked_symbol_count: usize,
    blocks_new_entries: bool,
    residual_spot_total_notional_usdt: f64,
    residual_perp_total_notional_usdt: f64,
    spot_balances: Vec<Value>,
    perp_positions: Vec<Value>,
    residuals: Vec<Value>,
    errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SpotFuturesExecutionCycleReport {
    evaluated_at: chrono::DateTime<Utc>,
    mode: &'static str,
    live_orders_enabled: bool,
    new_entries_paused: bool,
    close_only: bool,
    candidate_count: usize,
    planned_count: usize,
    submitted_count: usize,
    accepted_count: usize,
    rejected_count: usize,
    skipped_count: usize,
    blocked_reason: Option<String>,
    plans: Vec<Value>,
    order_events: Vec<Value>,
    bundle_events: Vec<Value>,
    readback_events: Vec<Value>,
}

impl SpotFuturesExecutionCycleReport {
    fn empty(
        mode: ExecutionMode,
        live_orders_enabled: bool,
        config: &SpotFuturesArbitrageConfig,
        candidate_count: usize,
        blocked_reason: Option<String>,
    ) -> Self {
        Self {
            evaluated_at: Utc::now(),
            mode: mode.as_str(),
            live_orders_enabled,
            new_entries_paused: config.risk.start_paused_new_entries,
            close_only: config.risk.start_close_only,
            candidate_count,
            planned_count: 0,
            submitted_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            skipped_count: candidate_count,
            blocked_reason,
            plans: Vec::new(),
            order_events: Vec::new(),
            bundle_events: Vec::new(),
            readback_events: Vec::new(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = read_yaml_config(&args.config)?;
    let typed_config: SpotFuturesArbitrageConfig =
        serde_json::from_value(config.clone()).context("parse spot-futures config")?;
    let live_orders_enabled = live_orders_enabled(&args, &config)?;
    let execution = build_execution_client(&args, &config, live_orders_enabled)?;
    let mut runtime = SpotFuturesArbitrageRuntime::new();
    runtime
        .start(strategy_context(
            &args,
            config,
            Arc::clone(&execution.client),
        ))
        .await?;
    emit_report(
        &args,
        &runtime,
        live_orders_enabled,
        &execution,
        &typed_config,
    )
    .await?;
    if args.once {
        runtime.stop().await?;
        return Ok(());
    }
    let mut interval = tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    loop {
        interval.tick().await;
        emit_report(
            &args,
            &runtime,
            live_orders_enabled,
            &execution,
            &typed_config,
        )
        .await?;
    }
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/spot_futures_arbitrage_usdt.yml"),
        strategy_id: "spot_futures_arb_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        once: false,
        snapshot_interval_ms: 30_000,
        dashboard_snapshot_path: Some(PathBuf::from(
            "logs/spot_futures_arbitrage/dashboard_snapshot.json",
        )),
        execution_mode: ExecutionMode::Noop,
        quiet_stdout: false,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--dashboard-snapshot-path" => {
                args.dashboard_snapshot_path = Some(PathBuf::from(next_value(
                    &mut values,
                    "--dashboard-snapshot-path",
                )?))
            }
            "--disable-dashboard-snapshot" => args.dashboard_snapshot_path = None,
            "--quiet-stdout" => args.quiet_stdout = true,
            "--execution-mode" => {
                args.execution_mode =
                    parse_execution_mode(&next_value(&mut values, "--execution-mode")?)?
            }
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--help" | "-h" => {
                println!(
                    "spot-futures-arbitrage-runtime --config <path> [--execution-mode noop|router-dry-run|router-live] [--dashboard-snapshot-path <path>] [--quiet-stdout] [--once]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(args)
}

fn parse_execution_mode(value: &str) -> Result<ExecutionMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "noop" => Ok(ExecutionMode::Noop),
        "router-dry-run" | "dry-run" => Ok(ExecutionMode::RouterDryRun),
        "router-live" | "live" => Ok(ExecutionMode::RouterLive),
        other => bail!("unknown --execution-mode: {other}"),
    }
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &PathBuf) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

fn live_orders_enabled(args: &Args, config: &serde_json::Value) -> Result<bool> {
    let config_live = config
        .get("enable_live_trading")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if matches!(args.execution_mode, ExecutionMode::RouterLive) && !config_live {
        bail!("--execution-mode router-live requires enable_live_trading: true in config");
    }
    Ok(matches!(args.execution_mode, ExecutionMode::RouterLive) && config_live)
}

struct ExecutionClientBundle {
    client: Arc<dyn StrategyExecutionClient>,
    gateway: Option<LocalGatewayClient>,
    gateway_adapters: Vec<String>,
}

fn build_execution_client(
    args: &Args,
    config: &serde_json::Value,
    live_orders_enabled: bool,
) -> Result<ExecutionClientBundle> {
    match args.execution_mode {
        ExecutionMode::Noop => Ok(ExecutionClientBundle {
            client: Arc::new(NoopExecutionClient),
            gateway: None,
            gateway_adapters: Vec::new(),
        }),
        ExecutionMode::RouterDryRun | ExecutionMode::RouterLive => {
            let gateway_config = GatewayAppConfig::from_env_reader(gateway_env);
            let gateway_adapters = gateway_config.adapters.clone();
            let gateway = gateway_config.build_gateway()?;
            let gateway = InProcessGatewayClient::new(Arc::new(gateway));
            let router_config = if live_orders_enabled {
                ExecutionRouterConfig::live()
            } else {
                ExecutionRouterConfig::dry_run()
            };
            let router = Arc::new(ExecutionRouter::new(router_config, gateway.clone()));
            Ok(ExecutionClientBundle {
                client: Arc::new(RouterBackedSpotFuturesExecutionClient {
                    router,
                    account_by_exchange: account_by_exchange(config),
                }),
                gateway: Some(gateway),
                gateway_adapters,
            })
        }
    }
}

fn gateway_env(key: &str) -> Option<String> {
    match key {
        "RUSTCTA_GATEWAY_ADAPTERS" => {
            first_env(&[key]).or_else(|| Some("gateio,bitget,okx".to_string()))
        }
        "RUSTCTA_BITGET_API_KEY" => first_env(&[key, "BITGET_API_KEY"]),
        "RUSTCTA_BITGET_API_SECRET" => first_env(&[key, "BITGET_API_SECRET"]),
        "RUSTCTA_BITGET_API_PASSPHRASE" => {
            first_env(&[key, "BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE"])
        }
        "RUSTCTA_GATEIO_API_KEY" => first_env(&[
            key,
            "GATEIO_API_KEY",
            "GATE_API_KEY",
            "GATE__53636022__API_KEY",
            "GATE__16076371__API_KEY",
        ]),
        "RUSTCTA_GATEIO_API_SECRET" => first_env(&[
            key,
            "GATEIO_API_SECRET",
            "GATE_API_SECRET",
            "GATE__53636022__API_SECRET",
            "GATE__16076371__API_SECRET",
        ]),
        "RUSTCTA_OKX_API_KEY" => first_env(&[key, "OKX_API_KEY", "OKX_SPOT_API_KEY"]),
        "RUSTCTA_OKX_API_SECRET" => first_env(&[key, "OKX_API_SECRET", "OKX_SPOT_API_SECRET"]),
        "RUSTCTA_OKX_API_PASSPHRASE" => first_env(&[key, "OKX_PASSPHRASE", "OKX_SPOT_PASSPHRASE"]),
        _ => std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty()),
    }
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn account_by_exchange(config: &serde_json::Value) -> BTreeMap<String, String> {
    config
        .get("accounts")
        .and_then(Value::as_object)
        .map(|accounts| {
            accounts
                .iter()
                .filter_map(|(exchange, account)| {
                    account
                        .as_str()
                        .map(|account| (gateway_exchange_id(exchange), account.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn run_market_scan_once(
    args: &Args,
    gateway: &LocalGatewayClient,
    config: &SpotFuturesArbitrageConfig,
) -> MarketScanReport {
    let scanned_at = Utc::now();
    let mut opportunities = Vec::new();
    let mut audits = Vec::new();
    let mut reference_prices = Vec::new();
    let mut errors = Vec::new();
    let mut route_count = 0_usize;
    for symbol_text in config.active_symbols() {
        let Some(strategy_symbol) = StrategyCanonicalSymbol::parse(&symbol_text) else {
            errors.push(format!("invalid configured symbol {symbol_text}"));
            continue;
        };
        for spot_exchange in &config.universe.enabled_spot_exchanges {
            for perp_exchange in &config.universe.enabled_perp_exchanges {
                route_count += 1;
                let route = SpotFuturesRoute {
                    route_id: format!(
                        "{}:{}:{}",
                        spot_exchange.trim().to_ascii_lowercase(),
                        perp_exchange.trim().to_ascii_lowercase(),
                        strategy_symbol.as_pair()
                    ),
                    canonical_symbol: strategy_symbol.clone(),
                    spot_exchange: spot_exchange.trim().to_ascii_lowercase(),
                    perp_exchange: perp_exchange.trim().to_ascii_lowercase(),
                };
                match scan_route_once(args, gateway, config, &route, scanned_at).await {
                    Ok((opportunity, audit, reference_price)) => {
                        if let Some(opportunity) = opportunity {
                            opportunities.push(secret_free_json(opportunity));
                        }
                        audits.push(secret_free_json(audit));
                        reference_prices.push(reference_price);
                    }
                    Err(error) => errors.push(format!("{}: {error}", route.route_id)),
                }
            }
        }
    }
    let accepted_count = opportunities.len();
    let rejected_count = audits
        .iter()
        .filter(|audit| {
            !audit
                .get("accepted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();
    MarketScanReport {
        scanned_at,
        route_count,
        accepted_count,
        rejected_count,
        error_count: errors.len(),
        reference_prices,
        opportunities,
        audits,
        errors,
    }
}

async fn scan_route_once(
    args: &Args,
    gateway: &LocalGatewayClient,
    config: &SpotFuturesArbitrageConfig,
    route: &SpotFuturesRoute,
    now: chrono::DateTime<Utc>,
) -> Result<(
    Option<rustcta_strategy_spot_futures_arbitrage::SpotFuturesOpportunity>,
    rustcta_strategy_spot_futures_arbitrage::SpotFuturesOpportunityAudit,
    Value,
)> {
    let spot_scope = symbol_scope(
        &route.spot_exchange,
        MarketType::Spot,
        &route.canonical_symbol,
    )?;
    let perp_scope = symbol_scope(
        &route.perp_exchange,
        MarketType::Perpetual,
        &route.canonical_symbol,
    )?;
    let spot_book =
        fetch_order_book(args, gateway, &spot_scope, config.market.depth_levels).await?;
    let perp_book =
        fetch_order_book(args, gateway, &perp_scope, config.market.depth_levels).await?;
    let spot_rules = fetch_symbol_rules(args, gateway, spot_scope.clone()).await?;
    let perp_rules = fetch_symbol_rules(args, gateway, perp_scope.clone()).await?;
    let spot_precision = precision_from_rules(&spot_rules);
    let perp_precision = precision_from_rules(&perp_rules);
    let funding = fetch_funding(args, gateway, perp_scope.clone())
        .await
        .ok()
        .flatten();
    let spot_fee = fetch_fee(args, gateway, spot_scope)
        .await
        .unwrap_or_default();
    let perp_fee = fetch_fee(args, gateway, perp_scope)
        .await
        .unwrap_or_default();
    let reference_price = json!({
        "route_id": route.route_id,
        "symbol": route.canonical_symbol,
        "base": route.canonical_symbol.base,
        "quote": route.canonical_symbol.quote,
        "spot_exchange": route.spot_exchange,
        "perp_exchange": route.perp_exchange,
        "spot_best_ask_price": spot_book.best_ask_price,
        "spot_best_bid_price": spot_book.best_bid_price,
        "perp_best_bid_price": perp_book.best_bid_price,
        "perp_best_ask_price": perp_book.best_ask_price,
        "reference_price_usdt": spot_book.best_ask_price,
        "observed_at": now,
    });
    let (opportunity, audit) = evaluate_spot_futures_opportunity(
        route,
        &spot_book,
        &perp_book,
        funding.as_ref(),
        spot_precision,
        perp_precision,
        spot_fee,
        perp_fee,
        &config.selection,
        &config.funding,
        &config.execution,
        &config.sizing,
        &config.universe.excluded_bases,
        now,
        config.market.stale_quote_ms,
        config.market.depth_levels,
    );
    Ok((opportunity, audit, reference_price))
}

async fn fetch_order_book(
    args: &Args,
    gateway: &LocalGatewayClient,
    symbol: &SymbolScope,
    depth_levels: usize,
) -> Result<OrderBookTop> {
    let request_id = request_id("sf-book");
    let response = gateway
        .get_order_book(
            request_id.clone(),
            TenantId::new(args.tenant_id.clone()).map_sdk_err()?,
            Some(AccountId::new(args.account_id.clone()).map_sdk_err()?),
            OrderBookRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, request_id),
                symbol: symbol.clone(),
                depth: Some(depth_levels.max(1) as u32),
            },
        )
        .await?;
    let book = response.order_book;
    let bid = book.best_bid().context("order book missing best bid")?;
    let ask = book.best_ask().context("order book missing best ask")?;
    let market_type = match book.market_type {
        MarketType::Spot => SpotFuturesMarketType::Spot,
        MarketType::Perpetual => SpotFuturesMarketType::Perpetual,
        other => bail!("unsupported book market type {other:?}"),
    };
    let canonical = StrategyCanonicalSymbol::parse(book.canonical_symbol.as_str())
        .ok_or_else(|| anyhow::anyhow!("invalid book canonical symbol"))?;
    Ok(OrderBookTop {
        instrument: InstrumentKey::new(book.exchange_id.as_str(), market_type, canonical),
        best_bid_price: bid.price,
        best_bid_quantity: bid.quantity,
        best_ask_price: ask.price,
        best_ask_quantity: ask.quantity,
        levels: book.bids.len().min(book.asks.len()),
        received_at: book.received_at,
        sequence_gap: book.is_stale,
    })
}

async fn fetch_symbol_rules(
    args: &Args,
    gateway: &LocalGatewayClient,
    symbol: SymbolScope,
) -> Result<SymbolRules> {
    let request_id = request_id("sf-rules");
    let response = gateway
        .get_symbol_rules(
            request_id.clone(),
            TenantId::new(args.tenant_id.clone()).map_sdk_err()?,
            Some(AccountId::new(args.account_id.clone()).map_sdk_err()?),
            SymbolRulesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, request_id),
                symbols: vec![symbol],
            },
        )
        .await?;
    response
        .rules
        .into_iter()
        .next()
        .context("symbol rules response empty")
}

async fn fetch_funding(
    args: &Args,
    gateway: &LocalGatewayClient,
    symbol: SymbolScope,
) -> Result<Option<FundingSnapshot>> {
    let request_id = request_id("sf-funding");
    let response = gateway
        .get_funding_rates(
            request_id.clone(),
            TenantId::new(args.tenant_id.clone()).map_sdk_err()?,
            Some(AccountId::new(args.account_id.clone()).map_sdk_err()?),
            FundingRatesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, request_id),
                symbols: vec![symbol],
            },
        )
        .await?;
    let Some(rate) = response.rates.into_iter().next() else {
        return Ok(None);
    };
    let Some(canonical) = rate.symbol.canonical_symbol.as_ref() else {
        return Ok(None);
    };
    let strategy_symbol = StrategyCanonicalSymbol::parse(canonical.as_str())
        .ok_or_else(|| anyhow::anyhow!("invalid funding canonical symbol"))?;
    Ok(Some(FundingSnapshot {
        instrument: InstrumentKey::new(
            rate.symbol.exchange.as_str(),
            SpotFuturesMarketType::Perpetual,
            strategy_symbol,
        ),
        funding_rate: parse_f64_or_zero(&rate.funding_rate),
        predicted_funding_rate: rate
            .predicted_funding_rate
            .as_deref()
            .and_then(parse_f64_optional),
        funding_interval_hours: 8.0,
        next_funding_time: rate.next_funding_time,
        observed_at: rate.updated_at,
    }))
}

async fn fetch_fee(
    args: &Args,
    gateway: &LocalGatewayClient,
    symbol: SymbolScope,
) -> Result<FeeRates> {
    let request_id = request_id("sf-fees");
    let response = gateway
        .get_fees(
            request_id.clone(),
            TenantId::new(args.tenant_id.clone()).map_sdk_err()?,
            Some(AccountId::new(args.account_id.clone()).map_sdk_err()?),
            FeesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, request_id),
                symbols: vec![symbol],
            },
        )
        .await?;
    let Some(fee) = response.fees.into_iter().next() else {
        return Ok(FeeRates::default());
    };
    Ok(FeeRates {
        maker: parse_f64_or_default(&fee.maker_rate, FeeRates::default().maker),
        taker: parse_f64_or_default(&fee.taker_rate, FeeRates::default().taker),
    })
}

fn symbol_scope(
    exchange: &str,
    market_type: MarketType,
    symbol: &StrategyCanonicalSymbol,
) -> Result<SymbolScope> {
    let exchange = gateway_exchange_id(exchange);
    let exchange_id = ExchangeId::new(exchange.clone()).map_sdk_err()?;
    let canonical = CanonicalSymbol::new(&symbol.base, &symbol.quote).map_sdk_err()?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        market_type,
        exchange_symbol_text(&exchange, market_type, &canonical),
    )
    .map_sdk_err()?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn request_context(args: &Args, request_id: String) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = TenantId::new(args.tenant_id.clone()).ok();
    context.account_id = AccountId::new(args.account_id.clone()).ok();
    context.run_id = RunId::new(args.run_id.clone()).ok();
    context.request_id = Some(request_id);
    context
}

fn request_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    )
}

fn precision_from_rules(rule: &SymbolRules) -> SymbolPrecision {
    SymbolPrecision {
        price_tick: parse_positive_optional(rule.price_increment.as_deref().unwrap_or("0"))
            .unwrap_or(0.0),
        quantity_step: parse_positive_optional(rule.quantity_increment.as_deref().unwrap_or("0"))
            .unwrap_or(0.0),
        min_base_quantity: parse_positive_optional(rule.min_quantity.as_deref().unwrap_or("0"))
            .unwrap_or(0.0),
        min_notional_usdt: parse_positive_optional(rule.min_notional.as_deref().unwrap_or("0"))
            .unwrap_or(0.0),
        contract_size: 1.0,
    }
}

fn parse_positive_optional(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn parse_f64_optional(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn parse_f64_or_zero(value: &str) -> f64 {
    parse_f64_optional(value).unwrap_or(0.0)
}

fn parse_f64_or_default(value: &str, default: f64) -> f64 {
    parse_f64_optional(value).unwrap_or(default)
}

fn active_strategy_symbols(config: &SpotFuturesArbitrageConfig) -> Vec<StrategyCanonicalSymbol> {
    config
        .active_symbols()
        .into_iter()
        .filter_map(|symbol| StrategyCanonicalSymbol::parse(&symbol))
        .collect()
}

fn reconcile_price_map(market_scan: Option<&MarketScanReport>) -> BTreeMap<String, f64> {
    let mut prices = BTreeMap::new();
    let Some(scan) = market_scan else {
        return prices;
    };
    for reference in &scan.reference_prices {
        let Some(base) = reference
            .get("base")
            .and_then(Value::as_str)
            .map(|base| base.to_ascii_uppercase())
        else {
            continue;
        };
        let price = reference
            .get("reference_price_usdt")
            .and_then(Value::as_f64)
            .or_else(|| reference.get("spot_best_ask_price").and_then(Value::as_f64))
            .or_else(|| reference.get("perp_best_bid_price").and_then(Value::as_f64))
            .unwrap_or(0.0);
        if price.is_finite() && price > 0.0 {
            prices.entry(base).or_insert(price);
        }
    }
    prices
}

fn secret_free_json(value: impl Serialize) -> Value {
    serde_json::to_value(value)
        .unwrap_or_else(|error| json!({ "serialization_error": error.to_string() }))
}

fn secret_free_result_json<T, E>(result: &std::result::Result<T, E>) -> Value
where
    T: Serialize,
    E: std::fmt::Display,
{
    match result {
        Ok(value) => json!({
            "ok": true,
            "value": secret_free_json(value),
        }),
        Err(error) => json!({
            "ok": false,
            "error": error.to_string(),
        }),
    }
}

fn strategy_context(
    args: &Args,
    config: serde_json::Value,
    execution: Arc<dyn StrategyExecutionClient>,
) -> StrategyContext {
    StrategyContext::new(
        StrategyInstanceId::new(format!("{}:{}", args.strategy_id, args.run_id)),
        args.tenant_id.clone(),
        args.account_id.clone(),
        args.strategy_id.clone(),
        args.run_id.clone(),
        config,
        execution,
    )
}

async fn emit_report(
    args: &Args,
    runtime: &SpotFuturesArbitrageRuntime,
    live_orders_enabled: bool,
    execution: &ExecutionClientBundle,
    config: &SpotFuturesArbitrageConfig,
) -> Result<()> {
    let preflight = match execution.gateway.as_ref() {
        Some(gateway) => Some(run_preflight_once(args, gateway, config).await),
        None => None,
    };
    let market_scan = match execution.gateway.as_ref() {
        Some(gateway) => Some(run_market_scan_once(args, gateway, config).await),
        None => None,
    };
    let reconcile = match execution.gateway.as_ref() {
        Some(gateway) => {
            Some(run_reconcile_once(args, gateway, config, market_scan.as_ref()).await)
        }
        None => None,
    };
    let execution_cycle = run_execution_cycle_once(
        args,
        &strategy_context(
            args,
            serde_json::to_value(config).context("serialize spot-futures config")?,
            Arc::clone(&execution.client),
        ),
        Arc::clone(&execution.client),
        execution.gateway.as_ref(),
        live_orders_enabled,
        config,
        market_scan.as_ref(),
        reconcile.as_ref(),
    )
    .await;
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        root_free_runtime: true,
        live_orders_enabled,
        concrete_exchange_adapter_loaded: matches!(
            args.execution_mode,
            ExecutionMode::RouterDryRun | ExecutionMode::RouterLive
        ) && !execution.gateway_adapters.is_empty(),
        execution_mode: args.execution_mode.as_str(),
        gateway_adapters: execution.gateway_adapters.clone(),
        preflight,
        reconcile,
        market_scan,
        execution_cycle,
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    let report_value = serde_json::to_value(&report)?;
    if !args.quiet_stdout {
        println!("{}", serde_json::to_string(&report_value)?);
    }
    if let Some(path) = &args.dashboard_snapshot_path {
        write_dashboard_snapshot(args, &report, path).await?;
        write_scan_jsonl(
            path,
            report.preflight.as_ref(),
            report.reconcile.as_ref(),
            report.market_scan.as_ref(),
            &report.execution_cycle,
        )
        .await?;
    }
    Ok(())
}

async fn run_preflight_once(
    args: &Args,
    gateway: &LocalGatewayClient,
    config: &SpotFuturesArbitrageConfig,
) -> SpotFuturesPreflightReport {
    let checked_at = Utc::now();
    let mut exchange_ids: Vec<String> = config
        .universe
        .enabled_spot_exchanges
        .iter()
        .chain(config.universe.enabled_perp_exchanges.iter())
        .map(|exchange| gateway_exchange_id(exchange))
        .collect();
    exchange_ids.sort();
    exchange_ids.dedup();
    let exchanges: Result<Vec<_>> = exchange_ids
        .iter()
        .map(|exchange| {
            ExchangeId::new(exchange.clone())
                .map_sdk_err()
                .map_err(Into::into)
        })
        .collect();
    let mut report = SpotFuturesPreflightReport {
        checked_at,
        exchange_count: exchange_ids.len(),
        ready_for_live: false,
        blocks_live: true,
        missing_requirements: Vec::new(),
        capabilities: Vec::new(),
        errors: Vec::new(),
    };
    let exchanges = match exchanges {
        Ok(exchanges) => exchanges,
        Err(error) => {
            report.errors.push(error.to_string());
            report
                .missing_requirements
                .push("invalid_exchange_id".to_string());
            return report;
        }
    };
    let request_id = request_id("sf-capabilities");
    match gateway
        .get_capabilities(
            request_id.clone(),
            TenantId::new(args.tenant_id.clone())
                .map_sdk_err()
                .map_err(anyhow::Error::from)
                .unwrap_or_else(|_| TenantId::new("spot-futures-preflight").unwrap()),
            AccountId::new(args.account_id.clone())
                .ok()
                .or_else(|| AccountId::new("spot_futures_arb").ok()),
            GetCapabilitiesRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: request_context(args, request_id),
                exchanges,
            },
        )
        .await
    {
        Ok(response) => {
            for capability in response.capabilities {
                collect_capability_requirements(config, &capability, &mut report);
                report.capabilities.push(secret_free_json(capability));
            }
        }
        Err(error) => {
            report.errors.push(error.to_string());
            report
                .missing_requirements
                .push("gateway_capabilities_unavailable".to_string());
        }
    }
    report.missing_requirements.sort();
    report.missing_requirements.dedup();
    report.ready_for_live = report.errors.is_empty() && report.missing_requirements.is_empty();
    report.blocks_live = !report.ready_for_live;
    report
}

async fn run_reconcile_once(
    args: &Args,
    gateway: &LocalGatewayClient,
    config: &SpotFuturesArbitrageConfig,
    market_scan: Option<&MarketScanReport>,
) -> SpotFuturesReconcileReport {
    let checked_at = Utc::now();
    let active_symbols = active_strategy_symbols(config);
    let mut report = SpotFuturesReconcileReport {
        checked_at,
        checked_symbol_count: active_symbols.len(),
        blocks_new_entries: false,
        residual_spot_total_notional_usdt: 0.0,
        residual_perp_total_notional_usdt: 0.0,
        spot_balances: Vec::new(),
        perp_positions: Vec::new(),
        residuals: Vec::new(),
        errors: Vec::new(),
    };
    let tenant_id = match TenantId::new(args.tenant_id.clone()).map_sdk_err() {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            report.errors.push(error.to_string());
            report.blocks_new_entries = true;
            return report;
        }
    };
    let account_id = AccountId::new(args.account_id.clone()).ok();
    let prices = reconcile_price_map(market_scan);
    let residual_tolerance_notional = config.risk.max_unhedged_spot_notional_usdt.max(0.0);

    for spot_exchange in &config.universe.enabled_spot_exchanges {
        let exchange = match ExchangeId::new(gateway_exchange_id(spot_exchange)).map_sdk_err() {
            Ok(exchange) => exchange,
            Err(error) => {
                report.errors.push(format!("{spot_exchange}: {error}"));
                continue;
            }
        };
        let assets: Vec<String> = active_symbols
            .iter()
            .map(|symbol| symbol.base.clone())
            .collect();
        let request_id = request_id("sf-reconcile-balances");
        let result = gateway
            .get_balances(
                request_id.clone(),
                tenant_id.clone(),
                account_id.clone(),
                BalancesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: request_context(args, request_id),
                    exchange: exchange.clone(),
                    market_type: Some(MarketType::Spot),
                    assets: assets.clone(),
                },
            )
            .await;
        match result {
            Ok(response) => {
                for balance in response.balances {
                    for asset_balance in balance.balances {
                        let asset = asset_balance.asset.to_ascii_uppercase();
                        if !assets.iter().any(|configured| configured == &asset) {
                            continue;
                        }
                        let price = prices.get(&asset).copied().unwrap_or(0.0);
                        let notional = asset_balance.total.max(0.0) * price;
                        let blocks = notional > residual_tolerance_notional;
                        report.residual_spot_total_notional_usdt += notional;
                        report.spot_balances.push(json!({
                            "exchange": balance.exchange_id,
                            "market_type": balance.market_type,
                            "asset": asset,
                            "total": asset_balance.total,
                            "available": asset_balance.available,
                            "locked": asset_balance.locked,
                            "estimated_price_usdt": price,
                            "estimated_notional_usdt": notional,
                            "blocks_new_entries": blocks,
                            "observed_at": balance.observed_at,
                        }));
                        if blocks {
                            report.blocks_new_entries = true;
                            report.residuals.push(json!({
                                "residual_type": "unmanaged_spot_balance",
                                "exchange": balance.exchange_id,
                                "asset": asset,
                                "quantity": asset_balance.total,
                                "estimated_notional_usdt": notional,
                                "threshold_usdt": residual_tolerance_notional,
                            }));
                        }
                    }
                }
            }
            Err(error) => {
                report
                    .errors
                    .push(format!("{}:spot_balances:{error}", exchange.as_str()));
            }
        }
    }

    for perp_exchange in &config.universe.enabled_perp_exchanges {
        let exchange = match ExchangeId::new(gateway_exchange_id(perp_exchange)).map_sdk_err() {
            Ok(exchange) => exchange,
            Err(error) => {
                report.errors.push(format!("{perp_exchange}: {error}"));
                continue;
            }
        };
        let mut symbols = Vec::new();
        for symbol in &active_symbols {
            match symbol_scope(perp_exchange, MarketType::Perpetual, symbol) {
                Ok(scope) => symbols.push(scope.exchange_symbol),
                Err(error) => report.errors.push(format!(
                    "{}:{}: {error}",
                    exchange.as_str(),
                    symbol.as_pair()
                )),
            }
        }
        let request_id = request_id("sf-reconcile-positions");
        let result = gateway
            .get_positions(
                request_id.clone(),
                tenant_id.clone(),
                account_id.clone(),
                PositionsRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: request_context(args, request_id),
                    exchange: exchange.clone(),
                    market_type: Some(MarketType::Perpetual),
                    symbols,
                },
            )
            .await;
        match result {
            Ok(response) => {
                for position in response.positions {
                    let base = position.canonical_symbol.base_asset().to_ascii_uppercase();
                    let price = position
                        .mark_price
                        .or(position.entry_price)
                        .or_else(|| prices.get(&base).copied())
                        .unwrap_or(0.0);
                    let notional = position.quantity.max(0.0) * price;
                    let blocks = notional > residual_tolerance_notional;
                    report.residual_perp_total_notional_usdt += notional;
                    report.perp_positions.push(json!({
                        "exchange": position.exchange_id,
                        "market_type": position.market_type,
                        "symbol": position.canonical_symbol,
                        "side": position.side,
                        "quantity": position.quantity,
                        "entry_price": position.entry_price,
                        "mark_price": position.mark_price,
                        "estimated_notional_usdt": notional,
                        "blocks_new_entries": blocks,
                        "observed_at": position.observed_at,
                    }));
                    if blocks {
                        report.blocks_new_entries = true;
                        report.residuals.push(json!({
                            "residual_type": "unmanaged_perp_position",
                            "exchange": position.exchange_id,
                            "symbol": position.canonical_symbol,
                            "side": position.side,
                            "quantity": position.quantity,
                            "estimated_notional_usdt": notional,
                            "threshold_usdt": residual_tolerance_notional,
                        }));
                    }
                }
            }
            Err(error) => {
                report
                    .errors
                    .push(format!("{}:perp_positions:{error}", exchange.as_str()));
            }
        }
    }

    if config.risk.orphan_exposure_blocks_new_entries && !report.errors.is_empty() {
        report.blocks_new_entries = true;
    }
    report
}

fn collect_capability_requirements(
    config: &SpotFuturesArbitrageConfig,
    capability: &ExchangeClientCapabilities,
    report: &mut SpotFuturesPreflightReport,
) {
    let exchange = capability.exchange.as_str().to_string();
    if !capability.supports_public_rest {
        report
            .missing_requirements
            .push(format!("{exchange}:public_rest"));
    }
    if !capability.supports_symbol_rules {
        report
            .missing_requirements
            .push(format!("{exchange}:symbol_rules"));
    }
    if !capability.supports_order_book_snapshot {
        report
            .missing_requirements
            .push(format!("{exchange}:order_book_snapshot"));
    }
    if !capability.supports_fees {
        report.missing_requirements.push(format!("{exchange}:fees"));
    }
    if !capability.supports_private_rest {
        report
            .missing_requirements
            .push(format!("{exchange}:private_rest"));
    }
    if !capability.supports_balances {
        report
            .missing_requirements
            .push(format!("{exchange}:balances"));
    }
    if !capability.supports_place_order {
        report
            .missing_requirements
            .push(format!("{exchange}:place_order"));
    }
    if !capability.supports_cancel_order {
        report
            .missing_requirements
            .push(format!("{exchange}:cancel_order"));
    }
    if !capability.supports_query_order {
        report
            .missing_requirements
            .push(format!("{exchange}:query_order"));
    }
    if !capability.supports_open_orders {
        report
            .missing_requirements
            .push(format!("{exchange}:open_orders"));
    }
    if !capability.supports_recent_fills {
        report
            .missing_requirements
            .push(format!("{exchange}:recent_fills"));
    }
    if !capability.supports_post_only
        || !capability
            .supports_order_types
            .contains(&OrderType::PostOnly)
    {
        report
            .missing_requirements
            .push(format!("{exchange}:post_only"));
    }
    if !capability
        .supports_time_in_force
        .contains(&TimeInForce::IOC)
    {
        report.missing_requirements.push(format!("{exchange}:ioc"));
    }
    if config
        .universe
        .enabled_perp_exchanges
        .iter()
        .any(|venue| gateway_exchange_id(venue) == exchange)
    {
        if !capability.market_types.contains(&MarketType::Perpetual) {
            report
                .missing_requirements
                .push(format!("{exchange}:perpetual_market"));
        }
        if !capability.supports_positions {
            report
                .missing_requirements
                .push(format!("{exchange}:positions"));
        }
        if !capability.supports_reduce_only {
            report
                .missing_requirements
                .push(format!("{exchange}:reduce_only"));
        }
        if !capability.supports_funding_rates {
            report
                .missing_requirements
                .push(format!("{exchange}:funding_rates"));
        }
    }
    if config
        .universe
        .enabled_spot_exchanges
        .iter()
        .any(|venue| gateway_exchange_id(venue) == exchange)
        && !capability.market_types.contains(&MarketType::Spot)
    {
        report
            .missing_requirements
            .push(format!("{exchange}:spot_market"));
    }
}

async fn run_execution_cycle_once(
    args: &Args,
    ctx: &StrategyContext,
    execution: Arc<dyn StrategyExecutionClient>,
    gateway: Option<&LocalGatewayClient>,
    live_orders_enabled: bool,
    config: &SpotFuturesArbitrageConfig,
    market_scan: Option<&MarketScanReport>,
    reconcile: Option<&SpotFuturesReconcileReport>,
) -> SpotFuturesExecutionCycleReport {
    let candidates: Vec<SpotFuturesOpportunity> = market_scan
        .map(|scan| {
            scan.opportunities
                .iter()
                .filter_map(|value| serde_json::from_value(value.clone()).ok())
                .collect()
        })
        .unwrap_or_default();
    let candidate_count = candidates.len();
    let blocked_reason = execution_block_reason(args.execution_mode, config, reconcile);
    if let Some(reason) = blocked_reason.clone() {
        return SpotFuturesExecutionCycleReport::empty(
            args.execution_mode,
            live_orders_enabled,
            config,
            candidate_count,
            Some(reason),
        );
    }
    let mut report = SpotFuturesExecutionCycleReport::empty(
        args.execution_mode,
        live_orders_enabled,
        config,
        candidate_count,
        None,
    );
    report.skipped_count = 0;
    for opportunity in candidates
        .into_iter()
        .take(config.risk.max_open_bundles.max(1))
    {
        let now = Utc::now();
        let bundle_id = format!(
            "sfa-{}-{}",
            opportunity
                .route
                .route_id
                .replace(['/', ':'], "-")
                .to_ascii_lowercase(),
            now.timestamp_millis()
        );
        let mut bundle = SpotFuturesBundle::from_opportunity(&bundle_id, &opportunity, now);
        let client_order_id = format!("{bundle_id}-spot-open");
        let mut command = spot_futures_order_draft_to_execution_command(
            ctx,
            &client_order_id,
            &opportunity.spot_maker_order,
            now,
        );
        command
            .metadata
            .insert("bundle_id".to_string(), json!(bundle_id));
        command.metadata.insert(
            "opportunity_id".to_string(),
            json!(opportunity.opportunity_id),
        );
        command.metadata.insert(
            "execution_mode".to_string(),
            json!(args.execution_mode.as_str()),
        );
        report.planned_count += 1;
        report.plans.push(json!({
            "event_type": "spot_futures_order_plan",
            "bundle_id": bundle.bundle_id,
            "opportunity_id": bundle.opportunity_id,
            "route_id": bundle.route.route_id,
            "client_order_id": client_order_id,
            "role": command.metadata.get("role").cloned().unwrap_or(Value::Null),
            "exchange": command.exchange_id,
            "symbol": command.symbol,
            "side": command.side,
            "order_type": command.order_type,
            "time_in_force": command.time_in_force,
            "post_only": command.metadata.get("post_only").cloned().unwrap_or(Value::Null),
            "reduce_only": command.reduce_only,
            "quantity": command.quantity,
            "price": command.price,
            "expected_net_edge_bps": opportunity.expected_net_edge_bps,
            "target_notional_usdt": opportunity.target_notional_usdt,
        }));
        report.submitted_count += 1;
        match execution.submit_order(command).await {
            Ok(ack) => {
                let ack_value = secret_free_json(&ack);
                report.order_events.push(json!({
                    "event_type": "spot_futures_order_ack",
                    "bundle_id": bundle.bundle_id,
                    "opportunity_id": bundle.opportunity_id,
                    "client_order_id": ack.client_order_id,
                    "accepted": ack.accepted,
                    "execution_order_id": ack.execution_order_id,
                    "reason": ack.reason,
                    "received_at": ack.received_at,
                    "raw_ack": ack_value,
                }));
                report.readback_events.push(json!({
                    "event_type": "spot_futures_spot_maker_readback_plan",
                    "bundle_id": bundle.bundle_id,
                    "opportunity_id": bundle.opportunity_id,
                    "client_order_id": ack.client_order_id,
                    "execution_order_id": ack.execution_order_id,
                    "ack_accepted": ack.accepted,
                    "spot_maker_ttl_ms": config.execution.spot_maker_order_ttl_ms,
                    "requires_query_order": true,
                    "requires_recent_fills": true,
                    "requires_cancel_after_ttl": ack.accepted,
                    "will_submit_perp_hedge_after_confirmed_fill": ack.accepted,
                    "live_readback_hedge_enabled": config.execution.enable_spot_fill_readback_hedge,
                    "status": if ack.accepted {
                        "pending_spot_fill_readback"
                    } else {
                        "no_readback_without_accepted_order"
                    },
                }));
                if ack.accepted {
                    report.accepted_count += 1;
                    let event = SpotFuturesBundleEvent::SpotMakerSubmitted {
                        client_order_id: ack.client_order_id.clone(),
                        occurred_at: ack.received_at,
                    };
                    let actions =
                        rustcta_strategy_spot_futures_arbitrage::apply_spot_futures_bundle_event(
                            &mut bundle,
                            event.clone(),
                            &opportunity.hedge_after_fill.hedge_order,
                            config.execution.min_hedge_base_qty,
                            None,
                        );
                    report.bundle_events.push(json!({
                        "event_type": "spot_futures_bundle_event",
                        "bundle_id": bundle.bundle_id,
                        "opportunity_id": bundle.opportunity_id,
                        "event": event,
                        "status": bundle.status,
                        "actions": actions,
                    }));
                    if let Some(gateway) = gateway {
                        perform_spot_maker_readback_and_hedge(
                            args,
                            ctx,
                            gateway,
                            Arc::clone(&execution),
                            config,
                            &opportunity,
                            &mut bundle,
                            &ack.client_order_id,
                            ack.execution_order_id.as_deref(),
                            &mut report,
                        )
                        .await;
                    }
                } else {
                    report.rejected_count += 1;
                    let event = SpotFuturesBundleEvent::SpotMakerRejected {
                        reason: ack
                            .reason
                            .clone()
                            .unwrap_or_else(|| "order rejected".to_string()),
                        occurred_at: ack.received_at,
                    };
                    let actions =
                        rustcta_strategy_spot_futures_arbitrage::apply_spot_futures_bundle_event(
                            &mut bundle,
                            event.clone(),
                            &opportunity.hedge_after_fill.hedge_order,
                            config.execution.min_hedge_base_qty,
                            None,
                        );
                    report.bundle_events.push(json!({
                        "event_type": "spot_futures_bundle_event",
                        "bundle_id": bundle.bundle_id,
                        "opportunity_id": bundle.opportunity_id,
                        "event": event,
                        "status": bundle.status,
                        "actions": actions,
                    }));
                }
            }
            Err(error) => {
                report.rejected_count += 1;
                report.order_events.push(json!({
                    "event_type": "spot_futures_order_submit_error",
                    "bundle_id": bundle.bundle_id,
                    "opportunity_id": bundle.opportunity_id,
                    "client_order_id": client_order_id,
                    "error": error.to_string(),
                    "received_at": Utc::now(),
                }));
                report.readback_events.push(json!({
                    "event_type": "spot_futures_spot_maker_readback_plan",
                    "bundle_id": bundle.bundle_id,
                    "opportunity_id": bundle.opportunity_id,
                    "client_order_id": client_order_id,
                    "ack_accepted": false,
                    "requires_query_order": false,
                    "requires_recent_fills": false,
                    "requires_cancel_after_ttl": false,
                    "will_submit_perp_hedge_after_confirmed_fill": false,
                    "live_readback_hedge_enabled": config.execution.enable_spot_fill_readback_hedge,
                    "status": "submit_error_no_readback",
                    "error": error.to_string(),
                }));
            }
        }
    }
    if report.planned_count < candidate_count {
        report.skipped_count = candidate_count - report.planned_count;
    }
    report
}

async fn perform_spot_maker_readback_and_hedge(
    args: &Args,
    ctx: &StrategyContext,
    gateway: &LocalGatewayClient,
    execution: Arc<dyn StrategyExecutionClient>,
    config: &SpotFuturesArbitrageConfig,
    opportunity: &SpotFuturesOpportunity,
    bundle: &mut SpotFuturesBundle,
    spot_client_order_id: &str,
    spot_execution_order_id: Option<&str>,
    report: &mut SpotFuturesExecutionCycleReport,
) {
    if config.execution.spot_maker_order_ttl_ms > 0 {
        tokio::time::sleep(Duration::from_millis(
            config.execution.spot_maker_order_ttl_ms,
        ))
        .await;
    }
    let now = Utc::now();
    let cancel = ExecutionCancelCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: Some(spot_client_order_id.to_string()),
        execution_order_id: spot_execution_order_id.map(ToString::to_string),
        idempotency_key: format!(
            "{}:{}:{}:cancel",
            STRATEGY_KIND,
            ctx.run_id(),
            spot_client_order_id
        ),
        risk_profile_id: rustcta_strategy_spot_futures_arbitrage::DEFAULT_RISK_PROFILE_ID
            .to_string(),
        requested_at: now,
        exchange_id: opportunity.route.spot_exchange.clone(),
        symbol: opportunity.route.canonical_symbol.as_pair(),
        metadata: BTreeMap::from([
            ("strategy_kind".to_string(), json!(STRATEGY_KIND)),
            ("market_type".to_string(), json!("spot")),
            ("role".to_string(), json!("spot_maker_cancel_after_ttl")),
            ("bundle_id".to_string(), json!(bundle.bundle_id)),
            ("opportunity_id".to_string(), json!(bundle.opportunity_id)),
        ]),
    };
    let cancel_ack = execution.cancel_order(cancel).await;
    report.readback_events.push(json!({
        "event_type": "spot_futures_spot_maker_cancel_after_ttl",
        "bundle_id": bundle.bundle_id,
        "opportunity_id": bundle.opportunity_id,
        "client_order_id": spot_client_order_id,
        "result": secret_free_result_json(&cancel_ack),
        "occurred_at": Utc::now(),
    }));

    let symbol_scope = match symbol_scope(
        &opportunity.route.spot_exchange,
        MarketType::Spot,
        &opportunity.route.canonical_symbol,
    ) {
        Ok(symbol) => symbol,
        Err(error) => {
            report.readback_events.push(json!({
                "event_type": "spot_futures_spot_maker_readback_error",
                "bundle_id": bundle.bundle_id,
                "opportunity_id": bundle.opportunity_id,
                "client_order_id": spot_client_order_id,
                "error": error.to_string(),
            }));
            return;
        }
    };
    let account_id = AccountId::new(args.account_id.clone()).ok();
    let tenant_id = match TenantId::new(args.tenant_id.clone()).map_sdk_err() {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            report.readback_events.push(json!({
                "event_type": "spot_futures_spot_maker_readback_error",
                "bundle_id": bundle.bundle_id,
                "opportunity_id": bundle.opportunity_id,
                "client_order_id": spot_client_order_id,
                "error": error.to_string(),
            }));
            return;
        }
    };

    let query_request_id = request_id("sf-query-order");
    let query_result = gateway
        .query_order(
            query_request_id.clone(),
            tenant_id.clone(),
            account_id.clone(),
            QueryOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, query_request_id),
                symbol: symbol_scope.clone(),
                client_order_id: Some(spot_client_order_id.to_string()),
                exchange_order_id: spot_execution_order_id.map(ToString::to_string),
            },
        )
        .await;
    let order_filled_qty = query_result
        .as_ref()
        .ok()
        .and_then(|response| response.order.as_ref())
        .and_then(|order| parse_f64_optional(&order.filled_quantity))
        .unwrap_or(0.0);

    let fills_request_id = request_id("sf-recent-fills");
    let fills_result = gateway
        .get_recent_fills(
            fills_request_id.clone(),
            tenant_id,
            account_id,
            RecentFillsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, fills_request_id),
                exchange: symbol_scope.exchange.clone(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope),
                client_order_id: Some(spot_client_order_id.to_string()),
                exchange_order_id: spot_execution_order_id.map(ToString::to_string),
                from_trade_id: None,
                start_time: Some(bundle.opened_at),
                end_time: Some(Utc::now()),
                limit: Some(100),
                page: None,
            },
        )
        .await;
    let fills_qty: f64 = fills_result
        .as_ref()
        .ok()
        .map(|response| response.fills.iter().map(|fill| fill.quantity).sum())
        .unwrap_or(0.0);
    let filled_qty = order_filled_qty.max(fills_qty);
    report.readback_events.push(json!({
        "event_type": "spot_futures_spot_maker_readback_resolved",
        "bundle_id": bundle.bundle_id,
        "opportunity_id": bundle.opportunity_id,
        "client_order_id": spot_client_order_id,
        "execution_order_id": spot_execution_order_id,
        "order_filled_qty": order_filled_qty,
        "fills_qty": fills_qty,
        "confirmed_filled_qty": filled_qty,
        "query_result": secret_free_result_json(&query_result),
        "fills_result": secret_free_result_json(&fills_result),
        "occurred_at": Utc::now(),
    }));
    if filled_qty < config.execution.min_hedge_base_qty.max(0.0) || filled_qty <= 0.0 {
        report.readback_events.push(json!({
            "event_type": "spot_futures_perp_hedge_skipped",
            "bundle_id": bundle.bundle_id,
            "opportunity_id": bundle.opportunity_id,
            "reason": "no_confirmed_spot_fill",
            "confirmed_filled_qty": filled_qty,
        }));
        return;
    }

    let event = SpotFuturesBundleEvent::SpotMakerFill {
        filled_base_qty: filled_qty,
        cumulative_base_qty: filled_qty,
        final_fill: true,
        occurred_at: Utc::now(),
    };
    let actions = rustcta_strategy_spot_futures_arbitrage::apply_spot_futures_bundle_event(
        bundle,
        event.clone(),
        &opportunity.hedge_after_fill.hedge_order,
        config.execution.min_hedge_base_qty,
        None,
    );
    report.bundle_events.push(json!({
        "event_type": "spot_futures_bundle_event",
        "bundle_id": bundle.bundle_id,
        "opportunity_id": bundle.opportunity_id,
        "event": event,
        "status": bundle.status,
        "actions": actions,
    }));

    let mut hedge_order = opportunity.hedge_after_fill.hedge_order.clone();
    hedge_order.base_quantity = filled_qty;
    let hedge_client_order_id = format!("{}-perp-hedge", bundle.bundle_id);
    let mut hedge_command = spot_futures_order_draft_to_execution_command(
        ctx,
        &hedge_client_order_id,
        &hedge_order,
        Utc::now(),
    );
    hedge_command
        .metadata
        .insert("bundle_id".to_string(), json!(bundle.bundle_id));
    hedge_command
        .metadata
        .insert("opportunity_id".to_string(), json!(bundle.opportunity_id));
    report.plans.push(json!({
        "event_type": "spot_futures_order_plan",
        "bundle_id": bundle.bundle_id,
        "opportunity_id": bundle.opportunity_id,
        "client_order_id": hedge_client_order_id,
        "role": hedge_command.metadata.get("role").cloned().unwrap_or(Value::Null),
        "exchange": hedge_command.exchange_id,
        "symbol": hedge_command.symbol,
        "side": hedge_command.side,
        "order_type": hedge_command.order_type,
        "time_in_force": hedge_command.time_in_force,
        "reduce_only": hedge_command.reduce_only,
        "quantity": hedge_command.quantity,
        "price": hedge_command.price,
        "trigger": "confirmed_spot_fill_readback",
    }));
    match execution.submit_order(hedge_command).await {
        Ok(ack) => {
            report.order_events.push(json!({
                "event_type": "spot_futures_order_ack",
                "bundle_id": bundle.bundle_id,
                "opportunity_id": bundle.opportunity_id,
                "client_order_id": ack.client_order_id,
                "accepted": ack.accepted,
                "execution_order_id": ack.execution_order_id,
                "reason": ack.reason,
                "received_at": ack.received_at,
                "role": "perp_taker_short_hedge",
            }));
            let event = if ack.accepted {
                SpotFuturesBundleEvent::PerpHedgeSubmitted {
                    client_order_id: ack.client_order_id.clone(),
                    occurred_at: ack.received_at,
                }
            } else {
                SpotFuturesBundleEvent::PerpHedgeRejected {
                    reason: ack
                        .reason
                        .clone()
                        .unwrap_or_else(|| "perp hedge rejected".to_string()),
                    occurred_at: ack.received_at,
                }
            };
            let actions = rustcta_strategy_spot_futures_arbitrage::apply_spot_futures_bundle_event(
                bundle,
                event.clone(),
                &opportunity.hedge_after_fill.hedge_order,
                config.execution.min_hedge_base_qty,
                None,
            );
            report.bundle_events.push(json!({
                "event_type": "spot_futures_bundle_event",
                "bundle_id": bundle.bundle_id,
                "opportunity_id": bundle.opportunity_id,
                "event": event,
                "status": bundle.status,
                "actions": actions,
            }));
        }
        Err(error) => {
            report.order_events.push(json!({
                "event_type": "spot_futures_order_submit_error",
                "bundle_id": bundle.bundle_id,
                "opportunity_id": bundle.opportunity_id,
                "client_order_id": hedge_client_order_id,
                "role": "perp_taker_short_hedge",
                "error": error.to_string(),
                "received_at": Utc::now(),
            }));
        }
    }
}

fn execution_block_reason(
    execution_mode: ExecutionMode,
    config: &SpotFuturesArbitrageConfig,
    reconcile: Option<&SpotFuturesReconcileReport>,
) -> Option<String> {
    if matches!(execution_mode, ExecutionMode::Noop) {
        return Some("execution_mode_noop".to_string());
    }
    if matches!(execution_mode, ExecutionMode::RouterLive)
        && !config.execution.allow_live_order_submission
    {
        return Some("execution_allow_live_order_submission_false".to_string());
    }
    if matches!(execution_mode, ExecutionMode::RouterLive)
        && !config.execution.enable_spot_fill_readback_hedge
    {
        return Some("execution_spot_fill_readback_hedge_disabled".to_string());
    }
    if config.mode.eq_ignore_ascii_case("observe") {
        return Some("config_mode_observe".to_string());
    }
    if config.risk.start_paused_new_entries {
        return Some("risk_start_paused_new_entries".to_string());
    }
    if config.risk.start_close_only {
        return Some("risk_start_close_only".to_string());
    }
    if config.risk.orphan_exposure_blocks_new_entries {
        if let Some(reconcile) = reconcile {
            if reconcile.blocks_new_entries {
                return Some("reconcile_blocks_new_entries".to_string());
            }
        } else {
            return Some("reconcile_missing".to_string());
        }
    }
    None
}

async fn write_scan_jsonl(
    dashboard_snapshot_path: &PathBuf,
    preflight: Option<&SpotFuturesPreflightReport>,
    reconcile: Option<&SpotFuturesReconcileReport>,
    market_scan: Option<&MarketScanReport>,
    execution_cycle: &SpotFuturesExecutionCycleReport,
) -> Result<()> {
    let Some(dir) = dashboard_snapshot_path.parent() else {
        return Ok(());
    };
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))?;
    if let Some(preflight) = preflight {
        append_jsonl(
            &dir.join("preflight_reports.jsonl"),
            &json!({
                "event_type": "spot_futures_preflight_report",
                "preflight": preflight,
            }),
        )
        .await?;
    }
    if let Some(reconcile) = reconcile {
        append_jsonl(
            &dir.join("reconcile_reports.jsonl"),
            &json!({
                "event_type": "spot_futures_reconcile_report",
                "reconcile": reconcile,
            }),
        )
        .await?;
    }
    if let Some(scan) = market_scan {
        append_jsonl(
            &dir.join("scan_summaries.jsonl"),
            &json!({
                "event_type": "spot_futures_scan_summary",
                "scanned_at": scan.scanned_at,
                "route_count": scan.route_count,
                "accepted_count": scan.accepted_count,
                "rejected_count": scan.rejected_count,
                "error_count": scan.error_count,
                "reference_price_count": scan.reference_prices.len(),
                "errors": scan.errors,
            }),
        )
        .await?;
        for audit in &scan.audits {
            append_jsonl(
                &dir.join("opportunities.jsonl"),
                &json!({
                    "event_type": "spot_futures_opportunity_evaluated",
                    "scanned_at": scan.scanned_at,
                    "audit": audit,
                }),
            )
            .await?;
        }
        for opportunity in &scan.opportunities {
            append_jsonl(
                &dir.join("opportunities.jsonl"),
                &json!({
                    "event_type": "spot_futures_signal_accepted",
                    "scanned_at": scan.scanned_at,
                    "opportunity": opportunity,
                }),
            )
            .await?;
        }
    }
    append_jsonl(
        &dir.join("execution_cycles.jsonl"),
        &json!({
            "event_type": "spot_futures_execution_cycle",
            "cycle": execution_cycle,
        }),
    )
    .await?;
    for plan in &execution_cycle.plans {
        append_jsonl(&dir.join("order_commands.jsonl"), plan).await?;
    }
    for event in &execution_cycle.order_events {
        append_jsonl(&dir.join("order_events.jsonl"), event).await?;
    }
    for event in &execution_cycle.bundle_events {
        append_jsonl(&dir.join("bundle_events.jsonl"), event).await?;
    }
    for event in &execution_cycle.readback_events {
        append_jsonl(&dir.join("readback_events.jsonl"), event).await?;
    }
    Ok(())
}

async fn append_jsonl(path: &PathBuf, value: &Value) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(serde_json::to_string(value)?.as_bytes())
        .await
        .with_context(|| format!("write {}", path.display()))?;
    file.write_all(b"\n")
        .await
        .with_context(|| format!("write newline {}", path.display()))?;
    Ok(())
}

async fn write_dashboard_snapshot(
    args: &Args,
    report: &RuntimeReport,
    path: &PathBuf,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let payload = report.snapshot.get("payload").unwrap_or(&report.snapshot);
    let envelope = json!({
        "schema_version": 1,
        "strategy_id": &args.strategy_id,
        "strategy_kind": STRATEGY_KIND,
        "run_id": &args.run_id,
        "status": null,
        "generated_at": report.generated_at,
        "source": "typed_runtime_snapshot",
        "detail": {
            "root_free_runtime": report.root_free_runtime,
            "live_orders_enabled": report.live_orders_enabled,
            "concrete_exchange_adapter_loaded": report.concrete_exchange_adapter_loaded,
            "execution_mode": report.execution_mode,
            "gateway_adapters": report.gateway_adapters,
            "preflight": report.preflight,
            "reconcile": report.reconcile,
            "market_scan": report.market_scan,
            "execution_cycle": report.execution_cycle,
            "config_path": report.config_path,
            "runtime": report.snapshot,
            "migrated_from": report
                .snapshot
                .get("migrated_from")
                .or_else(|| payload.get("migrated_from"))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            "settings": report
                .snapshot
                .get("settings")
                .or_else(|| payload.get("settings"))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            "active_symbols": report
                .snapshot
                .get("active_symbols")
                .or_else(|| payload.get("active_symbols"))
                .cloned()
                .unwrap_or_else(|| json!([])),
            "market_data_subscriptions": report
                .snapshot
                .get("market_data_subscriptions")
                .or_else(|| payload.get("market_data_subscriptions"))
                .cloned()
                .unwrap_or_else(|| json!([])),
            "runtime_contract": report
                .snapshot
                .get("runtime_contract")
                .or_else(|| payload.get("runtime_contract"))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        }
    });
    let mut temp_path = path.clone();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dashboard_snapshot.json");
    temp_path.set_file_name(format!("{file_name}.tmp"));
    tokio::fs::write(&temp_path, serde_json::to_vec_pretty(&envelope)?)
        .await
        .with_context(|| format!("write {}", temp_path.display()))?;
    tokio::fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("rename {} to {}", temp_path.display(), path.display()))?;
    Ok(())
}

struct RouterBackedSpotFuturesExecutionClient {
    router: Arc<LocalExecutionRouter>,
    account_by_exchange: BTreeMap<String, String>,
}

#[async_trait]
impl StrategyExecutionClient for RouterBackedSpotFuturesExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let order = self.map_order_command(command)?;
        let ack = self
            .router
            .place_order(order)
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        Ok(ExecutionOrderAck {
            schema_version,
            accepted: ack.accepted,
            client_order_id,
            execution_order_id: ack.exchange_order_id,
            reason: ack.message,
            received_at: ack.acknowledged_at,
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let execution_order_id = command.execution_order_id.clone();
        let cancel = self.map_cancel_command(command)?;
        let ack = self
            .router
            .cancel_order(cancel)
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        Ok(ExecutionCancelAck {
            schema_version,
            accepted: ack.accepted,
            client_order_id,
            execution_order_id,
            reason: ack.message,
            received_at: ack.acknowledged_at,
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("raw intents are not routed by spot-futures execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

impl RouterBackedSpotFuturesExecutionClient {
    fn map_order_command(&self, command: ExecutionOrderCommand) -> SdkResult<OrderCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = ExchangeId::new(exchange.clone()).map_sdk_err()?;
        let market_type = gateway_market_type(&command.metadata)?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            exchange_symbol_text(&exchange, market_type, &canonical_symbol),
        )
        .map_sdk_err()?;
        let account_id = self
            .account_by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or(command.account_id);
        let identity = mutation_identity(
            command.tenant_id,
            account_id,
            command.strategy_id,
            command.run_id,
            command.idempotency_key,
            command.risk_profile_id,
            command.requested_at,
        )?;
        let order_type = order_type(command.order_type)?;
        let time_in_force = command
            .time_in_force
            .map(time_in_force)
            .transpose()?
            .unwrap_or_else(|| default_time_in_force(order_type));
        let quantity = parse_positive_f64("quantity", &command.quantity)?;
        let price = command
            .price
            .as_deref()
            .map(|value| parse_positive_f64("price", value))
            .transpose()?;
        let mut order = OrderCommand::new(
            identity,
            command.client_order_id.clone(),
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol,
            command.client_order_id.clone(),
            order_side(command.side),
            position_side(&command.metadata),
            order_type,
            time_in_force,
            quantity,
            price,
        );
        order.reduce_only = command.reduce_only;
        order.post_only = matches!(order.order_type, OrderType::PostOnly);
        order.max_slippage_bps = Some(5);
        order.correlation_id = command
            .metadata
            .get("bundle_id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        Ok(order)
    }

    fn map_cancel_command(&self, command: ExecutionCancelCommand) -> SdkResult<CancelCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = ExchangeId::new(exchange.clone()).map_sdk_err()?;
        let market_type = gateway_market_type(&command.metadata)?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            exchange_symbol_text(&exchange, market_type, &canonical_symbol),
        )
        .map_sdk_err()?;
        let account_id = self
            .account_by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or(command.account_id);
        let identity = mutation_identity(
            command.tenant_id,
            account_id,
            command.strategy_id,
            command.run_id,
            command.idempotency_key,
            command.risk_profile_id,
            command.requested_at,
        )?;
        let cancellation_ids = match (
            command.client_order_id.clone(),
            command.execution_order_id.clone(),
        ) {
            (Some(client_order_id), _) => CancellationIds::by_client_order_id(client_order_id),
            (None, Some(exchange_order_id)) => {
                CancellationIds::by_exchange_order_id(exchange_order_id)
            }
            (None, None) => {
                return Err(StrategySdkError::InvalidCommand(
                    "cancel command requires client_order_id or execution_order_id".to_string(),
                ));
            }
        };
        let command_id = command
            .client_order_id
            .clone()
            .or(command.execution_order_id.clone())
            .map(|id| format!("cancel:{id}"))
            .unwrap_or_else(|| "cancel:unknown".to_string());
        Ok(CancelCommand::new(
            identity,
            command_id,
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol,
            cancellation_ids,
        ))
    }
}

fn gateway_market_type(metadata: &BTreeMap<String, Value>) -> SdkResult<MarketType> {
    match metadata
        .get("market_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "spot" => Ok(MarketType::Spot),
        "perpetual" => Ok(MarketType::Perpetual),
        other => Err(StrategySdkError::InvalidCommand(format!(
            "spot-futures command requires metadata.market_type spot|perpetual, got {other:?}"
        ))),
    }
}

fn mutation_identity(
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    idempotency_key: String,
    risk_profile_id: String,
    requested_at: chrono::DateTime<Utc>,
) -> SdkResult<MutationIdentity> {
    Ok(MutationIdentity {
        tenant_id: TenantId::new(tenant_id).map_sdk_err()?,
        account_id: AccountId::new(account_id).map_sdk_err()?,
        strategy_id: StrategyId::new(strategy_id).map_sdk_err()?,
        run_id: RunId::new(run_id).map_sdk_err()?,
        idempotency_key,
        risk_profile_id,
        requested_at,
    })
}

fn gateway_exchange_id(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" | "gate_io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn exchange_symbol_text(
    exchange: &str,
    market_type: MarketType,
    canonical_symbol: &CanonicalSymbol,
) -> String {
    let base = canonical_symbol.base_asset();
    let quote = canonical_symbol.quote_asset();
    match (exchange, market_type) {
        ("gateio", _) => format!("{base}_{quote}"),
        ("okx", MarketType::Spot) => format!("{base}-{quote}"),
        ("okx", MarketType::Perpetual) => format!("{base}-{quote}-SWAP"),
        _ => format!("{base}{quote}"),
    }
}

fn order_side(side: rustcta_strategy_sdk::OrderSide) -> OrderSide {
    match side {
        rustcta_strategy_sdk::OrderSide::Buy => OrderSide::Buy,
        rustcta_strategy_sdk::OrderSide::Sell => OrderSide::Sell,
    }
}

fn order_type(order_type: rustcta_strategy_sdk::OrderType) -> SdkResult<OrderType> {
    Ok(match order_type {
        rustcta_strategy_sdk::OrderType::Market => OrderType::Market,
        rustcta_strategy_sdk::OrderType::Limit => OrderType::Limit,
        rustcta_strategy_sdk::OrderType::PostOnly => OrderType::PostOnly,
        rustcta_strategy_sdk::OrderType::ImmediateOrCancel => OrderType::IOC,
        rustcta_strategy_sdk::OrderType::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            OrderType::IOC
        }
        rustcta_strategy_sdk::OrderType::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom order type: {value}"
            )));
        }
    })
}

fn time_in_force(time_in_force: rustcta_strategy_sdk::TimeInForce) -> SdkResult<TimeInForce> {
    Ok(match time_in_force {
        rustcta_strategy_sdk::TimeInForce::GoodTilCanceled => TimeInForce::GTC,
        rustcta_strategy_sdk::TimeInForce::ImmediateOrCancel => TimeInForce::IOC,
        rustcta_strategy_sdk::TimeInForce::FillOrKill => TimeInForce::FOK,
        rustcta_strategy_sdk::TimeInForce::PostOnly => TimeInForce::GTX,
        rustcta_strategy_sdk::TimeInForce::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            TimeInForce::IOC
        }
        rustcta_strategy_sdk::TimeInForce::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom time-in-force: {value}"
            )));
        }
    })
}

fn default_time_in_force(order_type: OrderType) -> TimeInForce {
    match order_type {
        OrderType::IOC => TimeInForce::IOC,
        OrderType::FOK => TimeInForce::FOK,
        OrderType::PostOnly => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

fn position_side(metadata: &BTreeMap<String, Value>) -> PositionSide {
    match metadata
        .get("position_side")
        .or_else(|| metadata.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" | "open_long" | "close_long" | "spot_maker_buy" | "spot_taker_close_sell" => {
            PositionSide::Long
        }
        "short"
        | "open_short"
        | "close_short"
        | "perp_taker_short_hedge"
        | "perp_reduce_only_close_buy" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_positive_f64(field: &str, value: &str) -> SdkResult<f64> {
    let parsed = value.parse::<f64>().map_err(|error| {
        StrategySdkError::InvalidCommand(format!("{field} must be a number: {error}"))
    })?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(StrategySdkError::InvalidCommand(format!(
            "{field} must be positive"
        )))
    }
}

trait MapSdkErr<T> {
    fn map_sdk_err(self) -> SdkResult<T>;
}

impl<T, E> MapSdkErr<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn map_sdk_err(self) -> SdkResult<T> {
        self.map_err(|error| StrategySdkError::InvalidCommand(error.to_string()))
    }
}

struct NoopExecutionClient;

#[async_trait]
impl StrategyExecutionClient for NoopExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        Ok(ExecutionOrderAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: None,
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution_config() -> SpotFuturesArbitrageConfig {
        let mut config = SpotFuturesArbitrageConfig::default();
        config.mode = "paper".to_string();
        config.risk.start_paused_new_entries = false;
        config.risk.start_close_only = false;
        config
    }

    fn clean_reconcile() -> SpotFuturesReconcileReport {
        SpotFuturesReconcileReport {
            checked_at: Utc::now(),
            checked_symbol_count: 0,
            blocks_new_entries: false,
            residual_spot_total_notional_usdt: 0.0,
            residual_perp_total_notional_usdt: 0.0,
            spot_balances: Vec::new(),
            perp_positions: Vec::new(),
            residuals: Vec::new(),
            errors: Vec::new(),
        }
    }

    #[test]
    fn router_live_should_require_explicit_order_submission_gate() {
        let config = execution_config();

        assert_eq!(
            execution_block_reason(ExecutionMode::RouterLive, &config, None),
            Some("execution_allow_live_order_submission_false".to_string())
        );
    }

    #[test]
    fn router_live_should_require_spot_fill_readback_hedge_gate() {
        let mut config = execution_config();
        config.execution.allow_live_order_submission = true;

        assert_eq!(
            execution_block_reason(ExecutionMode::RouterLive, &config, None),
            Some("execution_spot_fill_readback_hedge_disabled".to_string())
        );
    }

    #[test]
    fn router_live_should_require_reconcile_when_all_live_gates_are_enabled() {
        let mut config = execution_config();
        config.execution.allow_live_order_submission = true;
        config.execution.enable_spot_fill_readback_hedge = true;

        assert_eq!(
            execution_block_reason(ExecutionMode::RouterLive, &config, None),
            Some("reconcile_missing".to_string())
        );
    }

    #[test]
    fn router_live_should_pass_when_live_gates_and_reconcile_are_clean() {
        let mut config = execution_config();
        let reconcile = clean_reconcile();
        config.execution.allow_live_order_submission = true;
        config.execution.enable_spot_fill_readback_hedge = true;

        assert_eq!(
            execution_block_reason(ExecutionMode::RouterLive, &config, Some(&reconcile)),
            None
        );
    }
}
