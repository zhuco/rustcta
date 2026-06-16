use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_hedged_grid::{
    core::{ExecutionConfig, GridConfig, PrecisionConfig, RiskLimits},
    hedged_grid_cancel_action_to_execution_command, hedged_grid_order_draft_to_execution_command,
    EngineAction, FeeConfig, FillEvent, FollowConfig, GridEngine, HedgedGridConfig,
    HedgedGridCoreConfig, HedgedGridRuntime, MarketSnapshot, PositionState, PriceReference,
    RiskReference, STRATEGY_KIND,
};
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionEvent, ExecutionIntent,
    ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, HttpStrategyExecutionClient,
    HttpStrategyPlatformClient, MarketType as SdkMarketType, RuntimeAccountConfigRequest,
    RuntimeFill, RuntimeMarketSnapshotRequest, RuntimeRecentFillsRequest, SdkResult,
    StrategyContext, StrategyEvent, StrategyExecutionClient, StrategyInstanceId, StrategyRuntime,
};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Debug)]
struct Args {
    config: PathBuf,
    strategy_id: String,
    run_id: String,
    tenant_id: String,
    account_id: String,
    once: bool,
    enable_live_orders: bool,
    execution_endpoint: Option<String>,
    snapshot_interval_ms: u64,
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
    snapshot: serde_json::Value,
}

struct RuntimeServices {
    execution: Arc<dyn StrategyExecutionClient>,
    platform: Option<HttpStrategyPlatformClient>,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
}

struct LiveGridDriver {
    config: HedgedGridConfig,
    execution_exchange: String,
    execution_market_type: SdkMarketType,
    engine: GridEngine,
    seen_fills: BTreeSet<String>,
    fill_start_time: DateTime<Utc>,
}

impl LiveGridDriver {
    async fn tick(
        &mut self,
        args: &Args,
        services: &RuntimeServices,
        ctx: &StrategyContext,
        runtime: &mut HedgedGridRuntime,
    ) -> Result<()> {
        let snapshot = load_live_market_snapshot(
            args,
            services,
            &self.config,
            &self.execution_exchange,
            &self.execution_market_type,
        )
        .await?;
        let fills = load_recent_fills(
            args,
            services,
            &self.config,
            &self.execution_exchange,
            &self.execution_market_type,
            self.fill_start_time,
        )
        .await?;
        for fill in fills {
            let fill_key = runtime_fill_key(&fill);
            if !self.seen_fills.insert(fill_key) {
                continue;
            }
            let Some(client_order_id) = fill.client_order_id.as_deref() else {
                continue;
            };
            let Some(record) = self.engine.order_record(client_order_id) else {
                continue;
            };
            let partial = record.filled_qty + fill.quantity + f64::EPSILON < record.qty;
            let fill_event = FillEvent {
                order_id: client_order_id.to_string(),
                intent: record.intent,
                fill_qty: fill.quantity,
                fill_price: fill.price,
                timestamp: fill.filled_at,
                partial,
            };
            let actions = self.engine.handle_fill(fill_event, &snapshot);
            execute_engine_actions(
                services,
                ctx,
                runtime,
                &self.config,
                &self.execution_exchange,
                &self.execution_market_type,
                &mut self.engine,
                actions,
                false,
            )
            .await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = read_yaml_config(&args.config)?;
    let services = runtime_services(&args, &config)?;
    let mut runtime = HedgedGridRuntime::new();
    let ctx = strategy_context(&args, config.clone(), Arc::clone(&services.execution));
    runtime.start(ctx.clone()).await?;
    let mut live_driver =
        bootstrap_initial_grid(&args, &config, &services, &ctx, &mut runtime).await?;
    emit_report(&args, &services, &runtime).await?;
    if args.once {
        runtime.stop().await?;
        return Ok(());
    }
    let mut interval = tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    loop {
        interval.tick().await;
        if let Some(driver) = live_driver.as_mut() {
            driver.tick(&args, &services, &ctx, &mut runtime).await?;
        }
        emit_report(&args, &services, &runtime).await?;
    }
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/hedged_grid.yml"),
        strategy_id: "hedged_grid_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        once: false,
        enable_live_orders: false,
        execution_endpoint: None,
        snapshot_interval_ms: 30_000,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--execution-endpoint" => {
                args.execution_endpoint = Some(next_value(&mut values, "--execution-endpoint")?)
            }
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--enable-live-orders" => args.enable_live_orders = true,
            "--help" | "-h" => {
                println!(
                    "hedged-grid-runtime --config <path> [--execution-endpoint <url>] [--enable-live-orders] [--once]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(args)
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

fn runtime_services(args: &Args, config: &Value) -> Result<RuntimeServices> {
    let dry_run = config
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let config_live_orders = config
        .get("enable_live_orders")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if dry_run || !(args.enable_live_orders || config_live_orders) {
        return Ok(RuntimeServices {
            execution: Arc::new(NoopExecutionClient),
            platform: None,
            live_orders_enabled: false,
            concrete_exchange_adapter_loaded: false,
        });
    }

    let _spot_exchange = config
        .get("spot_exchange")
        .and_then(Value::as_str)
        .context("live orders require config.spot_exchange")?;
    let _hedge_exchange = config
        .get("hedge_exchange")
        .and_then(Value::as_str)
        .context("live orders require config.hedge_exchange")?;
    let endpoint = args
        .execution_endpoint
        .as_deref()
        .or_else(|| config.get("execution_endpoint").and_then(Value::as_str))
        .unwrap_or("http://127.0.0.1:18081");

    Ok(RuntimeServices {
        execution: Arc::new(HttpStrategyExecutionClient::new(endpoint)),
        platform: Some(HttpStrategyPlatformClient::new(endpoint)),
        live_orders_enabled: true,
        concrete_exchange_adapter_loaded: false,
    })
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
    services: &RuntimeServices,
    runtime: &HedgedGridRuntime,
) -> Result<()> {
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        root_free_runtime: !services.live_orders_enabled,
        live_orders_enabled: services.live_orders_enabled,
        concrete_exchange_adapter_loaded: services.concrete_exchange_adapter_loaded,
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    println!("{}", serde_json::to_string(&json!(report))?);
    Ok(())
}

async fn bootstrap_initial_grid(
    args: &Args,
    config_value: &Value,
    services: &RuntimeServices,
    ctx: &StrategyContext,
    runtime: &mut HedgedGridRuntime,
) -> Result<Option<LiveGridDriver>> {
    if !services.live_orders_enabled {
        return Ok(None);
    }
    let execution_market_type = live_execution_market_type(config_value)?;
    let config: HedgedGridConfig =
        serde_json::from_value(config_value.clone()).context("parse hedged-grid live config")?;
    let execution_exchange = live_execution_exchange(&config, &execution_market_type).to_string();
    let (market, snapshot) = load_live_market_snapshot_with_rules(
        args,
        services,
        &config,
        &execution_exchange,
        &execution_market_type,
    )
    .await?;
    ensure_live_account_ready(
        args,
        services,
        &config,
        &execution_exchange,
        &execution_market_type,
    )
    .await?;
    let core_config = core_config_from_runtime_config(&config, &market)?;
    let position = PositionState {
        equity: config
            .max_inventory_quote
            .parse::<f64>()
            .unwrap_or(0.0)
            .max(1.0),
        maintenance_margin: 0.0,
        mark_price: snapshot.mark_price,
        ..PositionState::default()
    };
    let mut engine = GridEngine::new(core_config, true).map_err(|error| anyhow::anyhow!(error))?;
    engine.update_position(position);
    let actions = engine.rebuild_grid(&snapshot);
    execute_engine_actions(
        services,
        ctx,
        runtime,
        &config,
        &execution_exchange,
        &execution_market_type,
        &mut engine,
        actions,
        true,
    )
    .await?;
    Ok(Some(LiveGridDriver {
        config,
        execution_exchange,
        execution_market_type,
        engine,
        seen_fills: BTreeSet::new(),
        fill_start_time: Utc::now(),
    }))
}

async fn load_live_market_snapshot(
    args: &Args,
    services: &RuntimeServices,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
) -> Result<MarketSnapshot> {
    let (_, snapshot) = load_live_market_snapshot_with_rules(
        args,
        services,
        config,
        execution_exchange,
        execution_market_type,
    )
    .await?;
    Ok(snapshot)
}

async fn load_live_market_snapshot_with_rules(
    args: &Args,
    services: &RuntimeServices,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
) -> Result<(rustcta_strategy_sdk::RuntimeMarketSnapshot, MarketSnapshot)> {
    let platform = services
        .platform
        .as_ref()
        .context("live runtime missing platform client")?;
    let market = platform
        .market_snapshot(RuntimeMarketSnapshotRequest {
            schema_version: 1,
            tenant_id: args.tenant_id.clone(),
            account_id: args.account_id.clone(),
            run_id: args.run_id.clone(),
            exchange_id: execution_exchange.to_string(),
            symbol: config.symbol.clone(),
            market_type: execution_market_type.clone(),
            requested_at: Utc::now(),
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    let snapshot = MarketSnapshot {
        best_bid: market.best_bid,
        best_ask: market.best_ask,
        last_price: market.last_price,
        mark_price: market.mark_price,
        timestamp: market.received_at,
    };
    Ok((market, snapshot))
}

async fn ensure_live_account_ready(
    args: &Args,
    services: &RuntimeServices,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
) -> Result<()> {
    if !matches!(
        execution_market_type,
        SdkMarketType::Perpetual | SdkMarketType::Futures | SdkMarketType::Option
    ) {
        return Ok(());
    }
    let platform = services
        .platform
        .as_ref()
        .context("live runtime missing platform client")?;
    let account_config = platform
        .account_config(RuntimeAccountConfigRequest {
            schema_version: 1,
            tenant_id: args.tenant_id.clone(),
            account_id: args.account_id.clone(),
            run_id: args.run_id.clone(),
            exchange_id: execution_exchange.to_string(),
            symbol: config.symbol.clone(),
            market_type: execution_market_type.clone(),
            requested_at: Utc::now(),
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    match account_config
        .position_mode
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("hedge") => Ok(()),
        Some(mode) => bail!(
            "live hedged-grid requires hedge position mode on {} {}, current mode is {mode}",
            execution_exchange,
            config.symbol
        ),
        None => bail!(
            "live hedged-grid requires hedge position mode on {} {}, but gateway did not return position_mode",
            execution_exchange,
            config.symbol
        ),
    }
}

async fn load_recent_fills(
    args: &Args,
    services: &RuntimeServices,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
    start_time: DateTime<Utc>,
) -> Result<Vec<RuntimeFill>> {
    let platform = services
        .platform
        .as_ref()
        .context("live runtime missing platform client")?;
    Ok(platform
        .recent_fills(RuntimeRecentFillsRequest {
            schema_version: 1,
            tenant_id: args.tenant_id.clone(),
            account_id: args.account_id.clone(),
            run_id: args.run_id.clone(),
            exchange_id: execution_exchange.to_string(),
            symbol: config.symbol.clone(),
            market_type: execution_market_type.clone(),
            start_time: Some(start_time),
            limit: Some(100),
            requested_at: Utc::now(),
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?
        .fills)
}

async fn execute_engine_actions(
    services: &RuntimeServices,
    ctx: &StrategyContext,
    runtime: &mut HedgedGridRuntime,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
    engine: &mut GridEngine,
    actions: Vec<EngineAction>,
    rollback_on_failure: bool,
) -> Result<()> {
    let mut accepted_orders: Vec<(ExecutionOrderCommand, ExecutionOrderAck)> = Vec::new();
    for action in actions {
        match action {
            EngineAction::Place(draft) => {
                let mut command = hedged_grid_order_draft_to_execution_command(
                    ctx,
                    execution_exchange,
                    &config.symbol,
                    "hedged-grid-live",
                    &draft,
                    Utc::now(),
                );
                command.metadata.insert(
                    "market_type".to_string(),
                    json!(live_execution_market_type_label(execution_market_type)),
                );
                let ack = match services.execution.submit_order(command.clone()).await {
                    Ok(ack) => ack,
                    Err(error) => {
                        engine.handle_order_reject(&command.client_order_id);
                        if rollback_on_failure {
                            rollback_bootstrap_orders(services, &accepted_orders).await?;
                        }
                        return Err(anyhow::anyhow!(error)).with_context(|| {
                            format!("submit grid order {}", command.client_order_id)
                        });
                    }
                };
                record_execution_ack(runtime, "grid-place", &command.client_order_id, &ack).await?;
                if !ack.accepted {
                    engine.handle_order_reject(&command.client_order_id);
                    if rollback_on_failure {
                        rollback_bootstrap_orders(services, &accepted_orders).await?;
                    }
                    bail!(
                        "grid order {} rejected: {}",
                        ack.client_order_id,
                        ack.reason
                            .unwrap_or_else(|| "unknown rejection".to_string())
                    );
                }
                accepted_orders.push((command, ack));
            }
            EngineAction::Cancel { order_id, reason } => {
                let mut command = hedged_grid_cancel_action_to_execution_command(
                    ctx,
                    execution_exchange,
                    &config.symbol,
                    "hedged-grid-live",
                    &order_id,
                    &reason,
                    Utc::now(),
                );
                command.idempotency_key = format!("{}:cancel:{reason}", command.idempotency_key);
                command.metadata.insert(
                    "market_type".to_string(),
                    json!(live_execution_market_type_label(execution_market_type)),
                );
                let ack = services
                    .execution
                    .cancel_order(command.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
                    .with_context(|| format!("cancel grid order {order_id}"))?;
                record_cancel_ack(runtime, "grid-cancel", &order_id, &ack).await?;
                if !ack.accepted {
                    bail!(
                        "cancel grid order {order_id} rejected: {}",
                        ack.reason
                            .unwrap_or_else(|| "unknown rejection".to_string())
                    );
                }
            }
        }
    }
    Ok(())
}

async fn record_execution_ack(
    runtime: &mut HedgedGridRuntime,
    prefix: &str,
    client_order_id: &str,
    ack: &ExecutionOrderAck,
) -> Result<()> {
    runtime
        .handle_event(StrategyEvent::Execution(ExecutionEvent {
            schema_version: 1,
            event_id: format!("{prefix}-{client_order_id}"),
            client_order_id: Some(client_order_id.to_string()),
            occurred_at: ack.received_at,
            payload: serde_json::to_value(ack)?,
        }))
        .await?;
    Ok(())
}

async fn record_cancel_ack(
    runtime: &mut HedgedGridRuntime,
    prefix: &str,
    client_order_id: &str,
    ack: &ExecutionCancelAck,
) -> Result<()> {
    runtime
        .handle_event(StrategyEvent::Execution(ExecutionEvent {
            schema_version: 1,
            event_id: format!("{prefix}-{client_order_id}"),
            client_order_id: Some(client_order_id.to_string()),
            occurred_at: ack.received_at,
            payload: serde_json::to_value(ack)?,
        }))
        .await?;
    Ok(())
}

fn runtime_fill_key(fill: &RuntimeFill) -> String {
    fill.fill_id
        .clone()
        .or_else(|| {
            fill.order_id
                .as_ref()
                .map(|order_id| format!("{order_id}:{}:{}", fill.price, fill.quantity))
        })
        .or_else(|| {
            fill.client_order_id
                .as_ref()
                .map(|order_id| format!("{order_id}:{}:{}", fill.price, fill.quantity))
        })
        .unwrap_or_else(|| format!("{}:{}:{}", fill.filled_at, fill.price, fill.quantity))
}

fn live_execution_market_type(config: &Value) -> Result<SdkMarketType> {
    let raw = config
        .get("execution_market_type")
        .or_else(|| config.get("market_type"))
        .and_then(Value::as_str)
        .unwrap_or("perpetual")
        .trim()
        .to_ascii_lowercase();
    match raw.as_str() {
        "spot" => Ok(SdkMarketType::Spot),
        "margin" => Ok(SdkMarketType::Margin),
        "perpetual" | "perp" | "swap" => Ok(SdkMarketType::Perpetual),
        "futures" | "future" => Ok(SdkMarketType::Futures),
        "option" | "options" => Ok(SdkMarketType::Option),
        _ => bail!("unsupported execution_market_type {raw}"),
    }
}

fn live_execution_exchange<'a>(
    config: &'a HedgedGridConfig,
    market_type: &SdkMarketType,
) -> &'a str {
    match market_type {
        SdkMarketType::Spot | SdkMarketType::Margin => &config.spot_exchange,
        SdkMarketType::Perpetual | SdkMarketType::Futures | SdkMarketType::Option => {
            &config.hedge_exchange
        }
        SdkMarketType::Custom(_) => &config.hedge_exchange,
    }
}

fn live_execution_market_type_label(market_type: &SdkMarketType) -> &str {
    match market_type {
        SdkMarketType::Spot => "spot",
        SdkMarketType::Margin => "margin",
        SdkMarketType::Perpetual => "perpetual",
        SdkMarketType::Futures => "futures",
        SdkMarketType::Option => "option",
        SdkMarketType::Custom(value) => value.as_str(),
    }
}

async fn rollback_bootstrap_orders(
    services: &RuntimeServices,
    accepted_orders: &[(ExecutionOrderCommand, ExecutionOrderAck)],
) -> Result<()> {
    for (command, ack) in accepted_orders.iter().rev() {
        let mut metadata = command.metadata.clone();
        metadata.insert(
            "cancel_reason".to_string(),
            json!("bootstrap_rollback_after_rejection"),
        );
        let cancel = ExecutionCancelCommand {
            schema_version: command.schema_version,
            tenant_id: command.tenant_id.clone(),
            account_id: command.account_id.clone(),
            strategy_id: command.strategy_id.clone(),
            run_id: command.run_id.clone(),
            client_order_id: Some(command.client_order_id.clone()),
            execution_order_id: ack.execution_order_id.clone(),
            idempotency_key: format!("{}:bootstrap-rollback-cancel", command.idempotency_key),
            risk_profile_id: command.risk_profile_id.clone(),
            requested_at: Utc::now(),
            exchange_id: command.exchange_id.clone(),
            symbol: command.symbol.clone(),
            metadata,
        };
        let cancel_ack = services
            .execution
            .cancel_order(cancel)
            .await
            .map_err(|error| anyhow::anyhow!(error))
            .with_context(|| format!("cancel bootstrap order {}", command.client_order_id))?;
        if !cancel_ack.accepted {
            bail!(
                "cancel bootstrap order {} rejected: {}",
                command.client_order_id,
                cancel_ack
                    .reason
                    .unwrap_or_else(|| "unknown rejection".to_string())
            );
        }
    }
    Ok(())
}

fn core_config_from_runtime_config(
    config: &HedgedGridConfig,
    market: &rustcta_strategy_sdk::RuntimeMarketSnapshot,
) -> Result<HedgedGridCoreConfig> {
    let spacing_mode = config.grid_spacing_mode.trim().to_ascii_lowercase();
    let grid_spacing_abs = if spacing_mode == "abs" {
        Some(
            config
                .grid_spacing_abs
                .unwrap_or(config.grid_spacing_bps.max(1.0)),
        )
    } else {
        None
    };
    let grid_spacing_pct = if grid_spacing_abs.is_some() {
        0.0
    } else {
        config.grid_spacing_bps / 10_000.0
    };
    let max_inventory = config
        .max_inventory_quote
        .parse::<f64>()
        .context("max_inventory_quote must be numeric")?;
    Ok(HedgedGridCoreConfig {
        symbol: config.symbol.clone(),
        require_hedge_mode: true,
        price_reference: PriceReference::Mid,
        risk_reference: RiskReference::Mark,
        grid: GridConfig {
            levels_per_side: config.grid_order_count as usize,
            grid_spacing_pct,
            grid_spacing_abs,
            order_notional: config.order_notional_usdt,
            order_qty: None,
            fill_remaining_slots_with_opens: true,
            strict_pairing: false,
            refill_open_slots_enabled: true,
            normalize_open_grid_enabled: true,
            follow_open_enabled: true,
            repair_near_gap_enabled: true,
        },
        follow: FollowConfig::default(),
        execution: ExecutionConfig {
            cooldown_ms: 0,
            post_only: true,
            post_only_retries: 3,
        },
        precision: PrecisionConfig {
            tick_size: market.tick_size,
            step_size: market.step_size,
            min_qty: market.min_qty,
            min_notional: market.min_notional,
            price_digits: None,
            qty_digits: None,
        },
        fees: FeeConfig::default(),
        risk: RiskLimits {
            max_net_notional: max_inventory,
            max_total_notional: max_inventory,
            margin_ratio_limit: 0.8,
            funding_rate_limit: 1.0,
            funding_cost_limit: max_inventory,
        },
    })
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
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingExecutionClient {
        orders: Mutex<Vec<ExecutionOrderCommand>>,
        cancels: Mutex<Vec<ExecutionCancelCommand>>,
    }

    #[async_trait]
    impl StrategyExecutionClient for RecordingExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            self.orders
                .lock()
                .expect("orders lock poisoned")
                .push(command.clone());
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: Some("accepted-order".to_string()),
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            self.cancels
                .lock()
                .expect("cancels lock poisoned")
                .push(command.clone());
            Ok(ExecutionCancelAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: intent.schema_version,
                accepted: false,
                intent_kind: intent.intent_kind,
                reason: Some("raw intents are not used by runtime tests".to_string()),
                received_at: Utc::now(),
                payload: json!({}),
            })
        }
    }

    #[tokio::test]
    async fn live_engine_actions_should_submit_initial_orders_and_refill_after_fill() -> Result<()>
    {
        let execution = Arc::new(RecordingExecutionClient::default());
        let services = RuntimeServices {
            execution: execution.clone(),
            platform: None,
            live_orders_enabled: true,
            concrete_exchange_adapter_loaded: false,
        };
        let args = Args {
            config: PathBuf::from("memory.yml"),
            strategy_id: "hedged_grid_SOL".to_string(),
            run_id: "test-run".to_string(),
            tenant_id: "local".to_string(),
            account_id: "default".to_string(),
            once: true,
            enable_live_orders: true,
            execution_endpoint: None,
            snapshot_interval_ms: 1000,
        };
        let config = HedgedGridConfig {
            symbol: "SOLUSDC".to_string(),
            spot_exchange: "binance".to_string(),
            hedge_exchange: "binance".to_string(),
            grid_spacing_bps: 25.0,
            grid_spacing_mode: "pct".to_string(),
            grid_spacing_abs: None,
            grid_order_count: 1,
            order_notional_usdt: 5.5,
            max_inventory_quote: "22".to_string(),
            dry_run: false,
        };
        let ctx = strategy_context(
            &args,
            json!({
                "symbol": config.symbol.clone(),
                "spot_exchange": config.spot_exchange.clone(),
                "hedge_exchange": config.hedge_exchange.clone(),
                "grid_spacing_bps": config.grid_spacing_bps,
                "grid_spacing_mode": config.grid_spacing_mode.clone(),
                "grid_order_count": config.grid_order_count,
                "order_notional_usdt": config.order_notional_usdt,
                "max_inventory_quote": config.max_inventory_quote.clone(),
                "dry_run": config.dry_run,
                "execution_market_type": "perpetual",
            }),
            execution.clone(),
        );
        let mut runtime = HedgedGridRuntime::new();
        runtime.start(ctx.clone()).await?;
        let market = rustcta_strategy_sdk::RuntimeMarketSnapshot {
            schema_version: 1,
            exchange_id: "binance".to_string(),
            symbol: "SOLUSDC".to_string(),
            market_type: SdkMarketType::Perpetual,
            best_bid: 75.0,
            best_ask: 75.1,
            last_price: 75.05,
            mark_price: 75.05,
            tick_size: 0.01,
            step_size: 0.01,
            min_qty: Some(0.01),
            min_notional: Some(5.0),
            received_at: Utc::now(),
        };
        let snapshot = MarketSnapshot {
            best_bid: market.best_bid,
            best_ask: market.best_ask,
            last_price: market.last_price,
            mark_price: market.mark_price,
            timestamp: market.received_at,
        };
        let core_config = core_config_from_runtime_config(&config, &market)?;
        let mut engine =
            GridEngine::new(core_config, true).map_err(|error| anyhow::anyhow!(error))?;
        engine.update_position(PositionState {
            equity: 22.0,
            mark_price: snapshot.mark_price,
            ..PositionState::default()
        });

        let initial_actions = engine.rebuild_grid(&snapshot);
        execute_engine_actions(
            &services,
            &ctx,
            &mut runtime,
            &config,
            "binance",
            &SdkMarketType::Perpetual,
            &mut engine,
            initial_actions,
            true,
        )
        .await?;

        let initial_order_count = execution.orders.lock().expect("orders lock poisoned").len();
        assert_eq!(initial_order_count, 2);
        assert!(execution
            .orders
            .lock()
            .expect("orders lock poisoned")
            .iter()
            .all(
                |order| order.metadata.get("market_type").and_then(Value::as_str)
                    == Some("perpetual")
            ));

        let filled = engine
            .buy_orders()
            .into_iter()
            .next()
            .context("expected initial buy order")?;
        let refill_actions = engine.handle_fill(
            FillEvent {
                order_id: filled.id,
                intent: filled.intent,
                fill_qty: filled.qty,
                fill_price: filled.price,
                timestamp: Utc::now(),
                partial: false,
            },
            &snapshot,
        );
        execute_engine_actions(
            &services,
            &ctx,
            &mut runtime,
            &config,
            "binance",
            &SdkMarketType::Perpetual,
            &mut engine,
            refill_actions,
            false,
        )
        .await?;

        let final_order_count = execution.orders.lock().expect("orders lock poisoned").len();
        assert!(
            final_order_count > initial_order_count,
            "fill handling should submit at least one replacement/close order"
        );
        Ok(())
    }
}
