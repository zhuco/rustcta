use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{SinkExt, StreamExt};
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
    HttpStrategyPlatformClient, MarketType as SdkMarketType, OrderSide as SdkOrderSide,
    RuntimeAccountConfigRequest, RuntimeFill, RuntimeMarketSnapshotRequest,
    RuntimePositionsRequest, RuntimeRecentFillsRequest, SdkResult, StrategyContext, StrategyEvent,
    StrategyExecutionClient, StrategyInstanceId, StrategyRuntime,
};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const RECENT_FILLS_PAGE_LIMIT: u32 = 1_000;
const RECENT_FILLS_MAX_PAGES: usize = 20;

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
    execution_order_map: BTreeMap<String, String>,
    fill_cursor_time: DateTime<Utc>,
    user_stream_events: Option<mpsc::Receiver<UserStreamEvent>>,
    private_stream_backfill_due: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveOrderMapping {
    execution_order_id: String,
    client_order_id: String,
}

#[derive(Debug, Clone)]
enum UserStreamEvent {
    Connected {
        reconnected: bool,
        at: DateTime<Utc>,
    },
    Disconnected {
        reason: String,
        at: DateTime<Utc>,
    },
    Fill(RuntimeFill),
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
        let (stream_fills, stream_backfill_due) = self.drain_user_stream_events();
        self.process_fills(services, ctx, runtime, &snapshot, stream_fills)
            .await?;

        let should_backfill = self.user_stream_events.is_none()
            || stream_backfill_due
            || self.private_stream_backfill_due;
        if should_backfill {
            match load_recent_fills_since(
                args,
                services,
                &self.config,
                &self.execution_exchange,
                &self.execution_market_type,
                self.fill_cursor_time,
            )
            .await
            {
                Ok(fills) => {
                    self.process_fills(services, ctx, runtime, &snapshot, fills)
                        .await?;
                    self.private_stream_backfill_due = false;
                }
                Err(error) => {
                    eprintln!(
                        "hedged-grid recent-fills backfill failed symbol={} start_time={}: {error:#}",
                        self.config.symbol, self.fill_cursor_time
                    );
                }
            }
        }
        Ok(())
    }

    fn drain_user_stream_events(&mut self) -> (Vec<RuntimeFill>, bool) {
        let mut fills = Vec::new();
        let mut backfill_due = false;
        let Some(receiver) = self.user_stream_events.as_mut() else {
            return (fills, backfill_due);
        };
        while let Ok(event) = receiver.try_recv() {
            match event {
                UserStreamEvent::Connected { reconnected, at } => {
                    if reconnected || self.private_stream_backfill_due {
                        backfill_due = true;
                    }
                    eprintln!(
                        "hedged-grid user stream connected symbol={} reconnected={} at={}",
                        self.config.symbol, reconnected, at
                    );
                }
                UserStreamEvent::Disconnected { reason, at } => {
                    self.private_stream_backfill_due = true;
                    eprintln!(
                        "hedged-grid user stream disconnected symbol={} at={} reason={}",
                        self.config.symbol, at, reason
                    );
                }
                UserStreamEvent::Fill(fill) => fills.push(fill),
            }
        }
        (fills, backfill_due)
    }

    async fn process_fills(
        &mut self,
        services: &RuntimeServices,
        ctx: &StrategyContext,
        runtime: &mut HedgedGridRuntime,
        snapshot: &MarketSnapshot,
        mut fills: Vec<RuntimeFill>,
    ) -> Result<usize> {
        fills.sort_by(|left, right| {
            left.filled_at
                .cmp(&right.filled_at)
                .then_with(|| runtime_fill_key(left).cmp(&runtime_fill_key(right)))
        });
        let mut processed = 0;
        for fill in fills {
            self.advance_fill_cursor(&fill);
            let fill_key = runtime_fill_key(&fill);
            if !self.seen_fills.insert(fill_key) {
                continue;
            }
            let Some(client_order_id) = self.resolve_fill_client_order_id(&fill) else {
                continue;
            };
            let Some(record) = self.engine.order_record(&client_order_id) else {
                continue;
            };
            let partial = record.filled_qty + fill.quantity + f64::EPSILON < record.qty;
            let fill_event = FillEvent {
                order_id: client_order_id.clone(),
                intent: record.intent,
                fill_qty: fill.quantity,
                fill_price: fill.price,
                timestamp: fill.filled_at,
                partial,
            };
            let actions = self.engine.handle_fill(fill_event, &snapshot);
            if !partial {
                remove_execution_order_mappings_for_client(
                    &mut self.execution_order_map,
                    &client_order_id,
                );
            }
            let mappings = execute_engine_actions(
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
            insert_execution_order_mappings(&mut self.execution_order_map, mappings);
            processed += 1;
        }
        Ok(processed)
    }

    fn resolve_fill_client_order_id(&self, fill: &RuntimeFill) -> Option<String> {
        if let Some(client_order_id) = fill.client_order_id.as_deref() {
            if self.engine.order_record(client_order_id).is_some() {
                return Some(client_order_id.to_string());
            }
        }
        fill.order_id
            .as_deref()
            .and_then(|order_id| self.execution_order_map.get(order_id))
            .cloned()
    }

    fn advance_fill_cursor(&mut self, fill: &RuntimeFill) {
        let next_cursor = fill.filled_at + ChronoDuration::milliseconds(1);
        if next_cursor > self.fill_cursor_time {
            self.fill_cursor_time = next_cursor;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = normalize_runtime_config(&args, read_yaml_config(&args.config)?)?;
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
            if let Err(error) = driver.tick(&args, &services, &ctx, &mut runtime).await {
                eprintln!(
                    "hedged-grid live tick failed strategy_id={} run_id={}: {error:#}",
                    args.strategy_id, args.run_id
                );
            }
        }
        if let Err(error) = emit_report(&args, &services, &runtime).await {
            eprintln!(
                "hedged-grid report emission failed strategy_id={} run_id={}: {error:#}",
                args.strategy_id, args.run_id
            );
        }
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

fn normalize_runtime_config(args: &Args, config: Value) -> Result<Value> {
    if config.get("symbol").is_some() {
        return Ok(config);
    }

    let symbols = config
        .get("symbols")
        .and_then(Value::as_array)
        .context("hedged-grid config requires symbol or symbols[]")?;
    let requested_symbol = std::env::var("RUSTCTA_SYMBOL")
        .ok()
        .map(|symbol| normalize_runtime_symbol(&symbol));
    let strategy_id = args.strategy_id.to_ascii_uppercase();

    let mut selected_engine = None;
    let mut selected_symbol = None;
    for symbol_config in symbols {
        if symbol_config
            .get("enabled")
            .and_then(Value::as_bool)
            .is_some_and(|enabled| !enabled)
        {
            continue;
        }
        let Some(engine) = symbol_config.get("engine").and_then(Value::as_object) else {
            continue;
        };
        let Some(raw_symbol) = engine.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let normalized_symbol = normalize_runtime_symbol(raw_symbol);
        let requested_match = requested_symbol
            .as_deref()
            .is_some_and(|requested| requested == normalized_symbol);
        let strategy_match = requested_symbol.is_none()
            && strategy_id_matches_symbol(&strategy_id, &normalized_symbol);
        if requested_match || strategy_match {
            selected_engine = Some(engine.clone());
            selected_symbol = Some(normalized_symbol);
            break;
        }
    }

    let mut engine = selected_engine.with_context(|| {
        format!(
            "no enabled symbols[] entry matched strategy_id={} requested_symbol={:?}",
            args.strategy_id, requested_symbol
        )
    })?;
    let symbol = selected_symbol.context("selected engine missing normalized symbol")?;
    engine.insert("symbol".to_string(), json!(symbol.clone()));

    let mut normalized = config.as_object().cloned().unwrap_or_else(Map::new);
    normalized.insert("symbol".to_string(), json!(symbol));
    normalized.insert("engine".to_string(), Value::Object(engine.clone()));

    if let Some(grid) = engine.get("grid").and_then(Value::as_object) {
        if let Some(levels) = grid.get("levels_per_side").and_then(Value::as_u64) {
            normalized.insert("grid_order_count".to_string(), json!(levels));
        }
        if let Some(spacing_pct) = grid.get("grid_spacing_pct").and_then(Value::as_f64) {
            normalized.insert(
                "grid_spacing_bps".to_string(),
                json!(spacing_pct * 10_000.0),
            );
            normalized.insert("grid_spacing_mode".to_string(), json!("pct"));
        }
        if let Some(spacing_abs) = grid.get("grid_spacing_abs").and_then(Value::as_f64) {
            normalized.insert("grid_spacing_abs".to_string(), json!(spacing_abs));
            normalized.insert("grid_spacing_mode".to_string(), json!("abs"));
        }
        if let Some(order_notional) = grid.get("order_notional").and_then(Value::as_f64) {
            normalized.insert("order_notional_usdt".to_string(), json!(order_notional));
        }
    }
    if let Some(risk) = engine.get("risk").and_then(Value::as_object) {
        let max_inventory = risk
            .get("max_total_notional")
            .and_then(Value::as_f64)
            .filter(|value| *value > 0.0)
            .or_else(|| risk.get("max_net_notional").and_then(Value::as_f64))
            .unwrap_or(0.0);
        normalized.insert(
            "max_inventory_quote".to_string(),
            json!(max_inventory.to_string()),
        );
    }

    Ok(Value::Object(normalized))
}

fn normalize_runtime_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

fn strategy_id_matches_symbol(strategy_id: &str, normalized_symbol: &str) -> bool {
    if strategy_id.contains(normalized_symbol) {
        return true;
    }
    let base = ["USDT", "USDC", "BUSD", "USD"]
        .iter()
        .find_map(|quote| normalized_symbol.strip_suffix(quote))
        .unwrap_or(normalized_symbol);
    strategy_id.contains(base)
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
    let core_config = core_config_from_runtime_config(&config, config_value, &market)?;
    let position = load_live_position_state(
        args,
        services,
        &config,
        &execution_exchange,
        &execution_market_type,
        &snapshot,
    )
    .await?;
    let mut engine = GridEngine::new(core_config, true).map_err(|error| anyhow::anyhow!(error))?;
    engine.update_position(position);
    let actions = engine.rebuild_grid(&snapshot);
    let fill_cursor_time = Utc::now();
    let mappings = execute_engine_actions(
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
    let mut execution_order_map = BTreeMap::new();
    insert_execution_order_mappings(&mut execution_order_map, mappings);
    let user_stream_events =
        start_user_stream_events(args, &config, &execution_exchange, &execution_market_type);
    Ok(Some(LiveGridDriver {
        config,
        execution_exchange,
        execution_market_type,
        engine,
        seen_fills: BTreeSet::new(),
        execution_order_map,
        fill_cursor_time,
        private_stream_backfill_due: user_stream_events.is_some(),
        user_stream_events,
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

async fn load_live_position_state(
    args: &Args,
    services: &RuntimeServices,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
    snapshot: &MarketSnapshot,
) -> Result<PositionState> {
    let max_inventory = config
        .max_inventory_quote
        .parse::<f64>()
        .unwrap_or(0.0)
        .max(1.0);
    let mut position = PositionState {
        equity: max_inventory,
        maintenance_margin: 0.0,
        mark_price: snapshot.mark_price,
        ..PositionState::default()
    };
    if !matches!(
        execution_market_type,
        SdkMarketType::Perpetual | SdkMarketType::Futures | SdkMarketType::Option
    ) {
        return Ok(position);
    }

    let platform = services
        .platform
        .as_ref()
        .context("live runtime missing platform client")?;
    let positions = platform
        .positions(RuntimePositionsRequest {
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

    for venue_position in positions.positions {
        let qty = venue_position.quantity.abs();
        if qty <= 0.0 {
            continue;
        }
        if venue_position.mark_price > 0.0 {
            position.mark_price = venue_position.mark_price;
        }
        match venue_position.position_side.to_ascii_uppercase().as_str() {
            "LONG" => {
                position.long_qty += qty;
                position.long_available += qty;
                if venue_position.entry_price > 0.0 {
                    position.long_entry_price = venue_position.entry_price;
                }
            }
            "SHORT" => {
                position.short_qty += qty;
                position.short_available += qty;
                if venue_position.entry_price > 0.0 {
                    position.short_entry_price = venue_position.entry_price;
                }
            }
            _ => {}
        }
    }
    Ok(position)
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

async fn load_recent_fills_since(
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
    let mut cursor = start_time;
    let mut fills = Vec::new();
    for page_index in 0..RECENT_FILLS_MAX_PAGES {
        let mut page = platform
            .recent_fills(RuntimeRecentFillsRequest {
                schema_version: 1,
                tenant_id: args.tenant_id.clone(),
                account_id: args.account_id.clone(),
                run_id: args.run_id.clone(),
                exchange_id: execution_exchange.to_string(),
                symbol: config.symbol.clone(),
                market_type: execution_market_type.clone(),
                start_time: Some(cursor),
                limit: Some(RECENT_FILLS_PAGE_LIMIT),
                requested_at: Utc::now(),
            })
            .await
            .map_err(|error| anyhow::anyhow!(error))?
            .fills;
        if page.is_empty() {
            break;
        }
        page.sort_by(|left, right| {
            left.filled_at
                .cmp(&right.filled_at)
                .then_with(|| runtime_fill_key(left).cmp(&runtime_fill_key(right)))
        });
        let page_len = page.len();
        let max_filled_at = page.iter().map(|fill| fill.filled_at).max();
        fills.extend(page);
        if page_len < RECENT_FILLS_PAGE_LIMIT as usize {
            break;
        }
        let Some(max_filled_at) = max_filled_at else {
            break;
        };
        let next_cursor = max_filled_at + ChronoDuration::milliseconds(1);
        if next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
        if page_index + 1 == RECENT_FILLS_MAX_PAGES {
            eprintln!(
                "hedged-grid recent-fills backfill reached page cap symbol={} start_time={} last_cursor={}",
                config.symbol, start_time, cursor
            );
        }
    }
    Ok(fills)
}

#[derive(Debug, Clone)]
struct BinanceUserStreamConfig {
    rest_base_url: String,
    ws_base_url: String,
    api_key: String,
    exchange_id: String,
    symbol: String,
    market_type: SdkMarketType,
}

fn start_user_stream_events(
    args: &Args,
    config: &HedgedGridConfig,
    execution_exchange: &str,
    execution_market_type: &SdkMarketType,
) -> Option<mpsc::Receiver<UserStreamEvent>> {
    if !env_bool("HEDGED_GRID_USER_STREAM_ENABLED").unwrap_or(true) {
        eprintln!(
            "hedged-grid user stream disabled by HEDGED_GRID_USER_STREAM_ENABLED strategy_id={}",
            args.strategy_id
        );
        return None;
    }
    if !execution_exchange.eq_ignore_ascii_case("binance")
        || !matches!(execution_market_type, SdkMarketType::Perpetual)
    {
        eprintln!(
            "hedged-grid user stream unavailable for exchange={} market_type={:?}; using paginated REST backfill",
            execution_exchange, execution_market_type
        );
        return None;
    }
    let Some(stream_config) =
        BinanceUserStreamConfig::from_env(config, execution_exchange, execution_market_type)
    else {
        eprintln!(
            "hedged-grid Binance user stream missing API key for symbol={}; using paginated REST backfill",
            config.symbol
        );
        return None;
    };
    let (sender, receiver) = mpsc::channel(4_096);
    tokio::spawn(run_binance_user_stream(stream_config, sender));
    Some(receiver)
}

impl BinanceUserStreamConfig {
    fn from_env(
        config: &HedgedGridConfig,
        execution_exchange: &str,
        execution_market_type: &SdkMarketType,
    ) -> Option<Self> {
        let api_key = first_non_empty_env(&[
            "BINANCE_USDM_API_KEY",
            "BINANCE_FUTURES_API_KEY",
            "BINANCE_API_KEY",
            "BINANCE_SPOT_API_KEY",
        ])?;
        Some(Self {
            rest_base_url: first_non_empty_env(&[
                "BINANCE_USDM_REST_BASE_URL",
                "BINANCE_FUTURES_REST_BASE_URL",
                "BINANCE_FAPI_BASE_URL",
            ])
            .unwrap_or_else(|| "https://fapi.binance.com".to_string()),
            ws_base_url: first_non_empty_env(&[
                "BINANCE_USDM_PRIVATE_WS_URL",
                "BINANCE_FUTURES_PRIVATE_WS_URL",
                "BINANCE_USDM_USER_WS_URL",
                "BINANCE_PRIVATE_WS_URL",
            ])
            .unwrap_or_else(|| "wss://fstream.binance.com/private/ws".to_string()),
            api_key,
            exchange_id: execution_exchange.to_string(),
            symbol: normalize_runtime_symbol(&config.symbol),
            market_type: execution_market_type.clone(),
        })
    }
}

async fn run_binance_user_stream(
    config: BinanceUserStreamConfig,
    sender: mpsc::Sender<UserStreamEvent>,
) {
    let mut reconnect_count = 0_u64;
    loop {
        let reconnected = reconnect_count > 0;
        let result =
            run_binance_user_stream_once(config.clone(), sender.clone(), reconnected).await;
        let reason = match result {
            Ok(()) => "websocket closed".to_string(),
            Err(error) => format!("{error:#}"),
        };
        if sender
            .send(UserStreamEvent::Disconnected {
                reason,
                at: Utc::now(),
            })
            .await
            .is_err()
        {
            break;
        }
        reconnect_count = reconnect_count.saturating_add(1);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_binance_user_stream_once(
    config: BinanceUserStreamConfig,
    sender: mpsc::Sender<UserStreamEvent>,
    reconnected: bool,
) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .user_agent("RustCTA-HedgedGrid/0.3")
        .build()
        .context("build Binance user stream HTTP client")?;
    let listen_key = create_binance_usdm_listen_key(&http, &config).await?;
    let websocket_url = format!(
        "{}/{}",
        config.ws_base_url.trim_end_matches('/'),
        listen_key
    );
    let stream_result = async {
        let (mut websocket, _) = connect_async(&websocket_url)
            .await
            .with_context(|| format!("connect Binance user stream {websocket_url}"))?;
        sender
            .send(UserStreamEvent::Connected {
                reconnected,
                at: Utc::now(),
            })
            .await
            .context("send user stream connected event")?;
        let mut keepalive = tokio::time::interval(Duration::from_secs(30 * 60));
        keepalive.tick().await;
        loop {
            tokio::select! {
                maybe_message = websocket.next() => {
                    let Some(message) = maybe_message else {
                        bail!("Binance user stream ended");
                    };
                    match message.context("read Binance user stream message")? {
                        Message::Text(text) => {
                            handle_binance_user_stream_text(&config, &sender, &text).await?;
                        }
                        Message::Binary(bytes) => {
                            let text = String::from_utf8(bytes)
                                .context("Binance user stream binary message was not UTF-8")?;
                            handle_binance_user_stream_text(&config, &sender, &text).await?;
                        }
                        Message::Ping(payload) => {
                            websocket
                                .send(Message::Pong(payload))
                                .await
                                .context("send Binance user stream pong")?;
                        }
                        Message::Pong(_) => {}
                        Message::Close(frame) => {
                            bail!("Binance user stream closed: {:?}", frame);
                        }
                        Message::Frame(_) => {}
                    }
                }
                _ = keepalive.tick() => {
                    renew_binance_usdm_listen_key(&http, &config, &listen_key).await?;
                }
            }
        }
    }
    .await;
    let _ = delete_binance_usdm_listen_key(&http, &config, &listen_key).await;
    stream_result
}

async fn handle_binance_user_stream_text(
    config: &BinanceUserStreamConfig,
    sender: &mpsc::Sender<UserStreamEvent>,
    text: &str,
) -> Result<()> {
    let value: Value = serde_json::from_str(text).context("parse Binance user stream JSON")?;
    if let Some(fill) = parse_binance_user_stream_fill(config, &value)? {
        sender
            .send(UserStreamEvent::Fill(fill))
            .await
            .context("send Binance user stream fill")?;
    }
    Ok(())
}

async fn create_binance_usdm_listen_key(
    http: &reqwest::Client,
    config: &BinanceUserStreamConfig,
) -> Result<String> {
    let value = binance_listen_key_request(http, config, reqwest::Method::POST, None).await?;
    value
        .get("listenKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("Binance listenKey response missing listenKey")
}

async fn renew_binance_usdm_listen_key(
    http: &reqwest::Client,
    config: &BinanceUserStreamConfig,
    listen_key: &str,
) -> Result<()> {
    binance_listen_key_request(http, config, reqwest::Method::PUT, Some(listen_key))
        .await
        .map(|_| ())
}

async fn delete_binance_usdm_listen_key(
    http: &reqwest::Client,
    config: &BinanceUserStreamConfig,
    listen_key: &str,
) -> Result<()> {
    binance_listen_key_request(http, config, reqwest::Method::DELETE, Some(listen_key))
        .await
        .map(|_| ())
}

async fn binance_listen_key_request(
    http: &reqwest::Client,
    config: &BinanceUserStreamConfig,
    method: reqwest::Method,
    listen_key: Option<&str>,
) -> Result<Value> {
    let url = format!(
        "{}/fapi/v1/listenKey",
        config.rest_base_url.trim_end_matches('/')
    );
    let mut request = http
        .request(method, url)
        .header("X-MBX-APIKEY", &config.api_key)
        .header("Content-Type", "application/x-www-form-urlencoded");
    if let Some(listen_key) = listen_key {
        request = request.query(&[("listenKey", listen_key)]);
    }
    let response = request
        .send()
        .await
        .context("send Binance listenKey request")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Binance listenKey response")?;
    if !status.is_success() {
        bail!("Binance listenKey HTTP {status}: {body}");
    }
    if body.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(&body).context("parse Binance listenKey response")
}

fn parse_binance_user_stream_fill(
    config: &BinanceUserStreamConfig,
    value: &Value,
) -> Result<Option<RuntimeFill>> {
    let event_type = value.get("e").and_then(Value::as_str).unwrap_or_default();
    if event_type != "ORDER_TRADE_UPDATE" {
        return Ok(None);
    }
    let payload = value.get("o").unwrap_or(value);
    let execution_type = payload.get("x").and_then(Value::as_str).unwrap_or_default();
    if !execution_type.eq_ignore_ascii_case("TRADE") {
        return Ok(None);
    }
    let symbol = value_string(payload.get("s"))
        .map(|symbol| normalize_runtime_symbol(&symbol))
        .unwrap_or_default();
    if symbol != config.symbol {
        return Ok(None);
    }
    let quantity =
        value_f64(payload.get("l").or_else(|| payload.get("lastFilledQty"))).unwrap_or(0.0);
    if quantity <= 0.0 {
        return Ok(None);
    }
    let price =
        value_f64(payload.get("L").or_else(|| payload.get("lastFilledPrice"))).unwrap_or(0.0);
    if price <= 0.0 {
        return Ok(None);
    }
    let side = match payload
        .get("S")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "BUY" => SdkOrderSide::Buy,
        "SELL" => SdkOrderSide::Sell,
        other => bail!("unsupported Binance user stream side {other}"),
    };
    let event_time = value_i64(payload.get("T").or_else(|| value.get("E")))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now);
    let raw_trade_id = value_string(payload.get("t"));
    let fill_id = raw_trade_id.filter(|id| id != "0" && id != "-1");
    Ok(Some(RuntimeFill {
        schema_version: 1,
        exchange_id: config.exchange_id.clone(),
        symbol: config.symbol.clone(),
        market_type: config.market_type.clone(),
        fill_id,
        order_id: value_string(payload.get("i")),
        client_order_id: value_string(payload.get("c")),
        side,
        position_side: payload
            .get("ps")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .unwrap_or_else(|| "NONE".to_string()),
        price,
        quantity,
        filled_at: event_time,
        received_at: Utc::now(),
    }))
}

fn value_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
    .filter(|value| !value.trim().is_empty())
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn first_non_empty_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
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
) -> Result<Vec<LiveOrderMapping>> {
    let mut accepted_orders: Vec<(ExecutionOrderCommand, ExecutionOrderAck)> = Vec::new();
    let mut mappings = Vec::new();
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
                if let Some(execution_order_id) = ack.execution_order_id.clone() {
                    mappings.push(LiveOrderMapping {
                        execution_order_id,
                        client_order_id: command.client_order_id.clone(),
                    });
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
    Ok(mappings)
}

fn insert_execution_order_mappings(
    execution_order_map: &mut BTreeMap<String, String>,
    mappings: Vec<LiveOrderMapping>,
) {
    for mapping in mappings {
        execution_order_map.insert(mapping.execution_order_id, mapping.client_order_id);
    }
}

fn remove_execution_order_mappings_for_client(
    execution_order_map: &mut BTreeMap<String, String>,
    client_order_id: &str,
) {
    execution_order_map
        .retain(|_, mapped_client_order_id| mapped_client_order_id != client_order_id);
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
            fill.order_id.as_ref().map(|order_id| {
                format!(
                    "{order_id}:{}:{}:{}",
                    fill.filled_at, fill.price, fill.quantity
                )
            })
        })
        .or_else(|| {
            fill.client_order_id.as_ref().map(|order_id| {
                format!(
                    "{order_id}:{}:{}:{}",
                    fill.filled_at, fill.price, fill.quantity
                )
            })
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
    config_value: &Value,
    market: &rustcta_strategy_sdk::RuntimeMarketSnapshot,
) -> Result<HedgedGridCoreConfig> {
    if let Some(engine_config) = config_value.get("engine") {
        let mut core_config: HedgedGridCoreConfig =
            serde_json::from_value(engine_config.clone())
                .context("parse selected hedged-grid engine config")?;
        core_config.symbol = config.symbol.clone();
        return Ok(core_config);
    }

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
                client_order_id: command.client_order_id.clone(),
                execution_order_id: Some(format!("venue-{}", command.client_order_id)),
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
        let core_config = core_config_from_runtime_config(&config, &json!({}), &market)?;
        let mut engine =
            GridEngine::new(core_config, true).map_err(|error| anyhow::anyhow!(error))?;
        engine.update_position(PositionState {
            equity: 22.0,
            mark_price: snapshot.mark_price,
            ..PositionState::default()
        });

        let initial_actions = engine.rebuild_grid(&snapshot);
        let initial_mappings = execute_engine_actions(
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
        let mut execution_order_map = BTreeMap::new();
        insert_execution_order_mappings(&mut execution_order_map, initial_mappings);

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
        let driver = LiveGridDriver {
            config: config.clone(),
            execution_exchange: "binance".to_string(),
            execution_market_type: SdkMarketType::Perpetual,
            engine: engine.clone(),
            seen_fills: BTreeSet::new(),
            execution_order_map: execution_order_map.clone(),
            fill_cursor_time: Utc::now(),
            user_stream_events: None,
            private_stream_backfill_due: false,
        };
        let fill_without_client_order_id = RuntimeFill {
            schema_version: 1,
            exchange_id: "binance".to_string(),
            symbol: "SOLUSDC".to_string(),
            market_type: SdkMarketType::Perpetual,
            fill_id: Some("trade-1".to_string()),
            order_id: Some(format!("venue-{}", filled.id)),
            client_order_id: None,
            side: filled.intent.side().into(),
            position_side: format!("{:?}", filled.intent.position_side()),
            price: filled.price,
            quantity: filled.qty,
            filled_at: Utc::now(),
            received_at: Utc::now(),
        };
        assert_eq!(
            driver
                .resolve_fill_client_order_id(&fill_without_client_order_id)
                .as_deref(),
            Some(filled.id.as_str())
        );
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

    #[test]
    fn binance_user_stream_fill_should_parse_trade_update() -> Result<()> {
        let config = BinanceUserStreamConfig {
            rest_base_url: "https://fapi.binance.com".to_string(),
            ws_base_url: "wss://fstream.binance.com/private/ws".to_string(),
            api_key: "key".to_string(),
            exchange_id: "binance".to_string(),
            symbol: "SOLUSDC".to_string(),
            market_type: SdkMarketType::Perpetual,
        };
        let value = json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1780000000100_i64,
            "o": {
                "s": "SOLUSDC",
                "c": "grid-123",
                "S": "BUY",
                "x": "TRADE",
                "i": 987654321_i64,
                "l": "0.08",
                "L": "70.59",
                "t": 123456_i64,
                "T": 1780000000000_i64,
                "ps": "LONG"
            }
        });

        let fill =
            parse_binance_user_stream_fill(&config, &value)?.context("expected trade fill")?;

        assert_eq!(fill.exchange_id, "binance");
        assert_eq!(fill.symbol, "SOLUSDC");
        assert_eq!(fill.client_order_id.as_deref(), Some("grid-123"));
        assert_eq!(fill.order_id.as_deref(), Some("987654321"));
        assert_eq!(fill.fill_id.as_deref(), Some("123456"));
        assert_eq!(fill.side, SdkOrderSide::Buy);
        assert_eq!(fill.position_side, "LONG");
        assert_eq!(fill.quantity, 0.08);
        assert_eq!(fill.price, 70.59);
        Ok(())
    }
}
