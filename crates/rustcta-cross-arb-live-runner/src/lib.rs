use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{SinkExt, StreamExt};
use rustcta_event_ledger::{
    AuditActor, AuditActorType, AuditOutcome, AuditRecord, EventIdentity, EventKind,
    FillLedgerRecord, JsonlLedger, LedgerEvent, LedgerWriter, OrderLifecycleRecord,
};
use rustcta_exchange_api::{
    ExchangeClientCapabilities, OpenOrdersRequest, PositionsRequest, QueryOrderRequest,
    RecentFillsRequest, RequestContext, SymbolRules, SymbolRulesRequest, SymbolScope,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient,
};
use rustcta_execution_api::{CancelCommand, CancellationIds, MutationIdentity, OrderCommand};
use rustcta_execution_router::{ExecutionRouter, ExecutionRouterConfig};
use rustcta_strategy_cross_exchange_arbitrage::{
    evaluate_dual_taker_close, evaluate_dual_taker_open_opportunities,
    CanonicalSymbol as StrategyCanonicalSymbol, CrossExchangeArbitrageConfig,
    CrossExchangeArbitrageRuntime, DualTakerOpenOpportunity, ExchangeFeeRates,
    ExchangeId as StrategyExchangeId, FeeModel, FeeRole, OpenArbitragePosition, OpenBlockReason,
    OrderBookTop, PrecisionRegistry, QuantityUnit, SymbolPrecision, TakerOrderDraft,
    TakerOrderRole, STRATEGY_KIND,
};
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, MarketType as SdkMarketType,
    OrderSide as SdkOrderSide, OrderType as SdkOrderType, SdkResult, StrategyContext,
    StrategyExecutionClient, StrategyInstanceId, StrategyRuntime, StrategySdkError,
    TimeInForce as SdkTimeInForce,
};
use rustcta_tools_ops::private_ws_observe::{
    spawn_private_ws_observe_tasks, PrivateWsObserveConfig, PrivateWsObserveEvent,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId as GatewayExchangeId,
    ExchangePosition as GatewayPosition, ExchangeSymbol, Fill, LiquidityRole,
    MarketType as GatewayMarketType, OrderSide as GatewayOrderSide,
    OrderStatus as GatewayOrderStatus, OrderType as GatewayOrderType,
    PositionSide as GatewayPositionSide, RunId, StrategyId, TenantId,
    TimeInForce as GatewayTimeInForce,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type LocalGatewayClient = InProcessGatewayClient<AdapterBackedGateway>;
type LocalExecutionRouter = ExecutionRouter<LocalGatewayClient>;
type DisabledExchangeSymbols = BTreeMap<(String, String), String>;
type DisabledOpenExchanges = BTreeMap<String, String>;
const FAILED_OPEN_ROUTE_COOLDOWN_SECS: i64 = 900;
const LIVE_WS_BINANCE_CHUNK_SIZE: usize = 80;
const LIVE_WS_BITGET_CHUNK_SIZE: usize = 50;
const LIVE_WS_GATE_CHUNK_SIZE: usize = 30;
const LIVE_WS_CONNECT_TIMEOUT_MS: u64 = 15_000;
const LIVE_WS_SUBSCRIBE_PAUSE_MS: u64 = 30;
// Keep reader-level throttling disabled; dashboard refresh controls UI cadence.
const LIVE_WS_MIN_BOOK_UPDATE_MS: u64 = 0;
const LIVE_WS_RECONNECT_DELAY_MS: u64 = 2_000;
const DEFAULT_DASHBOARD_REFRESH_MS: u64 = 5_000;
const DEFAULT_PRIVATE_WS_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_PRIVATE_WS_RECONNECT_DELAY_MS: u64 = 2_000;

#[derive(Debug, Clone)]
pub struct LiveRunnerArgs {
    pub config: PathBuf,
    pub strategy_id: String,
    pub run_id: String,
    pub tenant_id: String,
    pub account_id: String,
    pub lock_file: PathBuf,
    pub dashboard_snapshot_path: Option<PathBuf>,
    pub market_data_snapshot_path: Option<PathBuf>,
    pub profit_history_path: Option<PathBuf>,
    pub trade_ledger_path: Option<PathBuf>,
    pub once: bool,
    pub dashboard_refresh_ms: u64,
    pub run_seconds: u64,
    pub market_data_snapshot_stale_ms: u64,
    pub market_data_snapshot_readiness_wait_ms: u64,
    pub market_data_source: LiveMarketDataSource,
    pub enable_live_trading: bool,
    pub allow_rest_readback_confirmation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveMarketDataSource {
    DirectWebsocket,
    SnapshotFile,
}

impl LiveMarketDataSource {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "direct_websocket" | "direct-ws" | "websocket" | "ws" => Ok(Self::DirectWebsocket),
            "snapshot_file" | "snapshot-file" | "file" | "public_websocket_snapshot" => {
                bail!("snapshot_file market data source has been removed from the live runner; use direct_websocket")
            }
            other => bail!("unsupported --market-data-source {other}; use direct_websocket"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DirectWebsocket => "direct_websocket",
            Self::SnapshotFile => "snapshot_file",
        }
    }
}

impl Default for LiveRunnerArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("config/cross_exchange_arbitrage_usdt.yml"),
            strategy_id: "cross_arb_live".to_string(),
            run_id: "local".to_string(),
            tenant_id: "local".to_string(),
            account_id: "cross_arb_3venues".to_string(),
            lock_file: PathBuf::from(
                "logs/cross_exchange_arbitrage/cross_exchange_arbitrage_usdt.lock",
            ),
            dashboard_snapshot_path: Some(PathBuf::from(
                "logs/cross_exchange_arbitrage/cross_arb_dashboard_snapshot.json",
            )),
            market_data_snapshot_path: Some(PathBuf::from(
                "logs/cross_exchange_arbitrage/cross_arb_dashboard_snapshot.json",
            )),
            profit_history_path: Some(PathBuf::from(
                "logs/cross_exchange_arbitrage/profit_history.jsonl",
            )),
            trade_ledger_path: Some(PathBuf::from(
                "logs/cross_exchange_arbitrage/trade_events.jsonl",
            )),
            once: false,
            dashboard_refresh_ms: DEFAULT_DASHBOARD_REFRESH_MS,
            run_seconds: 0,
            market_data_snapshot_stale_ms: 10_000,
            market_data_snapshot_readiness_wait_ms: 12_000,
            market_data_source: LiveMarketDataSource::DirectWebsocket,
            enable_live_trading: false,
            allow_rest_readback_confirmation: false,
        }
    }
}

impl LiveRunnerArgs {
    pub fn from_env_args() -> Result<Self> {
        Self::from_iter(std::env::args().skip(1))
    }

    pub fn from_iter(values: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut values = values.into_iter();
        let mut args = Self::default();
        while let Some(arg) = values.next() {
            match arg.as_str() {
                "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
                "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
                "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
                "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
                "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
                "--lock-file" => {
                    args.lock_file = PathBuf::from(next_value(&mut values, "--lock-file")?)
                }
                "--dashboard-snapshot-path" => {
                    args.dashboard_snapshot_path = Some(PathBuf::from(next_value(
                        &mut values,
                        "--dashboard-snapshot-path",
                    )?))
                }
                "--profit-history-path" => {
                    args.profit_history_path = Some(PathBuf::from(next_value(
                        &mut values,
                        "--profit-history-path",
                    )?))
                }
                "--trade-ledger-path" => {
                    args.trade_ledger_path = Some(PathBuf::from(next_value(
                        &mut values,
                        "--trade-ledger-path",
                    )?))
                }
                "--no-trade-ledger" => args.trade_ledger_path = None,
                "--market-data-snapshot-path" => {
                    bail!("--market-data-snapshot-path has been removed from the live runner; use direct_websocket for trading market data and --dashboard-snapshot-path for UI output.")
                }
                "--market-data-source" => {
                    args.market_data_source = LiveMarketDataSource::parse(&next_value(
                        &mut values,
                        "--market-data-source",
                    )?)?
                }
                "--no-dashboard-snapshot" => args.dashboard_snapshot_path = None,
                "--dashboard-refresh-ms" => {
                    args.dashboard_refresh_ms = next_value(&mut values, "--dashboard-refresh-ms")?
                        .parse()
                        .context("--dashboard-refresh-ms must be a positive integer")?
                }
                "--snapshot-interval-ms" => {
                    bail!("--snapshot-interval-ms has been removed from the live runner; use --dashboard-refresh-ms for UI snapshots. Trading is websocket-event driven.")
                }
                "--run-seconds" => {
                    args.run_seconds = next_value(&mut values, "--run-seconds")?
                        .parse()
                        .context("--run-seconds must be a positive integer")?
                }
                "--market-data-snapshot-stale-ms" => {
                    bail!("--market-data-snapshot-stale-ms has been removed from the live runner; market data is direct websocket event driven.")
                }
                "--market-data-snapshot-readiness-wait-ms" => {
                    bail!("--market-data-snapshot-readiness-wait-ms has been removed from the live runner; market data is direct websocket event driven.")
                }
                "--once" => args.once = true,
                "--enable-live-trading" => args.enable_live_trading = true,
                "--allow-rest-readback-confirmation" => {
                    args.allow_rest_readback_confirmation = true;
                }
                "--help" | "-h" => {
                    println!(
                        "cross-exchange-arbitrage-live-runner --config <path> [--enable-live-trading] [--dashboard-refresh-ms <ms>] [--dashboard-snapshot-path <path>] [--trade-ledger-path <path>] [--once] [--run-seconds <seconds>]"
                    );
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }
        Ok(args)
    }
}

#[derive(Debug, Clone, Serialize)]
struct CapabilityGateReport {
    passed: bool,
    target_market_type: String,
    required_exchanges: Vec<String>,
    loaded_adapters: Vec<String>,
    degraded_requirements: Vec<String>,
    missing_requirements: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LiveRunnerReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    lock_file: String,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    gateway_owned_credentials: bool,
    credential_source_boundary: &'static str,
    market_data_provider_connected: bool,
    startup_position_takeover_enabled: bool,
    analysis_only_reason: Option<&'static str>,
    capability_gate: CapabilityGateReport,
    snapshot: Option<Value>,
}

#[derive(Debug, Clone, Default)]
struct LiveDashboardData {
    market_data_provider_connected: bool,
    market_snapshots: Vec<Value>,
    opportunities: Vec<Value>,
    route_health: Vec<Value>,
    private_events: Vec<Value>,
    position_bundles: Vec<Value>,
    open_orders: Vec<Value>,
    tops: Vec<OrderBookTop>,
    typed_opportunities: Vec<DualTakerOpenOpportunity>,
    controls: LiveExecutionControls,
    quality_controls: LiveExecutionQualityControls,
    runtime_new_entries_block_reason: Option<String>,
    market_data_row_source: &'static str,
}

struct LiveMarketDataProvider {
    source: LiveMarketDataSource,
    direct_ws: Option<DirectWebsocketMarketData>,
    private_ws: Arc<PrivateUserWsObserver>,
}

#[derive(Clone)]
struct LiveRuntimeSinks {
    trade_ledger: Option<TradeLedgerSink>,
}

impl LiveRuntimeSinks {
    #[cfg(test)]
    fn disabled() -> Self {
        Self { trade_ledger: None }
    }

    fn start(args: &LiveRunnerArgs, config: &Value) -> Self {
        let trade_ledger_path = args
            .trade_ledger_path
            .clone()
            .or_else(|| {
                text_at_path(config, &["persistence", "trade_ledger_path"]).map(PathBuf::from)
            })
            .or_else(|| text_at_path(config, &["logging", "trade_ledger_path"]).map(PathBuf::from));
        Self {
            trade_ledger: trade_ledger_path
                .map(|path| TradeLedgerSink::start(path, trade_ledger_queue_capacity(config))),
        }
    }

    fn record_value_event(&self, args: &LiveRunnerArgs, action: &str, event: &Value) {
        if let Some(sink) = &self.trade_ledger {
            sink.try_record(trade_audit_event(args, action, event.clone()));
        }
    }

    fn record_latency_event(&self, args: &LiveRunnerArgs, event: Value) {
        if let Some(sink) = &self.trade_ledger {
            sink.try_record(trade_audit_event(args, "cross_arb_latency_span", event));
        }
    }

    fn record_order_leg(
        &self,
        args: &LiveRunnerArgs,
        target_market_type: GatewayMarketType,
        bundle_id: &str,
        lifecycle: &str,
        leg: &ReconciledOrderLeg,
        requested_at: DateTime<Utc>,
    ) {
        let Some(sink) = &self.trade_ledger else {
            return;
        };
        if let Some(event) = trade_order_event(
            args,
            target_market_type,
            bundle_id,
            lifecycle,
            leg,
            requested_at,
        ) {
            sink.try_record(event);
        }
        if let Some(event) = trade_fill_event(args, target_market_type, bundle_id, lifecycle, leg) {
            sink.try_record(event);
        }
    }
}

#[derive(Clone)]
struct TradeLedgerSink {
    tx: mpsc::Sender<LedgerEvent>,
}

impl TradeLedgerSink {
    fn start(path: PathBuf, queue_capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel(queue_capacity.max(1));
        tokio::spawn(async move {
            let ledger = JsonlLedger::new(path.clone());
            while let Some(event) = rx.recv().await {
                if let Err(error) = ledger.append(event).await {
                    tracing::warn!(
                        target: "rustcta::trade_ledger",
                        ledger_path = %path.display(),
                        error = %error,
                        "failed to append trade ledger event"
                    );
                }
            }
        });
        Self { tx }
    }

    fn try_record(&self, event: LedgerEvent) {
        if let Err(error) = self.tx.try_send(event) {
            tracing::warn!(
                target: "rustcta::trade_ledger",
                error = %error,
                "dropped trade ledger event because writer queue is unavailable"
            );
        }
    }
}

impl LiveMarketDataProvider {
    fn start(
        args: &LiveRunnerArgs,
        strategy_config: &CrossExchangeArbitrageConfig,
        required_exchanges: &[String],
        config_value: &Value,
    ) -> Result<Self> {
        let direct_ws = match args.market_data_source {
            LiveMarketDataSource::DirectWebsocket => Some(DirectWebsocketMarketData::start(
                strategy_config.active_symbols(),
                required_exchanges,
            )?),
            LiveMarketDataSource::SnapshotFile => None,
        };
        let private_ws = PrivateUserWsObserver::start(
            required_exchanges,
            private_ws_observe_config_from_runtime(config_value),
        );
        Ok(Self {
            source: args.market_data_source,
            direct_ws,
            private_ws,
        })
    }

    async fn next_snapshot(
        &mut self,
        args: &LiveRunnerArgs,
        strategy_config: &CrossExchangeArbitrageConfig,
        required_exchanges: &[String],
        fee_model: &FeeModel,
        precision_registry: &PrecisionRegistry,
        disabled_exchange_symbols: &DisabledExchangeSymbols,
        disabled_open_exchanges: &DisabledOpenExchanges,
        dashboard_tick: &mut tokio::time::Interval,
    ) -> Result<MarketDataSnapshot> {
        match self.source {
            LiveMarketDataSource::SnapshotFile => {
                let dashboard = observe_ws_market_data_snapshot(
                    args,
                    strategy_config,
                    required_exchanges,
                    fee_model,
                    precision_registry,
                    disabled_exchange_symbols,
                    disabled_open_exchanges,
                )
                .await?;
                Ok(MarketDataSnapshot {
                    dashboard,
                    trigger: MarketDataTrigger::DashboardTick,
                })
            }
            LiveMarketDataSource::DirectWebsocket => {
                let direct_ws = self
                    .direct_ws
                    .as_mut()
                    .context("direct websocket market data provider is not started")?;
                let trigger = direct_ws.next_trigger(dashboard_tick).await;
                let mut dashboard = direct_ws.dashboard_data(
                    strategy_config,
                    required_exchanges,
                    fee_model,
                    precision_registry,
                    disabled_exchange_symbols,
                    disabled_open_exchanges,
                )?;
                self.private_ws.drain_into(&mut dashboard).await;
                Ok(MarketDataSnapshot { dashboard, trigger })
            }
        }
    }

    fn private_ws(&self) -> Arc<PrivateUserWsObserver> {
        Arc::clone(&self.private_ws)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketDataTrigger {
    BookEvent,
    DashboardTick,
}

struct MarketDataSnapshot {
    dashboard: LiveDashboardData,
    trigger: MarketDataTrigger,
}

#[derive(Debug)]
struct DirectWebsocketMarketData {
    rx: mpsc::Receiver<DirectWsEvent>,
    tops: BTreeMap<(String, String), OrderBookTop>,
    connected: BTreeMap<(String, usize), (usize, DateTime<Utc>)>,
    route_health: Vec<Value>,
}

#[derive(Debug)]
enum DirectWsEvent {
    Connected {
        exchange: String,
        connection_index: usize,
        symbols: usize,
        at: DateTime<Utc>,
    },
    Top(OrderBookTop),
    RouteHealth(Value),
}

#[derive(Debug)]
struct PrivateUserWsObserver {
    state: Mutex<PrivateWsObserveState>,
}

impl PrivateUserWsObserver {
    fn start(required_exchanges: &[String], config: PrivateWsObserveConfig) -> Arc<Self> {
        let exchanges = required_exchanges
            .iter()
            .map(|exchange| gateway_exchange_id(exchange))
            .collect::<Vec<_>>();
        let (tx, rx) = mpsc::channel(exchanges.len().max(1) * 256);
        spawn_private_ws_observe_tasks(&exchanges, config, tx);
        let observer = Arc::new(Self {
            state: Mutex::new(PrivateWsObserveState::new(exchanges)),
        });
        spawn_private_ws_event_collector(rx, Arc::clone(&observer));
        observer
    }

    async fn drain_into(&self, dashboard: &mut LiveDashboardData) {
        let state = self.state.lock().await;
        dashboard
            .route_health
            .extend(state.status.values().cloned());
        dashboard
            .private_events
            .extend(state.events.iter().cloned());
    }

    async fn wait_for_order(
        &self,
        exchange: &str,
        client_order_id: &str,
        since: DateTime<Utc>,
        timeout_ms: u64,
    ) -> Option<Value> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
        loop {
            if let Some(event) =
                self.state
                    .lock()
                    .await
                    .matching_order_event(exchange, client_order_id, since)
            {
                return Some(event);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[derive(Debug)]
struct PrivateWsObserveState {
    status: BTreeMap<String, Value>,
    events: Vec<Value>,
}

impl PrivateWsObserveState {
    fn new(_required_exchanges: Vec<String>) -> Self {
        Self {
            status: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    fn apply(&mut self, event: PrivateWsObserveEvent) {
        match event {
            PrivateWsObserveEvent::Status { exchange, row } => {
                self.status.insert(gateway_exchange_id(&exchange), row);
            }
            PrivateWsObserveEvent::PrivateEvent(row) => {
                self.events.push(row);
                if self.events.len() > 500 {
                    let overflow = self.events.len() - 500;
                    self.events.drain(0..overflow);
                }
            }
        }
    }

    fn matching_order_event(
        &self,
        exchange: &str,
        client_order_id: &str,
        since: DateTime<Utc>,
    ) -> Option<Value> {
        let exchange = gateway_exchange_id(exchange);
        self.events.iter().rev().find_map(|event| {
            let event_exchange = event.get("exchange").and_then(Value::as_str)?;
            if gateway_exchange_id(event_exchange) != exchange {
                return None;
            }
            if event.get("client_order_id").and_then(Value::as_str) != Some(client_order_id) {
                return None;
            }
            let observed_at = datetime_any_field(event, &["observed_at"]).unwrap_or(since);
            if observed_at < since {
                return None;
            }
            let private_kind = event.get("private_kind").and_then(Value::as_str);
            let status = event.get("order_status").and_then(Value::as_str);
            if private_kind == Some("fill")
                || status.is_some_and(|status| {
                    matches!(
                        status.to_ascii_lowercase().as_str(),
                        "filled" | "full_fill" | "full-fill" | "closed"
                    )
                })
            {
                return Some(event.clone());
            }
            None
        })
    }
}

fn spawn_private_ws_event_collector(
    mut rx: mpsc::Receiver<PrivateWsObserveEvent>,
    observer: Arc<PrivateUserWsObserver>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            observer.state.lock().await.apply(event);
        }
    });
}

#[derive(Debug, Clone, Default)]
struct LiveExecutionState {
    open_bundles: BTreeMap<String, LiveOpenBundle>,
    symbol_cooldowns: BTreeMap<String, DateTime<Utc>>,
    route_cooldowns: BTreeMap<String, RouteCooldown>,
    recent_events: Vec<Value>,
    recent_open_orders: Vec<Value>,
    unmanaged_external_positions: Vec<UnmanagedExternalPosition>,
    manual_intervention_required: bool,
    manual_intervention_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LiveExecutionControls {
    start_paused_new_entries: bool,
    start_close_only: bool,
}

#[derive(Debug, Clone, Copy)]
struct LiveExecutionMode {
    live_orders_enabled: bool,
    require_release_binary: bool,
    is_release_binary: bool,
    private_ws_confirmation_required: bool,
    rest_readback_confirmation_allowed: bool,
}

impl LiveExecutionMode {
    fn from_config(args: &LiveRunnerArgs, config: &Value) -> Self {
        let config_trading_enabled = bool_at_path(config, &["execution", "trading_enabled"])
            .or_else(|| bool_at_path(config, &["trading_enabled"]))
            .unwrap_or(false);
        let require_release_binary =
            bool_at_path(config, &["execution", "require_release_binary"]).unwrap_or(true);
        let private_ws_confirmation_required =
            bool_at_path(config, &["execution", "private_ws_confirmation_required"])
                .unwrap_or(true);
        let rest_readback_confirmation_allowed = args.allow_rest_readback_confirmation
            || bool_at_path(config, &["execution", "allow_rest_readback_confirmation"])
                .unwrap_or(false);
        Self {
            live_orders_enabled: config_trading_enabled && args.enable_live_trading,
            require_release_binary,
            is_release_binary: !cfg!(debug_assertions),
            private_ws_confirmation_required,
            rest_readback_confirmation_allowed,
        }
    }

    fn validate(self) -> Result<()> {
        if !self.live_orders_enabled {
            return Ok(());
        }
        if self.require_release_binary && !self.is_release_binary {
            bail!("live trading requires a release binary; rebuild with cargo build --release");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct LiveConfirmationPolicy {
    private_ws: Arc<PrivateUserWsObserver>,
    require_private_ws: bool,
    allow_rest_readback: bool,
    private_ws_timeout_ms: u64,
}

impl LiveExecutionControls {
    fn allow_new_entries(self, runtime_allow_new_entries: bool) -> bool {
        runtime_allow_new_entries && !self.start_paused_new_entries && !self.start_close_only
    }

    fn new_entries_block_reason(self, runtime_allow_new_entries: bool) -> Option<&'static str> {
        if self.start_close_only {
            return Some("close-only control is enabled");
        }
        if self.start_paused_new_entries {
            return Some("new entries are paused by control config");
        }
        if !runtime_allow_new_entries {
            return Some("new entries are stopped by runtime deadline");
        }
        None
    }
}

#[derive(Debug, Clone, Copy)]
struct LiveExecutionQualityControls {
    min_open_net_edge_pct: f64,
    min_open_raw_spread_pct: f64,
    min_open_executable_depth_ratio: f64,
    min_close_net_profit_pct: f64,
}

impl Default for LiveExecutionQualityControls {
    fn default() -> Self {
        Self {
            min_open_net_edge_pct: 0.0,
            min_open_raw_spread_pct: 0.0,
            min_open_executable_depth_ratio: 1.0,
            min_close_net_profit_pct: 0.0,
        }
    }
}

impl LiveExecutionQualityControls {
    fn from_config(
        config: &Value,
        strategy_config: &CrossExchangeArbitrageConfig,
    ) -> LiveExecutionQualityControls {
        let min_open_net_edge_pct =
            f64_at_path(config, &["execution_quality", "min_open_net_edge_pct"])
                .or_else(|| f64_at_path(config, &["risk", "min_live_open_net_edge_pct"]))
                .unwrap_or(strategy_config.dual_taker.min_open_net_profit_pct)
                .max(strategy_config.dual_taker.min_open_net_profit_pct)
                .max(0.0);
        let min_open_raw_spread_pct =
            f64_at_path(config, &["execution_quality", "min_open_raw_spread_pct"])
                .or_else(|| f64_at_path(config, &["risk", "min_live_open_raw_spread_pct"]))
                .unwrap_or(strategy_config.dual_taker.min_open_spread_pct)
                .max(strategy_config.dual_taker.min_open_spread_pct)
                .max(0.0);
        let min_open_executable_depth_ratio = f64_at_path(
            config,
            &["execution_quality", "min_open_executable_depth_ratio"],
        )
        .or_else(|| f64_at_path(config, &["risk", "min_live_open_executable_depth_ratio"]))
        .unwrap_or(1.0)
        .max(1.0);
        let min_close_net_profit_pct =
            f64_at_path(config, &["execution_quality", "min_close_net_profit_pct"])
                .or_else(|| f64_at_path(config, &["risk", "min_live_close_net_profit_pct"]))
                .unwrap_or(strategy_config.dual_taker.close_min_net_profit_pct)
                .max(strategy_config.dual_taker.close_min_net_profit_pct)
                .max(0.0);
        LiveExecutionQualityControls {
            min_open_net_edge_pct,
            min_open_raw_spread_pct,
            min_open_executable_depth_ratio,
            min_close_net_profit_pct,
        }
    }

    fn open_reject_reasons(&self, opportunity: &DualTakerOpenOpportunity) -> Vec<String> {
        let mut reasons = Vec::new();
        if opportunity.spread_pct < self.min_open_raw_spread_pct {
            reasons.push(format!(
                "live raw spread {} is below execution quality min {}",
                format_float(opportunity.spread_pct),
                format_float(self.min_open_raw_spread_pct)
            ));
        }
        if opportunity.expected_net_profit_pct < self.min_open_net_edge_pct {
            reasons.push(format!(
                "live expected net edge {} is below execution quality min {}",
                format_float(opportunity.expected_net_profit_pct),
                format_float(self.min_open_net_edge_pct)
            ));
        }
        let min_depth = opportunity
            .long_notional_usdt
            .max(opportunity.short_notional_usdt)
            * self.min_open_executable_depth_ratio;
        if opportunity.executable_top_depth_usdt < min_depth {
            reasons.push(format!(
                "top-of-book executable depth {} is below execution quality requirement {}",
                format_float(opportunity.executable_top_depth_usdt),
                format_float(min_depth)
            ));
        }
        reasons
    }

    fn open_allows(&self, opportunity: &DualTakerOpenOpportunity) -> bool {
        self.open_reject_reasons(opportunity).is_empty()
    }

    fn row_reject_reasons(&self, row: &Value) -> Vec<String> {
        let raw_open_spread_pct = f64_field(
            row,
            &["raw_open_spread_pct", "raw_spread_pct", "spread_pct"],
        )
        .unwrap_or(0.0);
        let expected_net_profit_pct = f64_field(row, &["expected_net_profit_pct"]).unwrap_or(0.0);
        let executable_top_depth_usdt =
            f64_field(row, &["executable_top_depth_usdt"]).unwrap_or(0.0);
        let long_notional_usdt = f64_field(row, &["long_notional_usdt"]).unwrap_or(0.0);
        let short_notional_usdt = f64_field(row, &["short_notional_usdt"]).unwrap_or(0.0);
        let mut reasons = Vec::new();
        if raw_open_spread_pct < self.min_open_raw_spread_pct {
            reasons.push(format!(
                "live raw spread {} is below execution quality min {}",
                format_float(raw_open_spread_pct),
                format_float(self.min_open_raw_spread_pct)
            ));
        }
        if expected_net_profit_pct < self.min_open_net_edge_pct {
            reasons.push(format!(
                "live expected net edge {} is below execution quality min {}",
                format_float(expected_net_profit_pct),
                format_float(self.min_open_net_edge_pct)
            ));
        }
        let min_depth =
            long_notional_usdt.max(short_notional_usdt) * self.min_open_executable_depth_ratio;
        if min_depth > 0.0 && executable_top_depth_usdt < min_depth {
            reasons.push(format!(
                "top-of-book executable depth {} is below execution quality requirement {}",
                format_float(executable_top_depth_usdt),
                format_float(min_depth)
            ));
        }
        reasons
    }

    fn close_allows(
        &self,
        close: &rustcta_strategy_cross_exchange_arbitrage::DualTakerCloseEvaluation,
    ) -> bool {
        close.net_profit_pct >= self.min_close_net_profit_pct
    }
}

#[derive(Debug, Clone)]
struct RouteCooldown {
    until: DateTime<Utc>,
    symbol: String,
    reason: String,
    source_bundle_id: String,
}

#[derive(Debug, Clone)]
struct LiveOpenBundle {
    bundle_id: String,
    position: OpenArbitragePosition,
    open_long: ReconciledOrderLeg,
    open_short: ReconciledOrderLeg,
    opened_at: DateTime<Utc>,
    open_fee_usdt: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct ExternalPositionSnapshot {
    exchange: String,
    canonical_symbol: String,
    side: GatewayPositionSide,
    quantity: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct UnmanagedExternalPosition {
    exchange: String,
    canonical_symbol: String,
    side: GatewayPositionSide,
    quantity: f64,
    reason: String,
}

#[derive(Debug, Clone)]
struct ReconciledOrderLeg {
    exchange: String,
    symbol: String,
    role: String,
    side: String,
    position_side: String,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    accepted: bool,
    status: String,
    planned_price: f64,
    planned_base_quantity: f64,
    planned_order_quantity: f64,
    actual_fill_price: Option<f64>,
    actual_base_quantity: Option<f64>,
    actual_order_quantity: Option<f64>,
    actual_notional_usdt: Option<f64>,
    fee_usdt: f64,
    submitted_at: Option<DateTime<Utc>>,
    acked_at: Option<DateTime<Utc>>,
    filled_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

impl ReconciledOrderLeg {
    fn filled(&self) -> bool {
        self.actual_fill_price.is_some()
            && self
                .actual_base_quantity
                .is_some_and(|quantity| quantity > 0.0)
    }
}

pub async fn run_live_runner(
    args: LiveRunnerArgs,
    gateway: AdapterBackedGateway,
    loaded_adapters: Vec<String>,
) -> Result<()> {
    let _singleton_guard = ProcessSingletonGuard::acquire(&args.lock_file)?;
    let config_value = read_yaml_config(&args.config)?;
    let strategy_config = CrossExchangeArbitrageConfig::from_runtime_value(&config_value);
    validate_live_symbol_universe(&strategy_config)?;
    let execution_mode = LiveExecutionMode::from_config(&args, &config_value);
    execution_mode.validate()?;
    let sinks = LiveRuntimeSinks::start(&args, &config_value);
    let live_orders_enabled = execution_mode.live_orders_enabled;
    let execution_controls = live_execution_controls_from_config(&config_value);
    let fee_model = fee_model_from_config(&config_value);
    let target_market_type = gateway_market_type(&strategy_config.market_type)?;
    let disabled_exchange_symbols =
        disabled_exchange_symbols_from_config(&config_value, &target_market_type);
    let disabled_open_exchanges = disabled_open_exchanges_from_config(&config_value);
    let required_exchanges = gateway_exchange_ids(strategy_config.active_venues());
    let account_by_exchange =
        exchange_account_map(&config_value, &required_exchanges, &args.account_id);

    let gateway = Arc::new(gateway);
    let gateway_client = InProcessGatewayClient::new(gateway);
    let capability_gate = validate_gateway_capabilities(
        &gateway_client,
        &args,
        target_market_type,
        &required_exchanges,
        &loaded_adapters,
        live_orders_enabled,
    )
    .await?;

    if !capability_gate.passed {
        emit_report(
            &args,
            &capability_gate,
            &strategy_config,
            &LiveDashboardData::default(),
            None,
            false,
            true,
        )
        .await?;
        bail!(
            "cross exchange arbitrage live runner blocked by capability gate: {}",
            capability_gate.missing_requirements.join("; ")
        );
    }
    let precision_registry =
        load_precision_registry(&gateway_client, &args, &strategy_config, target_market_type)
            .await
            .unwrap_or_else(|_| PrecisionRegistry::default());

    let router = Arc::new(ExecutionRouter::new(
        ExecutionRouterConfig::live(),
        gateway_client.clone(),
    ));
    let execution = Arc::new(RouterBackedStrategyExecutionClient {
        router,
        market_type: target_market_type,
        account_by_exchange,
    });
    let quality_controls =
        LiveExecutionQualityControls::from_config(&config_value, &strategy_config);
    validate_live_market_data_source(&args, live_orders_enabled)?;
    let ctx = strategy_context(&args, config_value.clone(), execution);
    let mut execution_state = LiveExecutionState::default();
    restore_route_cooldowns_from_profit_history(&args, &mut execution_state, Utc::now())?;
    if live_orders_enabled {
        recover_open_bundles_from_dashboard_snapshot(
            &gateway_client,
            &args,
            &strategy_config,
            target_market_type,
            &mut execution_state,
        )
        .await?;
    }
    let mut runtime = CrossExchangeArbitrageRuntime::new();
    runtime.start(ctx.clone()).await?;
    let mut market_data_provider =
        LiveMarketDataProvider::start(&args, &strategy_config, &required_exchanges, &config_value)?;
    let confirmation = LiveConfirmationPolicy {
        private_ws: market_data_provider.private_ws(),
        require_private_ws: execution_mode.private_ws_confirmation_required,
        allow_rest_readback: execution_mode.rest_readback_confirmation_allowed,
        private_ws_timeout_ms: private_ws_confirmation_timeout_ms(&config_value),
    };
    let dashboard_refresh_ms = dashboard_refresh_ms_from_config(&config_value, &args);
    let mut dashboard_tick = tokio::time::interval(Duration::from_millis(dashboard_refresh_ms));
    dashboard_tick.tick().await;
    let first_snapshot = market_data_provider
        .next_snapshot(
            &args,
            &strategy_config,
            &required_exchanges,
            &fee_model,
            &precision_registry,
            &disabled_exchange_symbols,
            &disabled_open_exchanges,
            &mut dashboard_tick,
        )
        .await?;
    let mut dashboard_data = first_snapshot.dashboard;
    dashboard_data.controls = execution_controls;
    dashboard_data.quality_controls = quality_controls;
    run_live_execution_cycle(
        &gateway_client,
        &ctx,
        &args,
        &strategy_config,
        target_market_type,
        &fee_model,
        &precision_registry,
        live_orders_enabled,
        execution_controls.allow_new_entries(true),
        execution_controls.new_entries_block_reason(true),
        quality_controls,
        &confirmation,
        &sinks,
        &mut execution_state,
        &mut dashboard_data,
    )
    .await?;
    emit_report(
        &args,
        &capability_gate,
        &strategy_config,
        &dashboard_data,
        Some(serde_json::to_value(runtime.snapshot().await?)?),
        live_orders_enabled,
        true,
    )
    .await?;

    if args.once {
        runtime.stop().await?;
        return Ok(());
    }

    let run_deadline =
        (args.run_seconds > 0).then(|| Instant::now() + Duration::from_secs(args.run_seconds));
    let mut stopping_new_entries = false;
    loop {
        let market_snapshot = market_data_provider
            .next_snapshot(
                &args,
                &strategy_config,
                &required_exchanges,
                &fee_model,
                &precision_registry,
                &disabled_exchange_symbols,
                &disabled_open_exchanges,
                &mut dashboard_tick,
            )
            .await?;
        if run_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            stopping_new_entries = true;
        }
        dashboard_data = market_snapshot.dashboard;
        dashboard_data.controls = execution_controls;
        dashboard_data.quality_controls = quality_controls;
        run_live_execution_cycle(
            &gateway_client,
            &ctx,
            &args,
            &strategy_config,
            target_market_type,
            &fee_model,
            &precision_registry,
            live_orders_enabled,
            execution_controls.allow_new_entries(!stopping_new_entries),
            execution_controls.new_entries_block_reason(!stopping_new_entries),
            quality_controls,
            &confirmation,
            &sinks,
            &mut execution_state,
            &mut dashboard_data,
        )
        .await?;
        if market_snapshot.trigger == MarketDataTrigger::DashboardTick {
            emit_report(
                &args,
                &capability_gate,
                &strategy_config,
                &dashboard_data,
                Some(serde_json::to_value(runtime.snapshot().await?)?),
                live_orders_enabled,
                true,
            )
            .await?;
        }
        if stopping_new_entries && execution_state.open_bundles.is_empty() {
            runtime.stop().await?;
            return Ok(());
        }
    }
}

fn validate_live_market_data_source(
    args: &LiveRunnerArgs,
    live_orders_enabled: bool,
) -> Result<()> {
    if !live_orders_enabled {
        return Ok(());
    }
    match args.market_data_source {
        LiveMarketDataSource::SnapshotFile => bail!(
            "live order execution cannot use market-data-source=snapshot_file; file snapshots are audit/UI output only. Use direct_websocket after same-process WS market data is enabled."
        ),
        LiveMarketDataSource::DirectWebsocket => Ok(()),
    }
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &Path) -> Result<Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

fn live_execution_controls_from_config(config: &Value) -> LiveExecutionControls {
    LiveExecutionControls {
        start_paused_new_entries: bool_at_path(config, &["controls", "start_paused_new_entries"])
            .unwrap_or(false),
        start_close_only: bool_at_path(config, &["controls", "start_close_only"]).unwrap_or(false),
    }
}

fn validate_live_symbol_universe(config: &CrossExchangeArbitrageConfig) -> Result<()> {
    anyhow::ensure!(
        !config.active_symbols().is_empty(),
        "cross-arb runner requires an explicit non-empty symbol universe"
    );
    anyhow::ensure!(
        config.active_venues().len() >= 2,
        "cross-arb runner requires at least two enabled exchanges"
    );
    Ok(())
}

fn dashboard_refresh_ms_from_config(config: &Value, args: &LiveRunnerArgs) -> u64 {
    u64_at_path(config, &["dashboard", "refresh_ms"])
        .or_else(|| u64_at_path(config, &["dashboard", "refresh_interval_ms"]))
        .unwrap_or(args.dashboard_refresh_ms)
        .max(250)
}

fn private_ws_confirmation_timeout_ms(config: &Value) -> u64 {
    u64_at_path(config, &["execution", "private_ws_confirmation_timeout_ms"])
        .or_else(|| u64_at_path(config, &["private_ws", "confirmation_timeout_ms"]))
        .unwrap_or(DEFAULT_PRIVATE_WS_TIMEOUT_MS)
        .max(1)
}

fn private_ws_observe_config_from_runtime(config: &Value) -> PrivateWsObserveConfig {
    PrivateWsObserveConfig {
        timeout_ms: u64_at_path(config, &["private_ws", "timeout_ms"])
            .unwrap_or(DEFAULT_PRIVATE_WS_TIMEOUT_MS),
        reconnect_delay_ms: u64_at_path(config, &["private_ws", "reconnect_delay_ms"])
            .unwrap_or(DEFAULT_PRIVATE_WS_RECONNECT_DELAY_MS),
        gateio_user_id: text_at_path(config, &["private_ws", "gateio_user_id"]).map(str::to_string),
    }
}

impl DirectWebsocketMarketData {
    fn start(symbols: Vec<String>, required_exchanges: &[String]) -> Result<Self> {
        let connections = build_direct_ws_connections(required_exchanges, &symbols)?;
        anyhow::ensure!(
            !connections.is_empty(),
            "direct websocket market data source has no connections"
        );
        let (tx, rx) = mpsc::channel(connections.len().max(1) * 256);
        for (index, connection) in connections.into_iter().enumerate() {
            let tx = tx.clone();
            tokio::spawn(async move {
                run_direct_ws_connection_task(index + 1, connection, tx).await;
            });
        }
        drop(tx);
        Ok(Self {
            rx,
            tops: BTreeMap::new(),
            connected: BTreeMap::new(),
            route_health: Vec::new(),
        })
    }

    fn dashboard_data(
        &mut self,
        strategy_config: &CrossExchangeArbitrageConfig,
        required_exchanges: &[String],
        fee_model: &FeeModel,
        precision_registry: &PrecisionRegistry,
        disabled_exchange_symbols: &DisabledExchangeSymbols,
        disabled_open_exchanges: &DisabledOpenExchanges,
    ) -> Result<LiveDashboardData> {
        self.drain_ready_events();
        let now = Utc::now();
        let active_symbols = strategy_config
            .active_symbols()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let required_exchange_set = required_exchanges
            .iter()
            .map(|exchange| gateway_exchange_id(exchange))
            .collect::<BTreeSet<_>>();
        let mut display_tops = Vec::new();
        let mut fresh_tops = Vec::new();
        let mut market_rows = Vec::new();
        let mut route_health = self.route_health.clone();

        for top in self.tops.values() {
            if !required_exchange_set.contains(top.exchange.as_str()) {
                continue;
            }
            if !active_symbols.contains(&top.canonical_symbol.as_pair()) {
                continue;
            }
            market_rows.push(market_snapshot_row(top, now));
            display_tops.push(top.clone());
            if top.is_fresh(now, strategy_config.dual_taker.orderbook_stale_ms) {
                fresh_tops.push(top.clone());
            } else {
                route_health.push(json!({
                    "route_id": format!("direct_ws:{}:{}:book", top.exchange, top.canonical_symbol.as_pair()),
                    "exchange": top.exchange.as_str(),
                    "symbol": top.canonical_symbol.as_pair(),
                    "component": "direct_websocket_orderbook",
                    "status": "stale",
                    "age_ms": top.age_ms(now),
                    "observed_at": now,
                }));
            }
        }

        route_health.extend(direct_ws_route_health_rows(
            &active_symbols,
            required_exchanges,
            &self.tops,
            &self.connected,
            now,
            strategy_config.dual_taker.orderbook_stale_ms,
        ));

        let evaluated_opportunities = evaluate_dual_taker_open_opportunities(
            &fresh_tops,
            precision_registry,
            fee_model,
            &strategy_config.dual_taker,
            now,
        );
        let mut typed_opportunities = Vec::new();
        for opportunity in evaluated_opportunities {
            if let Some((exchange, symbol, reason)) =
                opportunity_disabled_exchange_symbol(&opportunity, disabled_exchange_symbols)
            {
                route_health.push(json!({
                    "route_id": format!("disabled_exchange_symbol:{}:{}:{}", exchange, symbol, opportunity.opportunity_id),
                    "exchange": exchange,
                    "symbol": symbol,
                    "component": "live_runner_route_filter",
                    "status": "disabled",
                    "reason": reason,
                    "observed_at": now,
                }));
                continue;
            }
            if let Some((exchange, reason)) =
                opportunity_disabled_open_exchange(&opportunity, disabled_open_exchanges)
            {
                route_health.push(json!({
                    "route_id": format!("disabled_open_exchange:{}:{}", exchange, opportunity.opportunity_id),
                    "exchange": exchange,
                    "symbol": opportunity.canonical_symbol.as_pair(),
                    "component": "live_runner_route_filter",
                    "status": "disabled",
                    "reason": reason,
                    "observed_at": now,
                }));
                continue;
            }
            typed_opportunities.push(opportunity);
        }
        retain_best_opportunity_per_symbol(&mut typed_opportunities);
        let opportunities = display_opportunity_rows(
            strategy_config,
            fee_model,
            precision_registry,
            &display_tops,
            &typed_opportunities,
            disabled_exchange_symbols,
            disabled_open_exchanges,
            now,
        );

        Ok(LiveDashboardData {
            market_data_provider_connected: !display_tops.is_empty(),
            market_snapshots: market_rows,
            opportunities,
            route_health,
            private_events: Vec::new(),
            position_bundles: Vec::new(),
            open_orders: Vec::new(),
            tops: fresh_tops,
            typed_opportunities,
            controls: LiveExecutionControls::default(),
            quality_controls: LiveExecutionQualityControls::default(),
            runtime_new_entries_block_reason: None,
            market_data_row_source: "direct_websocket_orderbook",
        })
    }

    async fn next_trigger(
        &mut self,
        dashboard_tick: &mut tokio::time::Interval,
    ) -> MarketDataTrigger {
        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    if let Some(event) = event {
                        self.apply_event(event);
                        self.drain_ready_events();
                        return MarketDataTrigger::BookEvent;
                    }
                    dashboard_tick.tick().await;
                    return MarketDataTrigger::DashboardTick;
                }
                _ = dashboard_tick.tick() => {
                    self.drain_ready_events();
                    return MarketDataTrigger::DashboardTick;
                }
            }
        }
    }

    fn drain_ready_events(&mut self) {
        while let Ok(event) = self.rx.try_recv() {
            self.apply_event(event);
        }
    }

    fn apply_event(&mut self, event: DirectWsEvent) {
        match event {
            DirectWsEvent::Connected {
                exchange,
                connection_index,
                symbols,
                at,
            } => {
                self.connected.insert(
                    (gateway_exchange_id(&exchange), connection_index),
                    (symbols, at),
                );
            }
            DirectWsEvent::Top(top) => {
                self.tops.insert(
                    (
                        top.exchange.as_str().to_string(),
                        top.canonical_symbol.as_pair(),
                    ),
                    top,
                );
            }
            DirectWsEvent::RouteHealth(row) => {
                self.route_health.push(row);
                if self.route_health.len() > 500 {
                    let overflow = self.route_health.len() - 500;
                    self.route_health.drain(0..overflow);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct DirectWsConnection {
    exchange: String,
    url: String,
    subscribe_messages: Vec<String>,
    symbols: Vec<String>,
}

fn build_direct_ws_connections(
    required_exchanges: &[String],
    symbols: &[String],
) -> Result<Vec<DirectWsConnection>> {
    let mut connections = Vec::new();
    for exchange in required_exchanges {
        let ws_exchange = direct_ws_exchange_id(exchange);
        let chunk_size = match ws_exchange.as_str() {
            "binance" => LIVE_WS_BINANCE_CHUNK_SIZE,
            "bitget" => LIVE_WS_BITGET_CHUNK_SIZE,
            "gate" => LIVE_WS_GATE_CHUNK_SIZE,
            other => bail!("direct websocket source supports binance, bitget, gateio; got {other}"),
        };
        for chunk in symbols.chunks(chunk_size.max(1)) {
            connections.push(match ws_exchange.as_str() {
                "binance" => direct_ws_binance_connection(chunk),
                "bitget" => direct_ws_bitget_connection(chunk),
                "gate" => direct_ws_gate_connection(chunk),
                _ => unreachable!(),
            });
        }
    }
    Ok(connections)
}

fn direct_ws_binance_connection(symbols: &[String]) -> DirectWsConnection {
    let streams = symbols
        .iter()
        .map(|symbol| {
            format!(
                "{}@depth5@100ms",
                compact_ws_symbol(symbol).to_ascii_lowercase()
            )
        })
        .collect::<Vec<_>>()
        .join("/");
    DirectWsConnection {
        exchange: "binance".to_string(),
        url: format!("wss://fstream.binance.com/stream?streams={streams}"),
        subscribe_messages: Vec::new(),
        symbols: symbols.to_vec(),
    }
}

fn direct_ws_bitget_connection(symbols: &[String]) -> DirectWsConnection {
    let args = symbols
        .iter()
        .map(|symbol| {
            json!({
                "instType": "USDT-FUTURES",
                "channel": "books5",
                "instId": compact_ws_symbol(symbol),
            })
        })
        .collect::<Vec<_>>();
    DirectWsConnection {
        exchange: "bitget".to_string(),
        url: "wss://ws.bitget.com/v2/ws/public".to_string(),
        subscribe_messages: vec![json!({ "op": "subscribe", "args": args }).to_string()],
        symbols: symbols.to_vec(),
    }
}

fn direct_ws_gate_connection(symbols: &[String]) -> DirectWsConnection {
    let subscribe_messages = symbols
        .iter()
        .map(|symbol| {
            json!({
                "time": Utc::now().timestamp(),
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": [gate_ws_symbol(symbol), "5", "0"],
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    DirectWsConnection {
        exchange: "gate".to_string(),
        url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
        subscribe_messages,
        symbols: symbols.to_vec(),
    }
}

async fn run_direct_ws_connection_task(
    connection_index: usize,
    connection: DirectWsConnection,
    tx: mpsc::Sender<DirectWsEvent>,
) {
    loop {
        if let Err(error) = run_direct_ws_connection_once(connection_index, &connection, &tx).await
        {
            let _ = tx
                .send(DirectWsEvent::RouteHealth(json!({
                    "route_id": format!("direct_ws:{}:connection:{connection_index}", connection.exchange),
                    "exchange": gateway_exchange_id(&connection.exchange),
                    "component": "direct_websocket_orderbook",
                    "connection_index": connection_index,
                    "status": "error",
                    "message": error.to_string(),
                    "observed_at": Utc::now(),
                })))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(LIVE_WS_RECONNECT_DELAY_MS)).await;
    }
}

async fn run_direct_ws_connection_once(
    connection_index: usize,
    connection: &DirectWsConnection,
    tx: &mpsc::Sender<DirectWsEvent>,
) -> Result<()> {
    let timeout_duration = Duration::from_millis(LIVE_WS_CONNECT_TIMEOUT_MS);
    let (mut ws, _) =
        tokio::time::timeout(timeout_duration, connect_async(connection.url.as_str()))
            .await
            .context("connect timed out")?
            .with_context(|| format!("connect {}", connection.url))?;
    tx.send(DirectWsEvent::Connected {
        exchange: connection.exchange.clone(),
        connection_index,
        symbols: connection.symbols.len(),
        at: Utc::now(),
    })
    .await
    .ok();

    for message in &connection.subscribe_messages {
        ws.send(Message::Text(message.clone()))
            .await
            .with_context(|| format!("send subscribe for {}", connection.exchange))?;
        tokio::time::sleep(Duration::from_millis(LIVE_WS_SUBSCRIBE_PAUSE_MS)).await;
    }

    let min_book_update = Duration::from_millis(LIVE_WS_MIN_BOOK_UPDATE_MS);
    let mut last_symbol_emit = BTreeMap::<String, Instant>::new();
    loop {
        let message = ws
            .next()
            .await
            .context("websocket closed")?
            .context("read websocket message")?;
        match message {
            Message::Text(text) => {
                if let Some(symbol) = fast_ws_message_symbol(&connection.exchange, &text) {
                    let now = Instant::now();
                    if last_symbol_emit
                        .get(&symbol)
                        .is_some_and(|last| now.duration_since(*last) < min_book_update)
                    {
                        continue;
                    }
                    last_symbol_emit.insert(symbol, now);
                }
                if let Some(top) = parse_direct_ws_order_book_top(&connection.exchange, &text) {
                    tx.send(DirectWsEvent::Top(top)).await.ok();
                } else if is_direct_ws_error_text(&text) {
                    tx.send(DirectWsEvent::RouteHealth(json!({
                        "route_id": format!("direct_ws:{}:connection:{connection_index}", connection.exchange),
                        "exchange": gateway_exchange_id(&connection.exchange),
                        "component": "direct_websocket_orderbook",
                        "connection_index": connection_index,
                        "status": "error",
                        "message": truncate_text(&text, 240),
                        "observed_at": Utc::now(),
                    })))
                    .await
                    .ok();
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload)).await.ok();
            }
            Message::Close(frame) => bail!("websocket closed: {frame:?}"),
            _ => {}
        }
    }
}

fn parse_direct_ws_order_book_top(exchange: &str, text: &str) -> Option<OrderBookTop> {
    let value = serde_json::from_str::<Value>(text).ok()?;
    let symbol = direct_ws_message_symbol(exchange, &value)?;
    let (book, exchange_timestamp) = match exchange {
        "binance" => {
            let data = value.get("data").unwrap_or(&value);
            (
                data,
                millis_timestamp(data.get("E").or_else(|| data.get("T"))),
            )
        }
        "bitget" => {
            let data = value
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())?;
            (data, millis_timestamp(data.get("ts")))
        }
        "gate" | "gateio" => {
            let result = value.get("result")?;
            (
                result,
                millis_timestamp(
                    result
                        .get("t")
                        .or_else(|| result.get("time_ms"))
                        .or_else(|| value.get("time_ms")),
                ),
            )
        }
        _ => return None,
    };
    let bid = first_book_level(book.get("b").or_else(|| book.get("bids"))?)?;
    let ask = first_book_level(book.get("a").or_else(|| book.get("asks"))?)?;
    let (base, quote) = canonical_parts_from_exchange_symbol(&symbol)?;
    let now = Utc::now();
    Some(OrderBookTop {
        exchange: StrategyExchangeId::new(gateway_exchange_id(exchange)),
        canonical_symbol: StrategyCanonicalSymbol::new(base, quote),
        best_bid_price: bid.0,
        best_bid_quantity: bid.1,
        best_ask_price: ask.0,
        best_ask_quantity: ask.1,
        levels: book_level_count(book),
        exchange_timestamp,
        received_at: now,
        latency_ms: exchange_timestamp.and_then(|timestamp| {
            now.signed_duration_since(timestamp)
                .num_milliseconds()
                .try_into()
                .ok()
        }),
    })
    .filter(|top| top.is_valid(1))
}

fn direct_ws_message_symbol(exchange: &str, value: &Value) -> Option<String> {
    match exchange {
        "binance" => value
            .get("data")
            .and_then(|data| data.get("s"))
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("stream")
                    .and_then(Value::as_str)
                    .and_then(|stream| stream.split('@').next())
            })
            .map(ToString::to_string),
        "bitget" => value
            .get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("data")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("instId"))
                    .and_then(Value::as_str)
            })
            .map(ToString::to_string),
        "gate" | "gateio" => value
            .get("result")
            .and_then(|result| {
                result
                    .get("contract")
                    .or_else(|| result.get("s"))
                    .or_else(|| result.get("currency_pair"))
            })
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("payload")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(Value::as_str)
            })
            .map(ToString::to_string),
        _ => None,
    }
}

fn fast_ws_message_symbol(exchange: &str, text: &str) -> Option<String> {
    let raw = match exchange {
        "binance" => quoted_after(text, "\"stream\":\"")
            .and_then(|stream| stream.split('@').next().map(ToString::to_string))
            .or_else(|| quoted_after(text, "\"s\":\"")),
        "bitget" => quoted_after(text, "\"instId\":\""),
        "gate" | "gateio" => {
            quoted_after(text, "\"contract\":\"").or_else(|| quoted_after(text, "\"payload\":[\""))
        }
        _ => None,
    }?;
    Some(raw.replace('_', "").to_ascii_uppercase())
}

fn first_book_level(value: &Value) -> Option<(f64, f64)> {
    let item = value.as_array()?.first()?;
    if let Some(items) = item.as_array() {
        return Some((number_at(items.first()?)?, number_at(items.get(1)?)?));
    }
    if let Some(object) = item.as_object() {
        let price = object
            .get("p")
            .or_else(|| object.get("price"))
            .or_else(|| object.get("px"))?;
        let quantity = object
            .get("s")
            .or_else(|| object.get("size"))
            .or_else(|| object.get("q"))
            .or_else(|| object.get("quantity"))?;
        return Some((number_at(price)?, number_at(quantity)?));
    }
    None
}

fn book_level_count(book: &Value) -> usize {
    let bids = book
        .get("b")
        .or_else(|| book.get("bids"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let asks = book
        .get("a")
        .or_else(|| book.get("asks"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    bids.min(asks).max(1)
}

fn market_snapshot_row(top: &OrderBookTop, now: DateTime<Utc>) -> Value {
    json!({
        "exchange": top.exchange.to_string(),
        "canonical_symbol": top.canonical_symbol.as_pair(),
        "symbol": top.canonical_symbol.as_pair(),
        "best_bid_price": top.best_bid_price,
        "best_bid_quantity": top.best_bid_quantity,
        "best_ask_price": top.best_ask_price,
        "best_ask_quantity": top.best_ask_quantity,
        "spread_pct": (top.best_ask_price - top.best_bid_price) / top.best_bid_price.max(1.0),
        "levels": top.levels,
        "exchange_timestamp": top.exchange_timestamp,
        "received_at": top.received_at,
        "age_ms": top.age_ms(now),
        "latency_ms": top.latency_ms,
        "source": "direct_websocket_orderbook",
    })
}

fn direct_ws_route_health_rows(
    symbols: &BTreeSet<String>,
    required_exchanges: &[String],
    tops: &BTreeMap<(String, String), OrderBookTop>,
    connected: &BTreeMap<(String, usize), (usize, DateTime<Utc>)>,
    now: DateTime<Utc>,
    stale_ms: u64,
) -> Vec<Value> {
    let mut rows = Vec::new();
    for exchange in required_exchanges {
        let exchange_id = gateway_exchange_id(exchange);
        let connection_count = connected
            .keys()
            .filter(|(connected_exchange, _)| connected_exchange == &exchange_id)
            .count();
        for symbol in symbols {
            let key = (exchange_id.clone(), symbol.clone());
            let (status, received_at, age_ms) = match tops.get(&key) {
                Some(top) if top.is_fresh(now, stale_ms) => (
                    "ok",
                    Value::String(top.received_at.to_rfc3339()),
                    json!(top.age_ms(now)),
                ),
                Some(top) => (
                    "stale",
                    Value::String(top.received_at.to_rfc3339()),
                    json!(top.age_ms(now)),
                ),
                None if connection_count > 0 => ("starting", Value::Null, Value::Null),
                None => ("disconnected", Value::Null, Value::Null),
            };
            rows.push(json!({
                "route_id": format!("direct_ws:{exchange_id}:{symbol}:book"),
                "exchange": exchange_id,
                "symbol": symbol,
                "component": "direct_websocket_orderbook",
                "status": status,
                "connection_count": connection_count,
                "observed_at": now,
                "received_at": received_at,
                "age_ms": age_ms,
            }));
        }
    }
    rows
}

fn millis_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let millis = value.and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis)
}

fn number_at(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
}

fn canonical_parts_from_exchange_symbol(symbol: &str) -> Option<(String, String)> {
    let compact = symbol.replace(['_', '-', '/'], "").to_ascii_uppercase();
    let base = compact.strip_suffix("USDT")?;
    if base.is_empty() {
        return None;
    }
    Some((base.to_string(), "USDT".to_string()))
}

fn direct_ws_exchange_id(exchange: &str) -> String {
    match gateway_exchange_id(exchange).as_str() {
        "gateio" => "gate".to_string(),
        other => other.to_string(),
    }
}

fn compact_ws_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}

fn gate_ws_symbol(symbol: &str) -> String {
    symbol.replace('/', "_").to_ascii_uppercase()
}

fn quoted_after(text: &str, needle: &str) -> Option<String> {
    let start = text.find(needle)? + needle.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn is_direct_ws_error_text(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .is_some_and(|value| {
            value.get("event").and_then(Value::as_str) == Some("error")
                || value.get("error").is_some()
                || value
                    .get("code")
                    .and_then(Value::as_str)
                    .is_some_and(|code| !matches!(code, "0" | "00000"))
        })
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn disabled_exchange_symbols_from_config(
    config: &Value,
    target_market_type: &GatewayMarketType,
) -> DisabledExchangeSymbols {
    let mut disabled = DisabledExchangeSymbols::new();
    for item in config
        .get("disabled")
        .and_then(|disabled| disabled.get("exchange_symbols"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            config
                .get("risk")
                .and_then(|risk| risk.get("disabled_exchange_symbols"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .chain(
            config
                .get("disabled_exchange_symbols")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
    {
        let Some((exchange, symbol, reason)) =
            disabled_exchange_symbol_entry(item, target_market_type)
        else {
            continue;
        };
        disabled.insert((exchange, symbol), reason);
    }
    disabled
}

fn disabled_open_exchanges_from_config(config: &Value) -> DisabledOpenExchanges {
    let mut disabled = DisabledOpenExchanges::new();
    for item in config
        .get("execution")
        .and_then(|execution| execution.get("open_disabled_exchanges"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            config
                .get("risk")
                .and_then(|risk| risk.get("open_disabled_exchanges"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .chain(
            config
                .get("open_disabled_exchanges")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
    {
        let Some((exchange, reason)) = disabled_open_exchange_entry(item) else {
            continue;
        };
        disabled.insert(exchange, reason);
    }
    disabled
}

fn disabled_open_exchange_entry(item: &Value) -> Option<(String, String)> {
    if let Some(exchange) = item.as_str() {
        let exchange = gateway_exchange_id(exchange.trim());
        if exchange.is_empty() {
            return None;
        }
        return Some((exchange, "new entries disabled by config".to_string()));
    }
    let exchange = item
        .get("exchange")
        .and_then(Value::as_str)
        .map(gateway_exchange_id)?;
    if exchange.is_empty() {
        return None;
    }
    let reason = item
        .get("reason")
        .and_then(Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or("new entries disabled by config")
        .to_string();
    Some((exchange, reason))
}

fn disabled_exchange_symbol_entry(
    item: &Value,
    target_market_type: &GatewayMarketType,
) -> Option<(String, String, String)> {
    if let Some(value) = item.as_str() {
        let (exchange, symbol) = value.split_once(':')?;
        let exchange = gateway_exchange_id(exchange.trim());
        let symbol = normalize_config_symbol(symbol.trim())?;
        return Some((exchange, symbol, "disabled by config".to_string()));
    }

    let exchange = item
        .get("exchange")
        .and_then(Value::as_str)
        .map(gateway_exchange_id)?;
    let symbol = item
        .get("symbol")
        .and_then(Value::as_str)
        .and_then(normalize_config_symbol)?;
    if let Some(market_type) = item.get("market_type").and_then(Value::as_str) {
        if !market_type_matches(market_type, target_market_type) {
            return None;
        }
    }
    let reason = item
        .get("reason")
        .and_then(Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or("disabled by config")
        .to_string();
    Some((exchange, symbol, reason))
}

fn normalize_config_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = if let Some((base, quote)) = trimmed.split_once('/') {
        format!(
            "{}/{}",
            base.trim().to_ascii_uppercase(),
            quote.trim().to_ascii_uppercase()
        )
    } else if let Some((base, quote)) = trimmed.split_once('_') {
        format!(
            "{}/{}",
            base.trim().to_ascii_uppercase(),
            quote.trim().to_ascii_uppercase()
        )
    } else {
        let upper = trimmed.replace('-', "").to_ascii_uppercase();
        if let Some(base) = upper.strip_suffix("USDT") {
            if base.is_empty() {
                return None;
            }
            format!("{base}/USDT")
        } else {
            upper
        }
    };
    Some(normalized)
}

fn market_type_matches(configured: &str, target: &GatewayMarketType) -> bool {
    let configured = configured.trim().to_ascii_lowercase();
    matches!(
        (configured.as_str(), target),
        ("spot", GatewayMarketType::Spot)
            | ("perpetual", GatewayMarketType::Perpetual)
            | ("perp", GatewayMarketType::Perpetual)
            | ("swap", GatewayMarketType::Perpetual)
            | ("futures", GatewayMarketType::Futures)
            | ("future", GatewayMarketType::Futures)
            | ("option", GatewayMarketType::Option)
    )
}

fn strategy_context(
    args: &LiveRunnerArgs,
    config: Value,
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
    args: &LiveRunnerArgs,
    capability_gate: &CapabilityGateReport,
    strategy_config: &CrossExchangeArbitrageConfig,
    dashboard_data: &LiveDashboardData,
    snapshot: Option<Value>,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
) -> Result<()> {
    let report = LiveRunnerReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        lock_file: args.lock_file.display().to_string(),
        live_orders_enabled,
        concrete_exchange_adapter_loaded,
        gateway_owned_credentials: true,
        credential_source_boundary: "gateway_app",
        market_data_provider_connected: dashboard_data.market_data_provider_connected,
        startup_position_takeover_enabled: false,
        analysis_only_reason: (!live_orders_enabled).then_some("live trading switch is disabled"),
        capability_gate: capability_gate.clone(),
        snapshot,
    };
    if let Some(path) = args.dashboard_snapshot_path.as_ref() {
        let dashboard = legacy_dashboard_snapshot(args, &report, strategy_config, dashboard_data)?;
        write_json_atomic(path, &dashboard)
            .with_context(|| format!("write dashboard snapshot {}", path.display()))?;
    }
    tracing::info!(
        target: "rustcta::cross_arb_live_runner",
        report = %serde_json::to_string(&report)?,
        "cross-arb live runner report"
    );
    Ok(())
}

fn legacy_dashboard_snapshot(
    args: &LiveRunnerArgs,
    report: &LiveRunnerReport,
    strategy_config: &CrossExchangeArbitrageConfig,
    dashboard_data: &LiveDashboardData,
) -> Result<Value> {
    let symbols = strategy_config.active_symbols();
    let venues = strategy_config.active_venues();
    let payload = report
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.get("payload"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let status = report
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.get("status"))
        .cloned()
        .unwrap_or_else(|| {
            if report.capability_gate.passed {
                json!("running")
            } else {
                json!("blocked")
            }
        });
    let latest_event_at = payload
        .get("last_event_at")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| report.generated_at.to_rfc3339());
    let started_at = payload.get("started_at").cloned().unwrap_or(Value::Null);
    let profit_rows = match args.profit_history_path.as_ref() {
        Some(path) => read_jsonl_rows(path, 500)?,
        None => Vec::new(),
    };
    let profit_summary =
        profit_summary_from_rows(&profit_rows, strategy_config.max_consecutive_losses);
    let risk_events = dashboard_risk_events(report, &profit_summary, dashboard_data.controls);
    let exchange_status = dashboard_exchange_status(report);
    let dashboard = json!({
        "data_source": "cross-exchange-arbitrage-live-runner",
        "online": report.capability_gate.passed,
        "target_refresh_ms": args.dashboard_refresh_ms,
        "dashboard_refresh_ms": args.dashboard_refresh_ms,
        "latest_event_at": latest_event_at,
        "event_dir": args
            .profit_history_path
            .as_ref()
            .and_then(|path| path.parent())
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "event_file_count": usize::from(args.profit_history_path.as_ref().is_some_and(|path| path.exists())),
        "summary": {
            "strategy_id": report.strategy_id,
            "strategy_kind": report.strategy_kind,
            "run_id": report.run_id,
            "status": status,
            "started_at": started_at,
            "latest_event_at": latest_event_at,
            "enabled_exchanges": venues,
            "enabled_symbols": symbols,
            "configured_symbols": payload
                .get("configured_symbols")
                .cloned()
                .unwrap_or_else(|| json!(strategy_config.active_symbols().len())),
            "opportunity_count": dashboard_data.opportunities.len(),
            "market_can_open_opportunities": dashboard_data
                .opportunities
                .iter()
                .filter(|row| row
                    .get("market_can_open")
                    .and_then(Value::as_bool)
                    .unwrap_or(false))
                .count(),
            "can_open_opportunities": dashboard_data
                .opportunities
                .iter()
                .filter(|row| row.get("can_open").and_then(Value::as_bool).unwrap_or(false))
                .count(),
            "live_orders_enabled": report.live_orders_enabled,
            "analysis_only": !report.live_orders_enabled,
            "analysis_only_reason": report.analysis_only_reason,
            "capability_gate_passed": report.capability_gate.passed,
            "private_stream_mode": "private_user_ws_order_fill_confirmation",
            "max_consecutive_losses": strategy_config.max_consecutive_losses,
            "consecutive_loss_closes": profit_summary
                .get("consecutive_losing_trades")
                .cloned()
                .unwrap_or_else(|| json!(0)),
            "stopped_by_loss_guard": profit_summary
                .get("stopped_by_loss_guard")
                .cloned()
                .unwrap_or_else(|| json!(false)),
            "new_entries_block_reason": dashboard_data.runtime_new_entries_block_reason,
        },
        "settings": {
            "enabled_exchanges": strategy_config.active_venues(),
            "symbols": strategy_config.active_symbols(),
            "market_type": format!("{:?}", strategy_config.market_type).to_ascii_lowercase(),
            "target_notional_usdt": strategy_config.dual_taker.target_notional_usdt,
            "min_open_spread_pct": strategy_config.dual_taker.min_open_spread_pct,
            "min_open_net_profit_pct": strategy_config.dual_taker.min_open_net_profit_pct,
            "min_open_maker_taker_net_edge": strategy_config.dual_taker.min_open_net_profit_pct,
            "max_open_spread_pct": strategy_config.dual_taker.max_open_spread_pct,
            "close_min_net_profit_pct": strategy_config.dual_taker.close_min_net_profit_pct,
            "max_open_bundles": strategy_config.dual_taker.max_open_bundles,
            "max_hold_secs": strategy_config.dual_taker.max_hold_secs,
            "close_on_max_hold_requires_profit": strategy_config
                .dual_taker
                .close_on_max_hold_requires_profit,
            "execution": {
                "trading_enabled": report.live_orders_enabled,
                "analysis_only": !report.live_orders_enabled,
                "analysis_only_reason": report.analysis_only_reason,
                "live_orders_enabled": report.live_orders_enabled,
                "market_data_source": args.market_data_source.as_str(),
                "order_place_path": "rest",
                "order_query_path": if args.allow_rest_readback_confirmation { "rest_fallback_enabled" } else { "private_ws_primary" },
                "private_reconciliation": "private_user_websocket_order_fill_confirmation",
                "start_paused_new_entries": dashboard_data.controls.start_paused_new_entries,
                "start_close_only": dashboard_data.controls.start_close_only,
                "new_entries_allowed_by_control": !dashboard_data
                    .controls
                    .start_paused_new_entries
                    && !dashboard_data.controls.start_close_only,
            },
            "execution_quality": {
                "min_open_raw_spread_pct": dashboard_data
                    .quality_controls
                    .min_open_raw_spread_pct,
                "min_open_net_edge_pct": dashboard_data
                    .quality_controls
                    .min_open_net_edge_pct,
                "min_open_executable_depth_ratio": dashboard_data
                    .quality_controls
                    .min_open_executable_depth_ratio,
                "min_close_net_profit_pct": dashboard_data
                    .quality_controls
                    .min_close_net_profit_pct,
            },
            "risk": {
                "max_consecutive_losses": strategy_config.max_consecutive_losses,
                "orderbook_stale_ms": strategy_config.dual_taker.orderbook_stale_ms,
                "max_open_bundles": strategy_config.dual_taker.max_open_bundles,
                "max_hold_secs": strategy_config.dual_taker.max_hold_secs,
                "close_on_max_hold_requires_profit": strategy_config
                    .dual_taker
                    .close_on_max_hold_requires_profit,
            }
        },
        "opportunities": dashboard_data.opportunities.clone(),
        "signals": [],
        "hedge_records": [],
        "hedge_repair_tasks": [],
        "market_snapshots": dashboard_data.market_snapshots.clone(),
        "exchange_status": exchange_status,
        "route_health": dashboard_data.route_health.clone(),
        "private_events": dashboard_data.private_events.clone(),
        "risk_events": risk_events,
        "instruments": dashboard_instruments(strategy_config),
        "instrument_feasibility": {
            "known_symbols": strategy_config.active_symbols().len(),
            "known_exchanges": strategy_config.active_venues().len(),
            "source": "runner_config"
        },
        "position_bundles": dashboard_data.position_bundles.clone(),
        "open_orders": dashboard_data.open_orders.clone(),
        "arbitrage_results": profit_rows,
        "profit_summary": profit_summary,
        "account_console": dashboard_account_console(report),
        "account_readiness": {
            "ready": report.capability_gate.passed,
            "mode": "gateway_owned_credentials",
            "required_exchanges": report.capability_gate.required_exchanges,
        },
        "strategy_readiness": {
            "ready": report.capability_gate.passed,
            "market_data_provider_connected": report.market_data_provider_connected,
            "startup_position_takeover_enabled": report.startup_position_takeover_enabled,
            "missing_requirements": report.capability_gate.missing_requirements,
            "degraded_requirements": report.capability_gate.degraded_requirements,
        }
    });
    Ok(json!({
        "generated_at": report.generated_at,
        "live_trading_enabled": report.live_orders_enabled,
        "live_preflight_enabled": report.capability_gate.passed,
        "data_source": dashboard
            .get("data_source")
            .cloned()
            .unwrap_or_else(|| json!("cross-exchange-arbitrage-live-runner")),
        "online": dashboard
            .get("online")
            .cloned()
            .unwrap_or_else(|| json!(report.capability_gate.passed)),
        "target_refresh_ms": dashboard
            .get("target_refresh_ms")
            .cloned()
            .unwrap_or_else(|| json!(args.dashboard_refresh_ms)),
        "latest_event_at": dashboard
            .get("latest_event_at")
            .cloned()
            .unwrap_or_else(|| json!(report.generated_at.to_rfc3339())),
        "summary": dashboard
            .get("summary")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "settings": dashboard
            .get("settings")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "risk_events": dashboard
            .get("risk_events")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "fees": [],
        "exchanges": dashboard
            .get("exchange_status")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "arbitrage_opportunities": dashboard
            .get("opportunities")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "cross_arb_market_snapshots": dashboard
            .get("market_snapshots")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "opportunities": dashboard
            .get("opportunities")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "signals": dashboard
            .get("signals")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "hedge_records": dashboard
            .get("hedge_records")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "hedge_repair_tasks": dashboard
            .get("hedge_repair_tasks")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "market_snapshots": dashboard
            .get("market_snapshots")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "exchange_status": dashboard
            .get("exchange_status")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "route_health": dashboard
            .get("route_health")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "private_events": dashboard
            .get("private_events")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "instruments": dashboard
            .get("instruments")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "instrument_feasibility": dashboard
            .get("instrument_feasibility")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "position_bundles": dashboard
            .get("position_bundles")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "open_orders": dashboard
            .get("open_orders")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "arbitrage_results": dashboard
            .get("arbitrage_results")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "profit_summary": dashboard
            .get("profit_summary")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "account_console": dashboard
            .get("account_console")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "account_readiness": dashboard
            .get("account_readiness")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "strategy_readiness": dashboard
            .get("strategy_readiness")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "cross_arb_dashboard": dashboard,
    }))
}

fn dashboard_exchange_status(report: &LiveRunnerReport) -> Vec<Value> {
    report
        .capability_gate
        .required_exchanges
        .iter()
        .map(|exchange| {
            let exchange_missing = report
                .capability_gate
                .missing_requirements
                .iter()
                .any(|message| message.starts_with(exchange));
            let exchange_degraded = report
                .capability_gate
                .degraded_requirements
                .iter()
                .any(|message| message.starts_with(exchange));
            json!({
                "exchange": exchange,
                "market_type": report.capability_gate.target_market_type,
                "status": if exchange_missing {
                    "blocked"
                } else if exchange_degraded {
                    "degraded"
                } else {
                    "ready"
                },
                "adapter_loaded": report.capability_gate.loaded_adapters.contains(exchange),
                "private_rest": true,
                "private_stream": true,
                "reconciliation": "private_user_websocket_observe_plus_rest_query",
            })
        })
        .collect()
}

fn dashboard_account_console(report: &LiveRunnerReport) -> Vec<Value> {
    report
        .capability_gate
        .required_exchanges
        .iter()
        .map(|exchange| {
            json!({
                "exchange": exchange,
                "credential_source": report.credential_source_boundary,
                "gateway_owned_credentials": report.gateway_owned_credentials,
                "configured": true,
            })
        })
        .collect()
}

fn dashboard_instruments(strategy_config: &CrossExchangeArbitrageConfig) -> Vec<Value> {
    strategy_config
        .active_venues()
        .into_iter()
        .flat_map(|exchange| {
            strategy_config.active_symbols().into_iter().map(move |symbol| {
                json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "market_type": format!("{:?}", strategy_config.market_type).to_ascii_lowercase(),
                    "source": "runner_config"
                })
            })
        })
        .collect()
}

fn dashboard_risk_events(
    report: &LiveRunnerReport,
    profit_summary: &Value,
    controls: LiveExecutionControls,
) -> Vec<Value> {
    let mut events = Vec::new();
    for message in &report.capability_gate.missing_requirements {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "capability_missing",
            "severity": "critical",
            "reason": message,
            "details": message,
        }));
    }
    for message in &report.capability_gate.degraded_requirements {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "capability_degraded",
            "severity": "warning",
            "reason": message,
            "details": message,
        }));
    }
    if !report.market_data_provider_connected {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "market_data_provider",
            "severity": "warning",
            "reason": "market data provider is not connected",
            "details": "runner has passed gateway capability checks but has not connected a live market data loop",
        }));
    }
    if !report.startup_position_takeover_enabled {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "startup_position_takeover",
            "severity": "warning",
            "reason": "startup position takeover is not enabled",
            "details": "existing single-leg positions must be inspected before full live trading",
        }));
    }
    if profit_summary
        .get("stopped_by_loss_guard")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "risk_auto_stop",
            "severity": "critical",
            "reason": "max_consecutive_losses_reached",
            "details": "strategy stopped after configured consecutive losing closed arbitrages",
        }));
    }
    if controls.start_close_only {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "new_entries_blocked_by_control",
            "severity": "warning",
            "reason": "close_only",
            "details": "close-only control is enabled; existing bundles may close when profitable but new entries are blocked",
        }));
    } else if controls.start_paused_new_entries {
        events.push(json!({
            "timestamp": report.generated_at,
            "event_type": "new_entries_blocked_by_control",
            "severity": "warning",
            "reason": "start_paused_new_entries",
            "details": "new entries are paused by control config; existing bundles may close when profitable",
        }));
    }
    events
}

fn read_jsonl_rows(path: &Path, limit: usize) -> Result<Vec<Value>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let mut rows = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            rows.push(value);
            if rows.len() > limit {
                let overflow = rows.len() - limit;
                rows.drain(0..overflow);
            }
        }
    }
    Ok(rows)
}

fn profit_summary_from_rows(rows: &[Value], max_consecutive_losses: u32) -> Value {
    let mut realized_profit_usdt = 0.0;
    let mut closed_arbitrages = 0_u64;
    let mut winning_arbitrages = 0_u64;
    let mut losing_arbitrages = 0_u64;
    let mut unknown_arbitrages = 0_u64;
    let mut consecutive_losing_trades = 0_u32;

    for row in rows {
        let lifecycle = row
            .get("lifecycle")
            .and_then(Value::as_str)
            .unwrap_or("close");
        if !matches!(
            lifecycle,
            "close"
                | "closed"
                | "manual_close"
                | "emergency_close"
                | "emergency_close_after_partial_close"
        ) {
            continue;
        }
        let Some(pnl) = f64_field(
            row,
            &[
                "actual_pnl_usdt",
                "realized_profit_usdt",
                "pnl_usdt",
                "profit_usdt",
            ],
        ) else {
            unknown_arbitrages += 1;
            continue;
        };
        realized_profit_usdt += pnl;
        closed_arbitrages += 1;
        if pnl < 0.0 {
            losing_arbitrages += 1;
            consecutive_losing_trades = consecutive_losing_trades.saturating_add(1);
        } else {
            winning_arbitrages += 1;
            consecutive_losing_trades = 0;
        }
    }
    let win_rate_pct = if closed_arbitrages == 0 {
        0.0
    } else {
        winning_arbitrages as f64 / closed_arbitrages as f64 * 100.0
    };
    json!({
        "realized_profit_usdt": realized_profit_usdt,
        "total_profit_usdt": realized_profit_usdt,
        "closed_arbitrages": closed_arbitrages,
        "winning_arbitrages": winning_arbitrages,
        "losing_arbitrages": losing_arbitrages,
        "unknown_arbitrages": unknown_arbitrages,
        "win_rate_pct": win_rate_pct,
        "consecutive_losing_trades": consecutive_losing_trades,
        "max_consecutive_losses": max_consecutive_losses,
        "stopped_by_loss_guard": consecutive_losing_trades >= max_consecutive_losses.max(1),
    })
}

fn f64_field(value: &Value, fields: &[&str]) -> Option<f64> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str()?.parse::<f64>().ok())
        })
    })
}

fn datetime_any_field(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|datetime| datetime.with_timezone(&Utc))
    })
}

fn text_field<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
    })
}

fn optional_text_field(value: &Value, fields: &[&str]) -> Option<String> {
    text_field(value, fields).map(ToString::to_string)
}

fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dashboard_snapshot.json");
    let tmp_path = path.with_file_name(format!("{file_name}.{}.tmp", std::process::id()));
    {
        let mut file =
            File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
        serde_json::to_writer_pretty(&mut file, value)
            .with_context(|| format!("serialize {}", tmp_path.display()))?;
        writeln!(file)?;
    }
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

async fn observe_ws_market_data_snapshot(
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    required_exchanges: &[String],
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    disabled_exchange_symbols: &DisabledExchangeSymbols,
    disabled_open_exchanges: &DisabledOpenExchanges,
) -> Result<LiveDashboardData> {
    let path = args
        .market_data_snapshot_path
        .as_ref()
        .context("live runner requires --market-data-snapshot-path from public WS observe")?;
    let deadline =
        Instant::now() + Duration::from_millis(args.market_data_snapshot_readiness_wait_ms);
    let snapshot = loop {
        match read_ready_ws_market_data_snapshot(
            path,
            required_exchanges,
            args.market_data_snapshot_stale_ms,
        ) {
            Ok(snapshot) => break snapshot,
            Err(error) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    };
    let dashboard = snapshot
        .get("cross_arb_dashboard")
        .or_else(|| snapshot.get("data"))
        .context("snapshot missing cross_arb_dashboard")?;

    let now = Utc::now();
    let active_symbols = strategy_config
        .active_symbols()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let required_exchange_set = required_exchanges
        .iter()
        .map(|exchange| gateway_exchange_id(exchange))
        .collect::<BTreeSet<_>>();
    let mut display_tops = Vec::new();
    let mut fresh_tops = Vec::new();
    let mut market_rows = Vec::new();
    let mut route_health = dashboard
        .get("route_health")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let private_events = dashboard
        .get("private_events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for row in dashboard
        .get("market_snapshots")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let Some(top) = order_book_top_from_ws_snapshot_row(&row) else {
            continue;
        };
        let exchange = gateway_exchange_id(top.exchange.as_str());
        if !required_exchange_set.contains(&exchange) {
            continue;
        }
        if !active_symbols.contains(&top.canonical_symbol.as_pair()) {
            continue;
        }
        let top = OrderBookTop {
            exchange: StrategyExchangeId::new(exchange.clone()),
            ..top
        };
        let fresh = top.is_fresh(now, strategy_config.dual_taker.orderbook_stale_ms);
        market_rows.push(row);
        display_tops.push(top.clone());
        if !fresh {
            route_health.push(json!({
                "route_id": format!("public_ws_snapshot:{}:{}:book", exchange, top.canonical_symbol.as_pair()),
                "exchange": exchange,
                "symbol": top.canonical_symbol.as_pair(),
                "component": "public_websocket_orderbook_snapshot",
                "status": "stale",
                "age_ms": top.age_ms(now),
                "observed_at": now,
            }));
            continue;
        }
        fresh_tops.push(top);
    }

    let evaluated_opportunities = evaluate_dual_taker_open_opportunities(
        &fresh_tops,
        precision_registry,
        fee_model,
        &strategy_config.dual_taker,
        now,
    );
    let mut typed_opportunities = Vec::new();
    for opportunity in evaluated_opportunities {
        if let Some((exchange, symbol, reason)) =
            opportunity_disabled_exchange_symbol(&opportunity, disabled_exchange_symbols)
        {
            route_health.push(json!({
                "route_id": format!("disabled_exchange_symbol:{}:{}:{}", exchange, symbol, opportunity.opportunity_id),
                "exchange": exchange,
                "symbol": symbol,
                "component": "live_runner_route_filter",
                "status": "disabled",
                "reason": reason,
                "observed_at": now,
            }));
            continue;
        }
        if let Some((exchange, reason)) =
            opportunity_disabled_open_exchange(&opportunity, disabled_open_exchanges)
        {
            route_health.push(json!({
                "route_id": format!("disabled_open_exchange:{}:{}", exchange, opportunity.opportunity_id),
                "exchange": exchange,
                "symbol": opportunity.canonical_symbol.as_pair(),
                "component": "live_runner_route_filter",
                "status": "disabled",
                "reason": reason,
                "observed_at": now,
            }));
            continue;
        }
        typed_opportunities.push(opportunity);
    }
    retain_best_opportunity_per_symbol(&mut typed_opportunities);
    let opportunities = display_opportunity_rows(
        strategy_config,
        fee_model,
        precision_registry,
        &display_tops,
        &typed_opportunities,
        disabled_exchange_symbols,
        disabled_open_exchanges,
        now,
    );

    Ok(LiveDashboardData {
        market_data_provider_connected: !display_tops.is_empty(),
        market_snapshots: market_rows,
        opportunities,
        route_health,
        private_events,
        position_bundles: Vec::new(),
        open_orders: Vec::new(),
        tops: fresh_tops,
        typed_opportunities,
        controls: LiveExecutionControls::default(),
        quality_controls: LiveExecutionQualityControls::default(),
        runtime_new_entries_block_reason: None,
        market_data_row_source: "snapshot_file_public_websocket",
    })
}

fn display_opportunity_rows(
    strategy_config: &CrossExchangeArbitrageConfig,
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    tops: &[OrderBookTop],
    executable_opportunities: &[DualTakerOpenOpportunity],
    disabled_exchange_symbols: &DisabledExchangeSymbols,
    disabled_open_exchanges: &DisabledOpenExchanges,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let mut executable_by_symbol = BTreeMap::new();
    for opportunity in executable_opportunities {
        executable_by_symbol.insert(opportunity.canonical_symbol.as_pair(), opportunity.clone());
    }

    strategy_config
        .active_symbols()
        .into_iter()
        .map(|symbol| {
            executable_by_symbol
                .remove(&symbol)
                .map(opportunity_row)
                .unwrap_or_else(|| {
                    display_spread_row_for_symbol(
                        &symbol,
                        strategy_config,
                        fee_model,
                        precision_registry,
                        tops,
                        disabled_exchange_symbols,
                        disabled_open_exchanges,
                        now,
                    )
                })
        })
        .collect()
}

fn display_spread_row_for_symbol(
    symbol: &str,
    strategy_config: &CrossExchangeArbitrageConfig,
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    tops: &[OrderBookTop],
    disabled_exchange_symbols: &DisabledExchangeSymbols,
    disabled_open_exchanges: &DisabledOpenExchanges,
    now: DateTime<Utc>,
) -> Value {
    let Some((base, quote)) = symbol.split_once('/') else {
        return unavailable_display_row(symbol, "invalid configured symbol");
    };
    let canonical_symbol = StrategyCanonicalSymbol::new(base, quote);
    let symbol_tops = tops
        .iter()
        .filter(|top| top.canonical_symbol == canonical_symbol)
        .collect::<Vec<_>>();
    if symbol_tops.len() < 2 {
        return unavailable_display_row(symbol, "missing live top-of-book on two venues");
    }

    let mut best_pair = None;
    let mut best_spread = f64::NEG_INFINITY;
    for long_book in &symbol_tops {
        for short_book in &symbol_tops {
            if long_book.exchange == short_book.exchange {
                continue;
            }
            if disabled_exchange_symbols.contains_key(&(
                gateway_exchange_id(long_book.exchange.as_str()),
                symbol.to_string(),
            )) || disabled_exchange_symbols.contains_key(&(
                gateway_exchange_id(short_book.exchange.as_str()),
                symbol.to_string(),
            )) {
                continue;
            }
            let spread_pct =
                (short_book.best_bid_price - long_book.best_ask_price) / long_book.best_ask_price;
            if spread_pct > best_spread {
                best_spread = spread_pct;
                best_pair = Some((*long_book, *short_book));
            }
        }
    }

    let Some((long_book, short_book)) = best_pair else {
        return unavailable_display_row(symbol, "all live routes are disabled");
    };

    let long_precision = precision_registry.get(&long_book.exchange, &canonical_symbol);
    let short_precision = precision_registry.get(&short_book.exchange, &canonical_symbol);
    let (quantity, executable_depth_usdt, quantity_reject) = display_quantity_plan(
        long_book,
        short_book,
        long_precision,
        short_precision,
        strategy_config.dual_taker.target_notional_usdt,
        strategy_config.dual_taker.top_of_book_capacity_ratio,
    );
    let long_notional_usdt = quantity * long_book.best_ask_price;
    let short_notional_usdt = quantity * short_book.best_bid_price;
    let estimated_open_fee_usdt =
        fee_model.fee_amount(&long_book.exchange, FeeRole::Taker, long_notional_usdt)
            + fee_model.fee_amount(&short_book.exchange, FeeRole::Taker, short_notional_usdt);
    let estimated_round_trip_fee_usdt = estimated_open_fee_usdt
        + fee_model.fee_amount(&long_book.exchange, FeeRole::Taker, long_notional_usdt)
        + fee_model.fee_amount(&short_book.exchange, FeeRole::Taker, short_notional_usdt);
    let expected_close_spread_pct = strategy_config
        .dual_taker
        .expected_close_spread_pct
        .max(0.0);
    let expected_spread_capture_pct = (best_spread - expected_close_spread_pct).max(0.0);
    let expected_gross_pnl_usdt = quantity * long_book.best_ask_price * expected_spread_capture_pct;
    let expected_net_pnl_usdt = expected_gross_pnl_usdt - estimated_round_trip_fee_usdt;
    let base_notional = long_notional_usdt.max(short_notional_usdt).max(1.0);
    let expected_net_profit_pct = expected_net_pnl_usdt / base_notional;

    let mut reject_reasons = Vec::new();
    if let Some(reason) = quantity_reject {
        reject_reasons.push(reason);
    }
    if !long_book.is_fresh(now, strategy_config.dual_taker.orderbook_stale_ms)
        || !short_book.is_fresh(now, strategy_config.dual_taker.orderbook_stale_ms)
    {
        reject_reasons.push(format!(
            "live top-of-book is stale: long_age_ms {}, short_age_ms {}, limit_ms {}",
            long_book.age_ms(now),
            short_book.age_ms(now),
            strategy_config.dual_taker.orderbook_stale_ms
        ));
    }
    if best_spread < strategy_config.dual_taker.min_open_spread_pct {
        reject_reasons.push(format!(
            "raw spread {} is below min open raw spread {}",
            format_float(best_spread),
            format_float(strategy_config.dual_taker.min_open_spread_pct)
        ));
    }
    if best_spread > strategy_config.dual_taker.max_open_spread_pct {
        reject_reasons.push(format!(
            "raw spread {} is above max open raw spread {}",
            format_float(best_spread),
            format_float(strategy_config.dual_taker.max_open_spread_pct)
        ));
    }
    if expected_net_profit_pct < strategy_config.dual_taker.min_open_net_profit_pct {
        reject_reasons.push(format!(
            "expected net edge {} is below min open net edge {}",
            format_float(expected_net_profit_pct),
            format_float(strategy_config.dual_taker.min_open_net_profit_pct)
        ));
    }
    for exchange in [
        gateway_exchange_id(long_book.exchange.as_str()),
        gateway_exchange_id(short_book.exchange.as_str()),
    ] {
        if let Some(reason) = disabled_open_exchanges.get(&exchange) {
            reject_reasons.push(format!("new entries disabled on {exchange}: {reason}"));
        }
    }

    json!({
        "opportunity_id": format!(
            "{}:{}:{}:{}",
            symbol,
            long_book.exchange,
            short_book.exchange,
            long_book.received_at.timestamp_millis().max(short_book.received_at.timestamp_millis())
        ),
        "canonical_symbol": symbol,
        "symbol": symbol,
        "long_exchange": long_book.exchange.to_string(),
        "short_exchange": short_book.exchange.to_string(),
        "long_entry_price": long_book.best_ask_price,
        "short_entry_price": short_book.best_bid_price,
        "long_best_bid_price": long_book.best_bid_price,
        "long_best_bid_quantity": long_book.best_bid_quantity,
        "long_best_ask_price": long_book.best_ask_price,
        "long_best_ask_quantity": long_book.best_ask_quantity,
        "short_best_bid_price": short_book.best_bid_price,
        "short_best_bid_quantity": short_book.best_bid_quantity,
        "short_best_ask_price": short_book.best_ask_price,
        "short_best_ask_quantity": short_book.best_ask_quantity,
        "spread_pct": best_spread,
        "raw_open_spread_pct": best_spread,
        "raw_spread_pct": best_spread,
        "raw_open_spread_bps": best_spread * 10_000.0,
        "spread_bps": best_spread * 10_000.0,
        "quantity": quantity,
        "target_notional_usdt": strategy_config.dual_taker.target_notional_usdt,
        "long_notional_usdt": long_notional_usdt,
        "short_notional_usdt": short_notional_usdt,
        "executable_top_depth_usdt": executable_depth_usdt,
        "estimated_open_fee_usdt": estimated_open_fee_usdt,
        "estimated_round_trip_fee_usdt": estimated_round_trip_fee_usdt,
        "expected_gross_pnl_usdt": expected_gross_pnl_usdt,
        "expected_net_pnl_usdt": expected_net_pnl_usdt,
        "expected_net_profit_pct": expected_net_profit_pct,
        "maker_taker_net_edge": expected_net_profit_pct,
        "submit_parallel": true,
        "orders": [],
        "can_open": false,
        "reject_reasons": reject_reasons.join("; "),
        "price_basis": "long best ask / short best bid",
        "raw_spread_price_basis": "long_best_ask_price / short_best_bid_price",
        "source": "public_websocket_snapshot_display",
        "book_age_ms": long_book.age_ms(now).max(short_book.age_ms(now)),
        "long_book_age_ms": long_book.age_ms(now),
        "short_book_age_ms": short_book.age_ms(now),
    })
}

fn display_quantity_plan(
    long_book: &OrderBookTop,
    short_book: &OrderBookTop,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
    target_notional_usdt: f64,
    top_of_book_capacity_ratio: f64,
) -> (f64, f64, Option<String>) {
    let capacity_ratio = top_of_book_capacity_ratio.min(1.0);
    let long_top_base_quantity =
        long_precision.base_quantity_from_order_quantity(long_book.best_ask_quantity);
    let short_top_base_quantity =
        short_precision.base_quantity_from_order_quantity(short_book.best_bid_quantity);
    let executable_depth_usdt = (long_top_base_quantity * long_book.best_ask_price)
        .min(short_top_base_quantity * short_book.best_bid_price)
        * capacity_ratio;
    let mut quantity = (target_notional_usdt / long_book.best_ask_price)
        .min(long_top_base_quantity * capacity_ratio)
        .min(short_top_base_quantity * capacity_ratio);
    quantity = display_normalized_shared_base_quantity(quantity, long_precision, short_precision);
    let reject = if executable_depth_usdt < target_notional_usdt {
        Some(format!(
            "top-of-book executable depth {} is below target notional {}",
            format_float(executable_depth_usdt),
            format_float(target_notional_usdt)
        ))
    } else if quantity <= 0.0
        || quantity < long_precision.min_base_quantity()
        || quantity < short_precision.min_base_quantity()
    {
        Some("quantity is below exchange minimum".to_string())
    } else if long_precision.min_notional_usdt > 0.0
        && quantity * long_book.best_ask_price < long_precision.min_notional_usdt
    {
        Some("long leg notional is below exchange minimum".to_string())
    } else if short_precision.min_notional_usdt > 0.0
        && quantity * short_book.best_bid_price < short_precision.min_notional_usdt
    {
        Some("short leg notional is below exchange minimum".to_string())
    } else {
        None
    };
    (quantity.max(0.0), executable_depth_usdt.max(0.0), reject)
}

fn display_normalized_shared_base_quantity(
    base_quantity: f64,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
) -> f64 {
    let mut quantity = base_quantity.max(0.0);
    for _ in 0..3 {
        let long_quantity = long_precision.normalized_base_quantity(quantity);
        let short_quantity = short_precision.normalized_base_quantity(quantity);
        let next_quantity = long_quantity.min(short_quantity);
        if (next_quantity - quantity).abs() <= 1e-12 {
            return next_quantity;
        }
        quantity = next_quantity;
    }
    quantity
}

fn unavailable_display_row(symbol: &str, reason: &str) -> Value {
    json!({
        "opportunity_id": format!("{symbol}:unavailable"),
        "canonical_symbol": symbol,
        "symbol": symbol,
        "long_exchange": "-",
        "short_exchange": "-",
        "long_entry_price": null,
        "short_entry_price": null,
        "long_best_bid_price": null,
        "long_best_bid_quantity": null,
        "long_best_ask_price": null,
        "long_best_ask_quantity": null,
        "short_best_bid_price": null,
        "short_best_bid_quantity": null,
        "short_best_ask_price": null,
        "short_best_ask_quantity": null,
        "spread_pct": 0.0,
        "raw_open_spread_pct": 0.0,
        "raw_spread_pct": 0.0,
        "raw_open_spread_bps": 0.0,
        "spread_bps": 0.0,
        "quantity": 0.0,
        "target_notional_usdt": 0.0,
        "long_notional_usdt": 0.0,
        "short_notional_usdt": 0.0,
        "executable_top_depth_usdt": 0.0,
        "estimated_open_fee_usdt": 0.0,
        "estimated_round_trip_fee_usdt": 0.0,
        "expected_gross_pnl_usdt": 0.0,
        "expected_net_pnl_usdt": 0.0,
        "expected_net_profit_pct": 0.0,
        "maker_taker_net_edge": 0.0,
        "submit_parallel": false,
        "orders": [],
        "can_open": false,
        "reject_reasons": reason,
        "price_basis": "missing live top-of-book",
        "raw_spread_price_basis": "missing live top-of-book",
        "source": "public_websocket_snapshot_display",
    })
}

fn retain_best_opportunity_per_symbol(opportunities: &mut Vec<DualTakerOpenOpportunity>) {
    opportunities.sort_by(|left, right| {
        right
            .expected_net_profit_pct
            .partial_cmp(&left.expected_net_profit_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .spread_pct
                    .partial_cmp(&left.spread_pct)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    let mut seen_symbols = BTreeSet::new();
    opportunities.retain(|opportunity| seen_symbols.insert(opportunity.canonical_symbol.as_pair()));
}

fn opportunity_disabled_exchange_symbol(
    opportunity: &DualTakerOpenOpportunity,
    disabled_exchange_symbols: &DisabledExchangeSymbols,
) -> Option<(String, String, String)> {
    for order in &opportunity.orders {
        let exchange = gateway_exchange_id(order.exchange.as_str());
        let symbol = order.canonical_symbol.as_pair();
        if let Some(reason) = disabled_exchange_symbols.get(&(exchange.clone(), symbol.clone())) {
            return Some((exchange, symbol, reason.clone()));
        }
    }
    None
}

fn opportunity_disabled_open_exchange(
    opportunity: &DualTakerOpenOpportunity,
    disabled_open_exchanges: &DisabledOpenExchanges,
) -> Option<(String, String)> {
    for order in &opportunity.orders {
        let exchange = gateway_exchange_id(order.exchange.as_str());
        if let Some(reason) = disabled_open_exchanges.get(&exchange) {
            return Some((exchange, reason.clone()));
        }
    }
    None
}

fn read_ready_ws_market_data_snapshot(
    path: &Path,
    required_exchanges: &[String],
    stale_ms: u64,
) -> Result<Value> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read public WS market data snapshot {}", path.display()))?;
    let snapshot: Value = serde_json::from_str(&raw)
        .with_context(|| format!("parse public WS market data snapshot {}", path.display()))?;
    let dashboard = snapshot
        .get("cross_arb_dashboard")
        .or_else(|| snapshot.get("data"))
        .context("snapshot missing cross_arb_dashboard")?;
    validate_ws_snapshot_readiness(dashboard, required_exchanges)?;
    validate_ws_snapshot_freshness(dashboard, stale_ms)?;
    Ok(snapshot)
}

fn validate_ws_snapshot_readiness(dashboard: &Value, required_exchanges: &[String]) -> Result<()> {
    let source = dashboard
        .get("data_source")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if source != "cross-exchange-arbitrage-ws-observe" {
        bail!(
            "market data snapshot source must be cross-exchange-arbitrage-ws-observe, got {source}"
        );
    }
    let execution_mode = dashboard
        .get("execution_mode")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if execution_mode != "public_ws_observe_only" {
        bail!("market data snapshot must come from public websocket observe mode, got {execution_mode}");
    }
    let private_stream_mode = dashboard
        .get("summary")
        .and_then(|value| value.get("private_stream_mode"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if private_stream_mode != "private_user_ws_observe" {
        bail!("private user websocket observe must be online before live execution, got {private_stream_mode}");
    }
    let exchange_status = dashboard
        .get("exchange_status")
        .and_then(Value::as_array)
        .context("snapshot missing exchange_status")?;
    for exchange in required_exchanges {
        let normalized = if exchange == "gateio" {
            "gate"
        } else {
            exchange
        };
        let row = exchange_status
            .iter()
            .find(|row| {
                row.get("exchange")
                    .and_then(Value::as_str)
                    .is_some_and(|value| {
                        value == normalized || gateway_exchange_id(value) == *exchange
                    })
            })
            .with_context(|| format!("snapshot missing exchange status for {exchange}"))?;
        let market_status = row
            .get("market_data_status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if market_status != "online" {
            let message = row
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            bail!(
                "{exchange} public websocket market data is not online: {market_status}; {message}"
            );
        }
        let private_status = row
            .get("private_stream_status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if private_status != "online" {
            let message = row
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            bail!("{exchange} private user websocket is not online: {private_status}; {message}");
        }
    }
    Ok(())
}

fn validate_ws_snapshot_freshness(dashboard: &Value, stale_ms: u64) -> Result<()> {
    let now = Utc::now();
    let latest = dashboard
        .get("latest_event_at")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .context("snapshot missing latest_event_at")?;
    let age_ms = now.signed_duration_since(latest).num_milliseconds();
    if age_ms > stale_ms as i64 {
        bail!("public websocket snapshot is stale: age_ms={age_ms}, limit_ms={stale_ms}");
    }
    Ok(())
}

fn order_book_top_from_ws_snapshot_row(row: &Value) -> Option<OrderBookTop> {
    let exchange = row.get("exchange").and_then(Value::as_str)?;
    let symbol = row
        .get("canonical_symbol")
        .or_else(|| row.get("symbol"))
        .and_then(Value::as_str)?;
    let (base, quote) = symbol.split_once('/')?;
    let received_at = datetime_field(row, "received_at")?;
    Some(OrderBookTop {
        exchange: StrategyExchangeId::new(gateway_exchange_id(exchange)),
        canonical_symbol: StrategyCanonicalSymbol::new(base, quote),
        best_bid_price: f64_field(row, &["best_bid_price"])?,
        best_bid_quantity: f64_field(row, &["best_bid_quantity"])?,
        best_ask_price: f64_field(row, &["best_ask_price"])?,
        best_ask_quantity: f64_field(row, &["best_ask_quantity"])?,
        levels: row
            .get("levels")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(1),
        exchange_timestamp: datetime_field(row, "exchange_timestamp"),
        received_at,
        latency_ms: row.get("latency_ms").and_then(Value::as_u64),
    })
    .filter(|top| top.is_valid(1))
}

fn datetime_field(row: &Value, field: &str) -> Option<DateTime<Utc>> {
    let value = row.get(field)?;
    if value.is_null() {
        return None;
    }
    value.as_str()?.parse::<DateTime<Utc>>().ok()
}

fn opportunity_row(
    opportunity: rustcta_strategy_cross_exchange_arbitrage::DualTakerOpenOpportunity,
) -> Value {
    json!({
        "opportunity_id": opportunity.opportunity_id,
        "canonical_symbol": opportunity.canonical_symbol.as_pair(),
        "symbol": opportunity.canonical_symbol.as_pair(),
        "long_exchange": opportunity.long_exchange.to_string(),
        "short_exchange": opportunity.short_exchange.to_string(),
        "long_entry_price": opportunity.long_entry_price,
        "short_entry_price": opportunity.short_entry_price,
        "long_best_bid_price": null,
        "long_best_bid_quantity": null,
        "long_best_ask_price": opportunity.long_entry_price,
        "long_best_ask_quantity": null,
        "short_best_bid_price": opportunity.short_entry_price,
        "short_best_bid_quantity": null,
        "short_best_ask_price": null,
        "short_best_ask_quantity": null,
        "spread_pct": opportunity.spread_pct,
        "raw_open_spread_pct": opportunity.spread_pct,
        "raw_spread_pct": opportunity.spread_pct,
        "raw_open_spread_bps": opportunity.spread_pct * 10_000.0,
        "spread_bps": opportunity.spread_pct * 10_000.0,
        "quantity": opportunity.quantity,
        "long_notional_usdt": opportunity.long_notional_usdt,
        "short_notional_usdt": opportunity.short_notional_usdt,
        "executable_top_depth_usdt": opportunity.executable_top_depth_usdt,
        "estimated_open_fee_usdt": opportunity.estimated_open_fee_usdt,
        "estimated_round_trip_fee_usdt": opportunity.estimated_round_trip_fee_usdt,
        "expected_gross_pnl_usdt": opportunity.expected_gross_pnl_usdt,
        "expected_net_pnl_usdt": opportunity.expected_net_pnl_usdt,
        "expected_net_profit_pct": opportunity.expected_net_profit_pct,
        "maker_taker_net_edge": opportunity.expected_net_profit_pct,
        "submit_parallel": opportunity.submit_parallel,
        "orders": opportunity.orders,
        "can_open": false,
        "reject_reasons": "not evaluated by live execution state yet",
        "price_basis": "long best ask / short best bid",
        "raw_spread_price_basis": "long_best_ask_price / short_best_bid_price",
        "source": "public_websocket_snapshot",
    })
}

fn fee_model_from_config(config: &Value) -> FeeModel {
    let default = ExchangeFeeRates {
        maker: f64_at_path(config, &["fees", "default_maker_fee_rate"]).unwrap_or(0.0002),
        taker: f64_at_path(config, &["fees", "default_taker_fee_rate"]).unwrap_or(0.0006),
    };
    let mut per_exchange = HashMap::new();
    if let Some(exchanges) = config
        .get("fees")
        .and_then(|value| value.get("per_exchange"))
        .and_then(Value::as_object)
    {
        for (exchange, value) in exchanges {
            let maker = value
                .get("maker")
                .and_then(value_as_f64)
                .unwrap_or(default.maker);
            let taker = value
                .get("taker")
                .and_then(value_as_f64)
                .unwrap_or(default.taker);
            per_exchange.insert(
                StrategyExchangeId::new(gateway_exchange_id(exchange)),
                ExchangeFeeRates { maker, taker },
            );
        }
    }
    FeeModel::new(default, per_exchange)
}

async fn load_precision_registry(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
) -> Result<PrecisionRegistry> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let gateio_contract_sizes =
        load_gateio_contract_sizes(strategy_config, target_market_type).await;
    let mut registry = PrecisionRegistry::default();
    for exchange in gateway_exchange_ids(strategy_config.active_venues()) {
        let exchange_id = GatewayExchangeId::new(exchange.clone())?;
        let symbols = strategy_config
            .active_symbols()
            .into_iter()
            .filter_map(|symbol| {
                let (base, quote) = symbol.split_once('/')?;
                if !is_ascii_symbol_part(base) || !is_ascii_symbol_part(quote) {
                    return None;
                }
                Some((base.to_string(), quote.to_string()))
            })
            .map(|(base, quote)| {
                let canonical_symbol = CanonicalSymbol::new(&base, &quote)?;
                let exchange_symbol = ExchangeSymbol::new(
                    exchange_id.clone(),
                    target_market_type,
                    exchange_symbol_text(&exchange, &canonical_symbol),
                )?;
                Ok(SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: target_market_type,
                    canonical_symbol: Some(canonical_symbol),
                    exchange_symbol,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if symbols.is_empty() {
            continue;
        }
        let request_id = format!("rules-{exchange}");
        let mut context = RequestContext::new(Utc::now());
        context.tenant_id = Some(tenant_id.clone());
        context.account_id = Some(account_id.clone());
        context.run_id = Some(run_id.clone());
        context.request_id = Some(request_id.clone());
        let response = match gateway
            .get_symbol_rules(
                request_id,
                tenant_id.clone(),
                Some(account_id.clone()),
                SymbolRulesRequest {
                    schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context,
                    symbols,
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(
                    target: "rustcta::cross_arb_live_runner",
                    exchange = %exchange,
                    error = %error,
                    "load symbol rules failed"
                );
                continue;
            }
        };
        for rule in response.rules {
            if let Some((strategy_exchange, symbol, precision)) =
                strategy_precision_from_rule(&rule, &gateio_contract_sizes)
            {
                registry.insert(strategy_exchange, symbol, precision);
            }
        }
    }
    Ok(registry)
}

async fn load_gateio_contract_sizes(
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
) -> BTreeMap<String, f64> {
    if target_market_type != GatewayMarketType::Perpetual
        || !gateway_exchange_ids(strategy_config.active_venues())
            .iter()
            .any(|exchange| exchange == "gateio")
    {
        return BTreeMap::new();
    }

    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("RustCTA-CrossArbLiveRunner/0.3")
        .build()
    else {
        return BTreeMap::new();
    };
    let url = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
    let Ok(response) = client.get(url).send().await else {
        tracing::warn!(
            target: "rustcta::cross_arb_live_runner",
            "load Gate.io contract sizes failed: request failed"
        );
        return BTreeMap::new();
    };
    let Ok(value) = response.json::<Value>().await else {
        tracing::warn!(
            target: "rustcta::cross_arb_live_runner",
            "load Gate.io contract sizes failed: invalid JSON"
        );
        return BTreeMap::new();
    };
    gateio_contract_sizes_from_value(&value)
}

fn gateio_contract_sizes_from_value(value: &Value) -> BTreeMap<String, f64> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let contract = row
                .get("name")
                .or_else(|| row.get("contract"))
                .and_then(Value::as_str)?
                .to_ascii_uppercase();
            let contract_size = row
                .get("quanto_multiplier")
                .and_then(value_to_positive_f64)
                .filter(|value| *value > 0.0)?;
            Some((contract, contract_size))
        })
        .collect()
}

fn is_ascii_symbol_part(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn strategy_precision_from_rule(
    rule: &SymbolRules,
    gateio_contract_sizes: &BTreeMap<String, f64>,
) -> Option<(StrategyExchangeId, StrategyCanonicalSymbol, SymbolPrecision)> {
    let canonical = rule.symbol.canonical_symbol.as_ref()?;
    let exchange = gateway_exchange_id(rule.symbol.exchange.as_str());
    let symbol = StrategyCanonicalSymbol::new(canonical.base_asset(), canonical.quote_asset());
    let quantity_unit =
        if exchange == "gateio" && rule.symbol.market_type == GatewayMarketType::Perpetual {
            QuantityUnit::Contracts
        } else {
            QuantityUnit::Base
        };
    Some((
        StrategyExchangeId::new(exchange),
        symbol,
        SymbolPrecision {
            price_tick: parse_positive_optional(rule.price_increment.as_deref().unwrap_or("0"))
                .unwrap_or(0.0),
            quantity_step: parse_positive_optional(
                rule.quantity_increment.as_deref().unwrap_or("0"),
            )
            .unwrap_or(0.0),
            min_quantity: parse_positive_optional(rule.min_quantity.as_deref().unwrap_or("0"))
                .unwrap_or(0.0),
            min_notional_usdt: parse_positive_optional(rule.min_notional.as_deref().unwrap_or("0"))
                .unwrap_or(0.0),
            quantity_unit,
            contract_size: if quantity_unit == QuantityUnit::Contracts {
                gateio_contract_sizes
                    .get(&rule.symbol.exchange_symbol.symbol.to_ascii_uppercase())
                    .copied()
                    .unwrap_or(1.0)
            } else {
                1.0
            },
        },
    ))
}

fn f64_at_path(value: &Value, path: &[&str]) -> Option<f64> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    value_as_f64(current)
}

fn bool_at_path(value: &Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_bool().or_else(|| {
        let text = current.as_str()?.trim().to_ascii_lowercase();
        match text.as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        }
    })
}

fn u64_at_path(value: &Value, path: &[&str]) -> Option<u64> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_u64().or_else(|| {
        current
            .as_str()?
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
    })
}

fn text_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
}

async fn run_live_execution_cycle(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    live_orders_enabled: bool,
    allow_new_entries: bool,
    new_entries_block_reason: Option<&'static str>,
    quality_controls: LiveExecutionQualityControls,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
    state: &mut LiveExecutionState,
    dashboard: &mut LiveDashboardData,
) -> Result<()> {
    state.recent_events.clear();
    state.recent_open_orders.clear();
    dashboard.runtime_new_entries_block_reason = new_entries_block_reason.map(ToString::to_string);

    if !live_orders_enabled {
        hydrate_dashboard_execution_state(
            state,
            strategy_config,
            fee_model,
            precision_registry,
            quality_controls,
            dashboard,
        );
        return Ok(());
    }

    if let Some(reason) = new_entries_block_reason {
        push_new_entries_control_event(state, reason);
    }

    let loss_guard_triggered_before_close =
        profit_history_loss_guard_triggered(args, strategy_config.max_consecutive_losses)?;
    if loss_guard_triggered_before_close {
        push_loss_guard_event(state);
        dashboard.runtime_new_entries_block_reason =
            Some("new entries blocked by consecutive loss guard".to_string());
    }

    if state.manual_intervention_required {
        state.recent_events.push(json!({
            "event_type": "manual_intervention_required",
            "severity": "critical",
            "message": state.manual_intervention_reason.clone().unwrap_or_else(|| {
                "live execution halted after an uncertain order state".to_string()
            }),
            "occurred_at": Utc::now(),
        }));
        hydrate_dashboard_execution_state(
            state,
            strategy_config,
            fee_model,
            precision_registry,
            quality_controls,
            dashboard,
        );
        return Ok(());
    }

    close_ready_bundles(
        gateway,
        ctx,
        args,
        strategy_config,
        target_market_type,
        fee_model,
        precision_registry,
        quality_controls,
        confirmation,
        sinks,
        state,
        dashboard,
    )
    .await?;

    let loss_guard_blocks_new_entries = loss_guard_triggered_before_close
        || profit_history_loss_guard_triggered(args, strategy_config.max_consecutive_losses)?;
    if loss_guard_blocks_new_entries {
        if !loss_guard_triggered_before_close {
            push_loss_guard_event(state);
        }
        dashboard.runtime_new_entries_block_reason =
            Some("new entries blocked by consecutive loss guard".to_string());
        hydrate_dashboard_execution_state(
            state,
            strategy_config,
            fee_model,
            precision_registry,
            quality_controls,
            dashboard,
        );
        return Ok(());
    }

    if allow_new_entries
        && has_eligible_open_opportunity(
            state,
            strategy_config,
            dashboard,
            quality_controls,
            Utc::now(),
        )
    {
        if !detect_unmanaged_external_positions(
            gateway,
            args,
            strategy_config,
            target_market_type,
            state,
        )
        .await?
        {
            if state.manual_intervention_required {
                dashboard.opportunities.clear();
                dashboard.typed_opportunities.clear();
            }
            hydrate_dashboard_execution_state(
                state,
                strategy_config,
                fee_model,
                precision_registry,
                quality_controls,
                dashboard,
            );
            return Ok(());
        }
        open_best_opportunity(
            gateway,
            ctx,
            args,
            strategy_config,
            target_market_type,
            fee_model,
            precision_registry,
            quality_controls,
            confirmation,
            sinks,
            state,
            dashboard,
        )
        .await?;
    }

    hydrate_dashboard_execution_state(
        state,
        strategy_config,
        fee_model,
        precision_registry,
        quality_controls,
        dashboard,
    );
    Ok(())
}

fn has_eligible_open_opportunity(
    state: &LiveExecutionState,
    strategy_config: &CrossExchangeArbitrageConfig,
    dashboard: &LiveDashboardData,
    quality_controls: LiveExecutionQualityControls,
    now: DateTime<Utc>,
) -> bool {
    let risk_state = live_risk_state(state);
    dashboard.typed_opportunities.iter().any(|opportunity| {
        opportunity.expected_net_profit_pct >= strategy_config.dual_taker.min_open_net_profit_pct
            && quality_controls.open_allows(opportunity)
            && opportunity.orders.len() >= 2
            && route_cooldown_block_reason(
                state,
                &opportunity.long_exchange,
                &opportunity.short_exchange,
                now,
            )
            .is_none()
            && risk_state
                .can_open(
                    &opportunity.canonical_symbol,
                    &opportunity.long_exchange,
                    &opportunity.short_exchange,
                    &strategy_config.dual_taker,
                    now,
                )
                .is_ok()
    })
}

fn route_cooldown_key(
    long_exchange: &StrategyExchangeId,
    short_exchange: &StrategyExchangeId,
) -> String {
    format!("{}->{}", long_exchange.as_str(), short_exchange.as_str())
}

fn route_cooldown_block_reason(
    state: &LiveExecutionState,
    long_exchange: &StrategyExchangeId,
    short_exchange: &StrategyExchangeId,
    now: DateTime<Utc>,
) -> Option<String> {
    let key = route_cooldown_key(long_exchange, short_exchange);
    let cooldown = state.route_cooldowns.get(&key)?;
    if cooldown.until <= now {
        return None;
    }
    Some(format!(
        "open route {} is cooling down until {} after {} on {} ({})",
        key,
        cooldown.until.to_rfc3339(),
        cooldown.reason,
        cooldown.symbol,
        cooldown.source_bundle_id
    ))
}

fn record_failed_open_route_cooldown(
    state: &mut LiveExecutionState,
    symbol: &StrategyCanonicalSymbol,
    long_exchange: &StrategyExchangeId,
    short_exchange: &StrategyExchangeId,
    bundle_id: &str,
    reason: &str,
    now: DateTime<Utc>,
) {
    let key = route_cooldown_key(long_exchange, short_exchange);
    let symbol_pair = symbol.as_pair();
    let until = now + ChronoDuration::seconds(FAILED_OPEN_ROUTE_COOLDOWN_SECS);
    state.route_cooldowns.insert(
        key.clone(),
        RouteCooldown {
            until,
            symbol: symbol_pair.clone(),
            reason: reason.to_string(),
            source_bundle_id: bundle_id.to_string(),
        },
    );
    state.recent_events.push(json!({
        "event_type": "open_route_cooldown",
        "severity": "warning",
        "route": key,
        "symbol": symbol_pair,
        "bundle_id": bundle_id,
        "cooldown_until": until,
        "cooldown_secs": FAILED_OPEN_ROUTE_COOLDOWN_SECS,
        "reason": reason,
        "message": "open route is cooling down after an incomplete or uncertain hedge attempt",
        "occurred_at": now,
    }));
}

fn restore_route_cooldowns_from_profit_history(
    args: &LiveRunnerArgs,
    state: &mut LiveExecutionState,
    now: DateTime<Utc>,
) -> Result<()> {
    let Some(path) = args.profit_history_path.as_ref() else {
        return Ok(());
    };
    let rows = read_jsonl_rows(path, 500)?;
    for row in rows {
        let Some(recorded_at) = datetime_any_field(&row, &["recorded_at", "planned_at"]) else {
            continue;
        };
        let age_secs = (now - recorded_at).num_seconds();
        if !(0..FAILED_OPEN_ROUTE_COOLDOWN_SECS).contains(&age_secs) {
            continue;
        }
        let Some((symbol, long_exchange, short_exchange, bundle_id, reason)) =
            route_cooldown_from_profit_row(&row)
        else {
            continue;
        };
        let key = route_cooldown_key(&long_exchange, &short_exchange);
        let until = recorded_at + ChronoDuration::seconds(FAILED_OPEN_ROUTE_COOLDOWN_SECS);
        state.route_cooldowns.insert(
            key.clone(),
            RouteCooldown {
                until,
                symbol: symbol.as_pair(),
                reason: reason.clone(),
                source_bundle_id: bundle_id.clone(),
            },
        );
        state.recent_events.push(json!({
            "event_type": "open_route_cooldown_restored",
            "severity": "warning",
            "route": key,
            "symbol": symbol.as_pair(),
            "bundle_id": bundle_id,
            "cooldown_until": until,
            "cooldown_secs": FAILED_OPEN_ROUTE_COOLDOWN_SECS,
            "reason": reason,
            "message": "open route cooldown was restored from recent profit history",
            "occurred_at": now,
        }));
    }
    Ok(())
}

fn route_cooldown_from_profit_row(
    row: &Value,
) -> Option<(
    StrategyCanonicalSymbol,
    StrategyExchangeId,
    StrategyExchangeId,
    String,
    String,
)> {
    let lifecycle = row.get("lifecycle").and_then(Value::as_str)?;
    let both_legs_filled = row
        .get("both_legs_filled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let reason = match lifecycle {
        "open" if !both_legs_filled => "recent_incomplete_open",
        "emergency_close" => "recent_open_emergency_close",
        "close" if !both_legs_filled => "recent_incomplete_close",
        "emergency_close_after_partial_close" => "recent_partial_close_emergency_repair",
        _ => return None,
    }
    .to_string();
    let symbol = strategy_symbol_from_text(text_field(row, &["canonical_symbol", "symbol"])?)?;
    let long_exchange = text_field(row, &["long_exchange"])
        .and_then(|exchange| Some(StrategyExchangeId::new(gateway_exchange_id(exchange))))
        .or_else(|| exchange_for_role(row, "open_long"))
        .or_else(|| exchange_for_role(row, "close_long"))?;
    let short_exchange = text_field(row, &["short_exchange"])
        .and_then(|exchange| Some(StrategyExchangeId::new(gateway_exchange_id(exchange))))
        .or_else(|| exchange_for_role(row, "open_short"))
        .or_else(|| exchange_for_role(row, "close_short"))?;
    let bundle_id = text_field(row, &["bundle_id"])
        .unwrap_or("unknown-bundle")
        .to_string();
    Some((symbol, long_exchange, short_exchange, bundle_id, reason))
}

fn exchange_for_role(row: &Value, role: &str) -> Option<StrategyExchangeId> {
    row.get("legs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            row.get("normal_close_legs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .find(|leg| text_field(leg, &["role"]) == Some(role))
        .and_then(|leg| text_field(leg, &["exchange"]))
        .map(|exchange| StrategyExchangeId::new(gateway_exchange_id(exchange)))
}

fn strategy_symbol_from_text(symbol: &str) -> Option<StrategyCanonicalSymbol> {
    let (base, quote) = symbol.split_once('/')?;
    Some(StrategyCanonicalSymbol::new(base, quote))
}

fn push_loss_guard_event(state: &mut LiveExecutionState) {
    state.recent_events.push(json!({
        "event_type": "risk_auto_stop",
        "severity": "critical",
        "message": "live order submission blocked because profit history reached max consecutive losses",
        "occurred_at": Utc::now(),
    }));
}

fn push_new_entries_control_event(state: &mut LiveExecutionState, reason: &str) {
    state.recent_events.push(json!({
        "event_type": "new_entries_blocked_by_control",
        "severity": "warning",
        "reason": reason,
        "message": format!("new entry submission is blocked: {reason}; existing bundles remain eligible for profitable close"),
        "occurred_at": Utc::now(),
    }));
}

async fn recover_open_bundles_from_dashboard_snapshot(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    state: &mut LiveExecutionState,
) -> Result<()> {
    let Some(path) = args.dashboard_snapshot_path.as_ref() else {
        return Ok(());
    };
    let Some(snapshot_bundles) = read_recoverable_open_bundles(path)? else {
        return Ok(());
    };
    if snapshot_bundles.is_empty() {
        return Ok(());
    }

    let external_positions =
        fetch_external_position_snapshots(gateway, args, strategy_config, target_market_type)
            .await
            .with_context(|| {
                format!(
                    "recover open bundles from dashboard snapshot {}",
                    path.display()
                )
            })?;

    let mut recovered = BTreeMap::new();
    let mut failures = Vec::new();
    for bundle in snapshot_bundles {
        let missing = recovered_bundle_position_mismatch(&bundle, &external_positions);
        if missing.is_empty() {
            state.symbol_cooldowns.insert(
                bundle.position.canonical_symbol.as_pair(),
                Utc::now() + ChronoDuration::seconds(30),
            );
            recovered.insert(bundle.bundle_id.clone(), bundle);
        } else {
            failures.push(format!("{}: {}", bundle.bundle_id, missing.join(", ")));
        }
    }

    if failures.is_empty() {
        let count = recovered.len();
        state.open_bundles = recovered;
        state.recent_events.push(json!({
            "event_type": "open_bundle_recovered",
            "severity": "warning",
            "message": format!("recovered {count} open bundle(s) from live dashboard snapshot"),
            "bundle_count": count,
            "occurred_at": Utc::now(),
        }));
        return Ok(());
    }

    state.open_bundles = recovered;
    state.manual_intervention_required = true;
    state.manual_intervention_reason = Some(format!(
        "live execution halted because dashboard snapshot open bundle recovery did not match exchange positions: {}",
        failures.join("; ")
    ));
    state.unmanaged_external_positions = unmanaged_external_positions(&external_positions, state);
    Ok(())
}

fn read_recoverable_open_bundles(path: &Path) -> Result<Option<Vec<LiveOpenBundle>>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    let snapshot: Value =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    let dashboard = snapshot
        .get("cross_arb_dashboard")
        .or_else(|| snapshot.get("data"))
        .unwrap_or(&snapshot);
    let bundles = dashboard
        .get("position_bundles")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|row| row.get("status").and_then(Value::as_str).unwrap_or("open") == "open")
                .filter_map(recover_open_bundle_from_row)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(Some(bundles))
}

fn recover_open_bundle_from_row(row: &Value) -> Option<LiveOpenBundle> {
    let bundle_id = text_field(row, &["bundle_id"])?.to_string();
    let symbol = text_field(row, &["canonical_symbol", "symbol"])?;
    let (base, quote) = symbol.split_once('/')?;
    let long_exchange =
        StrategyExchangeId::new(gateway_exchange_id(text_field(row, &["long_exchange"])?));
    let short_exchange =
        StrategyExchangeId::new(gateway_exchange_id(text_field(row, &["short_exchange"])?));
    let quantity = value_to_positive_f64(row.get("quantity")?)?;
    let long_entry_price = value_to_positive_f64(row.get("long_entry_price")?)?;
    let short_entry_price = value_to_positive_f64(row.get("short_entry_price")?)?;
    let opened_at = datetime_field(row, "opened_at").unwrap_or_else(Utc::now);
    let open_fee_usdt = f64_field(row, &["open_fee_usdt"]).unwrap_or(0.0).max(0.0);
    let position = OpenArbitragePosition {
        bundle_id: bundle_id.clone(),
        canonical_symbol: StrategyCanonicalSymbol::new(base, quote),
        long_exchange,
        short_exchange,
        quantity,
        long_entry_price,
        short_entry_price,
        opened_at,
    };
    let open_legs = row.get("open_legs").and_then(Value::as_array)?;
    let open_long = open_legs
        .iter()
        .find(|leg| text_field(leg, &["role"]) == Some("open_long"))
        .and_then(reconciled_leg_from_json)
        .or_else(|| recovered_leg_from_bundle_row(row, "open_long", "buy", "long"))?;
    let open_short = open_legs
        .iter()
        .find(|leg| text_field(leg, &["role"]) == Some("open_short"))
        .and_then(reconciled_leg_from_json)
        .or_else(|| recovered_leg_from_bundle_row(row, "open_short", "sell", "short"))?;
    Some(LiveOpenBundle {
        bundle_id,
        position,
        open_long,
        open_short,
        opened_at,
        open_fee_usdt,
    })
}

fn reconciled_leg_from_json(row: &Value) -> Option<ReconciledOrderLeg> {
    Some(ReconciledOrderLeg {
        exchange: gateway_exchange_id(text_field(row, &["exchange"])?),
        symbol: text_field(row, &["symbol", "canonical_symbol"])?.to_string(),
        role: text_field(row, &["role"])?.to_string(),
        side: text_field(row, &["side"]).unwrap_or_default().to_string(),
        position_side: text_field(row, &["position_side"])
            .unwrap_or_default()
            .to_string(),
        client_order_id: optional_text_field(row, &["client_order_id"]),
        exchange_order_id: optional_text_field(row, &["exchange_order_id"]),
        accepted: row.get("accepted").and_then(Value::as_bool).unwrap_or(true),
        status: text_field(row, &["status"]).unwrap_or("filled").to_string(),
        planned_price: f64_field(row, &["planned_execution_price", "planned_price"])
            .or_else(|| f64_field(row, &["actual_fill_price"]))
            .unwrap_or(0.0),
        planned_base_quantity: f64_field(row, &["planned_base_quantity"])
            .or_else(|| f64_field(row, &["actual_base_quantity"]))
            .unwrap_or(0.0),
        planned_order_quantity: f64_field(row, &["planned_order_quantity"])
            .or_else(|| f64_field(row, &["actual_order_quantity"]))
            .unwrap_or(0.0),
        actual_fill_price: f64_field(row, &["actual_fill_price"]),
        actual_base_quantity: f64_field(row, &["actual_base_quantity"]),
        actual_order_quantity: f64_field(row, &["actual_order_quantity"]),
        actual_notional_usdt: f64_field(row, &["actual_notional_usdt"]),
        fee_usdt: f64_field(row, &["fee_usdt"]).unwrap_or(0.0).max(0.0),
        submitted_at: datetime_field(row, "submitted_at"),
        acked_at: datetime_field(row, "acked_at"),
        filled_at: datetime_field(row, "filled_at"),
        error: optional_text_field(row, &["error"]),
    })
}

fn recovered_leg_from_bundle_row(
    row: &Value,
    role: &str,
    side: &str,
    position_side: &str,
) -> Option<ReconciledOrderLeg> {
    let symbol = text_field(row, &["canonical_symbol", "symbol"])?.to_string();
    let quantity = value_to_positive_f64(row.get("quantity")?)?;
    let (exchange_field, price_field) = if position_side == "long" {
        ("long_exchange", "long_entry_price")
    } else {
        ("short_exchange", "short_entry_price")
    };
    let exchange = gateway_exchange_id(text_field(row, &[exchange_field])?);
    let price = value_to_positive_f64(row.get(price_field)?)?;
    let notional = quantity * price;
    Some(ReconciledOrderLeg {
        exchange,
        symbol,
        role: role.to_string(),
        side: side.to_string(),
        position_side: position_side.to_string(),
        client_order_id: None,
        exchange_order_id: None,
        accepted: true,
        status: "filled".to_string(),
        planned_price: price,
        planned_base_quantity: quantity,
        planned_order_quantity: quantity,
        actual_fill_price: Some(price),
        actual_base_quantity: Some(quantity),
        actual_order_quantity: Some(quantity),
        actual_notional_usdt: Some(notional),
        fee_usdt: 0.0,
        submitted_at: None,
        acked_at: None,
        filled_at: datetime_field(row, "opened_at"),
        error: None,
    })
}

fn recovered_bundle_position_mismatch(
    bundle: &LiveOpenBundle,
    external_positions: &[ExternalPositionSnapshot],
) -> Vec<String> {
    let position = &bundle.position;
    let symbol = position.canonical_symbol.as_pair();
    let mut mismatch = Vec::new();
    if !external_positions.iter().any(|external| {
        external.exchange == position.long_exchange.as_str()
            && external.canonical_symbol == symbol
            && external.side == GatewayPositionSide::Long
            && bundle_leg_quantity_matches(bundle, &bundle.open_long, external.quantity)
    }) {
        mismatch.push(format!(
            "missing {} {} long qty={}",
            position.long_exchange, symbol, position.quantity
        ));
    }
    if !external_positions.iter().any(|external| {
        external.exchange == position.short_exchange.as_str()
            && external.canonical_symbol == symbol
            && external.side == GatewayPositionSide::Short
            && bundle_leg_quantity_matches(bundle, &bundle.open_short, external.quantity)
    }) {
        mismatch.push(format!(
            "missing {} {} short qty={}",
            position.short_exchange, symbol, position.quantity
        ));
    }
    mismatch
}

async fn detect_unmanaged_external_positions(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    state: &mut LiveExecutionState,
) -> Result<bool> {
    let external_positions = match fetch_external_position_snapshots(
        gateway,
        args,
        strategy_config,
        target_market_type,
    )
    .await
    {
        Ok(positions) => positions,
        Err(error) => {
            let message = format!(
                    "live execution halted because external position check failed before opening: {error:#}"
                );
            if state.open_bundles.is_empty() {
                state.manual_intervention_required = true;
                state.manual_intervention_reason = Some(message.clone());
                state.recent_events.push(json!({
                    "event_type": "manual_intervention_required",
                    "severity": "critical",
                    "message": message,
                    "occurred_at": Utc::now(),
                }));
            } else {
                state.recent_events.push(json!({
                    "event_type": "external_position_check_failed",
                    "severity": "warning",
                    "message": message,
                    "occurred_at": Utc::now(),
                }));
            }
            return Ok(false);
        }
    };
    let unmanaged = unmanaged_external_positions(&external_positions, state);
    state.unmanaged_external_positions = unmanaged;
    if state.unmanaged_external_positions.is_empty() {
        return Ok(true);
    }

    let summary = state
        .unmanaged_external_positions
        .iter()
        .map(|position| {
            format!(
                "{} {} {:?} qty={}",
                position.exchange, position.canonical_symbol, position.side, position.quantity
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    state.manual_intervention_required = true;
    state.manual_intervention_reason = Some(format!(
        "live execution halted because unmanaged exchange positions were detected: {summary}"
    ));
    state.recent_events.push(json!({
        "event_type": "manual_intervention_required",
        "severity": "critical",
        "message": state.manual_intervention_reason.clone().unwrap_or_else(|| {
            "live execution halted after detecting unmanaged exchange positions".to_string()
        }),
        "unmanaged_positions": state
            .unmanaged_external_positions
            .iter()
            .map(unmanaged_external_position_json)
            .collect::<Vec<_>>(),
        "occurred_at": Utc::now(),
    }));
    Ok(false)
}

async fn fetch_external_position_snapshots(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
) -> Result<Vec<ExternalPositionSnapshot>> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let mut snapshots = Vec::new();

    for exchange in gateway_exchange_ids(strategy_config.active_venues()) {
        let exchange_id = GatewayExchangeId::new(exchange.clone())?;
        let request_id = format!(
            "external-positions-{exchange}-{}",
            Utc::now().timestamp_millis()
        );
        let mut context = RequestContext::new(Utc::now());
        context.tenant_id = Some(tenant_id.clone());
        context.account_id = Some(account_id.clone());
        context.run_id = Some(run_id.clone());
        context.request_id = Some(request_id.clone());

        let response = gateway
            .get_positions(
                request_id,
                tenant_id.clone(),
                Some(account_id.clone()),
                PositionsRequest {
                    schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context,
                    exchange: exchange_id,
                    market_type: Some(target_market_type),
                    symbols: Vec::new(),
                },
            )
            .await
            .with_context(|| format!("fetch external positions for {exchange}"))?;
        snapshots.extend(
            response
                .positions
                .into_iter()
                .filter_map(external_position_snapshot),
        );
    }

    Ok(snapshots)
}

fn external_position_snapshot(position: GatewayPosition) -> Option<ExternalPositionSnapshot> {
    if position.quantity <= 1e-12
        || matches!(
            position.side,
            GatewayPositionSide::None | GatewayPositionSide::Net
        )
    {
        return None;
    }
    Some(ExternalPositionSnapshot {
        exchange: gateway_exchange_id(position.exchange_id.as_str()),
        canonical_symbol: position.canonical_symbol.as_str().to_string(),
        side: position.side,
        quantity: position.quantity,
    })
}

fn unmanaged_external_positions(
    external_positions: &[ExternalPositionSnapshot],
    state: &LiveExecutionState,
) -> Vec<UnmanagedExternalPosition> {
    external_positions
        .iter()
        .filter(|position| !external_position_managed_by_bundle(position, state))
        .map(|position| UnmanagedExternalPosition {
            exchange: position.exchange.clone(),
            canonical_symbol: position.canonical_symbol.clone(),
            side: position.side,
            quantity: position.quantity,
            reason: "position is present on exchange but not tracked by live runner open_bundles"
                .to_string(),
        })
        .collect()
}

fn external_position_managed_by_bundle(
    position: &ExternalPositionSnapshot,
    state: &LiveExecutionState,
) -> bool {
    state.open_bundles.values().any(|bundle| {
        let symbol = bundle.position.canonical_symbol.as_pair();
        symbol == position.canonical_symbol
            && ((bundle.position.long_exchange.as_str() == position.exchange
                && position.side == GatewayPositionSide::Long
                && bundle_leg_quantity_matches(bundle, &bundle.open_long, position.quantity))
                || (bundle.position.short_exchange.as_str() == position.exchange
                    && position.side == GatewayPositionSide::Short
                    && bundle_leg_quantity_matches(bundle, &bundle.open_short, position.quantity)))
    })
}

fn bundle_leg_quantity_matches(
    bundle: &LiveOpenBundle,
    leg: &ReconciledOrderLeg,
    external_quantity: f64,
) -> bool {
    let mut candidates = vec![bundle.position.quantity];
    for quantity in [
        leg.actual_base_quantity,
        leg.actual_order_quantity,
        Some(leg.planned_base_quantity),
        Some(leg.planned_order_quantity),
    ]
    .into_iter()
    .flatten()
    {
        if quantity.is_finite() && quantity > 0.0 {
            candidates.push(quantity);
        }
    }
    candidates
        .into_iter()
        .any(|candidate| quantities_close(candidate, external_quantity))
}

fn quantities_close(expected: f64, actual: f64) -> bool {
    (expected - actual).abs() <= 1e-8_f64.max(expected.abs() * 1e-6)
}

fn unmanaged_external_position_json(position: &UnmanagedExternalPosition) -> Value {
    json!({
        "exchange": position.exchange,
        "canonical_symbol": position.canonical_symbol,
        "symbol": position.canonical_symbol,
        "side": format!("{:?}", position.side).to_ascii_lowercase(),
        "quantity": position.quantity,
        "reason": position.reason,
    })
}

async fn close_ready_bundles(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    quality_controls: LiveExecutionQualityControls,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
    state: &mut LiveExecutionState,
    dashboard: &LiveDashboardData,
) -> Result<()> {
    let now = Utc::now();
    let bundle_ids = state.open_bundles.keys().cloned().collect::<Vec<_>>();
    for bundle_id in bundle_ids {
        let Some(bundle) = state.open_bundles.get(&bundle_id).cloned() else {
            continue;
        };
        let Some(long_book) = find_top(
            &dashboard.tops,
            &bundle.position.long_exchange,
            &bundle.position.canonical_symbol,
        ) else {
            continue;
        };
        let Some(short_book) = find_top(
            &dashboard.tops,
            &bundle.position.short_exchange,
            &bundle.position.canonical_symbol,
        ) else {
            continue;
        };
        let Some(close) = evaluate_dual_taker_close(
            &bundle.position,
            long_book,
            short_book,
            precision_registry,
            fee_model,
            &strategy_config.dual_taker,
            now,
        ) else {
            continue;
        };
        if !close.should_close || close.orders.len() < 2 || !quality_controls.close_allows(&close) {
            continue;
        }

        let execution = execute_taker_pair(
            gateway,
            ctx,
            args,
            target_market_type,
            &bundle_id,
            "close",
            &close.orders[0],
            &close.orders[1],
            confirmation,
            sinks,
        )
        .await?;
        let close_event = close_profit_event(&bundle, &execution, close.gross_pnl_usdt, now);
        append_profit_event(args.profit_history_path.as_ref(), &close_event)?;
        sinks.record_value_event(args, "cross_arb_close", &close_event);
        state.recent_events.push(close_event.clone());
        if execution.both_filled() {
            state.symbol_cooldowns.insert(
                bundle.position.canonical_symbol.as_pair(),
                now + ChronoDuration::seconds(
                    strategy_config.dual_taker.symbol_cooldown_secs.max(0),
                ),
            );
            state.open_bundles.remove(&bundle_id);
        } else if execution.any_filled() {
            record_failed_open_route_cooldown(
                state,
                &bundle.position.canonical_symbol,
                &bundle.position.long_exchange,
                &bundle.position.short_exchange,
                &bundle_id,
                "partial_close_required_emergency_repair",
                now,
            );
            let emergency_events = emergency_close_remaining_after_partial_close(
                gateway,
                ctx,
                args,
                target_market_type,
                strategy_config,
                precision_registry,
                confirmation,
                sinks,
                &bundle,
                &execution,
            )
            .await?;
            let emergency_completed = !emergency_events.is_empty()
                && emergency_events.iter().all(|event| {
                    event
                        .get("both_legs_filled")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                });
            for emergency_event in emergency_events {
                append_profit_event(args.profit_history_path.as_ref(), &emergency_event)?;
                sinks.record_value_event(args, "cross_arb_emergency_close", &emergency_event);
                state.recent_events.push(emergency_event);
            }
            if emergency_completed {
                state.symbol_cooldowns.insert(
                    bundle.position.canonical_symbol.as_pair(),
                    now + ChronoDuration::seconds(
                        strategy_config.dual_taker.symbol_cooldown_secs.max(0),
                    ),
                );
                state.open_bundles.remove(&bundle_id);
            } else {
                state.manual_intervention_required = true;
                state.manual_intervention_reason = Some(format!(
                    "close attempt for bundle {bundle_id} partially filled and the remaining exposure could not be emergency closed"
                ));
                break;
            }
        } else if execution.any_accepted() {
            record_failed_open_route_cooldown(
                state,
                &bundle.position.canonical_symbol,
                &bundle.position.long_exchange,
                &bundle.position.short_exchange,
                &bundle_id,
                "uncertain_close_attempt",
                now,
            );
            state.manual_intervention_required = true;
            state.manual_intervention_reason = Some(format!(
                "close attempt for bundle {bundle_id} ended with uncertain or partial fills; automatic retries are blocked to avoid over-closing"
            ));
            break;
        }
    }
    Ok(())
}

async fn open_best_opportunity(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    _fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    quality_controls: LiveExecutionQualityControls,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
    state: &mut LiveExecutionState,
    dashboard: &LiveDashboardData,
) -> Result<()> {
    if state.manual_intervention_required {
        return Ok(());
    }
    let now = Utc::now();
    let mut skipped_cooldown = None;
    let risk_state = live_risk_state(state);
    let opportunity = dashboard.typed_opportunities.iter().find(|opportunity| {
        if opportunity.expected_net_profit_pct < strategy_config.dual_taker.min_open_net_profit_pct
            || opportunity.orders.len() < 2
            || !quality_controls.open_allows(opportunity)
        {
            return false;
        }
        if route_cooldown_block_reason(
            state,
            &opportunity.long_exchange,
            &opportunity.short_exchange,
            now,
        )
        .is_some()
        {
            return false;
        }
        if let Err(reason) = risk_state.can_open(
            &opportunity.canonical_symbol,
            &opportunity.long_exchange,
            &opportunity.short_exchange,
            &strategy_config.dual_taker,
            now,
        ) {
            if reason == OpenBlockReason::SymbolCoolingDown {
                let symbol = opportunity.canonical_symbol.as_pair();
                if let Some(cooldown_until) = state.symbol_cooldowns.get(&symbol).copied() {
                    skipped_cooldown = Some((symbol, cooldown_until));
                }
            }
            return false;
        }
        true
    });
    let Some(opportunity) = opportunity.cloned() else {
        if let Some((symbol, cooldown_until)) = skipped_cooldown {
            state.recent_events.push(json!({
                "event_type": "symbol_cooldown",
                "severity": "info",
                "symbol": symbol,
                "cooldown_until": cooldown_until,
                "message": "new entry skipped because the symbol is cooling down after a completed close",
                "occurred_at": now,
            }));
        }
        return Ok(());
    };
    append_latency_event(
        args,
        sinks,
        opportunity_latency_span_event(&opportunity, dashboard, now),
    );
    let bundle_id = live_bundle_id(ctx, &opportunity, now);
    let symbol = opportunity.canonical_symbol.as_pair();
    let execution = execute_taker_pair(
        gateway,
        ctx,
        args,
        target_market_type,
        &bundle_id,
        "open",
        &opportunity.orders[0],
        &opportunity.orders[1],
        confirmation,
        sinks,
    )
    .await?;
    let open_event = open_profit_event(&bundle_id, &opportunity, &execution, now);
    append_profit_event(args.profit_history_path.as_ref(), &open_event)?;
    sinks.record_value_event(args, "cross_arb_open", &open_event);
    state.recent_events.push(open_event);

    if execution.both_filled() {
        let Some(position) =
            open_position_from_execution(&bundle_id, &opportunity, &execution, now)
        else {
            return Ok(());
        };
        state.open_bundles.insert(
            bundle_id.clone(),
            LiveOpenBundle {
                bundle_id,
                position,
                open_fee_usdt: execution.total_fee_usdt(),
                open_long: execution.first.clone(),
                open_short: execution.second.clone(),
                opened_at: now,
            },
        );
    } else if execution.any_filled() {
        record_failed_open_route_cooldown(
            state,
            &opportunity.canonical_symbol,
            &opportunity.long_exchange,
            &opportunity.short_exchange,
            &bundle_id,
            "single_leg_open_fill",
            now,
        );
        for emergency_event in emergency_close_unhedged_open_legs(
            gateway,
            ctx,
            args,
            target_market_type,
            strategy_config,
            precision_registry,
            confirmation,
            sinks,
            &bundle_id,
            &execution,
            &opportunity.orders[0],
            &opportunity.orders[1],
        )
        .await?
        {
            append_profit_event(args.profit_history_path.as_ref(), &emergency_event)?;
            sinks.record_value_event(args, "cross_arb_emergency_close", &emergency_event);
            state.recent_events.push(emergency_event);
        }
        let emergency_completed = state
            .recent_events
            .iter()
            .filter(|event| event.get("bundle_id").and_then(Value::as_str) == Some(&bundle_id))
            .filter(|event| {
                event.get("lifecycle").and_then(Value::as_str) == Some("emergency_close")
            })
            .any(|event| {
                event
                    .get("both_legs_filled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            });
        state.recent_events.push(json!({
            "event_type": "single_leg_fill_detected",
            "severity": "critical",
            "bundle_id": bundle_id,
            "message": "one or more live open legs filled without a complete hedge; reduce-only emergency close was attempted",
            "occurred_at": now,
        }));
        if emergency_completed
            && live_account_clear_after_open_attempt(
                gateway,
                args,
                strategy_config,
                target_market_type,
                state,
                &opportunity.orders,
            )
            .await?
        {
            state.symbol_cooldowns.insert(
                symbol.clone(),
                now + ChronoDuration::seconds(
                    strategy_config.dual_taker.symbol_cooldown_secs.max(0),
                ),
            );
            state.recent_events.push(json!({
                "event_type": "single_leg_emergency_close_recovered",
                "severity": "warning",
                "bundle_id": bundle_id,
                "symbol": symbol,
                "message": "single-leg open fill was emergency-closed and REST position reconciliation found no unmanaged exposure; new entries remain enabled",
                "occurred_at": now,
            }));
            return Ok(());
        }
        if !state.manual_intervention_required {
            state.manual_intervention_required = true;
            state.manual_intervention_reason = Some(format!(
                "open attempt for bundle {bundle_id} produced incomplete fills; automatic new entries are blocked"
            ));
        }
    } else if execution.any_accepted() {
        record_failed_open_route_cooldown(
            state,
            &opportunity.canonical_symbol,
            &opportunity.long_exchange,
            &opportunity.short_exchange,
            &bundle_id,
            "uncertain_open_attempt",
            now,
        );
        if live_account_clear_after_open_attempt(
            gateway,
            args,
            strategy_config,
            target_market_type,
            state,
            &opportunity.orders,
        )
        .await?
        {
            state.symbol_cooldowns.insert(
                symbol.clone(),
                now + ChronoDuration::seconds(
                    strategy_config.dual_taker.symbol_cooldown_secs.max(0),
                ),
            );
            state.recent_events.push(json!({
                "event_type": "uncertain_open_attempt_reconciled_empty",
                "severity": "warning",
                "bundle_id": bundle_id,
                "symbol": symbol,
                "message": "accepted open order state stayed uncertain, but REST position and open-order reconciliation found no exposure; new entries remain enabled",
                "occurred_at": now,
            }));
            return Ok(());
        }
        if !state.manual_intervention_required {
            state.manual_intervention_required = true;
            state.manual_intervention_reason = Some(format!(
                "open attempt for bundle {bundle_id} was accepted but fill state stayed uncertain; automatic new entries are blocked"
            ));
        }
    }
    Ok(())
}

async fn live_account_clear_after_open_attempt(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    strategy_config: &CrossExchangeArbitrageConfig,
    target_market_type: GatewayMarketType,
    state: &mut LiveExecutionState,
    orders: &[TakerOrderDraft],
) -> Result<bool> {
    if !detect_unmanaged_external_positions(
        gateway,
        args,
        strategy_config,
        target_market_type,
        state,
    )
    .await?
    {
        return Ok(false);
    }

    let open_orders = match fetch_open_orders_for_attempt(gateway, args, target_market_type, orders)
        .await
    {
        Ok(open_orders) => open_orders,
        Err(error) => {
            let message = format!(
                "live execution halted because open-order reconciliation failed after an uncertain open attempt: {error:#}"
            );
            state.manual_intervention_required = true;
            state.manual_intervention_reason = Some(message.clone());
            state.recent_events.push(json!({
                "event_type": "manual_intervention_required",
                "severity": "critical",
                "message": message,
                "occurred_at": Utc::now(),
            }));
            return Ok(false);
        }
    };
    state.recent_open_orders = open_orders;
    Ok(state.recent_open_orders.is_empty())
}

async fn fetch_open_orders_for_attempt(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    orders: &[TakerOrderDraft],
) -> Result<Vec<Value>> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let mut open_orders = Vec::new();
    let mut seen = BTreeSet::new();
    for order in orders {
        let symbol = draft_symbol_scope(order, target_market_type)?;
        let exchange = symbol.exchange.clone();
        let key = (exchange.to_string(), symbol.exchange_symbol.symbol.clone());
        if !seen.insert(key) {
            continue;
        }
        let request_id = format!(
            "open-orders-after-open-{}-{}",
            gateway_exchange_id(exchange.as_str()),
            Utc::now().timestamp_millis()
        );
        let mut context = RequestContext::new(Utc::now());
        context.tenant_id = Some(tenant_id.clone());
        context.account_id = Some(account_id.clone());
        context.run_id = Some(run_id.clone());
        context.request_id = Some(request_id.clone());
        let response = gateway
            .get_open_orders(
                request_id,
                tenant_id.clone(),
                Some(account_id.clone()),
                OpenOrdersRequest {
                    schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context,
                    exchange,
                    market_type: Some(target_market_type),
                    symbol: Some(symbol),
                    page: None,
                },
            )
            .await
            .context("fetch open orders after uncertain open attempt")?;
        for order in response.orders {
            open_orders.push(serde_json::to_value(order)?);
        }
    }
    Ok(open_orders)
}

#[derive(Debug, Clone)]
struct PairExecution {
    first: ReconciledOrderLeg,
    second: ReconciledOrderLeg,
    requested_at: DateTime<Utc>,
}

impl PairExecution {
    fn both_filled(&self) -> bool {
        self.first.filled() && self.second.filled()
    }

    fn any_filled(&self) -> bool {
        self.first.filled() || self.second.filled()
    }

    fn any_accepted(&self) -> bool {
        self.first.accepted || self.second.accepted
    }

    fn total_fee_usdt(&self) -> f64 {
        self.first.fee_usdt + self.second.fee_usdt
    }
}

async fn execute_taker_pair(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    first: &TakerOrderDraft,
    second: &TakerOrderDraft,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
) -> Result<PairExecution> {
    let requested_at = Utc::now();
    let first_command = execution_order_command(ctx, bundle_id, lifecycle, first, requested_at, 0);
    let second_command =
        execution_order_command(ctx, bundle_id, lifecycle, second, requested_at, 1);
    let first_client = ctx.execution();
    let second_client = ctx.execution();
    let (first_ack, second_ack) = tokio::join!(
        first_client.submit_order(first_command),
        second_client.submit_order(second_command)
    );
    let first = reconcile_order_leg(
        gateway,
        args,
        target_market_type,
        first,
        first_ack,
        requested_at,
        confirmation,
    )
    .await?;
    let second = reconcile_order_leg(
        gateway,
        args,
        target_market_type,
        second,
        second_ack,
        requested_at,
        confirmation,
    )
    .await?;
    append_latency_event(
        args,
        sinks,
        order_latency_span_event(bundle_id, lifecycle, &first, requested_at),
    );
    append_latency_event(
        args,
        sinks,
        order_latency_span_event(bundle_id, lifecycle, &second, requested_at),
    );
    sinks.record_order_leg(
        args,
        target_market_type,
        bundle_id,
        lifecycle,
        &first,
        requested_at,
    );
    sinks.record_order_leg(
        args,
        target_market_type,
        bundle_id,
        lifecycle,
        &second,
        requested_at,
    );
    Ok(PairExecution {
        first,
        second,
        requested_at,
    })
}

async fn execute_single_taker_order(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    order: &TakerOrderDraft,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
) -> Result<ReconciledOrderLeg> {
    let requested_at = Utc::now();
    let command = execution_order_command(ctx, bundle_id, lifecycle, order, requested_at, 0);
    let ack = ctx.execution().submit_order(command).await;
    let leg = reconcile_order_leg(
        gateway,
        args,
        target_market_type,
        order,
        ack,
        requested_at,
        confirmation,
    )
    .await?;
    append_latency_event(
        args,
        sinks,
        order_latency_span_event(bundle_id, lifecycle, &leg, requested_at),
    );
    sinks.record_order_leg(
        args,
        target_market_type,
        bundle_id,
        lifecycle,
        &leg,
        requested_at,
    );
    Ok(leg)
}

async fn execute_emergency_close_with_market_fallback(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    close_draft: &TakerOrderDraft,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
) -> Result<(ReconciledOrderLeg, Option<ReconciledOrderLeg>)> {
    let first_close = execute_single_taker_order(
        gateway,
        ctx,
        args,
        target_market_type,
        bundle_id,
        lifecycle,
        close_draft,
        confirmation,
        sinks,
    )
    .await?;
    if first_close.filled() {
        return Ok((first_close, None));
    }

    let fallback_lifecycle = format!("{lifecycle}_market_fallback");
    let fallback = execute_single_taker_order_with_type(
        gateway,
        ctx,
        args,
        target_market_type,
        bundle_id,
        &fallback_lifecycle,
        close_draft,
        SdkOrderType::Market,
        None,
        confirmation,
        sinks,
    )
    .await?;
    Ok((first_close, Some(fallback)))
}

async fn execute_single_taker_order_with_type(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    order: &TakerOrderDraft,
    order_type: SdkOrderType,
    time_in_force: Option<SdkTimeInForce>,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
) -> Result<ReconciledOrderLeg> {
    let requested_at = Utc::now();
    let command = execution_order_command_with_type(
        ctx,
        bundle_id,
        lifecycle,
        order,
        requested_at,
        0,
        order_type,
        time_in_force,
    );
    let ack = ctx.execution().submit_order(command).await;
    let leg = reconcile_order_leg(
        gateway,
        args,
        target_market_type,
        order,
        ack,
        requested_at,
        confirmation,
    )
    .await?;
    append_latency_event(
        args,
        sinks,
        order_latency_span_event(bundle_id, lifecycle, &leg, requested_at),
    );
    sinks.record_order_leg(
        args,
        target_market_type,
        bundle_id,
        lifecycle,
        &leg,
        requested_at,
    );
    Ok(leg)
}

async fn emergency_close_unhedged_open_legs(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    strategy_config: &CrossExchangeArbitrageConfig,
    precision_registry: &PrecisionRegistry,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
    bundle_id: &str,
    execution: &PairExecution,
    first_draft: &TakerOrderDraft,
    second_draft: &TakerOrderDraft,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    for (leg, original) in [
        (&execution.first, first_draft),
        (&execution.second, second_draft),
    ] {
        if !leg.filled() {
            continue;
        }
        let precision = precision_registry.get(&original.exchange, &original.canonical_symbol);
        let Some(close_draft) = emergency_close_draft(
            leg,
            original,
            strategy_config.dual_taker.taker_slippage_pct,
            precision,
        ) else {
            continue;
        };
        let (close_leg, fallback_close_leg) = execute_emergency_close_with_market_fallback(
            gateway,
            ctx,
            args,
            target_market_type,
            bundle_id,
            "emergency_close",
            &close_draft,
            confirmation,
            sinks,
        )
        .await?;
        let final_close_leg = fallback_close_leg.as_ref().unwrap_or(&close_leg);
        let actual_pnl_usdt = emergency_close_pnl(leg, final_close_leg);
        let actual_base_quantity = leg
            .actual_base_quantity
            .zip(final_close_leg.actual_base_quantity)
            .map(|(open, close)| open.min(close).max(0.0));
        let open_price = leg.actual_fill_price;
        let close_price = final_close_leg.actual_fill_price;
        let close_net_profit_pct =
            actual_pnl_usdt
                .zip(actual_base_quantity)
                .and_then(|(pnl, quantity)| {
                    let notional = quantity * open_price?;
                    Some(pnl / notional.max(1e-12))
                });
        events.push(json!({
            "event_kind": "cross_arb_price_audit",
            "bundle_id": bundle_id,
            "lifecycle": "emergency_close",
            "canonical_symbol": original.canonical_symbol.as_pair(),
            "exchange": original.exchange.to_string(),
            "quantity": actual_base_quantity,
            "open_price": open_price,
            "close_price": close_price,
            "actual_pnl_usdt": actual_pnl_usdt,
            "realized_profit_usdt": actual_pnl_usdt,
            "close_net_profit_pct": close_net_profit_pct,
            "filled_open_leg": leg_json(leg),
            "emergency_close_leg": leg_json(&close_leg),
            "emergency_close_fallback_leg": fallback_close_leg.as_ref().map(leg_json),
            "both_legs_filled": final_close_leg.filled(),
            "failure_reason": if final_close_leg.filled() {
                Value::Null
            } else {
                json!(final_close_leg.error.clone().unwrap_or_else(|| format!(
                    "emergency close not filled; status={}",
                    final_close_leg.status
                )))
            },
            "planned_at": execution.requested_at,
            "recorded_at": Utc::now(),
        }));
    }
    Ok(events)
}

async fn emergency_close_remaining_after_partial_close(
    gateway: &impl GatewayClient,
    ctx: &StrategyContext,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    strategy_config: &CrossExchangeArbitrageConfig,
    precision_registry: &PrecisionRegistry,
    confirmation: &LiveConfirmationPolicy,
    sinks: &LiveRuntimeSinks,
    bundle: &LiveOpenBundle,
    close_execution: &PairExecution,
) -> Result<Vec<Value>> {
    let close_long_filled = leg_for_role(close_execution, TakerOrderRole::CloseLong)
        .is_some_and(ReconciledOrderLeg::filled);
    let close_short_filled = leg_for_role(close_execution, TakerOrderRole::CloseShort)
        .is_some_and(ReconciledOrderLeg::filled);

    let remaining_open_leg = match (close_long_filled, close_short_filled) {
        (true, false) => Some(&bundle.open_short),
        (false, true) => Some(&bundle.open_long),
        _ => None,
    };
    let Some(open_leg) = remaining_open_leg else {
        return Ok(Vec::new());
    };

    let exchange = StrategyExchangeId::new(&open_leg.exchange);
    let precision = precision_registry.get(&exchange, &bundle.position.canonical_symbol);
    let Some(close_draft) = emergency_close_draft_from_open_leg(
        open_leg,
        &bundle.position.canonical_symbol,
        strategy_config.dual_taker.taker_slippage_pct,
        precision,
    ) else {
        return Ok(vec![emergency_close_after_partial_close_event(
            bundle,
            close_execution,
            open_leg,
            None,
            None,
            Some("failed to build emergency close draft for remaining open leg".to_string()),
        )]);
    };

    let (close_leg, fallback_close_leg) = execute_emergency_close_with_market_fallback(
        gateway,
        ctx,
        args,
        target_market_type,
        &bundle.bundle_id,
        "emergency_close_after_partial_close",
        &close_draft,
        confirmation,
        sinks,
    )
    .await?;

    Ok(vec![emergency_close_after_partial_close_event(
        bundle,
        close_execution,
        open_leg,
        Some(&close_leg),
        fallback_close_leg.as_ref(),
        None,
    )])
}

fn emergency_close_draft(
    filled_leg: &ReconciledOrderLeg,
    original: &TakerOrderDraft,
    slippage_pct: f64,
    precision: SymbolPrecision,
) -> Option<TakerOrderDraft> {
    let actual_order_quantity = filled_leg.actual_order_quantity?;
    let actual_base_quantity = filled_leg.actual_base_quantity?;
    let reference_price = filled_leg.actual_fill_price?;
    let (side, role) = match original.role {
        TakerOrderRole::OpenLong => (
            rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell,
            TakerOrderRole::EmergencyCloseLong,
        ),
        TakerOrderRole::OpenShort => (
            rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy,
            TakerOrderRole::EmergencyCloseShort,
        ),
        _ => return None,
    };
    let worst_acceptable_price = match side {
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy => ceil_to_step(
            reference_price * (1.0 + slippage_pct.max(0.0)),
            precision.price_tick,
        ),
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell => floor_to_step(
            reference_price * (1.0 - slippage_pct.max(0.0)),
            precision.price_tick,
        ),
    };
    Some(TakerOrderDraft {
        exchange: original.exchange.clone(),
        canonical_symbol: original.canonical_symbol.clone(),
        side,
        base_quantity: actual_base_quantity,
        quantity: actual_order_quantity,
        quantity_unit: original.quantity_unit,
        contract_size: original.contract_size,
        reference_price,
        worst_acceptable_price,
        reduce_only: true,
        role,
    })
}

fn emergency_close_draft_from_open_leg(
    open_leg: &ReconciledOrderLeg,
    canonical_symbol: &StrategyCanonicalSymbol,
    slippage_pct: f64,
    precision: SymbolPrecision,
) -> Option<TakerOrderDraft> {
    let actual_base_quantity = open_leg.actual_base_quantity?;
    let actual_order_quantity = open_leg
        .actual_order_quantity
        .unwrap_or_else(|| precision.normalized_order_quantity_from_base(actual_base_quantity));
    let reference_price = open_leg.actual_fill_price?;
    if actual_base_quantity <= 0.0 || actual_order_quantity <= 0.0 || reference_price <= 0.0 {
        return None;
    }
    let (side, role) = match open_leg.role.as_str() {
        "open_long" => (
            rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell,
            TakerOrderRole::EmergencyCloseLong,
        ),
        "open_short" => (
            rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy,
            TakerOrderRole::EmergencyCloseShort,
        ),
        _ => match open_leg.position_side.as_str() {
            "long" => (
                rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell,
                TakerOrderRole::EmergencyCloseLong,
            ),
            "short" => (
                rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy,
                TakerOrderRole::EmergencyCloseShort,
            ),
            _ => return None,
        },
    };
    let worst_acceptable_price = match side {
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy => ceil_to_step(
            reference_price * (1.0 + slippage_pct.max(0.0)),
            precision.price_tick,
        ),
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell => floor_to_step(
            reference_price * (1.0 - slippage_pct.max(0.0)),
            precision.price_tick,
        ),
    };
    Some(TakerOrderDraft {
        exchange: StrategyExchangeId::new(&open_leg.exchange),
        canonical_symbol: canonical_symbol.clone(),
        side,
        base_quantity: precision.base_quantity_from_order_quantity(actual_order_quantity),
        quantity: actual_order_quantity,
        quantity_unit: precision.quantity_unit,
        contract_size: precision.effective_contract_size(),
        reference_price,
        worst_acceptable_price,
        reduce_only: true,
        role,
    })
}

fn emergency_close_pnl(
    open_leg: &ReconciledOrderLeg,
    close_leg: &ReconciledOrderLeg,
) -> Option<f64> {
    let quantity = open_leg
        .actual_base_quantity?
        .min(close_leg.actual_base_quantity?)
        .max(0.0);
    let open_price = open_leg.actual_fill_price?;
    let close_price = close_leg.actual_fill_price?;
    let gross = match open_leg.role.as_str() {
        "open_long" => (close_price - open_price) * quantity,
        "open_short" => (open_price - close_price) * quantity,
        _ => return None,
    };
    Some(gross - open_leg.fee_usdt - close_leg.fee_usdt)
}

fn emergency_close_after_partial_close_event(
    bundle: &LiveOpenBundle,
    close_execution: &PairExecution,
    open_leg: &ReconciledOrderLeg,
    emergency_close_leg: Option<&ReconciledOrderLeg>,
    emergency_close_fallback_leg: Option<&ReconciledOrderLeg>,
    failure_reason: Option<String>,
) -> Value {
    let final_close_leg = emergency_close_fallback_leg.or(emergency_close_leg);
    let actual_pnl_usdt =
        final_close_leg.and_then(|close_leg| emergency_close_pnl(open_leg, close_leg));
    let actual_base_quantity = final_close_leg.and_then(|close_leg| {
        open_leg
            .actual_base_quantity
            .zip(close_leg.actual_base_quantity)
            .map(|(open, close)| open.min(close).max(0.0))
    });
    let open_price = open_leg.actual_fill_price;
    let close_price = final_close_leg.and_then(|close_leg| close_leg.actual_fill_price);
    let close_net_profit_pct =
        actual_pnl_usdt
            .zip(actual_base_quantity)
            .and_then(|(pnl, quantity)| {
                let notional = quantity * open_price?;
                Some(pnl / notional.max(1e-12))
            });
    let close_leg_filled = final_close_leg.is_some_and(ReconciledOrderLeg::filled);
    json!({
        "event_kind": "cross_arb_price_audit",
        "bundle_id": bundle.bundle_id,
        "lifecycle": "emergency_close_after_partial_close",
        "canonical_symbol": bundle.position.canonical_symbol.as_pair(),
        "exchange": open_leg.exchange,
        "quantity": actual_base_quantity,
        "open_price": open_price,
        "close_price": close_price,
        "actual_pnl_usdt": actual_pnl_usdt,
        "realized_profit_usdt": actual_pnl_usdt,
        "close_net_profit_pct": close_net_profit_pct,
        "filled_open_leg": leg_json(open_leg),
        "normal_close_legs": [leg_json(&close_execution.first), leg_json(&close_execution.second)],
        "emergency_close_leg": emergency_close_leg.map(leg_json),
        "emergency_close_fallback_leg": emergency_close_fallback_leg.map(leg_json),
        "both_legs_filled": close_leg_filled,
        "failure_reason": if close_leg_filled {
            Value::Null
        } else {
            json!(failure_reason.unwrap_or_else(|| {
                final_close_leg
                    .and_then(|leg| leg.error.clone())
                    .unwrap_or_else(|| {
                        final_close_leg.map_or_else(
                            || "emergency close was not submitted".to_string(),
                            |leg| format!("emergency close not filled; status={}", leg.status),
                        )
                    })
            }))
        },
        "planned_at": close_execution.requested_at,
        "recorded_at": Utc::now(),
    })
}

fn execution_order_command(
    ctx: &StrategyContext,
    bundle_id: &str,
    lifecycle: &str,
    order: &TakerOrderDraft,
    requested_at: DateTime<Utc>,
    leg_index: usize,
) -> ExecutionOrderCommand {
    execution_order_command_with_type(
        ctx,
        bundle_id,
        lifecycle,
        order,
        requested_at,
        leg_index,
        SdkOrderType::ImmediateOrCancel,
        Some(SdkTimeInForce::ImmediateOrCancel),
    )
}

fn execution_order_command_with_type(
    ctx: &StrategyContext,
    bundle_id: &str,
    lifecycle: &str,
    order: &TakerOrderDraft,
    requested_at: DateTime<Utc>,
    leg_index: usize,
    order_type: SdkOrderType,
    time_in_force: Option<SdkTimeInForce>,
) -> ExecutionOrderCommand {
    let role = role_name(order.role);
    let client_order_id = safe_client_order_id(
        ctx.strategy_id(),
        ctx.run_id(),
        bundle_id,
        lifecycle,
        role,
        leg_index,
        requested_at,
    );
    let idempotency_key = safe_id_part(&format!(
        "{}-{}-{}-{}-{}-{}-{}",
        ctx.run_id(),
        bundle_id,
        lifecycle,
        order.exchange,
        role,
        leg_index,
        requested_at.timestamp_millis(),
    ));
    let is_market_order = matches!(order_type, SdkOrderType::Market);
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id,
        idempotency_key,
        risk_profile_id: "cross-arb-live-dual-taker".to_string(),
        requested_at,
        exchange_id: order.exchange.to_string(),
        symbol: order.canonical_symbol.as_pair(),
        side: sdk_order_side(order.side),
        order_type,
        quantity: format_float(order.quantity),
        price: if is_market_order {
            None
        } else {
            Some(format_float(order.worst_acceptable_price))
        },
        time_in_force,
        reduce_only: order.reduce_only,
        metadata: BTreeMap::from([
            ("bundle_id".to_string(), json!(bundle_id)),
            ("role".to_string(), json!(role)),
            ("lifecycle".to_string(), json!(lifecycle)),
            (
                "position_side".to_string(),
                json!(position_side_name(order.role)),
            ),
            (
                "planned_execution_price".to_string(),
                json!(format_float(order.reference_price)),
            ),
            (
                "worst_acceptable_price".to_string(),
                json!(format_float(order.worst_acceptable_price)),
            ),
            (
                "planned_base_quantity".to_string(),
                json!(format_float(order.base_quantity)),
            ),
            (
                "exchange_order_quantity".to_string(),
                json!(format_float(order.quantity)),
            ),
        ]),
    }
}

async fn reconcile_order_leg(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    draft: &TakerOrderDraft,
    ack: SdkResult<ExecutionOrderAck>,
    requested_at: DateTime<Utc>,
    confirmation: &LiveConfirmationPolicy,
) -> Result<ReconciledOrderLeg> {
    let base_leg = || ReconciledOrderLeg {
        exchange: draft.exchange.to_string(),
        symbol: draft.canonical_symbol.as_pair(),
        role: role_name(draft.role).to_string(),
        side: strategy_side_name(draft.side).to_string(),
        position_side: position_side_name(draft.role).to_string(),
        client_order_id: None,
        exchange_order_id: None,
        accepted: false,
        status: "submit_failed".to_string(),
        planned_price: draft.reference_price,
        planned_base_quantity: draft.base_quantity,
        planned_order_quantity: draft.quantity,
        actual_fill_price: None,
        actual_base_quantity: None,
        actual_order_quantity: None,
        actual_notional_usdt: None,
        fee_usdt: 0.0,
        submitted_at: Some(requested_at),
        acked_at: None,
        filled_at: None,
        error: None,
    };
    let ack = match ack {
        Ok(ack) => ack,
        Err(error) => {
            let mut leg = base_leg();
            leg.error = Some(error.to_string());
            return Ok(leg);
        }
    };
    let mut leg = base_leg();
    leg.client_order_id = Some(ack.client_order_id.clone());
    leg.exchange_order_id = ack.execution_order_id.clone();
    leg.accepted = ack.accepted;
    leg.acked_at = Some(ack.received_at);
    leg.status = if ack.accepted {
        "accepted".to_string()
    } else {
        "rejected".to_string()
    };
    leg.error = ack.reason.clone().filter(|_| !ack.accepted);
    if !ack.accepted {
        return Ok(leg);
    }

    if let Some(private_event) = confirmation
        .private_ws
        .wait_for_order(
            draft.exchange.as_str(),
            &ack.client_order_id,
            requested_at,
            confirmation.private_ws_timeout_ms,
        )
        .await
    {
        apply_private_ws_event_to_leg(&mut leg, draft, &private_event);
        if leg.filled() {
            return Ok(leg);
        }
    }

    if confirmation.require_private_ws && !confirmation.allow_rest_readback {
        leg.status = "private_ws_confirmation_timeout".to_string();
        leg.error = Some(
            "private websocket did not confirm fill before timeout; REST readback is disabled for the trading hot path"
                .to_string(),
        );
        return Ok(leg);
    }

    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let symbol = draft_symbol_scope(draft, target_market_type)?;
    let exchange_id = symbol.exchange.clone();
    let query_request_id = format!("query-{}", ack.client_order_id);
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.run_id = Some(RunId::new(args.run_id.clone())?);
    context.request_id = Some(query_request_id.clone());
    let query = gateway
        .query_order(
            query_request_id,
            tenant_id.clone(),
            Some(account_id.clone()),
            QueryOrderRequest {
                schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context,
                symbol: symbol.clone(),
                client_order_id: Some(ack.client_order_id.clone()),
                exchange_order_id: ack.execution_order_id.clone(),
            },
        )
        .await;

    let order_state = match query {
        Ok(response) => response.order,
        Err(error) => {
            leg.error = Some(format!("query_order failed: {error}"));
            None
        }
    };

    if let Some(order) = order_state.as_ref() {
        leg.status = format!("{:?}", order.status).to_ascii_lowercase();
        leg.exchange_order_id = order.exchange_order_id.clone().or(leg.exchange_order_id);
        leg.actual_order_quantity = parse_positive_optional(&order.filled_quantity);
        leg.actual_fill_price = order
            .average_fill_price
            .as_deref()
            .and_then(parse_positive_optional);
        leg.filled_at = Some(order.updated_at);
    }

    let fills = recent_fills_for_order(
        gateway,
        args,
        target_market_type,
        exchange_id.clone(),
        symbol.clone(),
        &ack.client_order_id,
        ack.execution_order_id.as_deref(),
        requested_at,
    )
    .await
    .unwrap_or_default();
    apply_fills_to_leg(&mut leg, draft, &fills);
    if !leg.filled() {
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let fills = recent_fills_for_order(
                gateway,
                args,
                target_market_type,
                exchange_id.clone(),
                symbol.clone(),
                &ack.client_order_id,
                ack.execution_order_id.as_deref(),
                requested_at,
            )
            .await
            .unwrap_or_default();
            apply_fills_to_leg(&mut leg, draft, &fills);
            if leg.filled() {
                break;
            }
        }
    }

    if let (Some(quantity), Some(price)) = (leg.actual_order_quantity, leg.actual_fill_price) {
        let base_quantity = base_quantity_from_order_quantity(draft, quantity);
        leg.actual_base_quantity = Some(base_quantity);
        leg.actual_notional_usdt = Some(base_quantity * price);
    }
    Ok(leg)
}

async fn recent_fills_for_order(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    exchange: GatewayExchangeId,
    symbol: SymbolScope,
    client_order_id: &str,
    exchange_order_id: Option<&str>,
    requested_at: DateTime<Utc>,
) -> Result<Vec<Fill>> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let request_id = format!("fills-{client_order_id}");
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.run_id = Some(RunId::new(args.run_id.clone())?);
    context.request_id = Some(request_id.clone());
    let response = gateway
        .get_recent_fills(
            request_id,
            tenant_id,
            Some(account_id),
            RecentFillsRequest {
                schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context,
                exchange,
                market_type: Some(target_market_type),
                symbol: Some(symbol),
                client_order_id: Some(client_order_id.to_string()),
                exchange_order_id: exchange_order_id.map(ToString::to_string),
                from_trade_id: None,
                start_time: Some(requested_at - ChronoDuration::seconds(10)),
                end_time: Some(Utc::now() + ChronoDuration::seconds(10)),
                limit: Some(50),
                page: None,
            },
        )
        .await?;
    Ok(response.fills)
}

fn apply_fills_to_leg(leg: &mut ReconciledOrderLeg, draft: &TakerOrderDraft, fills: &[Fill]) {
    if fills.is_empty() {
        return;
    }
    let mut quantity = 0.0;
    let mut notional = 0.0;
    let mut fee_usdt = 0.0;
    let mut filled_at = leg.filled_at;
    for fill in fills {
        quantity += fill.quantity;
        notional += fill.price * fill.quantity;
        fee_usdt += fill.fee_amount.unwrap_or_default();
        filled_at = Some(filled_at.map_or(fill.filled_at, |current| current.max(fill.filled_at)));
    }
    if quantity > 0.0 {
        leg.actual_order_quantity.get_or_insert(quantity);
        leg.actual_fill_price.get_or_insert(notional / quantity);
        leg.actual_base_quantity
            .get_or_insert_with(|| base_quantity_from_order_quantity(draft, quantity));
        leg.actual_notional_usdt.get_or_insert_with(|| {
            leg.actual_base_quantity.unwrap_or_default() * (notional / quantity)
        });
    }
    leg.fee_usdt = fee_usdt;
    leg.filled_at = filled_at;
}

fn open_position_from_execution(
    bundle_id: &str,
    opportunity: &DualTakerOpenOpportunity,
    execution: &PairExecution,
    opened_at: DateTime<Utc>,
) -> Option<OpenArbitragePosition> {
    let long = leg_for_role(execution, TakerOrderRole::OpenLong)?;
    let short = leg_for_role(execution, TakerOrderRole::OpenShort)?;
    Some(OpenArbitragePosition {
        bundle_id: bundle_id.to_string(),
        canonical_symbol: opportunity.canonical_symbol.clone(),
        long_exchange: opportunity.long_exchange.clone(),
        short_exchange: opportunity.short_exchange.clone(),
        quantity: long
            .actual_base_quantity?
            .min(short.actual_base_quantity?)
            .max(0.0),
        long_entry_price: long.actual_fill_price?,
        short_entry_price: short.actual_fill_price?,
        opened_at,
    })
}

fn open_profit_event(
    bundle_id: &str,
    opportunity: &DualTakerOpenOpportunity,
    execution: &PairExecution,
    recorded_at: DateTime<Utc>,
) -> Value {
    json!({
        "event_kind": "cross_arb_price_audit",
        "bundle_id": bundle_id,
        "lifecycle": "open",
        "canonical_symbol": opportunity.canonical_symbol.as_pair(),
        "long_exchange": opportunity.long_exchange.to_string(),
        "short_exchange": opportunity.short_exchange.to_string(),
        "planned_spread_pct": opportunity.spread_pct,
        "expected_net_pnl_usdt": opportunity.expected_net_pnl_usdt,
        "actual_pnl_usdt": null,
        "both_legs_filled": execution.both_filled(),
        "legs": [leg_json(&execution.first), leg_json(&execution.second)],
        "failure_reason": pair_failure_reason(execution),
        "planned_at": execution.requested_at,
        "recorded_at": recorded_at,
    })
}

fn close_profit_event(
    bundle: &LiveOpenBundle,
    execution: &PairExecution,
    expected_gross_pnl_usdt: f64,
    recorded_at: DateTime<Utc>,
) -> Value {
    let close_long = leg_for_role(execution, TakerOrderRole::CloseLong);
    let close_short = leg_for_role(execution, TakerOrderRole::CloseShort);
    let close_quantity = close_long.zip(close_short).and_then(|(long, short)| {
        let quantity = bundle
            .position
            .quantity
            .min(long.actual_base_quantity?)
            .min(short.actual_base_quantity?);
        Some(quantity.max(0.0))
    });
    let actual_pnl_usdt = close_long.zip(close_short).and_then(|(long, short)| {
        let quantity = close_quantity?;
        let gross = (long.actual_fill_price? - bundle.position.long_entry_price) * quantity
            + (bundle.position.short_entry_price - short.actual_fill_price?) * quantity;
        Some(gross - bundle.open_fee_usdt - execution.total_fee_usdt())
    });
    let base_notional_usdt = close_quantity
        .map(|quantity| {
            (quantity * bundle.position.long_entry_price)
                .max(quantity * bundle.position.short_entry_price)
        })
        .unwrap_or_else(|| bundle.position.base_notional_usdt());
    let close_net_profit_pct = actual_pnl_usdt.map(|pnl| pnl / base_notional_usdt.max(1e-12));
    json!({
        "event_kind": "cross_arb_price_audit",
        "bundle_id": bundle.bundle_id,
        "lifecycle": "close",
        "canonical_symbol": bundle.position.canonical_symbol.as_pair(),
        "long_exchange": bundle.position.long_exchange.to_string(),
        "short_exchange": bundle.position.short_exchange.to_string(),
        "quantity": close_quantity,
        "long_entry_price": bundle.position.long_entry_price,
        "short_entry_price": bundle.position.short_entry_price,
        "long_close_price": close_long.and_then(|leg| leg.actual_fill_price),
        "short_close_price": close_short.and_then(|leg| leg.actual_fill_price),
        "expected_gross_pnl_usdt": expected_gross_pnl_usdt,
        "actual_pnl_usdt": actual_pnl_usdt,
        "realized_profit_usdt": actual_pnl_usdt,
        "close_net_profit_pct": close_net_profit_pct,
        "close_notional_usdt": base_notional_usdt,
        "open_fee_usdt": bundle.open_fee_usdt,
        "close_fee_usdt": execution.total_fee_usdt(),
        "both_legs_filled": execution.both_filled(),
        "legs": [leg_json(&execution.first), leg_json(&execution.second)],
        "failure_reason": pair_failure_reason(execution),
        "planned_at": execution.requested_at,
        "recorded_at": recorded_at,
    })
}

fn leg_json(leg: &ReconciledOrderLeg) -> Value {
    json!({
        "exchange": leg.exchange,
        "symbol": leg.symbol,
        "role": leg.role,
        "side": leg.side,
        "position_side": leg.position_side,
        "client_order_id": leg.client_order_id,
        "exchange_order_id": leg.exchange_order_id,
        "accepted": leg.accepted,
        "status": leg.status,
        "planned_execution_price": format_float(leg.planned_price),
        "actual_fill_price": leg.actual_fill_price.map(format_float),
        "planned_base_quantity": format_float(leg.planned_base_quantity),
        "actual_base_quantity": leg.actual_base_quantity.map(format_float),
        "planned_order_quantity": format_float(leg.planned_order_quantity),
        "actual_order_quantity": leg.actual_order_quantity.map(format_float),
        "actual_notional_usdt": leg.actual_notional_usdt.map(format_float),
        "fee_usdt": format_float(leg.fee_usdt),
        "submitted_at": leg.submitted_at,
        "acked_at": leg.acked_at,
        "filled_at": leg.filled_at,
        "error": leg.error,
    })
}

fn pair_failure_reason(execution: &PairExecution) -> Option<String> {
    if execution.both_filled() {
        return None;
    }
    let reasons = [&execution.first, &execution.second]
        .into_iter()
        .filter(|leg| !leg.filled())
        .map(|leg| {
            leg.error.clone().unwrap_or_else(|| {
                format!(
                    "{} {} not filled; status={}",
                    leg.exchange, leg.role, leg.status
                )
            })
        })
        .collect::<Vec<_>>();
    Some(reasons.join("; "))
}

fn hydrate_dashboard_execution_state(
    state: &LiveExecutionState,
    strategy_config: &CrossExchangeArbitrageConfig,
    fee_model: &FeeModel,
    precision_registry: &PrecisionRegistry,
    quality_controls: LiveExecutionQualityControls,
    dashboard: &mut LiveDashboardData,
) {
    let now = Utc::now();
    let risk_state = live_risk_state(state);
    let control_block_reason = dashboard.controls.new_entries_block_reason(true);
    let runtime_block_reason = dashboard.runtime_new_entries_block_reason.clone();
    let market_data_row_source = dashboard.market_data_row_source;
    for row in &mut dashboard.opportunities {
        row["source"] = json!(market_data_row_source);
        let expected_net_profit_pct = f64_field(row, &["expected_net_profit_pct"]).unwrap_or(0.0);
        let raw_open_spread_pct = f64_field(
            row,
            &["raw_open_spread_pct", "raw_spread_pct", "spread_pct"],
        )
        .unwrap_or(0.0);
        let has_executable_orders = row
            .get("orders")
            .and_then(Value::as_array)
            .is_some_and(|orders| orders.len() >= 2);
        let mut market_reject_reasons = market_reject_reasons_from_row(row);
        if !has_executable_orders {
            push_reject_reason(
                &mut market_reject_reasons,
                "display-only row has no executable order drafts".to_string(),
            );
        }
        if raw_open_spread_pct < strategy_config.dual_taker.min_open_spread_pct {
            push_reject_reason(
                &mut market_reject_reasons,
                format!(
                    "raw spread {} is below min open raw spread {}",
                    format_float(raw_open_spread_pct),
                    format_float(strategy_config.dual_taker.min_open_spread_pct)
                ),
            );
        }
        if raw_open_spread_pct > strategy_config.dual_taker.max_open_spread_pct {
            push_reject_reason(
                &mut market_reject_reasons,
                format!(
                    "raw spread {} is above max open raw spread {}",
                    format_float(raw_open_spread_pct),
                    format_float(strategy_config.dual_taker.max_open_spread_pct)
                ),
            );
        }
        if expected_net_profit_pct < strategy_config.dual_taker.min_open_net_profit_pct {
            push_reject_reason(
                &mut market_reject_reasons,
                format!(
                    "expected net edge {} is below min open net edge {}",
                    format_float(expected_net_profit_pct),
                    format_float(strategy_config.dual_taker.min_open_net_profit_pct)
                ),
            );
        }
        for reason in quality_controls.row_reject_reasons(row) {
            push_reject_reason(&mut market_reject_reasons, reason);
        }
        let market_can_open = market_reject_reasons.is_empty();
        let mut execution_reject_reasons = market_reject_reasons.clone();
        if let Some(reason) = control_block_reason {
            push_reject_reason(&mut execution_reject_reasons, reason.to_string());
        }
        if let Some(reason) = runtime_block_reason.as_ref() {
            push_reject_reason(&mut execution_reject_reasons, reason.clone());
        }
        if let Some(reason) =
            row_entry_block_reason(row, state, &risk_state, &strategy_config.dual_taker, now)
        {
            push_reject_reason(&mut execution_reject_reasons, reason);
        }
        row["market_can_open"] = json!(market_can_open);
        row["market_reject_reasons"] = reasons_value(&market_reject_reasons);
        row["can_open"] = json!(execution_reject_reasons.is_empty());
        row["execution_reject_reasons"] = reasons_value(&execution_reject_reasons);
        row["reject_reasons"] = reasons_value(&execution_reject_reasons);
    }
    let mut private_events = dashboard.private_events.clone();
    for event in &state.recent_events {
        private_events.push(event.clone());
    }
    let keep = 500usize;
    if private_events.len() > keep {
        private_events.drain(0..private_events.len() - keep);
    }
    dashboard.private_events = private_events;
    if state.manual_intervention_required && !state.unmanaged_external_positions.is_empty() {
        dashboard.private_events.push(json!({
            "event_type": "manual_intervention_required",
            "severity": "critical",
            "message": state.manual_intervention_reason.clone().unwrap_or_else(|| {
                "live execution halted after detecting unmanaged exchange positions".to_string()
            }),
            "unmanaged_positions": state
                .unmanaged_external_positions
                .iter()
                .map(unmanaged_external_position_json)
                .collect::<Vec<_>>(),
            "occurred_at": now,
        }));
    }
    dashboard.open_orders = state.recent_open_orders.clone();
    dashboard.position_bundles = state
        .open_bundles
        .values()
        .map(|bundle| {
            let close = find_top(
                &dashboard.tops,
                &bundle.position.long_exchange,
                &bundle.position.canonical_symbol,
            )
            .zip(find_top(
                &dashboard.tops,
                &bundle.position.short_exchange,
                &bundle.position.canonical_symbol,
            ))
            .and_then(|(long_book, short_book)| {
                evaluate_dual_taker_close(
                    &bundle.position,
                    long_book,
                    short_book,
                    precision_registry,
                    fee_model,
                    &strategy_config.dual_taker,
                    now,
                )
            });
            open_bundle_row(
                bundle,
                close.as_ref(),
                strategy_config,
                quality_controls,
                now,
            )
        })
        .collect::<Vec<_>>();
}

fn reject_reasons_from_row(row: &Value) -> Vec<String> {
    text_field(row, &["reject_reasons", "reject_reason"])
        .into_iter()
        .flat_map(|text| text.split(';'))
        .map(str::trim)
        .filter(|reason| {
            !reason.is_empty() && *reason != "-" && !reason.starts_with("not evaluated")
        })
        .map(ToString::to_string)
        .fold(Vec::new(), |mut reasons, reason| {
            push_reject_reason(&mut reasons, reason);
            reasons
        })
}

fn market_reject_reasons_from_row(row: &Value) -> Vec<String> {
    if row.get("market_reject_reasons").is_some() {
        return text_field(row, &["market_reject_reasons"])
            .into_iter()
            .flat_map(|text| text.split(';'))
            .map(str::trim)
            .filter(|reason| !reason.is_empty() && *reason != "-")
            .map(ToString::to_string)
            .fold(Vec::new(), |mut reasons, reason| {
                push_reject_reason(&mut reasons, reason);
                reasons
            });
    }
    reject_reasons_from_row(row)
}

fn reasons_value(reasons: &[String]) -> Value {
    if reasons.is_empty() {
        Value::String("-".to_string())
    } else {
        Value::String(reasons.join("; "))
    }
}

fn push_reject_reason(reasons: &mut Vec<String>, reason: String) {
    let reason = reason.trim();
    if reason.is_empty() || reason == "-" {
        return;
    }
    if !reasons.iter().any(|existing| existing == reason) {
        reasons.push(reason.to_string());
    }
}

fn row_entry_block_reason(
    row: &Value,
    state: &LiveExecutionState,
    risk_state: &rustcta_strategy_cross_exchange_arbitrage::ArbitrageRiskState,
    config: &rustcta_strategy_cross_exchange_arbitrage::DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Option<String> {
    if state.manual_intervention_required {
        return Some(
            state
                .manual_intervention_reason
                .clone()
                .unwrap_or_else(|| "manual intervention required".to_string()),
        );
    }
    let symbol = row_symbol(row)?;
    let long_exchange = row_exchange(row, &["long_exchange"])?;
    let short_exchange = row_exchange(row, &["short_exchange"])?;
    risk_state
        .can_open(&symbol, &long_exchange, &short_exchange, config, now)
        .err()
        .map(open_block_reason_text)
        .or_else(|| route_cooldown_block_reason(state, &long_exchange, &short_exchange, now))
}

fn live_risk_state(
    state: &LiveExecutionState,
) -> rustcta_strategy_cross_exchange_arbitrage::ArbitrageRiskState {
    let mut risk_state = rustcta_strategy_cross_exchange_arbitrage::ArbitrageRiskState::default();
    risk_state.open_positions = state
        .open_bundles
        .values()
        .map(|bundle| (bundle.bundle_id.clone(), bundle.position.clone()))
        .collect();
    risk_state.symbol_cooldowns = state.symbol_cooldowns.clone();
    risk_state.strategy_halted = state.manual_intervention_required;
    risk_state
}

fn row_symbol(row: &Value) -> Option<StrategyCanonicalSymbol> {
    let symbol = text_field(row, &["canonical_symbol", "symbol"])?;
    let (base, quote) = symbol.split_once('/')?;
    Some(StrategyCanonicalSymbol::new(base, quote))
}

fn row_exchange(row: &Value, fields: &[&str]) -> Option<StrategyExchangeId> {
    text_field(row, fields).map(|exchange| StrategyExchangeId::new(gateway_exchange_id(exchange)))
}

fn open_block_reason_text(reason: OpenBlockReason) -> String {
    match reason {
        OpenBlockReason::StrategyHalted => "strategy is halted".to_string(),
        OpenBlockReason::MaxOpenBundles => "max open bundle limit reached".to_string(),
        OpenBlockReason::SymbolAlreadyActive => {
            "same symbol already has an active open bundle".to_string()
        }
        OpenBlockReason::SymbolCoolingDown => {
            "symbol is cooling down after a completed close".to_string()
        }
        OpenBlockReason::ExchangePositionLimit => {
            "exchange open position limit reached".to_string()
        }
    }
}

fn open_bundle_row(
    bundle: &LiveOpenBundle,
    close: Option<&rustcta_strategy_cross_exchange_arbitrage::DualTakerCloseEvaluation>,
    strategy_config: &CrossExchangeArbitrageConfig,
    quality_controls: LiveExecutionQualityControls,
    evaluated_at: DateTime<Utc>,
) -> Value {
    let entry_net_edge_pct = (bundle.position.short_entry_price - bundle.position.long_entry_price)
        / bundle.position.long_entry_price.max(1e-12);
    let held_secs = bundle.position.held_secs(evaluated_at);
    let max_hold_secs = strategy_config.dual_taker.max_hold_secs;
    let max_hold_expired = held_secs >= max_hold_secs;
    let close_on_max_hold_requires_profit =
        strategy_config.dual_taker.close_on_max_hold_requires_profit;
    let mut row = json!({
        "bundle_id": bundle.bundle_id,
        "canonical_symbol": bundle.position.canonical_symbol.as_pair(),
        "symbol": bundle.position.canonical_symbol.as_pair(),
        "long_exchange": bundle.position.long_exchange.to_string(),
        "short_exchange": bundle.position.short_exchange.to_string(),
        "status": "open",
        "quantity": bundle.position.quantity,
        "long_entry_price": bundle.position.long_entry_price,
        "short_entry_price": bundle.position.short_entry_price,
        "entry_net_edge_pct": entry_net_edge_pct,
        "opened_at": bundle.opened_at,
        "updated_at": evaluated_at,
        "evaluated_at": evaluated_at,
        "held_secs": held_secs,
        "max_hold_secs": max_hold_secs,
        "max_hold_expired": max_hold_expired,
        "close_on_max_hold_requires_profit": close_on_max_hold_requires_profit,
        "max_hold_close_blocked_by_profit": false,
        "open_fee_usdt": bundle.open_fee_usdt,
        "close_min_net_profit_pct": strategy_config.dual_taker.close_min_net_profit_pct,
        "execution_quality_close_min_net_profit_pct": quality_controls.min_close_net_profit_pct,
        "close_threshold_pct": strategy_config.dual_taker.close_min_net_profit_pct,
        "open_legs": [leg_json(&bundle.open_long), leg_json(&bundle.open_short)],
    });
    if let Some(close) = close {
        let close_profit_target_met =
            close.net_profit_pct >= strategy_config.dual_taker.close_min_net_profit_pct;
        let close_quality_target_met = quality_controls.close_allows(close);
        row["long_close_price"] = json!(close.long_close_price);
        row["short_close_price"] = json!(close.short_close_price);
        row["close_gross_pnl_usdt"] = json!(close.gross_pnl_usdt);
        row["close_fee_usdt"] = json!(close.total_fee_usdt);
        row["close_net_pnl_usdt"] = json!(close.net_pnl_usdt);
        row["close_net_profit_pct"] = json!(close.net_profit_pct);
        row["close_candidate_profit_pct"] = json!(close.net_profit_pct);
        row["close_quality_target_met"] = json!(close_quality_target_met);
        row["closeable"] = json!(close.should_close && close_quality_target_met);
        row["close_ready"] = json!(close.should_close && close_quality_target_met);
        row["max_hold_close_blocked_by_profit"] = json!(
            max_hold_expired
                && close_on_max_hold_requires_profit
                && !close_profit_target_met
                && !close.should_close
        );
        if close.should_close && !close_quality_target_met {
            row["close_block_reason"] = json!(format!(
                "close net profit {} is below execution quality min {}",
                format_float(close.net_profit_pct),
                format_float(quality_controls.min_close_net_profit_pct)
            ));
        }
        row["close_reason"] = close
            .reason
            .as_ref()
            .map(|reason| format!("{reason:?}").to_ascii_lowercase())
            .map(Value::String)
            .unwrap_or(Value::Null);
        row["close_route"] = json!(format!(
            "{} close_long @ {} / {} close_short @ {}",
            bundle.position.long_exchange,
            format_float(close.long_close_price),
            bundle.position.short_exchange,
            format_float(close.short_close_price)
        ));
    } else {
        row["closeable"] = json!(false);
        row["close_ready"] = json!(false);
    }
    row
}

fn profit_history_loss_guard_triggered(
    args: &LiveRunnerArgs,
    max_consecutive_losses: u32,
) -> Result<bool> {
    let Some(path) = args.profit_history_path.as_ref() else {
        return Ok(false);
    };
    let rows = read_jsonl_rows(path, 500)?;
    Ok(profit_summary_from_rows(&rows, max_consecutive_losses)
        .get("stopped_by_loss_guard")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

fn append_profit_event(path: Option<&PathBuf>, event: &Value) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    serde_json::to_writer(&mut file, event).with_context(|| format!("write {}", path.display()))?;
    writeln!(file).with_context(|| format!("write newline {}", path.display()))?;
    Ok(())
}

fn append_latency_event(args: &LiveRunnerArgs, sinks: &LiveRuntimeSinks, event: Value) {
    sinks.record_latency_event(args, event);
}

fn trade_ledger_queue_capacity(config: &Value) -> usize {
    u64_at_path(config, &["logging", "trade_ledger_queue_capacity"])
        .or_else(|| u64_at_path(config, &["persistence", "trade_ledger_queue_capacity"]))
        .unwrap_or(8192)
        .clamp(1, 1_000_000) as usize
}

fn trade_identity(
    args: &LiveRunnerArgs,
    source: &str,
    occurred_at: DateTime<Utc>,
) -> EventIdentity {
    EventIdentity::new(
        TenantId::new(args.tenant_id.clone()).unwrap_or_else(|_| TenantId::unchecked("local")),
        source,
        occurred_at,
    )
    .with_account(
        AccountId::new(args.account_id.clone()).unwrap_or_else(|_| AccountId::unchecked("local")),
    )
    .with_strategy_run(
        StrategyId::new(args.strategy_id.clone())
            .unwrap_or_else(|_| StrategyId::unchecked("cross_arb_live")),
        RunId::new(args.run_id.clone()).unwrap_or_else(|_| RunId::unchecked("local")),
    )
}

fn trade_audit_event(args: &LiveRunnerArgs, action: &str, payload: Value) -> LedgerEvent {
    let occurred_at = datetime_any_field(&payload, &["occurred_at", "recorded_at", "planned_at"])
        .unwrap_or_else(Utc::now);
    let mut record = AuditRecord::new(
        trade_identity(args, "cross-exchange-arbitrage-live-runner", occurred_at),
        AuditActor::new(AuditActorType::Strategy, args.strategy_id.clone()),
        action,
        AuditOutcome::Succeeded,
    );
    record.metadata = payload;
    LedgerEvent::audit(record)
}

fn trade_order_event(
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    leg: &ReconciledOrderLeg,
    requested_at: DateTime<Utc>,
) -> Option<LedgerEvent> {
    let symbol = CanonicalSymbol::parse(&leg.symbol).ok()?;
    let exchange = GatewayExchangeId::new(leg.exchange.clone()).ok()?;
    let side = ledger_order_side(&leg.side)?;
    let position_side = ledger_position_side(&leg.position_side);
    let status = ledger_order_status(&leg.status);
    let mut record = OrderLifecycleRecord::new(
        trade_identity(
            args,
            "cross-exchange-arbitrage-live-runner",
            leg.acked_at.unwrap_or(requested_at),
        )
        .with_command(
            leg.client_order_id
                .clone()
                .unwrap_or_else(|| bundle_id.to_string()),
        )
        .with_correlation_id(bundle_id.to_string()),
        if leg.accepted {
            EventKind::OrderAckEvent
        } else {
            EventKind::RejectionEvent
        },
        exchange,
        target_market_type,
        symbol,
        leg.client_order_id
            .clone()
            .unwrap_or_else(|| format!("{bundle_id}:{lifecycle}:{}", leg.role)),
        side,
        position_side,
        status,
        leg.planned_order_quantity,
    );
    record.exchange_order_id = leg.exchange_order_id.clone();
    record.requested_price = Some(leg.planned_price).filter(|price| price.is_finite());
    record.filled_quantity = leg.actual_order_quantity.unwrap_or(0.0);
    record.average_fill_price = leg.actual_fill_price;
    record.message = leg.error.clone();
    record.metadata = json!({
        "bundle_id": bundle_id,
        "lifecycle": lifecycle,
        "role": leg.role,
        "planned_base_quantity": format_float(leg.planned_base_quantity),
        "planned_order_quantity": format_float(leg.planned_order_quantity),
        "actual_base_quantity": leg.actual_base_quantity.map(format_float),
        "actual_notional_usdt": leg.actual_notional_usdt.map(format_float),
        "submitted_at": leg.submitted_at,
        "acked_at": leg.acked_at,
        "filled_at": leg.filled_at,
        "status_text": leg.status,
    });
    Some(LedgerEvent::order(record))
}

fn trade_fill_event(
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    bundle_id: &str,
    lifecycle: &str,
    leg: &ReconciledOrderLeg,
) -> Option<LedgerEvent> {
    if !leg.filled() {
        return None;
    }
    let symbol = CanonicalSymbol::parse(&leg.symbol).ok()?;
    let exchange = GatewayExchangeId::new(leg.exchange.clone()).ok()?;
    let price = leg.actual_fill_price?;
    let quantity = leg.actual_order_quantity.or(leg.actual_base_quantity)?;
    let filled_at = leg.filled_at.unwrap_or_else(Utc::now);
    let mut record = FillLedgerRecord::new(
        trade_identity(args, "cross-exchange-arbitrage-live-runner", filled_at)
            .with_command(
                leg.client_order_id
                    .clone()
                    .unwrap_or_else(|| bundle_id.to_string()),
            )
            .with_correlation_id(bundle_id.to_string()),
        exchange,
        target_market_type,
        symbol,
        ledger_order_side(&leg.side)?,
        ledger_position_side(&leg.position_side),
        LiquidityRole::Taker,
        price,
        quantity,
        filled_at,
    );
    record.client_order_id = leg.client_order_id.clone();
    record.exchange_order_id = leg.exchange_order_id.clone();
    record.quote_quantity = leg.actual_notional_usdt;
    record.fee_amount = Some(leg.fee_usdt).filter(|fee| fee.is_finite());
    record.fee_asset = Some("USDT".to_string()).filter(|_| leg.fee_usdt > 0.0);
    record.metadata = json!({
        "bundle_id": bundle_id,
        "lifecycle": lifecycle,
        "role": leg.role,
        "actual_base_quantity": leg.actual_base_quantity.map(format_float),
        "actual_order_quantity": leg.actual_order_quantity.map(format_float),
        "actual_notional_usdt": leg.actual_notional_usdt.map(format_float),
        "submitted_at": leg.submitted_at,
        "acked_at": leg.acked_at,
        "filled_at": leg.filled_at,
    });
    Some(LedgerEvent::fill(record))
}

fn ledger_order_side(side: &str) -> Option<GatewayOrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Some(GatewayOrderSide::Buy),
        "sell" => Some(GatewayOrderSide::Sell),
        _ => None,
    }
}

fn ledger_position_side(position_side: &str) -> GatewayPositionSide {
    match position_side.trim().to_ascii_lowercase().as_str() {
        "long" => GatewayPositionSide::Long,
        "short" => GatewayPositionSide::Short,
        "net" => GatewayPositionSide::Net,
        _ => GatewayPositionSide::None,
    }
}

fn ledger_order_status(status: &str) -> GatewayOrderStatus {
    let normalized = status.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match normalized.as_str() {
        "new" => GatewayOrderStatus::New,
        "open" | "accepted" => GatewayOrderStatus::Open,
        "partially_filled" | "partial_fill" | "partial_filled" => {
            GatewayOrderStatus::PartiallyFilled
        }
        "filled" | "full_fill" | "closed" | "private_ws_confirmed" => GatewayOrderStatus::Filled,
        "pending_cancel" => GatewayOrderStatus::PendingCancel,
        "cancelled" | "canceled" => GatewayOrderStatus::Cancelled,
        "rejected" | "submit_failed" | "private_ws_confirmation_timeout" => {
            GatewayOrderStatus::Rejected
        }
        "expired" => GatewayOrderStatus::Expired,
        _ => GatewayOrderStatus::Unknown,
    }
}

fn apply_private_ws_event_to_leg(
    leg: &mut ReconciledOrderLeg,
    draft: &TakerOrderDraft,
    event: &Value,
) {
    leg.status = event
        .get("order_status")
        .and_then(Value::as_str)
        .unwrap_or("private_ws_confirmed")
        .to_ascii_lowercase();
    leg.exchange_order_id = text_field(event, &["exchange_order_id"])
        .map(ToString::to_string)
        .or_else(|| leg.exchange_order_id.clone());
    leg.actual_order_quantity = f64_field(event, &["quantity"]);
    leg.actual_fill_price = f64_field(event, &["price"]);
    leg.filled_at = datetime_any_field(event, &["observed_at"]).or(Some(Utc::now()));
    if let (Some(quantity), Some(price)) = (leg.actual_order_quantity, leg.actual_fill_price) {
        let base_quantity = base_quantity_from_order_quantity(draft, quantity);
        leg.actual_base_quantity = Some(base_quantity);
        leg.actual_notional_usdt = Some(base_quantity * price);
    }
}

fn order_latency_span_event(
    bundle_id: &str,
    lifecycle: &str,
    leg: &ReconciledOrderLeg,
    submitted_at: DateTime<Utc>,
) -> Value {
    let submit_at = leg.submitted_at.unwrap_or(submitted_at);
    let ack_at = leg.acked_at.unwrap_or_else(Utc::now);
    let fill_at = leg.filled_at;
    json!({
        "event_kind": "cross_arb_latency_span",
        "span_kind": "order_submit_ack_fill",
        "bundle_id": bundle_id,
        "lifecycle": lifecycle,
        "exchange": leg.exchange,
        "symbol": leg.symbol,
        "role": leg.role,
        "client_order_id": leg.client_order_id,
        "submit_started_at": submit_at,
        "exchange_ack_at": ack_at,
        "private_fill_at": fill_at,
        "submit_to_ack_ms": ack_at.signed_duration_since(submit_at).num_milliseconds(),
        "fill_confirm_latency_ms": fill_at
            .map(|at| at.signed_duration_since(submit_at).num_milliseconds()),
        "filled": leg.filled(),
        "status": leg.status,
        "recorded_at": Utc::now(),
    })
}

fn opportunity_latency_span_event(
    opportunity: &DualTakerOpenOpportunity,
    dashboard: &LiveDashboardData,
    decision_at: DateTime<Utc>,
) -> Value {
    let long_top = find_top(
        &dashboard.tops,
        &opportunity.long_exchange,
        &opportunity.canonical_symbol,
    );
    let short_top = find_top(
        &dashboard.tops,
        &opportunity.short_exchange,
        &opportunity.canonical_symbol,
    );
    let exchange_ts = long_top
        .and_then(|top| top.exchange_timestamp)
        .into_iter()
        .chain(short_top.and_then(|top| top.exchange_timestamp))
        .max();
    let received_at = long_top
        .map(|top| top.received_at)
        .into_iter()
        .chain(short_top.map(|top| top.received_at))
        .max();
    json!({
        "event_kind": "cross_arb_latency_span",
        "span_kind": "opportunity_decision",
        "opportunity_id": opportunity.opportunity_id,
        "canonical_symbol": opportunity.canonical_symbol.as_pair(),
        "long_exchange": opportunity.long_exchange.to_string(),
        "short_exchange": opportunity.short_exchange.to_string(),
        "exchange_ts": exchange_ts,
        "received_at": received_at,
        "decision_started_at": decision_at,
        "decision_finished_at": decision_at,
        "market_data_latency_ms": exchange_ts.zip(received_at)
            .map(|(exchange_ts, received_at)| received_at.signed_duration_since(exchange_ts).num_milliseconds()),
        "decision_latency_ms": received_at
            .map(|received_at| decision_at.signed_duration_since(received_at).num_milliseconds()),
        "expected_net_profit_pct": opportunity.expected_net_profit_pct,
        "raw_open_spread_pct": opportunity.spread_pct,
        "recorded_at": Utc::now(),
    })
}

fn find_top<'a>(
    tops: &'a [OrderBookTop],
    exchange: &StrategyExchangeId,
    symbol: &StrategyCanonicalSymbol,
) -> Option<&'a OrderBookTop> {
    tops.iter()
        .find(|top| &top.exchange == exchange && &top.canonical_symbol == symbol)
}

fn leg_for_role(execution: &PairExecution, role: TakerOrderRole) -> Option<&ReconciledOrderLeg> {
    let role = role_name(role);
    [&execution.first, &execution.second]
        .into_iter()
        .find(|leg| leg.role == role)
}

fn draft_symbol_scope(
    draft: &TakerOrderDraft,
    target_market_type: GatewayMarketType,
) -> Result<SymbolScope> {
    let exchange_id = GatewayExchangeId::new(gateway_exchange_id(draft.exchange.as_str()))?;
    let canonical_symbol =
        CanonicalSymbol::new(&draft.canonical_symbol.base, &draft.canonical_symbol.quote)?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        target_market_type,
        exchange_symbol_text(&exchange_id.to_string(), &canonical_symbol),
    )?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: target_market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol,
    })
}

fn sdk_order_side(side: rustcta_strategy_cross_exchange_arbitrage::OrderSide) -> SdkOrderSide {
    match side {
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy => SdkOrderSide::Buy,
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell => SdkOrderSide::Sell,
    }
}

fn strategy_side_name(side: rustcta_strategy_cross_exchange_arbitrage::OrderSide) -> &'static str {
    match side {
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Buy => "buy",
        rustcta_strategy_cross_exchange_arbitrage::OrderSide::Sell => "sell",
    }
}

fn role_name(role: TakerOrderRole) -> &'static str {
    match role {
        TakerOrderRole::OpenLong => "open_long",
        TakerOrderRole::OpenShort => "open_short",
        TakerOrderRole::CloseLong => "close_long",
        TakerOrderRole::CloseShort => "close_short",
        TakerOrderRole::EmergencyCloseLong => "emergency_close_long",
        TakerOrderRole::EmergencyCloseShort => "emergency_close_short",
    }
}

fn position_side_name(role: TakerOrderRole) -> &'static str {
    match role {
        TakerOrderRole::OpenLong
        | TakerOrderRole::CloseLong
        | TakerOrderRole::EmergencyCloseLong => "long",
        TakerOrderRole::OpenShort
        | TakerOrderRole::CloseShort
        | TakerOrderRole::EmergencyCloseShort => "short",
    }
}

fn base_quantity_from_order_quantity(draft: &TakerOrderDraft, order_quantity: f64) -> f64 {
    match draft.quantity_unit {
        rustcta_strategy_cross_exchange_arbitrage::QuantityUnit::Base => order_quantity.max(0.0),
        rustcta_strategy_cross_exchange_arbitrage::QuantityUnit::Contracts => {
            order_quantity.max(0.0) * draft.contract_size.max(0.0)
        }
    }
}

fn live_bundle_id(
    ctx: &StrategyContext,
    opportunity: &DualTakerOpenOpportunity,
    now: DateTime<Utc>,
) -> String {
    safe_id_part(&format!(
        "{}-{}-{}-{}-{}",
        ctx.run_id(),
        opportunity.canonical_symbol.as_pair(),
        opportunity.long_exchange,
        opportunity.short_exchange,
        now.timestamp_millis()
    ))
}

fn safe_client_order_id(
    strategy_id: &str,
    run_id: &str,
    bundle_id: &str,
    lifecycle: &str,
    role: &str,
    leg_index: usize,
    requested_at: DateTime<Utc>,
) -> String {
    let source = format!(
        "{}-{}-{}-{}-{}-{}-{}",
        strategy_id,
        run_id,
        bundle_id,
        lifecycle,
        role,
        leg_index,
        requested_at.timestamp_millis()
    );
    let hash = source.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let time_suffix = requested_at.timestamp_millis().rem_euclid(1_000_000_000);
    let role_code = role
        .split('_')
        .filter_map(|part| part.chars().next())
        .collect::<String>();
    let role_code = if role_code.is_empty() {
        "x".to_string()
    } else {
        safe_id_part(&role_code)
    };
    let value = format!("ca-{role_code}-{time_suffix:09}-{hash:08x}");
    safe_id_part(&value).chars().take(28).collect()
}

fn safe_id_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn format_float(value: f64) -> String {
    if !value.is_finite() {
        return "0".to_string();
    }
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value.max(0.0);
    }
    (((value / step) + 1e-12).floor() * step).max(0.0)
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value.max(0.0);
    }
    ((value / step).ceil() * step).max(0.0)
}

fn parse_positive_optional(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn value_to_positive_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn trade_capability_requirements(capability: &ExchangeClientCapabilities) -> Vec<String> {
    let exchange = capability.exchange.to_string();
    let mut requirements = Vec::new();
    if !capability.supports_private_rest {
        requirements.push(format!("{exchange} private REST is not enabled"));
    }
    if !capability.supports_positions {
        requirements.push(format!("{exchange} positions are not supported"));
    }
    if !capability.supports_place_order {
        requirements.push(format!("{exchange} place_order is not supported"));
    }
    if !capability.supports_cancel_order {
        requirements.push(format!("{exchange} cancel_order is not supported"));
    }
    if !(capability.supports_query_order || capability.supports_open_orders) {
        requirements.push(format!(
            "{exchange} order readback requires query_order or open_orders support"
        ));
    }
    if !(capability.supports_recent_fills || capability.supports_query_order) {
        requirements.push(format!(
            "{exchange} actual fill price readback requires recent_fills or query_order support"
        ));
    }
    if !capability.supports_reduce_only {
        requirements.push(format!("{exchange} reduce_only orders are not supported"));
    }
    if !capability
        .supports_time_in_force
        .contains(&GatewayTimeInForce::IOC)
    {
        requirements.push(format!("{exchange} IOC time-in-force is not supported"));
    }
    if !capability
        .supports_order_types
        .contains(&GatewayOrderType::IOC)
    {
        requirements.push(format!("{exchange} IOC order type is not supported"));
    }
    if !capability
        .supports_order_types
        .contains(&GatewayOrderType::Market)
    {
        requirements.push(format!("{exchange} market order type is not supported"));
    }
    requirements
}

async fn validate_gateway_capabilities(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    required_exchanges: &[String],
    loaded_adapters: &[String],
    live_orders_enabled: bool,
) -> Result<CapabilityGateReport> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.run_id = Some(run_id);
    context.request_id = Some("cross-arb-live-capability-gate".to_string());

    let exchanges = required_exchanges
        .iter()
        .map(|exchange| GatewayExchangeId::new(exchange.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let response = gateway
        .get_capabilities(
            "cross-arb-live-capability-gate".to_string(),
            tenant_id,
            Some(account_id),
            GetCapabilitiesRequest {
                schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context,
                exchanges,
            },
        )
        .await?;

    let mut degraded = Vec::new();
    let mut missing = Vec::new();
    let mut seen = BTreeSet::new();
    for capability in response.capabilities {
        let exchange = capability.exchange.to_string();
        seen.insert(exchange.clone());
        if !capability.market_types.contains(&target_market_type) {
            missing.push(format!(
                "{exchange} does not support target market type {target_market_type:?}; advertised={:?}",
                capability.market_types
            ));
        }
        if !capability.supports_symbol_rules {
            missing.push(format!("{exchange} symbol rules are not supported"));
        }
        if !capability.supports_order_book_snapshot {
            degraded.push(format!(
                "{exchange} REST order book snapshot is unavailable; live market data uses websocket order books"
            ));
        }
        let trade_requirements = trade_capability_requirements(&capability);
        if live_orders_enabled {
            missing.extend(trade_requirements);
        } else {
            degraded.extend(
                trade_requirements
                    .into_iter()
                    .map(|requirement| format!("{requirement} (required before live orders)")),
            );
        }
    }

    for exchange in required_exchanges {
        if !seen.contains(exchange) {
            missing.push(format!("{exchange} adapter is not loaded"));
        }
    }

    Ok(CapabilityGateReport {
        passed: missing.is_empty(),
        target_market_type: format!("{target_market_type:?}"),
        required_exchanges: required_exchanges.to_vec(),
        loaded_adapters: loaded_adapters.to_vec(),
        degraded_requirements: degraded,
        missing_requirements: missing,
    })
}

fn gateway_exchange_ids(venues: Vec<String>) -> Vec<String> {
    venues
        .into_iter()
        .map(|venue| gateway_exchange_id(&venue))
        .fold(Vec::new(), |mut exchanges, exchange| {
            if !exchanges.contains(&exchange) {
                exchanges.push(exchange);
            }
            exchanges
        })
}

fn gateway_exchange_id(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gateio" | "gate.io" | "gate_io" => "gateio".to_string(),
        "binance" => "binance".to_string(),
        "bitget" => "bitget".to_string(),
        "" => "unknown".to_string(),
        other => other.to_string(),
    }
}

fn exchange_account_map(
    config: &Value,
    required_exchanges: &[String],
    default_account_id: &str,
) -> BTreeMap<String, String> {
    let mut accounts = BTreeMap::new();
    for exchange in required_exchanges {
        let account = account_id_for_exchange(config, exchange).unwrap_or(default_account_id);
        accounts.insert(exchange.clone(), account.to_string());
        if exchange == "gateio" {
            accounts.insert("gate".to_string(), account.to_string());
            accounts.insert("gate.io".to_string(), account.to_string());
        }
    }
    accounts
}

fn account_id_for_exchange<'a>(config: &'a Value, exchange: &str) -> Option<&'a str> {
    let exchange_configs = config.get("exchanges")?.as_object()?;
    for key in exchange_config_keys(exchange) {
        if let Some(account_id) = exchange_configs
            .get(*key)
            .and_then(|value| value.get("account_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(account_id);
        }
    }
    None
}

fn exchange_config_keys(exchange: &str) -> &'static [&'static str] {
    match exchange {
        "gateio" => &["gateio", "gate", "gate.io", "gate_io"],
        "binance" => &["binance"],
        "bitget" => &["bitget"],
        _ => &[],
    }
}

fn gateway_market_type(market_type: &SdkMarketType) -> Result<GatewayMarketType> {
    Ok(match market_type {
        SdkMarketType::Spot => GatewayMarketType::Spot,
        SdkMarketType::Margin => GatewayMarketType::Margin,
        SdkMarketType::Perpetual => GatewayMarketType::Perpetual,
        SdkMarketType::Futures => GatewayMarketType::Futures,
        SdkMarketType::Option => GatewayMarketType::Option,
        SdkMarketType::Custom(value) => bail!("unsupported custom market type: {value}"),
    })
}

struct RouterBackedStrategyExecutionClient {
    router: Arc<LocalExecutionRouter>,
    market_type: GatewayMarketType,
    account_by_exchange: BTreeMap<String, String>,
}

#[async_trait]
impl StrategyExecutionClient for RouterBackedStrategyExecutionClient {
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
            reason: Some("raw intents are not routed by the live execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

impl RouterBackedStrategyExecutionClient {
    fn map_order_command(&self, command: ExecutionOrderCommand) -> SdkResult<OrderCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            self.market_type,
            exchange_symbol_text(&exchange, &canonical_symbol),
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
            self.market_type,
            canonical_symbol,
            exchange_symbol,
            command.client_order_id,
            order_side(command.side),
            position_side(&command.metadata),
            order_type,
            time_in_force,
            quantity,
            price,
        );
        order.reduce_only = command.reduce_only;
        order.post_only = matches!(order.order_type, GatewayOrderType::PostOnly);
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
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            self.market_type,
            exchange_symbol_text(&exchange, &canonical_symbol),
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
            self.market_type,
            canonical_symbol,
            exchange_symbol,
            cancellation_ids,
        ))
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

fn exchange_symbol_text(exchange: &str, canonical_symbol: &CanonicalSymbol) -> String {
    let base = canonical_symbol.base_asset();
    let quote = canonical_symbol.quote_asset();
    match exchange {
        "gateio" => format!("{base}_{quote}"),
        _ => format!("{base}{quote}"),
    }
}

fn order_side(side: rustcta_strategy_sdk::OrderSide) -> GatewayOrderSide {
    match side {
        rustcta_strategy_sdk::OrderSide::Buy => GatewayOrderSide::Buy,
        rustcta_strategy_sdk::OrderSide::Sell => GatewayOrderSide::Sell,
    }
}

fn order_type(order_type: rustcta_strategy_sdk::OrderType) -> SdkResult<GatewayOrderType> {
    Ok(match order_type {
        rustcta_strategy_sdk::OrderType::Market => GatewayOrderType::Market,
        rustcta_strategy_sdk::OrderType::Limit => GatewayOrderType::Limit,
        rustcta_strategy_sdk::OrderType::PostOnly => GatewayOrderType::PostOnly,
        rustcta_strategy_sdk::OrderType::ImmediateOrCancel => GatewayOrderType::IOC,
        rustcta_strategy_sdk::OrderType::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayOrderType::IOC
        }
        rustcta_strategy_sdk::OrderType::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom order type: {value}"
            )));
        }
    })
}

fn time_in_force(
    time_in_force: rustcta_strategy_sdk::TimeInForce,
) -> SdkResult<GatewayTimeInForce> {
    Ok(match time_in_force {
        rustcta_strategy_sdk::TimeInForce::GoodTilCanceled => GatewayTimeInForce::GTC,
        rustcta_strategy_sdk::TimeInForce::ImmediateOrCancel => GatewayTimeInForce::IOC,
        rustcta_strategy_sdk::TimeInForce::FillOrKill => GatewayTimeInForce::FOK,
        rustcta_strategy_sdk::TimeInForce::PostOnly => GatewayTimeInForce::GTX,
        rustcta_strategy_sdk::TimeInForce::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayTimeInForce::IOC
        }
        rustcta_strategy_sdk::TimeInForce::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom time-in-force: {value}"
            )));
        }
    })
}

fn default_time_in_force(order_type: GatewayOrderType) -> GatewayTimeInForce {
    match order_type {
        GatewayOrderType::IOC => GatewayTimeInForce::IOC,
        GatewayOrderType::FOK => GatewayTimeInForce::FOK,
        GatewayOrderType::PostOnly => GatewayTimeInForce::GTX,
        _ => GatewayTimeInForce::GTC,
    }
}

fn position_side(metadata: &BTreeMap<String, Value>) -> GatewayPositionSide {
    match metadata
        .get("position_side")
        .or_else(|| metadata.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" | "open_long" | "close_long" | "emergency_close_long" => GatewayPositionSide::Long,
        "short" | "open_short" | "close_short" | "emergency_close_short" => {
            GatewayPositionSide::Short
        }
        _ => GatewayPositionSide::Net,
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

struct ProcessSingletonGuard {
    path: PathBuf,
    _file: File,
}

impl ProcessSingletonGuard {
    fn acquire(path: &Path) -> Result<Self> {
        match Self::try_create(path) {
            Ok(guard) => Ok(guard),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if lock_owner_is_alive(path)? {
                    bail!(
                        "cross-exchange arbitrage live runner already has a live lock at {}",
                        path.display()
                    );
                }
                std::fs::remove_file(path)
                    .with_context(|| format!("remove stale lock {}", path.display()))?;
                Self::try_create(path).with_context(|| {
                    format!(
                        "recreate singleton lock after stale cleanup {}",
                        path.display()
                    )
                })
            }
            Err(error) => {
                Err(error).with_context(|| format!("create singleton lock {}", path.display()))
            }
        }
    }

    fn try_create(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        writeln!(file, "pid={}", std::process::id())?;
        writeln!(file, "created_at={}", Utc::now().to_rfc3339())?;
        Ok(Self {
            path: path.to_path_buf(),
            _file: file,
        })
    }
}

impl Drop for ProcessSingletonGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_owner_is_alive(path: &Path) -> Result<bool> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read existing singleton lock {}", path.display()))?;
    let Some(pid) = raw
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.parse::<u32>().ok())
    else {
        bail!(
            "singleton lock {} exists but does not contain a parseable pid",
            path.display()
        );
    };
    Ok(process_is_alive(pid))
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

#[cfg(not(target_os = "linux"))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcta_exchange_api::{OrderState, ResponseMetadata, EXCHANGE_API_SCHEMA_VERSION};
    use rustcta_exchange_gateway::{
        GatewayOperation, GatewayProtocolRequest, GatewayProtocolResponse, GatewayRequestPayload,
        GatewayResponsePayload,
    };
    use rustcta_strategy_cross_exchange_arbitrage::{
        DualTakerArbitrageConfig, OrderSide as StrategyOrderSide, QuantityUnit,
    };
    use rustcta_types::{FillStatus, LiquidityRole, SchemaVersion};
    use std::sync::Mutex;

    fn test_confirmation_policy() -> LiveConfirmationPolicy {
        LiveConfirmationPolicy {
            private_ws: PrivateUserWsObserver::start(
                &[],
                private_ws_observe_config_from_runtime(&json!({})),
            ),
            require_private_ws: false,
            allow_rest_readback: true,
            private_ws_timeout_ms: 1,
        }
    }

    #[test]
    fn gateway_market_type_should_keep_perpetual_and_futures_distinct() {
        assert_eq!(
            gateway_market_type(&SdkMarketType::Perpetual).expect("perpetual"),
            GatewayMarketType::Perpetual
        );
        assert_eq!(
            gateway_market_type(&SdkMarketType::Futures).expect("futures"),
            GatewayMarketType::Futures
        );
    }

    #[test]
    fn live_runner_args_should_parse_bounded_runtime_controls() {
        let args = LiveRunnerArgs::from_iter([
            "--config".to_string(),
            "config/test.yml".to_string(),
            "--dashboard-refresh-ms".to_string(),
            "1000".to_string(),
            "--run-seconds".to_string(),
            "180".to_string(),
            "--market-data-source".to_string(),
            "direct_websocket".to_string(),
            "--trade-ledger-path".to_string(),
            "logs/trade_events.jsonl".to_string(),
            "--once".to_string(),
        ])
        .expect("args");

        assert_eq!(args.config, PathBuf::from("config/test.yml"));
        assert_eq!(args.dashboard_refresh_ms, 1000);
        assert_eq!(args.run_seconds, 180);
        assert_eq!(
            args.market_data_source,
            LiveMarketDataSource::DirectWebsocket
        );
        assert_eq!(
            args.trade_ledger_path,
            Some(PathBuf::from("logs/trade_events.jsonl"))
        );
        assert!(args.once);
    }

    #[test]
    fn live_runner_args_should_disable_trade_ledger() {
        let args =
            LiveRunnerArgs::from_iter(["--no-trade-ledger".to_string()]).expect("args parse");

        assert_eq!(args.trade_ledger_path, None);
    }

    #[test]
    fn live_runner_args_should_reject_snapshot_interval_ms() {
        let error =
            LiveRunnerArgs::from_iter(["--snapshot-interval-ms".to_string(), "1000".to_string()])
                .expect_err("removed flag should fail");

        assert!(error.to_string().contains("has been removed"));
    }

    #[test]
    fn market_data_snapshot_path_should_be_rejected() {
        let error = LiveRunnerArgs::from_iter([
            "--market-data-snapshot-path".to_string(),
            "logs/ws.json".to_string(),
        ])
        .expect_err("removed snapshot market data path should fail");

        assert!(error.to_string().contains("has been removed"));
    }

    #[test]
    fn market_data_source_snapshot_file_should_be_rejected() {
        let error = LiveRunnerArgs::from_iter([
            "--market-data-source".to_string(),
            "snapshot_file".to_string(),
        ])
        .expect_err("removed snapshot market data source should fail");

        assert!(error.to_string().contains("has been removed"));
    }

    #[test]
    fn live_market_data_source_guard_should_allow_direct_ws_execution() {
        let args = LiveRunnerArgs {
            market_data_source: LiveMarketDataSource::DirectWebsocket,
            ..LiveRunnerArgs::default()
        };

        validate_live_market_data_source(&args, true).expect("direct ws is the live path");
    }

    #[test]
    fn direct_ws_connections_should_chunk_enabled_symbols_by_exchange() {
        let symbols = (0..85)
            .map(|index| format!("SYM{index}/USDT"))
            .collect::<Vec<_>>();
        let connections =
            build_direct_ws_connections(&["binance".to_string(), "gateio".to_string()], &symbols)
                .expect("connections");

        let binance = connections
            .iter()
            .filter(|connection| connection.exchange == "binance")
            .collect::<Vec<_>>();
        let gate = connections
            .iter()
            .filter(|connection| connection.exchange == "gate")
            .collect::<Vec<_>>();
        assert_eq!(binance.len(), 2);
        assert_eq!(gate.len(), 3);
        assert!(binance[0].url.contains("fstream.binance.com"));
        assert_eq!(gate[0].subscribe_messages.len(), 30);
    }

    #[test]
    fn direct_ws_parsers_should_extract_binance_bitget_and_gate_tops() {
        let binance = r#"{"stream":"edgeusdt@depth5@100ms","data":{"E":1780940000000,"s":"EDGEUSDT","b":[["0.1","12"]],"a":[["0.2","13"]]}}"#;
        let top = parse_direct_ws_order_book_top("binance", binance).expect("binance");
        assert_eq!(top.exchange.as_str(), "binance");
        assert_eq!(top.canonical_symbol.as_pair(), "EDGE/USDT");
        assert_eq!(top.best_bid_price, 0.1);
        assert_eq!(top.best_ask_quantity, 13.0);

        let bitget = r#"{"arg":{"instType":"USDT-FUTURES","channel":"books5","instId":"EDGEUSDT"},"data":[{"instId":"EDGEUSDT","ts":"1780940000000","bids":[["0.3","14"]],"asks":[["0.4","15"]]}]}"#;
        let top = parse_direct_ws_order_book_top("bitget", bitget).expect("bitget");
        assert_eq!(top.exchange.as_str(), "bitget");
        assert_eq!(top.best_bid_quantity, 14.0);

        let gate = r#"{"channel":"futures.order_book","event":"update","result":{"t":1780940000000,"contract":"EDGE_USDT","b":[{"p":"0.5","s":16}],"a":[{"p":"0.6","s":17}]}}"#;
        let top = parse_direct_ws_order_book_top("gate", gate).expect("gate");
        assert_eq!(top.exchange.as_str(), "gateio");
        assert_eq!(top.best_ask_price, 0.6);
    }

    #[test]
    fn live_execution_controls_should_parse_close_only() {
        let controls = live_execution_controls_from_config(&json!({
            "controls": {
                "start_paused_new_entries": false,
                "start_close_only": true
            }
        }));

        assert!(controls.start_close_only);
        assert!(!controls.start_paused_new_entries);
        assert!(!controls.allow_new_entries(true));
        assert_eq!(
            controls.new_entries_block_reason(true),
            Some("close-only control is enabled")
        );
    }

    #[test]
    fn gateio_contract_sizes_should_use_quanto_multiplier() {
        let sizes = gateio_contract_sizes_from_value(&json!([
            {"name": "SPCX_USDT", "quanto_multiplier": "0.01"},
            {"contract": "ESPORTS_USDT", "quanto_multiplier": "100"},
            {"name": "BAD_USDT", "quanto_multiplier": "0"}
        ]));

        assert_eq!(sizes.get("SPCX_USDT"), Some(&0.01));
        assert_eq!(sizes.get("ESPORTS_USDT"), Some(&100.0));
        assert!(!sizes.contains_key("BAD_USDT"));
    }

    #[test]
    fn gateio_perpetual_precision_should_use_contract_multiplier_for_spcx() {
        let exchange = GatewayExchangeId::new("gateio").expect("gateio exchange");
        let canonical = CanonicalSymbol::new("SPCX", "USDT").expect("canonical");
        let rule = SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: SymbolScope {
                exchange,
                market_type: GatewayMarketType::Perpetual,
                canonical_symbol: Some(canonical),
                exchange_symbol: ExchangeSymbol::new(
                    GatewayExchangeId::new("gateio").expect("gateio exchange"),
                    GatewayMarketType::Perpetual,
                    "SPCX_USDT",
                )
                .expect("symbol"),
            },
            base_asset: "SPCX".to_string(),
            quote_asset: "USDT".to_string(),
            price_increment: Some("0.01".to_string()),
            quantity_increment: Some("1".to_string()),
            min_price: None,
            max_price: None,
            min_quantity: Some("1".to_string()),
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision: Some(2),
            quantity_precision: Some(0),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: true,
            updated_at: Utc::now(),
        };
        let sizes = BTreeMap::from([("SPCX_USDT".to_string(), 0.01)]);

        let (_, _, precision) =
            strategy_precision_from_rule(&rule, &sizes).expect("strategy precision");

        assert_eq!(precision.quantity_unit, QuantityUnit::Contracts);
        assert_eq!(precision.contract_size, 0.01);
        assert_eq!(precision.normalized_order_quantity_from_base(0.03324), 3.0);
        assert!((precision.normalized_base_quantity(0.03324) - 0.03).abs() < 1e-12);
    }

    #[test]
    fn gateio_contract_precision_should_keep_small_notional_spcx_opportunity() {
        let symbol = StrategyCanonicalSymbol::new("SPCX", "USDT");
        let gate = StrategyExchangeId::new("gateio");
        let binance = StrategyExchangeId::new("binance");
        let now = Utc::now();
        let mut registry = PrecisionRegistry::default();
        registry.insert(
            gate.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 1.0,
                min_quantity: 1.0,
                min_notional_usdt: 0.0,
                quantity_unit: QuantityUnit::Contracts,
                contract_size: 0.01,
            },
        );
        registry.insert(
            binance.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.01,
                min_quantity: 0.01,
                min_notional_usdt: 5.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );
        let config = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            max_open_spread_pct: 0.05,
            expected_close_spread_pct: 0.001,
            top_of_book_capacity_ratio: 0.8,
            orderbook_stale_ms: 10_000,
            ..DualTakerArbitrageConfig::default()
        };
        let opportunities = evaluate_dual_taker_open_opportunities(
            &[
                OrderBookTop {
                    exchange: gate,
                    canonical_symbol: symbol.clone(),
                    best_bid_price: 165.41,
                    best_bid_quantity: 18.0,
                    best_ask_price: 165.44,
                    best_ask_quantity: 19.0,
                    levels: 5,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
                OrderBookTop {
                    exchange: binance,
                    canonical_symbol: symbol,
                    best_bid_price: 173.17,
                    best_bid_quantity: 0.61,
                    best_ask_price: 173.19,
                    best_ask_quantity: 1.02,
                    levels: 5,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
            ],
            &registry,
            &FeeModel::default(),
            &config,
            now,
        );

        assert_eq!(opportunities.len(), 1);
        let gate_order = opportunities[0]
            .orders
            .iter()
            .find(|order| order.exchange.as_str() == "gateio")
            .expect("gate order");
        assert_eq!(gate_order.quantity_unit, QuantityUnit::Contracts);
        assert_eq!(gate_order.quantity, 3.0);
        assert!((gate_order.base_quantity - 0.03).abs() < 1e-12);
    }

    #[test]
    fn disabled_exchange_symbol_config_should_match_canonical_opportunity_orders() {
        let disabled = disabled_exchange_symbols_from_config(
            &json!({
                "disabled": {
                    "exchange_symbols": [
                        {
                            "exchange": "binance",
                            "market_type": "perpetual",
                            "symbol": "SPCX_USDT",
                            "reason": "binance_tradfi_perps_agreement_required"
                        }
                    ]
                }
            }),
            &GatewayMarketType::Perpetual,
        );
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "spcx-binance-gate".to_string(),
            canonical_symbol: StrategyCanonicalSymbol::new("SPCX", "USDT"),
            long_exchange: StrategyExchangeId::new("gateio"),
            short_exchange: StrategyExchangeId::new("binance"),
            long_entry_price: 165.0,
            short_entry_price: 166.0,
            spread_pct: 0.006,
            quantity: 0.03,
            long_notional_usdt: 4.95,
            short_notional_usdt: 4.98,
            executable_top_depth_usdt: 10.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.005,
            estimated_round_trip_fee_usdt: 0.01,
            expected_close_spread_pct: 0.001,
            expected_gross_pnl_usdt: 0.03,
            expected_net_pnl_usdt: 0.02,
            expected_net_profit_pct: 0.004,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("gateio"),
                    canonical_symbol: StrategyCanonicalSymbol::new("SPCX", "USDT"),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 0.03,
                    quantity: 3.0,
                    quantity_unit: QuantityUnit::Contracts,
                    contract_size: 0.01,
                    reference_price: 165.0,
                    worst_acceptable_price: 165.1,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: StrategyCanonicalSymbol::new("SPCX", "USDT"),
                    side: StrategyOrderSide::Sell,
                    base_quantity: 0.03,
                    quantity: 0.03,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 166.0,
                    worst_acceptable_price: 165.9,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };

        let disabled_match =
            opportunity_disabled_exchange_symbol(&opportunity, &disabled).expect("disabled route");

        assert_eq!(disabled_match.0, "binance");
        assert_eq!(disabled_match.1, "SPCX/USDT");
        assert_eq!(disabled_match.2, "binance_tradfi_perps_agreement_required");
    }

    #[test]
    fn disabled_open_exchange_config_should_match_opportunity_orders() {
        let disabled = disabled_open_exchanges_from_config(&json!({
            "execution": {
                "open_disabled_exchanges": [
                    {
                        "exchange": "gate",
                        "reason": "gateio_ioc_short_cancelled_during_live_test"
                    }
                ]
            }
        }));
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "ctr-binance-gate".to_string(),
            canonical_symbol: StrategyCanonicalSymbol::new("CTR", "USDT"),
            long_exchange: StrategyExchangeId::new("binance"),
            short_exchange: StrategyExchangeId::new("gateio"),
            long_entry_price: 0.01681,
            short_entry_price: 0.01895,
            spread_pct: 0.049,
            quantity: 300.0,
            long_notional_usdt: 5.043,
            short_notional_usdt: 5.685,
            executable_top_depth_usdt: 10.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.005,
            estimated_round_trip_fee_usdt: 0.01,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.24,
            expected_net_pnl_usdt: 0.23,
            expected_net_profit_pct: 0.04,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: StrategyCanonicalSymbol::new("CTR", "USDT"),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 300.0,
                    quantity: 300.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.01681,
                    worst_acceptable_price: 0.01682,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("gateio"),
                    canonical_symbol: StrategyCanonicalSymbol::new("CTR", "USDT"),
                    side: StrategyOrderSide::Sell,
                    base_quantity: 300.0,
                    quantity: 3.0,
                    quantity_unit: QuantityUnit::Contracts,
                    contract_size: 100.0,
                    reference_price: 0.01895,
                    worst_acceptable_price: 0.01894,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };

        let disabled_match =
            opportunity_disabled_open_exchange(&opportunity, &disabled).expect("disabled route");

        assert_eq!(disabled_match.0, "gateio");
        assert_eq!(
            disabled_match.1,
            "gateio_ioc_short_cancelled_during_live_test"
        );
    }

    #[test]
    fn retain_best_opportunity_per_symbol_should_keep_single_best_route() {
        fn opportunity(
            id: &str,
            long_exchange: &str,
            short_exchange: &str,
            expected_net_profit_pct: f64,
            spread_pct: f64,
        ) -> DualTakerOpenOpportunity {
            let symbol = StrategyCanonicalSymbol::new("EDGE", "USDT");
            DualTakerOpenOpportunity {
                opportunity_id: id.to_string(),
                canonical_symbol: symbol.clone(),
                long_exchange: StrategyExchangeId::new(long_exchange),
                short_exchange: StrategyExchangeId::new(short_exchange),
                long_entry_price: 100.0,
                short_entry_price: 100.0 * (1.0 + spread_pct),
                spread_pct,
                quantity: 0.055,
                long_notional_usdt: 5.5,
                short_notional_usdt: 5.5 * (1.0 + spread_pct),
                executable_top_depth_usdt: 20.0,
                top_of_book_capacity_ratio: 0.8,
                estimated_open_fee_usdt: 0.005,
                estimated_round_trip_fee_usdt: 0.01,
                expected_close_spread_pct: 0.001,
                expected_gross_pnl_usdt: 0.04,
                expected_net_pnl_usdt: expected_net_profit_pct * 5.5,
                expected_net_profit_pct,
                submit_parallel: true,
                orders: Vec::new(),
            }
        }

        let mut opportunities = vec![
            opportunity("edge-binance-gate", "binance", "gateio", 0.004, 0.006),
            opportunity("edge-binance-bitget", "binance", "bitget", 0.006, 0.005),
            {
                let mut other = opportunity("other-gate-bitget", "gateio", "bitget", 0.005, 0.007);
                other.canonical_symbol = StrategyCanonicalSymbol::new("OTHER", "USDT");
                other
            },
        ];

        retain_best_opportunity_per_symbol(&mut opportunities);

        assert_eq!(opportunities.len(), 2);
        assert_eq!(opportunities[0].canonical_symbol.as_pair(), "EDGE/USDT");
        assert_eq!(opportunities[0].opportunity_id, "edge-binance-bitget");
        assert_eq!(opportunities[1].canonical_symbol.as_pair(), "OTHER/USDT");
    }

    #[test]
    fn display_opportunity_rows_should_include_all_configured_symbols() {
        let now = Utc::now();
        let binance = StrategyExchangeId::new("binance");
        let bitget = StrategyExchangeId::new("bitget");
        let edge = StrategyCanonicalSymbol::new("EDGE", "USDT");
        let flat = StrategyCanonicalSymbol::new("FLAT", "USDT");
        let mut config = CrossExchangeArbitrageConfig::default();
        config.venues = vec!["binance".to_string(), "bitget".to_string()];
        config.symbols = vec!["EDGE/USDT".to_string(), "FLAT/USDT".to_string()];
        config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            max_open_spread_pct: 0.05,
            expected_close_spread_pct: 0.001,
            orderbook_stale_ms: 10_000,
            ..DualTakerArbitrageConfig::default()
        };
        let mut precision = PrecisionRegistry::default();
        for symbol in [&edge, &flat] {
            precision.insert(
                binance.clone(),
                symbol.clone(),
                SymbolPrecision {
                    price_tick: 0.0001,
                    quantity_step: 0.001,
                    min_quantity: 0.001,
                    min_notional_usdt: 5.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                },
            );
            precision.insert(
                bitget.clone(),
                symbol.clone(),
                SymbolPrecision {
                    price_tick: 0.0001,
                    quantity_step: 0.001,
                    min_quantity: 0.001,
                    min_notional_usdt: 5.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                },
            );
        }
        let tops = vec![
            OrderBookTop {
                exchange: binance.clone(),
                canonical_symbol: edge.clone(),
                best_bid_price: 99.9,
                best_bid_quantity: 1.0,
                best_ask_price: 100.0,
                best_ask_quantity: 1.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            OrderBookTop {
                exchange: bitget.clone(),
                canonical_symbol: edge.clone(),
                best_bid_price: 101.0,
                best_bid_quantity: 1.0,
                best_ask_price: 101.1,
                best_ask_quantity: 1.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            OrderBookTop {
                exchange: binance.clone(),
                canonical_symbol: flat.clone(),
                best_bid_price: 99.9,
                best_bid_quantity: 1.0,
                best_ask_price: 100.0,
                best_ask_quantity: 1.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            OrderBookTop {
                exchange: bitget,
                canonical_symbol: flat,
                best_bid_price: 100.1,
                best_bid_quantity: 1.0,
                best_ask_price: 100.2,
                best_ask_quantity: 1.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
        ];
        let mut executable = evaluate_dual_taker_open_opportunities(
            &tops,
            &precision,
            &FeeModel::default(),
            &config.dual_taker,
            now,
        );
        retain_best_opportunity_per_symbol(&mut executable);

        let rows = display_opportunity_rows(
            &config,
            &FeeModel::default(),
            &precision,
            &tops,
            &executable,
            &BTreeMap::new(),
            &BTreeMap::new(),
            now,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["symbol"], "EDGE/USDT");
        assert!(rows[0]["expected_net_profit_pct"].as_f64().unwrap() >= 0.004);
        assert_eq!(rows[1]["symbol"], "FLAT/USDT");
        assert_eq!(rows[1]["can_open"], json!(false));
        assert!(rows[1]["raw_open_spread_pct"].as_f64().unwrap() > 0.0);
        assert!(rows[1]["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("below min open raw spread"));
    }

    #[test]
    fn display_row_should_keep_raw_spread_when_open_exchange_is_disabled() {
        let now = Utc::now();
        let gate = StrategyExchangeId::new("gateio");
        let bitget = StrategyExchangeId::new("bitget");
        let symbol = StrategyCanonicalSymbol::new("SPCX", "USDT");
        let mut config = CrossExchangeArbitrageConfig::default();
        config.venues = vec!["gate".to_string(), "bitget".to_string()];
        config.symbols = vec!["SPCX/USDT".to_string()];
        config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            max_open_spread_pct: 0.05,
            expected_close_spread_pct: 0.002,
            orderbook_stale_ms: 10_000,
            ..DualTakerArbitrageConfig::default()
        };
        let mut precision = PrecisionRegistry::default();
        precision.insert(
            gate.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 1.0,
                min_quantity: 1.0,
                min_notional_usdt: 0.0,
                quantity_unit: QuantityUnit::Contracts,
                contract_size: 0.01,
            },
        );
        precision.insert(
            bitget.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.01,
                min_quantity: 0.01,
                min_notional_usdt: 5.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );
        let tops = vec![
            OrderBookTop {
                exchange: gate,
                canonical_symbol: symbol.clone(),
                best_bid_price: 163.2,
                best_bid_quantity: 4.0,
                best_ask_price: 163.25,
                best_ask_quantity: 4.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            OrderBookTop {
                exchange: bitget,
                canonical_symbol: symbol,
                best_bid_price: 170.78,
                best_bid_quantity: 0.1,
                best_ask_price: 170.89,
                best_ask_quantity: 0.1,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
        ];
        let disabled_open = BTreeMap::from([(
            "gateio".to_string(),
            "gateio_ioc_short_cancelled_during_live_test".to_string(),
        )]);

        let rows = display_opportunity_rows(
            &config,
            &FeeModel::default(),
            &precision,
            &tops,
            &[],
            &BTreeMap::new(),
            &disabled_open,
            now,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["symbol"], "SPCX/USDT");
        assert!(rows[0]["raw_open_spread_pct"].as_f64().unwrap() > 0.04);
        assert!(rows[0]["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("new entries disabled on gateio"));
    }

    #[test]
    fn display_row_should_keep_raw_spread_when_book_is_stale() {
        let now = Utc::now();
        let old = now - ChronoDuration::seconds(12);
        let gate = StrategyExchangeId::new("gateio");
        let bitget = StrategyExchangeId::new("bitget");
        let symbol = StrategyCanonicalSymbol::new("MOVE", "USDT");
        let mut config = CrossExchangeArbitrageConfig::default();
        config.venues = vec!["gate".to_string(), "bitget".to_string()];
        config.symbols = vec!["MOVE/USDT".to_string()];
        config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            max_open_spread_pct: 0.05,
            expected_close_spread_pct: 0.002,
            orderbook_stale_ms: 6_000,
            ..DualTakerArbitrageConfig::default()
        };
        let precision = PrecisionRegistry::default();
        let tops = vec![
            OrderBookTop {
                exchange: gate,
                canonical_symbol: symbol.clone(),
                best_bid_price: 0.01482,
                best_bid_quantity: 10_000.0,
                best_ask_price: 0.01484,
                best_ask_quantity: 10_000.0,
                levels: 1,
                exchange_timestamp: Some(old),
                received_at: old,
                latency_ms: Some(1),
            },
            OrderBookTop {
                exchange: bitget,
                canonical_symbol: symbol,
                best_bid_price: 0.01498,
                best_bid_quantity: 10_000.0,
                best_ask_price: 0.015,
                best_ask_quantity: 10_000.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
        ];

        let rows = display_opportunity_rows(
            &config,
            &FeeModel::default(),
            &precision,
            &tops,
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
            now,
        );

        assert_eq!(rows.len(), 1);
        assert!(rows[0]["raw_open_spread_pct"].as_f64().unwrap() > 0.004);
        let reason = rows[0]["reject_reasons"].as_str().expect("reject reason");
        assert!(reason.contains("live top-of-book is stale"));
        assert_eq!(rows[0]["can_open"], json!(false));
    }

    #[test]
    fn hydrate_should_not_mark_display_only_rows_openable() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let mut dashboard = LiveDashboardData {
            opportunities: vec![json!({
                "symbol": "EDGE/USDT",
                "canonical_symbol": "EDGE/USDT",
                "long_exchange": "gateio",
                "short_exchange": "binance",
                "raw_open_spread_pct": 5.4,
                "expected_net_profit_pct": 0.8,
                "orders": [],
                "can_open": false,
                "reject_reasons": "not evaluated"
            })],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &LiveExecutionState::default(),
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let row = &dashboard.opportunities[0];
        assert_eq!(row["can_open"], json!(false));
        assert!(row["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("display-only row"));
    }

    #[test]
    fn hydrate_should_preserve_display_reject_reasons() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let mut dashboard = LiveDashboardData {
            opportunities: vec![json!({
                "symbol": "MOVE/USDT",
                "canonical_symbol": "MOVE/USDT",
                "long_exchange": "gateio",
                "short_exchange": "binance",
                "raw_open_spread_pct": 0.008,
                "expected_net_profit_pct": 0.0045,
                "orders": [],
                "can_open": false,
                "reject_reasons": "new entries disabled on gateio: gateio_ioc_short_cancelled_during_live_test"
            })],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &LiveExecutionState::default(),
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let reason = dashboard.opportunities[0]["reject_reasons"]
            .as_str()
            .expect("reject reason");
        assert!(reason.contains("new entries disabled on gateio"));
        assert!(reason.contains("display-only row has no executable order drafts"));
        assert_eq!(reason.matches("new entries disabled on gateio").count(), 1);
    }

    #[test]
    fn hydrate_should_merge_observed_private_events_with_live_events() {
        let mut state = LiveExecutionState::default();
        state.recent_events.push(json!({
            "event_type": "live_order_reconciled",
            "source": "live_runner"
        }));
        let mut dashboard = LiveDashboardData {
            private_events: vec![json!({
                "private_kind": "fill",
                "source": "private_user_websocket",
                "exchange": "gateio"
            })],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &state,
            &CrossExchangeArbitrageConfig::default(),
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        assert_eq!(dashboard.private_events.len(), 2);
        assert!(dashboard
            .private_events
            .iter()
            .any(|event| event.get("source").and_then(Value::as_str)
                == Some("private_user_websocket")));
        assert!(dashboard
            .private_events
            .iter()
            .any(|event| event.get("source").and_then(Value::as_str) == Some("live_runner")));
    }

    #[test]
    fn hydrate_should_combine_symbol_active_and_row_reject_reasons() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let mut state = LiveExecutionState::default();
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        state.open_bundles.insert(
            "active-bundle".to_string(),
            LiveOpenBundle {
                bundle_id: "active-bundle".to_string(),
                position: OpenArbitragePosition {
                    bundle_id: "active-bundle".to_string(),
                    canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                    long_exchange: StrategyExchangeId::new("binance"),
                    short_exchange: StrategyExchangeId::new("bitget"),
                    quantity: 76.0,
                    long_entry_price: 0.07213,
                    short_entry_price: 0.07295,
                    opened_at,
                },
                open_long: filled_leg("binance", "open_long", "buy", "long", 0.07213, 76.0),
                open_short: filled_leg("bitget", "open_short", "sell", "short", 0.07295, 76.0),
                opened_at,
                open_fee_usdt: 0.00606746,
            },
        );
        let mut dashboard = LiveDashboardData {
            opportunities: vec![json!({
                "symbol": "ESPORTS/USDT",
                "canonical_symbol": "ESPORTS/USDT",
                "long_exchange": "gateio",
                "short_exchange": "binance",
                "raw_open_spread_pct": -0.001,
                "expected_net_profit_pct": -0.002,
                "orders": [],
                "can_open": false,
                "reject_reasons": "not evaluated"
            })],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &state,
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let reason = dashboard.opportunities[0]["reject_reasons"]
            .as_str()
            .expect("reject reason");
        assert!(reason.contains("same symbol already has an active open bundle"));
        assert!(reason.contains("display-only row"));
        assert!(reason.contains("below min open raw spread"));
        assert!(reason.contains("below min open net edge"));
    }

    #[test]
    fn hydrate_should_allow_different_symbol_when_another_bundle_is_open() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let mut state = LiveExecutionState::default();
        state.open_bundles.insert(
            "esports-active".to_string(),
            LiveOpenBundle {
                bundle_id: "esports-active".to_string(),
                position: OpenArbitragePosition {
                    bundle_id: "esports-active".to_string(),
                    canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                    long_exchange: StrategyExchangeId::new("binance"),
                    short_exchange: StrategyExchangeId::new("bitget"),
                    quantity: 76.0,
                    long_entry_price: 0.07213,
                    short_entry_price: 0.07295,
                    opened_at,
                },
                open_long: filled_leg("binance", "open_long", "buy", "long", 0.07213, 76.0),
                open_short: filled_leg("bitget", "open_short", "sell", "short", 0.07295, 76.0),
                opened_at,
                open_fee_usdt: 0.00606746,
            },
        );
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "move-edge".to_string(),
            canonical_symbol: StrategyCanonicalSymbol::new("MOVE", "USDT"),
            long_exchange: StrategyExchangeId::new("gateio"),
            short_exchange: StrategyExchangeId::new("binance"),
            long_entry_price: 0.10,
            short_entry_price: 0.101,
            spread_pct: 0.01,
            quantity: 55.0,
            long_notional_usdt: 5.5,
            short_notional_usdt: 5.555,
            executable_top_depth_usdt: 20.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.0055,
            estimated_round_trip_fee_usdt: 0.011,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.05,
            expected_net_pnl_usdt: 0.026,
            expected_net_profit_pct: 0.0047,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("gateio"),
                    canonical_symbol: StrategyCanonicalSymbol::new("MOVE", "USDT"),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 55.0,
                    quantity: 55.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.10,
                    worst_acceptable_price: 0.1001,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: StrategyCanonicalSymbol::new("MOVE", "USDT"),
                    side: StrategyOrderSide::Sell,
                    base_quantity: 55.0,
                    quantity: 55.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.101,
                    worst_acceptable_price: 0.1009,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };
        let mut dashboard = LiveDashboardData {
            opportunities: vec![opportunity_row(opportunity)],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &state,
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let row = &dashboard.opportunities[0];
        assert_eq!(row["symbol"], "MOVE/USDT");
        assert_eq!(row["can_open"], json!(true));
        assert_eq!(row["reject_reasons"], json!("-"));
    }

    #[test]
    fn hydrate_should_split_market_openable_from_runtime_blocks() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let opportunity = open_opportunity_for_test("market-open-runtime-blocked", "MOVE", 0.006);
        let mut dashboard = LiveDashboardData {
            controls: LiveExecutionControls {
                start_close_only: true,
                ..LiveExecutionControls::default()
            },
            opportunities: vec![opportunity_row(opportunity)],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &LiveExecutionState::default(),
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let row = &dashboard.opportunities[0];
        assert_eq!(row["market_can_open"], json!(true));
        assert_eq!(row["market_reject_reasons"], json!("-"));
        assert_eq!(row["can_open"], json!(false));
        assert!(row["execution_reject_reasons"]
            .as_str()
            .expect("execution reject reason")
            .contains("close-only control is enabled"));
        assert_eq!(row["reject_reasons"], row["execution_reject_reasons"]);
    }

    #[test]
    fn hydrate_should_block_open_when_execution_quality_edge_is_too_thin() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.max_open_spread_pct = 0.05;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let opportunity = open_opportunity_for_test("thin-edge", "MOVE", 0.006);
        let mut dashboard = LiveDashboardData {
            opportunities: vec![opportunity_row(opportunity)],
            ..LiveDashboardData::default()
        };
        let quality_controls = LiveExecutionQualityControls {
            min_open_net_edge_pct: 0.008,
            min_open_raw_spread_pct: 0.004,
            min_open_executable_depth_ratio: 1.0,
            min_close_net_profit_pct: 0.01,
        };

        hydrate_dashboard_execution_state(
            &LiveExecutionState::default(),
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            quality_controls,
            &mut dashboard,
        );

        let row = &dashboard.opportunities[0];
        assert_eq!(row["can_open"], json!(false));
        assert!(row["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("below execution quality min"));
    }

    #[test]
    fn profit_summary_should_count_partial_close_emergency_pnl() {
        let summary = profit_summary_from_rows(
            &[json!({
                "lifecycle": "emergency_close_after_partial_close",
                "actual_pnl_usdt": 0.0123,
            })],
            5,
        );

        assert_eq!(summary["closed_arbitrages"], json!(1));
        assert_eq!(summary["winning_arbitrages"], json!(1));
        assert_eq!(summary["realized_profit_usdt"], json!(0.0123));
    }

    #[test]
    fn profit_summary_should_not_count_unknown_pnl_as_win() {
        let summary = profit_summary_from_rows(
            &[
                json!({
                    "lifecycle": "close",
                    "actual_pnl_usdt": null,
                    "failure_reason": "close_long not filled",
                }),
                json!({
                    "lifecycle": "emergency_close",
                    "actual_pnl_usdt": -0.01,
                }),
            ],
            5,
        );

        assert_eq!(summary["closed_arbitrages"], json!(1));
        assert_eq!(summary["unknown_arbitrages"], json!(1));
        assert_eq!(summary["winning_arbitrages"], json!(0));
        assert_eq!(summary["losing_arbitrages"], json!(1));
        assert_eq!(summary["win_rate_pct"], json!(0.0));
        assert_eq!(summary["consecutive_losing_trades"], json!(1));
    }

    #[test]
    fn hydrate_should_keep_raw_spread_visible_when_same_symbol_bundle_blocks_entry() {
        let symbol = StrategyCanonicalSymbol::new("ESPORTS", "USDT");
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "esports-edge".to_string(),
            canonical_symbol: symbol.clone(),
            long_exchange: StrategyExchangeId::new("binance"),
            short_exchange: StrategyExchangeId::new("bitget"),
            long_entry_price: 0.07213,
            short_entry_price: 0.07295,
            spread_pct: 0.011368362678497175,
            quantity: 76.0,
            long_notional_usdt: 5.48188,
            short_notional_usdt: 5.5442,
            executable_top_depth_usdt: 20.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.00606746,
            estimated_round_trip_fee_usdt: 0.01213492,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.051,
            expected_net_pnl_usdt: 0.038,
            expected_net_profit_pct: 0.0068,
            submit_parallel: true,
            orders: Vec::new(),
        };
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let mut state = LiveExecutionState::default();
        state.open_bundles.insert(
            "active-bundle".to_string(),
            LiveOpenBundle {
                bundle_id: "active-bundle".to_string(),
                position: OpenArbitragePosition {
                    bundle_id: "active-bundle".to_string(),
                    canonical_symbol: symbol,
                    long_exchange: StrategyExchangeId::new("binance"),
                    short_exchange: StrategyExchangeId::new("bitget"),
                    quantity: 76.0,
                    long_entry_price: 0.07213,
                    short_entry_price: 0.07295,
                    opened_at,
                },
                open_long: filled_leg("binance", "open_long", "buy", "long", 0.07213, 76.0),
                open_short: filled_leg("bitget", "open_short", "sell", "short", 0.07295, 76.0),
                opened_at,
                open_fee_usdt: 0.00606746,
            },
        );
        let mut dashboard = LiveDashboardData {
            opportunities: vec![opportunity_row(opportunity)],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &state,
            &CrossExchangeArbitrageConfig::default(),
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        assert_eq!(dashboard.opportunities.len(), 1);
        let row = &dashboard.opportunities[0];
        assert_eq!(row["raw_open_spread_pct"], json!(0.011368362678497175));
        assert_eq!(row["spread_pct"], row["raw_open_spread_pct"]);
        assert_eq!(row["can_open"], json!(false));
        assert!(row["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("same symbol already has an active open bundle"));
    }

    #[test]
    fn hydrate_should_mark_negative_net_edge_not_openable_while_showing_raw_spread() {
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "raw-only".to_string(),
            canonical_symbol: StrategyCanonicalSymbol::new("IDOL", "USDT"),
            long_exchange: StrategyExchangeId::new("bitget"),
            short_exchange: StrategyExchangeId::new("gateio"),
            long_entry_price: 0.12,
            short_entry_price: 0.1205,
            spread_pct: 0.004166666666666649,
            quantity: 45.0,
            long_notional_usdt: 5.4,
            short_notional_usdt: 5.4225,
            executable_top_depth_usdt: 20.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.006,
            estimated_round_trip_fee_usdt: 0.012,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.004,
            expected_net_pnl_usdt: -0.0002,
            expected_net_profit_pct: -0.000037,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("bitget"),
                    canonical_symbol: StrategyCanonicalSymbol::new("IDOL", "USDT"),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 45.0,
                    quantity: 45.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.12,
                    worst_acceptable_price: 0.1201,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("gateio"),
                    canonical_symbol: StrategyCanonicalSymbol::new("IDOL", "USDT"),
                    side: StrategyOrderSide::Sell,
                    base_quantity: 45.0,
                    quantity: 45.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.1205,
                    worst_acceptable_price: 0.1204,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };
        let mut config = CrossExchangeArbitrageConfig::default();
        config.dual_taker.min_open_spread_pct = 0.004;
        config.dual_taker.min_open_net_profit_pct = 0.004;
        let mut dashboard = LiveDashboardData {
            opportunities: vec![opportunity_row(opportunity)],
            ..LiveDashboardData::default()
        };

        hydrate_dashboard_execution_state(
            &LiveExecutionState::default(),
            &config,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            LiveExecutionQualityControls::default(),
            &mut dashboard,
        );

        let row = &dashboard.opportunities[0];
        assert_eq!(row["raw_open_spread_pct"], json!(0.004166666666666649));
        assert_eq!(row["can_open"], json!(false));
        assert!(row["reject_reasons"]
            .as_str()
            .expect("reject reason")
            .contains("below min open net edge"));
    }

    #[test]
    fn disabled_exchange_symbol_config_should_ignore_other_market_types() {
        let disabled = disabled_exchange_symbols_from_config(
            &json!({
                "disabled": {
                    "exchange_symbols": [
                        {
                            "exchange": "binance",
                            "market_type": "spot",
                            "symbol": "SPCXUSDT"
                        }
                    ]
                }
            }),
            &GatewayMarketType::Perpetual,
        );

        assert!(disabled.is_empty());
    }

    #[test]
    fn client_order_id_should_fit_strict_exchange_limits() {
        let requested_at = Utc::now();
        let client_order_id = safe_client_order_id(
            "cross_arb_live",
            "server-hedge-test-current-live",
            "server-hedge-test-current-live-SPCX-USDT-gateio-binance-1780946968817",
            "emergency_close",
            "emergency_close_short",
            0,
            requested_at,
        );
        let retried_client_order_id = safe_client_order_id(
            "cross_arb_live",
            "server-hedge-test-current-live",
            "server-hedge-test-current-live-SPCX-USDT-gateio-binance-1780946968817",
            "emergency_close",
            "emergency_close_short",
            0,
            requested_at + chrono::Duration::milliseconds(1),
        );

        assert!(
            client_order_id.len() <= 28,
            "client_order_id={client_order_id}"
        );
        assert!(client_order_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'));
        assert!(client_order_id.starts_with("ca-ecs-"));
        assert_ne!(client_order_id, retried_client_order_id);
    }

    #[test]
    fn emergency_close_draft_should_round_price_to_symbol_tick() {
        let original = TakerOrderDraft {
            exchange: StrategyExchangeId::new("binance"),
            canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
            side: StrategyOrderSide::Buy,
            base_quantity: 81.0,
            quantity: 81.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
            reference_price: 0.06727,
            worst_acceptable_price: 0.06731,
            reduce_only: false,
            role: TakerOrderRole::OpenLong,
        };
        let filled_leg = ReconciledOrderLeg {
            exchange: "binance".to_string(),
            symbol: "ESPORTS/USDT".to_string(),
            role: "open_long".to_string(),
            side: "buy".to_string(),
            position_side: "long".to_string(),
            client_order_id: Some("open-a".to_string()),
            exchange_order_id: Some("100".to_string()),
            accepted: true,
            status: "filled".to_string(),
            planned_price: 0.06727,
            planned_base_quantity: 81.0,
            planned_order_quantity: 81.0,
            actual_fill_price: Some(0.06727),
            actual_base_quantity: Some(81.0),
            actual_order_quantity: Some(81.0),
            actual_notional_usdt: Some(5.44887),
            fee_usdt: 0.00272443,
            submitted_at: None,
            acked_at: None,
            filled_at: Some(Utc::now()),
            error: None,
        };
        let precision = SymbolPrecision {
            price_tick: 0.00001,
            quantity_step: 1.0,
            min_quantity: 1.0,
            min_notional_usdt: 5.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
        };

        let close =
            emergency_close_draft(&filled_leg, &original, 0.0005, precision).expect("close draft");

        assert_eq!(close.role, TakerOrderRole::EmergencyCloseLong);
        assert_eq!(close.side, StrategyOrderSide::Sell);
        assert_eq!(close.quantity, 81.0);
        assert!((close.worst_acceptable_price - 0.06723).abs() < 1e-12);
    }

    #[test]
    fn close_profit_event_should_persist_real_prices_and_pnl() {
        let symbol = StrategyCanonicalSymbol::new("TEST", "USDT");
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let bundle = LiveOpenBundle {
            bundle_id: "bundle-close-fields".to_string(),
            position: OpenArbitragePosition {
                bundle_id: "bundle-close-fields".to_string(),
                canonical_symbol: symbol,
                long_exchange: StrategyExchangeId::new("binance"),
                short_exchange: StrategyExchangeId::new("bitget"),
                quantity: 0.05,
                long_entry_price: 100.0,
                short_entry_price: 101.0,
                opened_at,
            },
            open_long: filled_leg("binance", "open_long", "buy", "long", 100.0, 0.05),
            open_short: filled_leg("bitget", "open_short", "sell", "short", 101.0, 0.05),
            opened_at,
            open_fee_usdt: 0.005025,
        };
        let execution = PairExecution {
            requested_at: Utc::now(),
            first: filled_leg("binance", "close_long", "sell", "long", 100.8, 0.05),
            second: filled_leg("bitget", "close_short", "buy", "short", 100.2, 0.05),
        };

        let event = close_profit_event(&bundle, &execution, 0.08, Utc::now());

        assert_eq!(event["lifecycle"], "close");
        assert_eq!(event["long_entry_price"], json!(100.0));
        assert_eq!(event["short_entry_price"], json!(101.0));
        assert_eq!(event["long_close_price"], json!(100.8));
        assert_eq!(event["short_close_price"], json!(100.2));
        assert_eq!(event["quantity"], json!(0.05));
        assert!(event["actual_pnl_usdt"].as_f64().unwrap() > 0.0);
        assert_eq!(event["realized_profit_usdt"], event["actual_pnl_usdt"]);
        assert!(event["close_net_profit_pct"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn partial_close_should_emergency_close_remaining_long_leg() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let symbol = StrategyCanonicalSymbol::new("TEST", "USDT");
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let bundle = LiveOpenBundle {
            bundle_id: "bundle-partial-close".to_string(),
            position: OpenArbitragePosition {
                bundle_id: "bundle-partial-close".to_string(),
                canonical_symbol: symbol.clone(),
                long_exchange: StrategyExchangeId::new("binance"),
                short_exchange: StrategyExchangeId::new("bitget"),
                quantity: 0.05,
                long_entry_price: 100.0,
                short_entry_price: 101.0,
                opened_at,
            },
            open_long: filled_leg("binance", "open_long", "buy", "long", 100.0, 0.05),
            open_short: filled_leg("bitget", "open_short", "sell", "short", 101.0, 0.05),
            opened_at,
            open_fee_usdt: 0.005025,
        };
        let close_execution = PairExecution {
            requested_at: Utc::now(),
            first: ReconciledOrderLeg {
                exchange: "binance".to_string(),
                symbol: "TEST/USDT".to_string(),
                role: "close_long".to_string(),
                side: "sell".to_string(),
                position_side: "long".to_string(),
                client_order_id: Some("close-long-cid".to_string()),
                exchange_order_id: Some("close-long-eid".to_string()),
                accepted: true,
                status: "expired".to_string(),
                planned_price: 100.8,
                planned_base_quantity: 0.05,
                planned_order_quantity: 0.05,
                actual_fill_price: None,
                actual_base_quantity: None,
                actual_order_quantity: None,
                actual_notional_usdt: None,
                fee_usdt: 0.0,
                submitted_at: None,
                acked_at: None,
                filled_at: None,
                error: None,
            },
            second: filled_leg("bitget", "close_short", "buy", "short", 100.2, 0.05),
        };
        let mut precision_registry = PrecisionRegistry::default();
        precision_registry.insert(
            StrategyExchangeId::new("binance"),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.001,
                min_quantity: 0.001,
                min_notional_usdt: 5.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );

        let events = emergency_close_remaining_after_partial_close(
            &EmergencyCloseFilledGateway {
                side: GatewayOrderSide::Sell,
                position_side: GatewayPositionSide::Long,
                quantity: 0.05,
                price: 100.8,
            },
            &ctx,
            &args,
            GatewayMarketType::Perpetual,
            &CrossExchangeArbitrageConfig::default(),
            &precision_registry,
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &bundle,
            &close_execution,
        )
        .await
        .expect("emergency close after partial close");

        assert_eq!(
            submissions.lock().expect("submissions").as_slice(),
            ["emergency_close_long"]
        );
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["lifecycle"], "emergency_close_after_partial_close");
        assert_eq!(event["exchange"], "binance");
        assert_eq!(event["both_legs_filled"], json!(true));
        assert_eq!(event["close_price"], json!(100.8));
        assert!(event["actual_pnl_usdt"].as_f64().unwrap() > 0.0);
        assert!(event["normal_close_legs"].is_array());
    }

    #[test]
    fn open_bundle_row_should_include_live_close_evaluation() {
        let symbol = StrategyCanonicalSymbol::new("TEST", "USDT");
        let binance = StrategyExchangeId::new("binance");
        let bitget = StrategyExchangeId::new("bitget");
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let bundle = LiveOpenBundle {
            bundle_id: "bundle-close-eval".to_string(),
            position: OpenArbitragePosition {
                bundle_id: "bundle-close-eval".to_string(),
                canonical_symbol: symbol.clone(),
                long_exchange: binance.clone(),
                short_exchange: bitget.clone(),
                quantity: 0.05,
                long_entry_price: 100.0,
                short_entry_price: 101.0,
                opened_at,
            },
            open_long: filled_leg("binance", "open_long", "buy", "long", 100.0, 0.05),
            open_short: filled_leg("bitget", "open_short", "sell", "short", 101.0, 0.05),
            opened_at,
            open_fee_usdt: 0.005025,
        };
        let config = CrossExchangeArbitrageConfig {
            dual_taker: DualTakerArbitrageConfig {
                close_min_net_profit_pct: 0.002,
                close_on_max_hold_requires_profit: true,
                orderbook_stale_ms: 10_000,
                ..DualTakerArbitrageConfig::default()
            },
            ..CrossExchangeArbitrageConfig::default()
        };
        let now = Utc::now();
        let mut precision = PrecisionRegistry::default();
        precision.insert(
            binance.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.001,
                min_quantity: 0.001,
                min_notional_usdt: 5.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );
        precision.insert(
            bitget.clone(),
            symbol.clone(),
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.001,
                min_quantity: 0.001,
                min_notional_usdt: 5.0,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
            },
        );
        let close = evaluate_dual_taker_close(
            &bundle.position,
            &OrderBookTop {
                exchange: binance,
                canonical_symbol: symbol.clone(),
                best_bid_price: 100.8,
                best_bid_quantity: 10.0,
                best_ask_price: 100.9,
                best_ask_quantity: 10.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            &OrderBookTop {
                exchange: bitget,
                canonical_symbol: symbol,
                best_bid_price: 100.1,
                best_bid_quantity: 10.0,
                best_ask_price: 100.2,
                best_ask_quantity: 10.0,
                levels: 1,
                exchange_timestamp: Some(now),
                received_at: now,
                latency_ms: Some(1),
            },
            &precision,
            &FeeModel::default(),
            &config.dual_taker,
            now,
        );

        let row = open_bundle_row(
            &bundle,
            close.as_ref(),
            &config,
            LiveExecutionQualityControls::default(),
            now,
        );

        assert_eq!(row["status"], "open");
        assert_eq!(row["long_close_price"], json!(100.8));
        assert_eq!(row["short_close_price"], json!(100.2));
        assert_eq!(row["closeable"], json!(true));
        assert_eq!(row["held_secs"], json!(60));
        assert_eq!(row["max_hold_secs"], json!(86_400));
        assert_eq!(row["max_hold_expired"], json!(false));
        assert_eq!(row["close_on_max_hold_requires_profit"], json!(true));
        assert_eq!(row["max_hold_close_blocked_by_profit"], json!(false));
        assert!(row["close_net_pnl_usdt"].as_f64().unwrap() > 0.0);
        assert!(row["close_net_profit_pct"].as_f64().unwrap() >= 0.002);
    }

    #[test]
    fn legacy_dashboard_snapshot_should_mirror_cross_arb_dashboard_at_root() {
        let args = LiveRunnerArgs {
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let report = LiveRunnerReport {
            generated_at: Utc::now(),
            strategy_kind: STRATEGY_KIND,
            strategy_id: "cross_arb_live".to_string(),
            run_id: "test-run".to_string(),
            config_path: "config/test.yml".to_string(),
            lock_file: "logs/test.lock".to_string(),
            live_orders_enabled: true,
            concrete_exchange_adapter_loaded: true,
            gateway_owned_credentials: true,
            credential_source_boundary: "gateway_app",
            market_data_provider_connected: true,
            startup_position_takeover_enabled: false,
            analysis_only_reason: None,
            capability_gate: CapabilityGateReport {
                passed: true,
                target_market_type: "Perpetual".to_string(),
                required_exchanges: vec!["binance".to_string(), "bitget".to_string()],
                loaded_adapters: vec!["binance".to_string(), "bitget".to_string()],
                degraded_requirements: Vec::new(),
                missing_requirements: Vec::new(),
            },
            snapshot: None,
        };
        let mut dashboard_data = LiveDashboardData::default();
        dashboard_data.opportunities.push(json!({
            "symbol": "TEST/USDT",
            "can_open": true
        }));
        dashboard_data.position_bundles.push(json!({
            "bundle_id": "bundle-a",
            "symbol": "TEST/USDT"
        }));

        let snapshot = legacy_dashboard_snapshot(
            &args,
            &report,
            &CrossExchangeArbitrageConfig::default(),
            &dashboard_data,
        )
        .expect("dashboard snapshot");

        assert_eq!(
            snapshot["summary"],
            snapshot["cross_arb_dashboard"]["summary"]
        );
        assert_eq!(
            snapshot["settings"],
            snapshot["cross_arb_dashboard"]["settings"]
        );
        assert_eq!(
            snapshot["opportunities"],
            snapshot["cross_arb_dashboard"]["opportunities"]
        );
        assert_eq!(
            snapshot["position_bundles"],
            snapshot["cross_arb_dashboard"]["position_bundles"]
        );
        assert_eq!(snapshot["summary"]["opportunity_count"], json!(1));
        assert_eq!(snapshot["summary"]["can_open_opportunities"], json!(1));
    }

    #[test]
    fn ws_snapshot_readiness_should_require_public_and_private_websockets() {
        let dashboard = ws_dashboard_fixture(Utc::now());
        validate_ws_snapshot_readiness(
            &dashboard,
            &[
                "binance".to_string(),
                "bitget".to_string(),
                "gateio".to_string(),
            ],
        )
        .expect("ready snapshot");

        let mut missing_private = dashboard.clone();
        missing_private["summary"]["private_stream_mode"] = json!("required_before_live_trading");
        let err = validate_ws_snapshot_readiness(&missing_private, &["binance".to_string()])
            .expect_err("private ws mode is required");
        assert!(err.to_string().contains("private user websocket observe"));

        let mut offline_private = dashboard.clone();
        offline_private["exchange_status"][0]["private_stream_status"] = json!("starting");
        let err = validate_ws_snapshot_readiness(&offline_private, &["binance".to_string()])
            .expect_err("private stream status is required");
        assert!(err
            .to_string()
            .contains("private user websocket is not online"));
    }

    #[test]
    fn ws_snapshot_row_should_build_strategy_top_of_book() {
        let now = Utc::now();
        let row = json!({
            "exchange": "gate",
            "canonical_symbol": "LTC/USDT",
            "best_bid_price": 100.0,
            "best_bid_quantity": 2.0,
            "best_ask_price": 100.1,
            "best_ask_quantity": 3.0,
            "levels": 1,
            "received_at": now,
            "exchange_timestamp": now,
            "latency_ms": 12,
            "source": "public_websocket_orderbook",
        });
        let top = order_book_top_from_ws_snapshot_row(&row).expect("top");
        assert_eq!(top.exchange.as_str(), "gateio");
        assert_eq!(top.canonical_symbol.as_pair(), "LTC/USDT");
        assert_eq!(top.best_bid_price, 100.0);
        assert_eq!(top.best_ask_price, 100.1);
    }

    fn ws_dashboard_fixture(now: DateTime<Utc>) -> Value {
        json!({
            "data_source": "cross-exchange-arbitrage-ws-observe",
            "execution_mode": "public_ws_observe_only",
            "summary": {
                "private_stream_mode": "private_user_ws_observe"
            },
            "exchange_status": [
                {"exchange": "binance", "market_data_status": "online", "private_stream_status": "online"},
                {"exchange": "bitget", "market_data_status": "online", "private_stream_status": "online"},
                {"exchange": "gate", "market_data_status": "online", "private_stream_status": "online"}
            ],
            "market_snapshots": [
                {
                    "exchange": "binance",
                    "canonical_symbol": "LTC/USDT",
                    "best_bid_price": 100.0,
                    "best_bid_quantity": 2.0,
                    "best_ask_price": 100.1,
                    "best_ask_quantity": 3.0,
                    "levels": 1,
                    "received_at": now,
                    "exchange_timestamp": now,
                    "latency_ms": 10,
                    "source": "public_websocket_orderbook"
                }
            ],
            "route_health": [],
            "private_events": []
        })
    }

    #[tokio::test]
    async fn reconcile_order_leg_should_combine_query_order_and_recent_fills() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            ..LiveRunnerArgs::default()
        };
        let draft = TakerOrderDraft {
            exchange: StrategyExchangeId::new("binance"),
            canonical_symbol: StrategyCanonicalSymbol::new("BTC", "USDT"),
            side: StrategyOrderSide::Buy,
            base_quantity: 0.001,
            quantity: 0.001,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
            reference_price: 100.0,
            worst_acceptable_price: 100.2,
            reduce_only: false,
            role: TakerOrderRole::OpenLong,
        };
        let ack = Ok(ExecutionOrderAck {
            schema_version: 1,
            accepted: true,
            client_order_id: "cid-1".to_string(),
            execution_order_id: Some("eid-1".to_string()),
            reason: None,
            received_at: Utc::now(),
        });

        let leg = reconcile_order_leg(
            &FilledGateway,
            &args,
            GatewayMarketType::Perpetual,
            &draft,
            ack,
            Utc::now(),
            &test_confirmation_policy(),
        )
        .await
        .expect("reconcile leg");

        assert!(leg.filled());
        assert_eq!(leg.client_order_id.as_deref(), Some("cid-1"));
        assert_eq!(leg.exchange_order_id.as_deref(), Some("eid-1"));
        assert_eq!(leg.status, "filled");
        assert_eq!(leg.actual_order_quantity, Some(0.001));
        assert_eq!(leg.actual_base_quantity, Some(0.001));
        assert_eq!(leg.actual_fill_price, Some(100.1));
        assert_eq!(leg.actual_notional_usdt, Some(0.1001));
        assert_eq!(leg.fee_usdt, 0.00005);
    }

    #[tokio::test]
    async fn loss_guard_should_block_new_entries_after_closing_existing_bundle() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-cross-arb-loss-guard-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).expect("temp dir");
        let profit_path = temp_dir.join("profit_history.jsonl");
        for index in 0..5 {
            append_profit_event(
                Some(&profit_path),
                &json!({
                    "lifecycle": "close",
                    "bundle_id": format!("loss-{index}"),
                    "actual_pnl_usdt": -0.01,
                    "recorded_at": Utc::now(),
                }),
            )
            .expect("seed loss");
        }

        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: Some(profit_path.clone()),
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.max_consecutive_losses = 5;
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            close_min_net_profit_pct: 0.0005,
            max_hold_secs: 0,
            ..DualTakerArbitrageConfig::default()
        };

        let mut precision_registry = PrecisionRegistry::default();
        let symbol = StrategyCanonicalSymbol::new("TEST", "USDT");
        let binance = StrategyExchangeId::new("binance");
        let bitget = StrategyExchangeId::new("bitget");
        let precision = SymbolPrecision {
            price_tick: 0.0001,
            quantity_step: 0.001,
            min_quantity: 0.001,
            min_notional_usdt: 5.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
        };
        precision_registry.insert(binance.clone(), symbol.clone(), precision);
        precision_registry.insert(bitget.clone(), symbol.clone(), precision);

        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let open_long = filled_leg("binance", "open_long", "buy", "long", 100.0, 0.055);
        let open_short = filled_leg("bitget", "open_short", "sell", "short", 101.0, 0.055);
        let bundle_id = "bundle-existing".to_string();
        let mut state = LiveExecutionState::default();
        state.open_bundles.insert(
            bundle_id.clone(),
            LiveOpenBundle {
                bundle_id: bundle_id.clone(),
                position: OpenArbitragePosition {
                    bundle_id: bundle_id.clone(),
                    canonical_symbol: symbol.clone(),
                    long_exchange: binance.clone(),
                    short_exchange: bitget.clone(),
                    quantity: 0.055,
                    long_entry_price: 100.0,
                    short_entry_price: 101.0,
                    opened_at,
                },
                open_long,
                open_short,
                opened_at,
                open_fee_usdt: 0.0055,
            },
        );

        let now = Utc::now();
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            market_snapshots: Vec::new(),
            opportunities: Vec::new(),
            route_health: Vec::new(),
            private_events: Vec::new(),
            position_bundles: Vec::new(),
            open_orders: Vec::new(),
            tops: vec![
                OrderBookTop {
                    exchange: binance,
                    canonical_symbol: symbol.clone(),
                    best_bid_price: 100.2,
                    best_bid_quantity: 10.0,
                    best_ask_price: 100.3,
                    best_ask_quantity: 10.0,
                    levels: 1,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
                OrderBookTop {
                    exchange: bitget,
                    canonical_symbol: symbol,
                    best_bid_price: 100.7,
                    best_bid_quantity: 10.0,
                    best_ask_price: 100.8,
                    best_ask_quantity: 10.0,
                    levels: 1,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
            ],
            typed_opportunities: vec![DualTakerOpenOpportunity {
                opportunity_id: "would-open".to_string(),
                canonical_symbol: StrategyCanonicalSymbol::new("NEXT", "USDT"),
                long_exchange: StrategyExchangeId::new("binance"),
                short_exchange: StrategyExchangeId::new("bitget"),
                long_entry_price: 10.0,
                short_entry_price: 10.1,
                spread_pct: 0.01,
                quantity: 0.55,
                long_notional_usdt: 5.5,
                short_notional_usdt: 5.555,
                executable_top_depth_usdt: 100.0,
                top_of_book_capacity_ratio: 0.8,
                estimated_open_fee_usdt: 0.0055,
                estimated_round_trip_fee_usdt: 0.011,
                expected_close_spread_pct: 0.001,
                expected_gross_pnl_usdt: 0.055,
                expected_net_pnl_usdt: 0.044,
                expected_net_profit_pct: 0.008,
                submit_parallel: true,
                orders: vec![
                    TakerOrderDraft {
                        exchange: StrategyExchangeId::new("binance"),
                        canonical_symbol: StrategyCanonicalSymbol::new("NEXT", "USDT"),
                        side: StrategyOrderSide::Buy,
                        base_quantity: 0.55,
                        quantity: 0.55,
                        quantity_unit: QuantityUnit::Base,
                        contract_size: 1.0,
                        reference_price: 10.0,
                        worst_acceptable_price: 10.01,
                        reduce_only: false,
                        role: TakerOrderRole::OpenLong,
                    },
                    TakerOrderDraft {
                        exchange: StrategyExchangeId::new("bitget"),
                        canonical_symbol: StrategyCanonicalSymbol::new("NEXT", "USDT"),
                        side: StrategyOrderSide::Sell,
                        base_quantity: 0.55,
                        quantity: 0.55,
                        quantity_unit: QuantityUnit::Base,
                        contract_size: 1.0,
                        reference_price: 10.1,
                        worst_acceptable_price: 10.09,
                        reduce_only: false,
                        role: TakerOrderRole::OpenShort,
                    },
                ],
            }],
            controls: LiveExecutionControls::default(),
            quality_controls: LiveExecutionQualityControls::default(),
            runtime_new_entries_block_reason: None,
            market_data_row_source: "test",
        };

        run_live_execution_cycle(
            &FilledGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &precision_registry,
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(state.open_bundles.is_empty());
        assert!(profit_history_loss_guard_triggered(&args, 5).expect("loss guard"));
        let submitted_roles = submissions.lock().expect("submissions").clone();
        assert_eq!(
            submitted_roles,
            vec!["close_long".to_string(), "close_short".to_string()]
        );
        assert!(
            dashboard
                .private_events
                .iter()
                .any(|event| event.get("event_type").and_then(Value::as_str)
                    == Some("risk_auto_stop"))
        );
        assert!(!dashboard.position_bundles.iter().any(|row| {
            row.get("bundle_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id == bundle_id)
        }));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn close_execution_quality_should_block_thin_profitable_close() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            close_min_net_profit_pct: 0.001,
            ..DualTakerArbitrageConfig::default()
        };

        let symbol = StrategyCanonicalSymbol::new("TEST", "USDT");
        let binance = StrategyExchangeId::new("binance");
        let bitget = StrategyExchangeId::new("bitget");
        let precision = SymbolPrecision {
            price_tick: 0.0001,
            quantity_step: 0.001,
            min_quantity: 0.001,
            min_notional_usdt: 5.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
        };
        let mut precision_registry = PrecisionRegistry::default();
        precision_registry.insert(binance.clone(), symbol.clone(), precision);
        precision_registry.insert(bitget.clone(), symbol.clone(), precision);

        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let bundle_id = "thin-close".to_string();
        let mut state = LiveExecutionState::default();
        state.open_bundles.insert(
            bundle_id.clone(),
            LiveOpenBundle {
                bundle_id: bundle_id.clone(),
                position: OpenArbitragePosition {
                    bundle_id: bundle_id.clone(),
                    canonical_symbol: symbol.clone(),
                    long_exchange: binance.clone(),
                    short_exchange: bitget.clone(),
                    quantity: 0.055,
                    long_entry_price: 100.0,
                    short_entry_price: 101.0,
                    opened_at,
                },
                open_long: filled_leg("binance", "open_long", "buy", "long", 100.0, 0.055),
                open_short: filled_leg("bitget", "open_short", "sell", "short", 101.0, 0.055),
                opened_at,
                open_fee_usdt: 0.0055,
            },
        );

        let now = Utc::now();
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            tops: vec![
                OrderBookTop {
                    exchange: binance,
                    canonical_symbol: symbol.clone(),
                    best_bid_price: 100.2,
                    best_bid_quantity: 10.0,
                    best_ask_price: 100.3,
                    best_ask_quantity: 10.0,
                    levels: 1,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
                OrderBookTop {
                    exchange: bitget,
                    canonical_symbol: symbol,
                    best_bid_price: 100.7,
                    best_bid_quantity: 10.0,
                    best_ask_price: 100.8,
                    best_ask_quantity: 10.0,
                    levels: 1,
                    exchange_timestamp: Some(now),
                    received_at: now,
                    latency_ms: Some(1),
                },
            ],
            ..LiveDashboardData::default()
        };
        let quality_controls = LiveExecutionQualityControls {
            min_open_net_edge_pct: 0.004,
            min_open_raw_spread_pct: 0.004,
            min_open_executable_depth_ratio: 1.0,
            min_close_net_profit_pct: 0.01,
        };

        run_live_execution_cycle(
            &FilledGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &precision_registry,
            true,
            false,
            Some("close-only control is enabled"),
            quality_controls,
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(submissions.lock().expect("submissions").is_empty());
        assert!(state.open_bundles.contains_key(&bundle_id));
        assert_eq!(dashboard.position_bundles[0]["close_ready"], json!(false));
        assert!(dashboard.position_bundles[0]["close_block_reason"]
            .as_str()
            .expect("close block reason")
            .contains("below execution quality min"));
    }

    #[tokio::test]
    async fn unmanaged_external_position_should_block_new_open_after_restart() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string()];
        strategy_config.symbols = vec!["ESPORTS/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            ..DualTakerArbitrageConfig::default()
        };

        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![DualTakerOpenOpportunity {
                opportunity_id: "would-open".to_string(),
                canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                long_exchange: StrategyExchangeId::new("binance"),
                short_exchange: StrategyExchangeId::new("bitget"),
                long_entry_price: 0.075,
                short_entry_price: 0.076,
                spread_pct: 0.013,
                quantity: 73.0,
                long_notional_usdt: 5.475,
                short_notional_usdt: 5.548,
                executable_top_depth_usdt: 20.0,
                top_of_book_capacity_ratio: 0.8,
                estimated_open_fee_usdt: 0.0055,
                estimated_round_trip_fee_usdt: 0.011,
                expected_close_spread_pct: 0.002,
                expected_gross_pnl_usdt: 0.06,
                expected_net_pnl_usdt: 0.049,
                expected_net_profit_pct: 0.008,
                submit_parallel: true,
                orders: vec![
                    TakerOrderDraft {
                        exchange: StrategyExchangeId::new("binance"),
                        canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                        side: StrategyOrderSide::Buy,
                        base_quantity: 73.0,
                        quantity: 73.0,
                        quantity_unit: QuantityUnit::Base,
                        contract_size: 1.0,
                        reference_price: 0.075,
                        worst_acceptable_price: 0.07504,
                        reduce_only: false,
                        role: TakerOrderRole::OpenLong,
                    },
                    TakerOrderDraft {
                        exchange: StrategyExchangeId::new("bitget"),
                        canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                        side: StrategyOrderSide::Sell,
                        base_quantity: 73.0,
                        quantity: 73.0,
                        quantity_unit: QuantityUnit::Base,
                        contract_size: 1.0,
                        reference_price: 0.076,
                        worst_acceptable_price: 0.07596,
                        reduce_only: false,
                        role: TakerOrderRole::OpenShort,
                    },
                ],
            }],
            ..LiveDashboardData::default()
        };
        let mut state = LiveExecutionState::default();

        run_live_execution_cycle(
            &UnmanagedPositionGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(state.manual_intervention_required);
        assert!(state.open_bundles.is_empty());
        assert!(dashboard.typed_opportunities.is_empty());
        assert!(submissions.lock().expect("submissions").is_empty());
        assert!(dashboard.private_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str) == Some("manual_intervention_required")
        }));
    }

    #[tokio::test]
    async fn partial_open_should_continue_after_successful_emergency_close_and_empty_reconcile() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["SAHARA/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            symbol_cooldown_secs: 300,
            ..DualTakerArbitrageConfig::default()
        };

        let symbol = StrategyCanonicalSymbol::new("SAHARA", "USDT");
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "sahara-binance-bitget".to_string(),
            canonical_symbol: symbol.clone(),
            long_exchange: StrategyExchangeId::new("binance"),
            short_exchange: StrategyExchangeId::new("bitget"),
            long_entry_price: 0.01483,
            short_entry_price: 0.01547,
            spread_pct: 0.01045,
            quantity: 359.0,
            long_notional_usdt: 5.32397,
            short_notional_usdt: 5.55373,
            executable_top_depth_usdt: 20.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.006,
            estimated_round_trip_fee_usdt: 0.012,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.04,
            expected_net_pnl_usdt: 0.03,
            expected_net_profit_pct: 0.006,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: symbol.clone(),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 359.0,
                    quantity: 359.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.01483,
                    worst_acceptable_price: 0.01484,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("bitget"),
                    canonical_symbol: symbol,
                    side: StrategyOrderSide::Sell,
                    base_quantity: 359.0,
                    quantity: 359.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.01547,
                    worst_acceptable_price: 0.01546,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![opportunity],
            ..LiveDashboardData::default()
        };
        let mut precision_registry = PrecisionRegistry::default();
        for exchange in ["binance", "bitget"] {
            precision_registry.insert(
                StrategyExchangeId::new(exchange),
                StrategyCanonicalSymbol::new("SAHARA", "USDT"),
                SymbolPrecision {
                    price_tick: 0.00001,
                    quantity_step: 1.0,
                    min_quantity: 1.0,
                    min_notional_usdt: 5.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                },
            );
        }
        let mut state = LiveExecutionState::default();

        run_live_execution_cycle(
            &PartialOpenThenRecoveredGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &precision_registry,
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(!state.manual_intervention_required);
        assert!(state.open_bundles.is_empty());
        assert!(state.recent_open_orders.is_empty());
        assert!(state.symbol_cooldowns.contains_key("SAHARA/USDT"));
        assert_eq!(
            submissions.lock().expect("submissions").as_slice(),
            ["open_long", "open_short", "emergency_close_long"]
        );
        assert!(state.recent_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str)
                == Some("single_leg_emergency_close_recovered")
        }));
        assert!(dashboard.private_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str)
                == Some("single_leg_emergency_close_recovered")
        }));
    }

    #[tokio::test]
    async fn partial_open_should_use_market_fallback_when_emergency_ioc_is_unfilled() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["SAHARA/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            symbol_cooldown_secs: 300,
            ..DualTakerArbitrageConfig::default()
        };

        let symbol = StrategyCanonicalSymbol::new("SAHARA", "USDT");
        let opportunity = DualTakerOpenOpportunity {
            opportunity_id: "sahara-bitget-binance".to_string(),
            canonical_symbol: symbol.clone(),
            long_exchange: StrategyExchangeId::new("bitget"),
            short_exchange: StrategyExchangeId::new("binance"),
            long_entry_price: 0.01415,
            short_entry_price: 0.01452,
            spread_pct: 0.0139,
            quantity: 384.0,
            long_notional_usdt: 5.4336,
            short_notional_usdt: 5.57568,
            executable_top_depth_usdt: 20.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.006,
            estimated_round_trip_fee_usdt: 0.012,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.06,
            expected_net_pnl_usdt: 0.05,
            expected_net_profit_pct: 0.006,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("bitget"),
                    canonical_symbol: symbol.clone(),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 384.0,
                    quantity: 384.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.01415,
                    worst_acceptable_price: 0.01416,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: symbol,
                    side: StrategyOrderSide::Sell,
                    base_quantity: 384.0,
                    quantity: 384.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 0.01452,
                    worst_acceptable_price: 0.01451,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        };
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![opportunity],
            ..LiveDashboardData::default()
        };
        let mut precision_registry = PrecisionRegistry::default();
        for exchange in ["binance", "bitget"] {
            precision_registry.insert(
                StrategyExchangeId::new(exchange),
                StrategyCanonicalSymbol::new("SAHARA", "USDT"),
                SymbolPrecision {
                    price_tick: 0.00001,
                    quantity_step: 1.0,
                    min_quantity: 1.0,
                    min_notional_usdt: 5.0,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                },
            );
        }
        let mut state = LiveExecutionState::default();

        let gateway = PartialOpenEmergencyFallbackGateway::default();
        run_live_execution_cycle(
            &gateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &precision_registry,
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(!state.manual_intervention_required);
        assert!(state.open_bundles.is_empty());
        assert!(state.recent_open_orders.is_empty());
        assert!(state.symbol_cooldowns.contains_key("SAHARA/USDT"));
        assert_eq!(
            submissions.lock().expect("submissions").as_slice(),
            [
                "open_long",
                "open_short",
                "emergency_close_long",
                "emergency_close_long"
            ]
        );
        let emergency_event = state
            .recent_events
            .iter()
            .find(|event| event.get("lifecycle").and_then(Value::as_str) == Some("emergency_close"))
            .expect("emergency close event");
        assert_eq!(emergency_event["both_legs_filled"], json!(true));
        assert!(emergency_event["emergency_close_leg"]["actual_fill_price"].is_null());
        assert_eq!(
            emergency_event["emergency_close_fallback_leg"]["actual_fill_price"],
            json!("0.01462")
        );
        assert!(emergency_event["actual_pnl_usdt"].as_f64().unwrap() > 0.0);
        assert!(state.route_cooldowns.contains_key("bitget->binance"));
    }

    #[tokio::test]
    async fn failed_open_route_cooldown_should_block_same_route_for_other_symbol() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["SAHARA/USDT".to_string(), "NEXT/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            symbol_cooldown_secs: 300,
            max_open_bundles: 10,
            max_positions_per_exchange: 10,
            ..DualTakerArbitrageConfig::default()
        };

        let mut first = open_opportunity_for_test("sahara-bitget-binance", "SAHARA", 0.006);
        first.long_exchange = StrategyExchangeId::new("bitget");
        first.short_exchange = StrategyExchangeId::new("binance");
        first.orders[0].exchange = StrategyExchangeId::new("bitget");
        first.orders[1].exchange = StrategyExchangeId::new("binance");
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![first],
            ..LiveDashboardData::default()
        };
        let mut state = LiveExecutionState::default();

        run_live_execution_cycle(
            &PartialOpenEmergencyFallbackGateway::default(),
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("first execution cycle");

        assert!(state.route_cooldowns.contains_key("bitget->binance"));
        let submitted_after_failure = submissions.lock().expect("submissions").len();

        let mut second = open_opportunity_for_test("next-bitget-binance", "NEXT", 0.007);
        second.long_exchange = StrategyExchangeId::new("bitget");
        second.short_exchange = StrategyExchangeId::new("binance");
        second.orders[0].exchange = StrategyExchangeId::new("bitget");
        second.orders[1].exchange = StrategyExchangeId::new("binance");
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![second],
            opportunities: vec![json!({
                "canonical_symbol": "NEXT/USDT",
                "symbol": "NEXT/USDT",
                "long_exchange": "bitget",
                "short_exchange": "binance",
                "raw_open_spread_pct": 0.01,
                "expected_net_profit_pct": 0.007,
                "orders": [{}, {}],
                "can_open": true
            })],
            ..LiveDashboardData::default()
        };

        run_live_execution_cycle(
            &FilledGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("second execution cycle");

        assert_eq!(
            submissions.lock().expect("submissions").len(),
            submitted_after_failure
        );
        assert_eq!(dashboard.opportunities[0]["can_open"], json!(false));
        assert!(dashboard.opportunities[0]["reject_reasons"]
            .as_str()
            .expect("reject reasons")
            .contains("open route bitget->binance is cooling down"));
    }

    #[test]
    fn route_cooldown_restore_should_read_recent_failed_profit_rows() {
        let profit_path = std::env::temp_dir().join(format!(
            "rustcta-route-cooldown-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let now = Utc::now();
        append_profit_event(
            Some(&profit_path),
            &json!({
                "lifecycle": "open",
                "both_legs_filled": false,
                "bundle_id": "server-live-MOVE-USDT-gateio-binance-test",
                "canonical_symbol": "MOVE/USDT",
                "long_exchange": "gateio",
                "short_exchange": "binance",
                "recorded_at": now - ChronoDuration::seconds(60),
                "legs": []
            }),
        )
        .expect("append recent row");
        append_profit_event(
            Some(&profit_path),
            &json!({
                "lifecycle": "open",
                "both_legs_filled": false,
                "bundle_id": "old-row",
                "canonical_symbol": "OLD/USDT",
                "long_exchange": "binance",
                "short_exchange": "bitget",
                "recorded_at": now - ChronoDuration::seconds(FAILED_OPEN_ROUTE_COOLDOWN_SECS + 1),
                "legs": []
            }),
        )
        .expect("append old row");
        let args = LiveRunnerArgs {
            profit_history_path: Some(profit_path.clone()),
            ..LiveRunnerArgs::default()
        };
        let mut state = LiveExecutionState::default();

        restore_route_cooldowns_from_profit_history(&args, &mut state, now).expect("restore");

        assert!(state.route_cooldowns.contains_key("gateio->binance"));
        assert!(!state.route_cooldowns.contains_key("binance->bitget"));
        assert!(state.recent_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str) == Some("open_route_cooldown_restored")
        }));
        let _ = std::fs::remove_file(profit_path);
    }

    #[tokio::test]
    async fn accepted_unfilled_open_should_continue_when_position_and_open_orders_are_empty() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["SAHARA/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            symbol_cooldown_secs: 300,
            ..DualTakerArbitrageConfig::default()
        };
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![open_opportunity_for_test("sahara", "SAHARA", 0.006)],
            ..LiveDashboardData::default()
        };
        let mut state = LiveExecutionState::default();

        run_live_execution_cycle(
            &AcceptedUnfilledThenRecoveredGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(!state.manual_intervention_required);
        assert!(state.open_bundles.is_empty());
        assert!(state.recent_open_orders.is_empty());
        assert!(state.symbol_cooldowns.contains_key("SAHARA/USDT"));
        assert_eq!(
            submissions.lock().expect("submissions").as_slice(),
            ["open_long", "open_short"]
        );
        assert!(state.recent_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str)
                == Some("uncertain_open_attempt_reconciled_empty")
        }));
    }

    #[tokio::test]
    async fn close_only_control_should_block_new_open_submission() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["SAHARA/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            max_open_bundles: 10,
            max_positions_per_exchange: 10,
            ..DualTakerArbitrageConfig::default()
        };
        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![open_opportunity_for_test("sahara", "SAHARA", 0.006)],
            ..LiveDashboardData::default()
        };
        let mut state = LiveExecutionState::default();

        run_live_execution_cycle(
            &FilledGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            false,
            Some("close-only control is enabled"),
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert!(state.open_bundles.is_empty());
        assert!(submissions.lock().expect("submissions").is_empty());
        assert!(dashboard.private_events.iter().any(|event| {
            event.get("event_type").and_then(Value::as_str)
                == Some("new_entries_blocked_by_control")
        }));
    }

    #[tokio::test]
    async fn live_execution_should_open_different_symbol_while_existing_bundle_is_active() {
        let args = LiveRunnerArgs {
            tenant_id: "tenant-a".to_string(),
            account_id: "account-a".to_string(),
            run_id: "run-a".to_string(),
            profit_history_path: None,
            ..LiveRunnerArgs::default()
        };
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let ctx = strategy_context(
            &args,
            json!({}),
            Arc::new(AckExecutionClient {
                submissions: submissions.clone(),
            }),
        );
        let mut strategy_config = CrossExchangeArbitrageConfig::default();
        strategy_config.dry_run = false;
        strategy_config.venues = vec!["binance".to_string(), "bitget".to_string()];
        strategy_config.symbols = vec!["ESPORTS/USDT".to_string(), "NEXT/USDT".to_string()];
        strategy_config.dual_taker = DualTakerArbitrageConfig {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.004,
            min_open_net_profit_pct: 0.004,
            max_open_bundles: 10,
            max_active_bundles_per_symbol: 1,
            max_positions_per_exchange: 10,
            ..DualTakerArbitrageConfig::default()
        };
        let opened_at = Utc::now() - ChronoDuration::seconds(60);
        let mut state = LiveExecutionState::default();
        state.open_bundles.insert(
            "esports-active".to_string(),
            LiveOpenBundle {
                bundle_id: "esports-active".to_string(),
                position: OpenArbitragePosition {
                    bundle_id: "esports-active".to_string(),
                    canonical_symbol: StrategyCanonicalSymbol::new("ESPORTS", "USDT"),
                    long_exchange: StrategyExchangeId::new("binance"),
                    short_exchange: StrategyExchangeId::new("bitget"),
                    quantity: 76.0,
                    long_entry_price: 0.07213,
                    short_entry_price: 0.07295,
                    opened_at,
                },
                open_long: filled_leg("binance", "open_long", "buy", "long", 0.07213, 76.0),
                open_short: filled_leg("bitget", "open_short", "sell", "short", 0.07295, 76.0),
                opened_at,
                open_fee_usdt: 0.00606746,
            },
        );

        let mut dashboard = LiveDashboardData {
            market_data_provider_connected: true,
            typed_opportunities: vec![
                open_opportunity_for_test("same-symbol", "ESPORTS", 0.009),
                open_opportunity_for_test("next-symbol", "NEXT", 0.008),
            ],
            ..LiveDashboardData::default()
        };

        run_live_execution_cycle(
            &FilledGateway,
            &ctx,
            &args,
            &strategy_config,
            GatewayMarketType::Perpetual,
            &FeeModel::default(),
            &PrecisionRegistry::default(),
            true,
            true,
            None,
            LiveExecutionQualityControls::default(),
            &test_confirmation_policy(),
            &LiveRuntimeSinks::disabled(),
            &mut state,
            &mut dashboard,
        )
        .await
        .expect("execution cycle");

        assert_eq!(state.open_bundles.len(), 2);
        assert!(state
            .open_bundles
            .values()
            .any(|bundle| { bundle.position.canonical_symbol.as_pair() == "ESPORTS/USDT" }));
        assert!(state
            .open_bundles
            .values()
            .any(|bundle| { bundle.position.canonical_symbol.as_pair() == "NEXT/USDT" }));
        let submitted_roles = submissions.lock().expect("submissions").clone();
        assert_eq!(
            submitted_roles,
            vec!["open_long".to_string(), "open_short".to_string()]
        );
    }

    #[test]
    fn recover_open_bundle_from_dashboard_row_should_preserve_live_fill_prices() {
        let row = json!({
            "bundle_id": "server-live-ESPORTS-USDT-binance-bitget-1780961578536",
            "canonical_symbol": "ESPORTS/USDT",
            "symbol": "ESPORTS/USDT",
            "long_exchange": "binance",
            "short_exchange": "bitget",
            "status": "open",
            "quantity": 76.0,
            "long_entry_price": 0.07213,
            "short_entry_price": 0.07295,
            "opened_at": "2026-06-08T23:32:58.536205023Z",
            "open_fee_usdt": 0.00606746,
            "open_legs": [
                {
                    "accepted": true,
                    "actual_base_quantity": "76",
                    "actual_fill_price": "0.07213",
                    "actual_notional_usdt": "5.48188",
                    "actual_order_quantity": "76",
                    "client_order_id": "ca-ol-961578536-e06b0b6292f2",
                    "error": null,
                    "exchange": "binance",
                    "exchange_order_id": "1054034084",
                    "fee_usdt": "0.00274094",
                    "filled_at": "2026-06-08T23:32:58.569Z",
                    "planned_base_quantity": "76",
                    "planned_execution_price": "0.07215",
                    "planned_order_quantity": "76",
                    "position_side": "long",
                    "role": "open_long",
                    "side": "buy",
                    "status": "filled",
                    "symbol": "ESPORTS/USDT"
                },
                {
                    "accepted": true,
                    "actual_base_quantity": "76",
                    "actual_fill_price": "0.07295",
                    "actual_notional_usdt": "5.5442",
                    "actual_order_quantity": "76",
                    "client_order_id": "ca-os-961578536-7e0ed5f590e8",
                    "error": null,
                    "exchange": "bitget",
                    "exchange_order_id": "1448023300863836161",
                    "fee_usdt": "0.00332652",
                    "filled_at": "2026-06-08T23:32:58.633Z",
                    "planned_base_quantity": "76",
                    "planned_execution_price": "0.07295",
                    "planned_order_quantity": "76",
                    "position_side": "short",
                    "role": "open_short",
                    "side": "sell",
                    "status": "unknown",
                    "symbol": "ESPORTS/USDT"
                }
            ]
        });

        let bundle = recover_open_bundle_from_row(&row).expect("recover bundle");
        assert_eq!(
            bundle.bundle_id,
            "server-live-ESPORTS-USDT-binance-bitget-1780961578536"
        );
        assert_eq!(bundle.position.canonical_symbol.as_pair(), "ESPORTS/USDT");
        assert_eq!(bundle.position.long_exchange.as_str(), "binance");
        assert_eq!(bundle.position.short_exchange.as_str(), "bitget");
        assert_eq!(bundle.position.quantity, 76.0);
        assert_eq!(bundle.position.long_entry_price, 0.07213);
        assert_eq!(bundle.position.short_entry_price, 0.07295);
        assert_eq!(bundle.open_fee_usdt, 0.00606746);
        assert!(bundle.open_long.filled());
        assert!(bundle.open_short.filled());
        assert_eq!(bundle.open_long.actual_fill_price, Some(0.07213));
        assert_eq!(bundle.open_short.actual_fill_price, Some(0.07295));
        assert_eq!(
            bundle.open_short.exchange_order_id.as_deref(),
            Some("1448023300863836161")
        );
    }

    #[test]
    fn recovered_bundle_position_mismatch_should_require_both_exchange_legs() {
        let symbol = StrategyCanonicalSymbol::new("ESPORTS", "USDT");
        let bundle = LiveOpenBundle {
            bundle_id: "bundle".to_string(),
            position: OpenArbitragePosition {
                bundle_id: "bundle".to_string(),
                canonical_symbol: symbol,
                long_exchange: StrategyExchangeId::new("binance"),
                short_exchange: StrategyExchangeId::new("bitget"),
                quantity: 76.0,
                long_entry_price: 0.07213,
                short_entry_price: 0.07295,
                opened_at: Utc::now(),
            },
            open_long: filled_leg("binance", "open_long", "buy", "long", 0.07213, 76.0),
            open_short: filled_leg("bitget", "open_short", "sell", "short", 0.07295, 76.0),
            opened_at: Utc::now(),
            open_fee_usdt: 0.00606746,
        };

        let complete = vec![
            ExternalPositionSnapshot {
                exchange: "binance".to_string(),
                canonical_symbol: "ESPORTS/USDT".to_string(),
                side: GatewayPositionSide::Long,
                quantity: 76.0,
            },
            ExternalPositionSnapshot {
                exchange: "bitget".to_string(),
                canonical_symbol: "ESPORTS/USDT".to_string(),
                side: GatewayPositionSide::Short,
                quantity: 76.0,
            },
        ];
        assert!(recovered_bundle_position_mismatch(&bundle, &complete).is_empty());

        let missing_short = vec![ExternalPositionSnapshot {
            exchange: "binance".to_string(),
            canonical_symbol: "ESPORTS/USDT".to_string(),
            side: GatewayPositionSide::Long,
            quantity: 76.0,
        }];
        let mismatch = recovered_bundle_position_mismatch(&bundle, &missing_short);
        assert_eq!(mismatch.len(), 1);
        assert!(mismatch[0].contains("missing bitget ESPORTS/USDT short qty=76"));
    }

    #[test]
    fn external_position_matching_should_accept_exchange_order_quantity_units() {
        let opened_at = Utc::now();
        let bundle = LiveOpenBundle {
            bundle_id: "spcx-bundle".to_string(),
            position: OpenArbitragePosition {
                bundle_id: "spcx-bundle".to_string(),
                canonical_symbol: StrategyCanonicalSymbol::new("SPCX", "USDT"),
                long_exchange: StrategyExchangeId::new("gateio"),
                short_exchange: StrategyExchangeId::new("bitget"),
                quantity: 0.03,
                long_entry_price: 163.13,
                short_entry_price: 170.78,
                opened_at,
            },
            open_long: ReconciledOrderLeg {
                actual_order_quantity: Some(3.0),
                planned_order_quantity: 3.0,
                ..filled_leg("gateio", "open_long", "buy", "long", 163.13, 0.03)
            },
            open_short: filled_leg("bitget", "open_short", "sell", "short", 170.78, 0.03),
            opened_at,
            open_fee_usdt: 0.00552099,
        };
        let positions = vec![
            ExternalPositionSnapshot {
                exchange: "gateio".to_string(),
                canonical_symbol: "SPCX/USDT".to_string(),
                side: GatewayPositionSide::Long,
                quantity: 3.0,
            },
            ExternalPositionSnapshot {
                exchange: "bitget".to_string(),
                canonical_symbol: "SPCX/USDT".to_string(),
                side: GatewayPositionSide::Short,
                quantity: 0.03,
            },
        ];
        let mut state = LiveExecutionState::default();
        state
            .open_bundles
            .insert(bundle.bundle_id.clone(), bundle.clone());

        assert!(recovered_bundle_position_mismatch(&bundle, &positions).is_empty());
        assert!(unmanaged_external_positions(&positions, &state).is_empty());
    }

    fn filled_leg(
        exchange: &str,
        role: &str,
        side: &str,
        position_side: &str,
        price: f64,
        quantity: f64,
    ) -> ReconciledOrderLeg {
        ReconciledOrderLeg {
            exchange: exchange.to_string(),
            symbol: "TEST/USDT".to_string(),
            role: role.to_string(),
            side: side.to_string(),
            position_side: position_side.to_string(),
            client_order_id: Some(format!("{role}-cid")),
            exchange_order_id: Some(format!("{role}-eid")),
            accepted: true,
            status: "filled".to_string(),
            planned_price: price,
            planned_base_quantity: quantity,
            planned_order_quantity: quantity,
            actual_fill_price: Some(price),
            actual_base_quantity: Some(quantity),
            actual_order_quantity: Some(quantity),
            actual_notional_usdt: Some(price * quantity),
            fee_usdt: price * quantity * 0.0005,
            submitted_at: None,
            acked_at: None,
            filled_at: Some(Utc::now()),
            error: None,
        }
    }

    fn open_opportunity_for_test(
        opportunity_id: &str,
        base: &str,
        expected_net_profit_pct: f64,
    ) -> DualTakerOpenOpportunity {
        let symbol = StrategyCanonicalSymbol::new(base, "USDT");
        DualTakerOpenOpportunity {
            opportunity_id: opportunity_id.to_string(),
            canonical_symbol: symbol.clone(),
            long_exchange: StrategyExchangeId::new("binance"),
            short_exchange: StrategyExchangeId::new("bitget"),
            long_entry_price: 100.0,
            short_entry_price: 101.0,
            spread_pct: 0.01,
            quantity: 0.055,
            long_notional_usdt: 5.5,
            short_notional_usdt: 5.555,
            executable_top_depth_usdt: 100.0,
            top_of_book_capacity_ratio: 0.8,
            estimated_open_fee_usdt: 0.0055,
            estimated_round_trip_fee_usdt: 0.011,
            expected_close_spread_pct: 0.002,
            expected_gross_pnl_usdt: 0.055,
            expected_net_pnl_usdt: expected_net_profit_pct * 5.5,
            expected_net_profit_pct,
            submit_parallel: true,
            orders: vec![
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("binance"),
                    canonical_symbol: symbol.clone(),
                    side: StrategyOrderSide::Buy,
                    base_quantity: 0.055,
                    quantity: 0.055,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 100.0,
                    worst_acceptable_price: 100.05,
                    reduce_only: false,
                    role: TakerOrderRole::OpenLong,
                },
                TakerOrderDraft {
                    exchange: StrategyExchangeId::new("bitget"),
                    canonical_symbol: symbol,
                    side: StrategyOrderSide::Sell,
                    base_quantity: 0.055,
                    quantity: 0.055,
                    quantity_unit: QuantityUnit::Base,
                    contract_size: 1.0,
                    reference_price: 101.0,
                    worst_acceptable_price: 100.95,
                    reduce_only: false,
                    role: TakerOrderRole::OpenShort,
                },
            ],
        }
    }

    struct AckExecutionClient {
        submissions: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl StrategyExecutionClient for AckExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            let role = command
                .metadata
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            self.submissions.lock().expect("submissions").push(role);
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: Some("mock-order".to_string()),
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
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
                reason: Some("raw intents are not supported in this test".to_string()),
                received_at: Utc::now(),
                payload: json!({}),
            })
        }
    }

    struct FilledGateway;

    struct EmergencyCloseFilledGateway {
        side: GatewayOrderSide,
        position_side: GatewayPositionSide,
        quantity: f64,
        price: f64,
    }

    struct PartialOpenThenRecoveredGateway;

    #[derive(Default)]
    struct PartialOpenEmergencyFallbackGateway {
        emergency_query_count: Mutex<u32>,
        fallback_client_order_id: Mutex<Option<String>>,
    }

    struct AcceptedUnfilledThenRecoveredGateway;

    struct UnmanagedPositionGateway;

    #[async_trait]
    impl GatewayClient for FilledGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::QueryOrder(request) => {
                    let exchange = request.symbol.exchange.clone();
                    let now = Utc::now();
                    let order = OrderState {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: exchange.clone(),
                        market_type: request.symbol.market_type,
                        canonical_symbol: request.symbol.canonical_symbol.clone(),
                        exchange_symbol: request.symbol.exchange_symbol.clone(),
                        client_order_id: request.client_order_id.clone(),
                        exchange_order_id: request.exchange_order_id.clone(),
                        side: GatewayOrderSide::Buy,
                        position_side: Some(GatewayPositionSide::Long),
                        order_type: GatewayOrderType::IOC,
                        time_in_force: Some(GatewayTimeInForce::IOC),
                        status: rustcta_types::OrderStatus::Filled,
                        quantity: "0.001".to_string(),
                        price: Some("100.2".to_string()),
                        filled_quantity: "0.001".to_string(),
                        average_fill_price: Some("100.1".to_string()),
                        reduce_only: false,
                        post_only: false,
                        created_at: Some(now),
                        updated_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::QueryOrder(
                            rustcta_exchange_api::QueryOrderResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                order: Some(order),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetRecentFills(request) => {
                    let exchange = request.exchange.clone();
                    let now = Utc::now();
                    let symbol = request.symbol.expect("symbol");
                    let canonical_symbol = symbol.canonical_symbol.expect("canonical symbol");
                    let fill = Fill {
                        schema_version: SchemaVersion::current(),
                        tenant_id: TenantId::new("tenant-a").expect("tenant"),
                        account_id: AccountId::new("account-a").expect("account"),
                        exchange_id: exchange.clone(),
                        market_type: GatewayMarketType::Perpetual,
                        canonical_symbol,
                        exchange_symbol: Some(symbol.exchange_symbol),
                        order_id: request.exchange_order_id.clone(),
                        client_order_id: request.client_order_id.clone(),
                        fill_id: Some("fill-1".to_string()),
                        side: GatewayOrderSide::Buy,
                        position_side: GatewayPositionSide::Long,
                        status: FillStatus::Confirmed,
                        liquidity_role: LiquidityRole::Taker,
                        price: 100.1,
                        quantity: 0.001,
                        quote_quantity: Some(0.1001),
                        fee_asset: Some("USDT".to_string()),
                        fee_amount: Some(0.00005),
                        fee_rate: Some(0.0005),
                        realized_pnl: None,
                        filled_at: now,
                        received_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::RecentFills(
                            rustcta_exchange_api::RecentFillsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                fills: vec![fill],
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetPositions(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::Positions(
                            rustcta_exchange_api::PositionsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                positions: Vec::new(),
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }

    #[async_trait]
    impl GatewayClient for EmergencyCloseFilledGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::QueryOrder(request) => {
                    let exchange = request.symbol.exchange.clone();
                    let now = Utc::now();
                    let order = OrderState {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: exchange.clone(),
                        market_type: request.symbol.market_type,
                        canonical_symbol: request.symbol.canonical_symbol.clone(),
                        exchange_symbol: request.symbol.exchange_symbol.clone(),
                        client_order_id: request.client_order_id.clone(),
                        exchange_order_id: request.exchange_order_id.clone(),
                        side: self.side,
                        position_side: Some(self.position_side),
                        order_type: GatewayOrderType::IOC,
                        time_in_force: Some(GatewayTimeInForce::IOC),
                        status: rustcta_types::OrderStatus::Filled,
                        quantity: format_float(self.quantity),
                        price: Some(format_float(self.price)),
                        filled_quantity: format_float(self.quantity),
                        average_fill_price: Some(format_float(self.price)),
                        reduce_only: true,
                        post_only: false,
                        created_at: Some(now),
                        updated_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::QueryOrder(
                            rustcta_exchange_api::QueryOrderResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                order: Some(order),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetRecentFills(request) => {
                    let exchange = request.exchange.clone();
                    let now = Utc::now();
                    let symbol = request.symbol.expect("symbol");
                    let canonical_symbol = symbol.canonical_symbol.expect("canonical symbol");
                    let fill = Fill {
                        schema_version: SchemaVersion::current(),
                        tenant_id: TenantId::new("tenant-a").expect("tenant"),
                        account_id: AccountId::new("account-a").expect("account"),
                        exchange_id: exchange.clone(),
                        market_type: GatewayMarketType::Perpetual,
                        canonical_symbol,
                        exchange_symbol: Some(symbol.exchange_symbol),
                        order_id: request.exchange_order_id.clone(),
                        client_order_id: request.client_order_id.clone(),
                        fill_id: Some("emergency-fill-1".to_string()),
                        side: self.side,
                        position_side: self.position_side,
                        status: FillStatus::Confirmed,
                        liquidity_role: LiquidityRole::Taker,
                        price: self.price,
                        quantity: self.quantity,
                        quote_quantity: Some(self.price * self.quantity),
                        fee_asset: Some("USDT".to_string()),
                        fee_amount: Some(self.price * self.quantity * 0.0005),
                        fee_rate: Some(0.0005),
                        realized_pnl: None,
                        filled_at: now,
                        received_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::RecentFills(
                            rustcta_exchange_api::RecentFillsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                fills: vec![fill],
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }

    #[async_trait]
    impl GatewayClient for PartialOpenThenRecoveredGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::QueryOrder(request) => {
                    let exchange = request.symbol.exchange.clone();
                    let now = Utc::now();
                    let client_order_id = request.client_order_id.clone().unwrap_or_default();
                    let (status, filled_quantity, average_fill_price, side, position_side) =
                        if client_order_id.contains("ecs") {
                            (
                                rustcta_types::OrderStatus::Filled,
                                "359".to_string(),
                                Some("0.01493".to_string()),
                                GatewayOrderSide::Sell,
                                GatewayPositionSide::Long,
                            )
                        } else if exchange.as_str() == "binance" {
                            (
                                rustcta_types::OrderStatus::Filled,
                                "359".to_string(),
                                Some("0.01483".to_string()),
                                GatewayOrderSide::Buy,
                                GatewayPositionSide::Long,
                            )
                        } else {
                            (
                                rustcta_types::OrderStatus::Unknown,
                                "0".to_string(),
                                None,
                                GatewayOrderSide::Sell,
                                GatewayPositionSide::Short,
                            )
                        };
                    let order = OrderState {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: exchange.clone(),
                        market_type: request.symbol.market_type,
                        canonical_symbol: request.symbol.canonical_symbol.clone(),
                        exchange_symbol: request.symbol.exchange_symbol.clone(),
                        client_order_id: request.client_order_id.clone(),
                        exchange_order_id: request.exchange_order_id.clone(),
                        side,
                        position_side: Some(position_side),
                        order_type: GatewayOrderType::IOC,
                        time_in_force: Some(GatewayTimeInForce::IOC),
                        status,
                        quantity: "359".to_string(),
                        price: average_fill_price.clone(),
                        filled_quantity,
                        average_fill_price,
                        reduce_only: client_order_id.contains("ecs"),
                        post_only: false,
                        created_at: Some(now),
                        updated_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::QueryOrder(
                            rustcta_exchange_api::QueryOrderResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                order: Some(order),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetRecentFills(request) => {
                    let exchange = request.exchange.clone();
                    let now = Utc::now();
                    let client_order_id = request.client_order_id.clone().unwrap_or_default();
                    let Some(symbol) = request.symbol else {
                        return Ok(GatewayProtocolResponse::rejected(
                            request_id,
                            GatewayOperation::GetRecentFills,
                            "symbol is required".to_string(),
                        ));
                    };
                    if exchange.as_str() != "binance" {
                        return Ok(GatewayProtocolResponse::accepted(
                            request_id,
                            operation,
                            GatewayResponsePayload::RecentFills(
                                rustcta_exchange_api::RecentFillsResponse {
                                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                    metadata: ResponseMetadata {
                                        request_id: request.context.request_id,
                                        ..ResponseMetadata::new(exchange, now)
                                    },
                                    fills: Vec::new(),
                                },
                            ),
                        ));
                    }
                    let (side, position_side, price) = if client_order_id.contains("ecs") {
                        (GatewayOrderSide::Sell, GatewayPositionSide::Long, 0.01493)
                    } else {
                        (GatewayOrderSide::Buy, GatewayPositionSide::Long, 0.01483)
                    };
                    let fill = Fill {
                        schema_version: SchemaVersion::current(),
                        tenant_id: TenantId::new("tenant-a").expect("tenant"),
                        account_id: AccountId::new("account-a").expect("account"),
                        exchange_id: exchange.clone(),
                        market_type: GatewayMarketType::Perpetual,
                        canonical_symbol: symbol.canonical_symbol.expect("canonical symbol"),
                        exchange_symbol: Some(symbol.exchange_symbol),
                        order_id: request.exchange_order_id.clone(),
                        client_order_id: request.client_order_id.clone(),
                        fill_id: Some(format!("fill-{client_order_id}")),
                        side,
                        position_side,
                        status: FillStatus::Confirmed,
                        liquidity_role: LiquidityRole::Taker,
                        price,
                        quantity: 359.0,
                        quote_quantity: Some(price * 359.0),
                        fee_asset: Some("USDT".to_string()),
                        fee_amount: Some(price * 359.0 * 0.0005),
                        fee_rate: Some(0.0005),
                        realized_pnl: None,
                        filled_at: now,
                        received_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::RecentFills(
                            rustcta_exchange_api::RecentFillsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                fills: vec![fill],
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetPositions(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::Positions(
                            rustcta_exchange_api::PositionsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                positions: Vec::new(),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetOpenOrders(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::OpenOrders(
                            rustcta_exchange_api::OpenOrdersResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                orders: Vec::new(),
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }

    #[async_trait]
    impl GatewayClient for PartialOpenEmergencyFallbackGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::QueryOrder(request) => {
                    let exchange = request.symbol.exchange.clone();
                    let now = Utc::now();
                    let client_order_id = request.client_order_id.clone().unwrap_or_default();
                    let is_emergency = client_order_id.contains("ecl");
                    let is_fallback = if is_emergency {
                        let mut count = self
                            .emergency_query_count
                            .lock()
                            .expect("emergency query count");
                        *count += 1;
                        let is_fallback = *count > 1;
                        if is_fallback {
                            *self
                                .fallback_client_order_id
                                .lock()
                                .expect("fallback client order id") = Some(client_order_id.clone());
                        }
                        is_fallback
                    } else {
                        false
                    };
                    let (status, filled_quantity, average_fill_price, side, position_side) =
                        if is_fallback {
                            (
                                rustcta_types::OrderStatus::Filled,
                                "384".to_string(),
                                Some("0.01462".to_string()),
                                GatewayOrderSide::Sell,
                                GatewayPositionSide::Long,
                            )
                        } else if is_emergency {
                            (
                                rustcta_types::OrderStatus::Unknown,
                                "0".to_string(),
                                None,
                                GatewayOrderSide::Sell,
                                GatewayPositionSide::Long,
                            )
                        } else if exchange.as_str() == "bitget" {
                            (
                                rustcta_types::OrderStatus::Filled,
                                "384".to_string(),
                                Some("0.01415".to_string()),
                                GatewayOrderSide::Buy,
                                GatewayPositionSide::Long,
                            )
                        } else {
                            (
                                rustcta_types::OrderStatus::Expired,
                                "0".to_string(),
                                None,
                                GatewayOrderSide::Sell,
                                GatewayPositionSide::Short,
                            )
                        };
                    let order = OrderState {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: exchange.clone(),
                        market_type: request.symbol.market_type,
                        canonical_symbol: request.symbol.canonical_symbol.clone(),
                        exchange_symbol: request.symbol.exchange_symbol.clone(),
                        client_order_id: request.client_order_id.clone(),
                        exchange_order_id: request.exchange_order_id.clone(),
                        side,
                        position_side: Some(position_side),
                        order_type: if is_fallback {
                            GatewayOrderType::Market
                        } else {
                            GatewayOrderType::IOC
                        },
                        time_in_force: if is_fallback {
                            Some(GatewayTimeInForce::GTC)
                        } else {
                            Some(GatewayTimeInForce::IOC)
                        },
                        status,
                        quantity: "384".to_string(),
                        price: average_fill_price.clone(),
                        filled_quantity,
                        average_fill_price,
                        reduce_only: is_emergency || is_fallback,
                        post_only: false,
                        created_at: Some(now),
                        updated_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::QueryOrder(
                            rustcta_exchange_api::QueryOrderResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                order: Some(order),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetRecentFills(request) => {
                    let exchange = request.exchange.clone();
                    let now = Utc::now();
                    let client_order_id = request.client_order_id.clone().unwrap_or_default();
                    let Some(symbol) = request.symbol else {
                        return Ok(GatewayProtocolResponse::rejected(
                            request_id,
                            GatewayOperation::GetRecentFills,
                            "symbol is required".to_string(),
                        ));
                    };
                    let is_emergency = client_order_id.contains("ecl");
                    let is_fallback = self
                        .fallback_client_order_id
                        .lock()
                        .expect("fallback client order id")
                        .as_deref()
                        == Some(client_order_id.as_str());
                    if exchange.as_str() != "bitget" || (is_emergency && !is_fallback) {
                        return Ok(GatewayProtocolResponse::accepted(
                            request_id,
                            operation,
                            GatewayResponsePayload::RecentFills(
                                rustcta_exchange_api::RecentFillsResponse {
                                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                    metadata: ResponseMetadata {
                                        request_id: request.context.request_id,
                                        ..ResponseMetadata::new(exchange, now)
                                    },
                                    fills: Vec::new(),
                                },
                            ),
                        ));
                    }
                    let (side, position_side, price) = if is_fallback {
                        (GatewayOrderSide::Sell, GatewayPositionSide::Long, 0.01462)
                    } else {
                        (GatewayOrderSide::Buy, GatewayPositionSide::Long, 0.01415)
                    };
                    let fill = Fill {
                        schema_version: SchemaVersion::current(),
                        tenant_id: TenantId::new("tenant-a").expect("tenant"),
                        account_id: AccountId::new("account-a").expect("account"),
                        exchange_id: exchange.clone(),
                        market_type: GatewayMarketType::Perpetual,
                        canonical_symbol: symbol.canonical_symbol.expect("canonical symbol"),
                        exchange_symbol: Some(symbol.exchange_symbol),
                        order_id: request.exchange_order_id.clone(),
                        client_order_id: request.client_order_id.clone(),
                        fill_id: Some(format!("fill-{client_order_id}")),
                        side,
                        position_side,
                        status: FillStatus::Confirmed,
                        liquidity_role: LiquidityRole::Taker,
                        price,
                        quantity: 384.0,
                        quote_quantity: Some(price * 384.0),
                        fee_asset: Some("USDT".to_string()),
                        fee_amount: Some(price * 384.0 * 0.0005),
                        fee_rate: Some(0.0005),
                        realized_pnl: None,
                        filled_at: now,
                        received_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::RecentFills(
                            rustcta_exchange_api::RecentFillsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                fills: vec![fill],
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetPositions(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::Positions(
                            rustcta_exchange_api::PositionsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                positions: Vec::new(),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetOpenOrders(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::OpenOrders(
                            rustcta_exchange_api::OpenOrdersResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                orders: Vec::new(),
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }

    #[async_trait]
    impl GatewayClient for AcceptedUnfilledThenRecoveredGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::QueryOrder(request) => {
                    let exchange = request.symbol.exchange.clone();
                    let now = Utc::now();
                    let order = OrderState {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        exchange: exchange.clone(),
                        market_type: request.symbol.market_type,
                        canonical_symbol: request.symbol.canonical_symbol.clone(),
                        exchange_symbol: request.symbol.exchange_symbol.clone(),
                        client_order_id: request.client_order_id.clone(),
                        exchange_order_id: request.exchange_order_id.clone(),
                        side: GatewayOrderSide::Buy,
                        position_side: Some(GatewayPositionSide::Long),
                        order_type: GatewayOrderType::IOC,
                        time_in_force: Some(GatewayTimeInForce::IOC),
                        status: rustcta_types::OrderStatus::Expired,
                        quantity: "0.055".to_string(),
                        price: Some("100.0".to_string()),
                        filled_quantity: "0".to_string(),
                        average_fill_price: None,
                        reduce_only: false,
                        post_only: false,
                        created_at: Some(now),
                        updated_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::QueryOrder(
                            rustcta_exchange_api::QueryOrderResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                order: Some(order),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetRecentFills(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::RecentFills(
                            rustcta_exchange_api::RecentFillsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                fills: Vec::new(),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetPositions(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::Positions(
                            rustcta_exchange_api::PositionsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                positions: Vec::new(),
                            },
                        ),
                    ))
                }
                GatewayRequestPayload::GetOpenOrders(request) => {
                    let now = Utc::now();
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::OpenOrders(
                            rustcta_exchange_api::OpenOrdersResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(request.exchange, now)
                                },
                                orders: Vec::new(),
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }

    #[async_trait]
    impl GatewayClient for UnmanagedPositionGateway {
        async fn send(
            &self,
            request: GatewayProtocolRequest,
        ) -> std::result::Result<GatewayProtocolResponse, rustcta_exchange_gateway::GatewayError>
        {
            let request_id = request.request_id.clone();
            let operation = request.operation;
            match request.payload {
                GatewayRequestPayload::GetPositions(request) => {
                    let now = Utc::now();
                    let exchange = request.exchange.clone();
                    let symbol = CanonicalSymbol::new("ESPORTS", "USDT").expect("canonical");
                    let position = GatewayPosition {
                        schema_version: SchemaVersion::current(),
                        tenant_id: TenantId::new("tenant-a").expect("tenant"),
                        account_id: AccountId::new("account-a").expect("account"),
                        exchange_id: exchange.clone(),
                        market_type: GatewayMarketType::Perpetual,
                        canonical_symbol: symbol.clone(),
                        exchange_symbol: Some(
                            ExchangeSymbol::new(
                                exchange.clone(),
                                GatewayMarketType::Perpetual,
                                exchange_symbol_text(exchange.as_str(), &symbol),
                            )
                            .expect("exchange symbol"),
                        ),
                        side: GatewayPositionSide::Long,
                        quantity: 72.0,
                        entry_price: Some(0.07566),
                        mark_price: Some(0.075),
                        liquidation_price: None,
                        unrealized_pnl: Some(-0.01),
                        leverage: Some(5.0),
                        observed_at: now,
                    };
                    Ok(GatewayProtocolResponse::accepted(
                        request_id,
                        operation,
                        GatewayResponsePayload::Positions(
                            rustcta_exchange_api::PositionsResponse {
                                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                                metadata: ResponseMetadata {
                                    request_id: request.context.request_id,
                                    ..ResponseMetadata::new(exchange, now)
                                },
                                positions: vec![position],
                            },
                        ),
                    ))
                }
                _ => Ok(GatewayProtocolResponse::rejected(
                    request_id,
                    GatewayOperation::GetStatus,
                    format!("unexpected operation {operation:?}"),
                )),
            }
        }
    }
}
